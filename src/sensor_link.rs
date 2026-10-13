//! Long-lived Sensor USART: probe-first connect (firmware-baud ping → optional opensleep
//! bootloader handshake with in-place baud switch), RX parsing (`0xAE` VibrationEnabled),
//! piezo priming, queued vibration, and automatic reconnect (backoff + RX health tracking).
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serialport::{ClearBuffer, SerialPort as _};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_serial::SerialPortBuilderExt;

use crate::frozen_frame::jump_to_firmware_frame;
use crate::sensor_frame::{
    enable_piezo_frame, enable_vibration_frame, ping_frame, set_piezo_freq_frame,
    set_piezo_gain_frame, VibrationBatch, PIEZO_GAIN_DEFAULT,
};
use crate::sensor_rx::{self, PiezoSetupState, PresenceCapDiag};

/// Bootloader serial speed — opensleep `BOOTLOADER_BAUD` (`src/sensor/manager.rs`).
const BOOTLOADER_BAUD: u32 = 38400;

/// Discovery pings per phase — opensleep `ping_device`: 3 pings, 500 ms answer wait each.
const DISCOVERY_PING_TRIES: u32 = 3;
#[cfg(not(test))]
const DISCOVERY_PING_WAIT_MS: u64 = 500;
#[cfg(test)]
const DISCOVERY_PING_WAIT_MS: u64 = 100;
/// After JumpToFirmware: wait at bootloader baud for the `0x90` JumpingToFirmware ack
/// before switching to firmware baud; the jump is retried this many times.
#[cfg(not(test))]
const JUMP_ACK_WAIT_MS: u64 = 1000;
#[cfg(test)]
const JUMP_ACK_WAIT_MS: u64 = 100;
const JUMP_ATTEMPTS: u32 = 3;
/// After the baud switch: ping/pong wait at firmware baud before the TX-only fallback.
#[cfg(not(test))]
const POST_HANDSHAKE_PONG_WAIT_MS: u64 = 5000;
#[cfg(not(test))]
const POST_HANDSHAKE_PING_INTERVAL_MS: u64 = 500;
#[cfg(test)]
const POST_HANDSHAKE_PONG_WAIT_MS: u64 = 400;
#[cfg(test)]
const POST_HANDSHAKE_PING_INTERVAL_MS: u64 = 100;
/// Read slice while waiting for frames (keeps the loops responsive).
#[cfg(not(test))]
const CONNECT_READ_SLICE_MS: u64 = 50;
#[cfg(test)]
const CONNECT_READ_SLICE_MS: u64 = 20;

/// opensleep `CONFIG_RES_TIME` — one piezo config command per tick until ACK’d / stream OK.
const CONFIG_RES_TIME: Duration = Duration::from_millis(800);
/// Keepalive ping + RX health check interval (independent of piezo setup).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// After piezo setup looked OK, no CRC-valid piezo samples for this long → re-run EnablePiezo / SetPiezoFreq.
const PIEZO_STREAM_STALL: Duration = Duration::from_secs(20);
/// No CRC-valid frame for this long → reconnect. Skipped with `--sensor-vibrate-no-ack-wait`
/// (RX known-broken there; a reconnect would only repeat bootloader traffic).
const RX_HEALTH_TIMEOUT: Duration = Duration::from_secs(45);
/// Reconnect backoff: start, doubling, cap. Reset after a link ran at least this long.
const RECONNECT_BACKOFF_START: Duration = Duration::from_secs(2);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);
const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum SensorLinkError {
    #[error("serial I/O: {0}")]
    Io(#[from] std::io::Error),
    // `tokio_serial::Error` is a re-export of `serialport::Error` — also covers
    // `SerialStream::set_baud_rate` / `clear` from the `serialport::SerialPort` trait.
    #[error("serial port: {0}")]
    Port(#[from] tokio_serial::Error),
}

#[derive(Debug)]
pub struct SensorLinkHandle {
    pub tx: mpsc::Sender<VibrationBatch>,
    /// `true` after inbound Sensor bytes include `0x7E` (opensleep‑style frame sync on RX at this baud).
    pub sensor_rx_framing: Arc<AtomicBool>,
    /// UTF‑8 bodies of Sensor `0x07` MCU text (MQTT **Sensor Message** text sensor).
    pub sensor_message_rx: mpsc::Receiver<String>,
    /// Piezo sample batches (`0x32` / `0x35`, opensleep `PiezoData` layout) — feed [`crate::hrv`] HR/HRV sensors.
    pub piezo_rx: mpsc::Receiver<crate::sensor_rx::SensorPiezoSamples>,
}

pub fn spawn(
    device: PathBuf,
    baud: u32,
    piezo_freq_hz: u32,
    force_bootloader_handshake: bool,
    vibrate_no_ack_wait: bool,
    capacitance_tx: Option<mpsc::Sender<crate::sensor_rx::SensorCapacitanceZones>>,
    capacitance_parse_diag: Option<std::sync::Arc<PresenceCapDiag>>,
) -> SensorLinkHandle {
    let (tx, rx) = mpsc::channel::<VibrationBatch>(32);
    let (sensor_mcu_tx, sensor_mcu_rx) = mpsc::channel::<String>(512);
    let (piezo_tx, piezo_rx) = mpsc::channel::<crate::sensor_rx::SensorPiezoSamples>(64);
    let sensor_rx_framing = Arc::new(AtomicBool::new(false));
    let sensor_rx_framing_reader = sensor_rx_framing.clone();
    tokio::spawn(async move {
        // `run` never returns (reconnect supervisor loop); the Result is for future clean shutdown.
        let _ = run(
            device,
            baud,
            piezo_freq_hz,
            force_bootloader_handshake,
            vibrate_no_ack_wait,
            rx,
            capacitance_tx,
            capacitance_parse_diag,
            sensor_mcu_tx,
            piezo_tx,
            sensor_rx_framing_reader,
        )
        .await;
    });
    SensorLinkHandle {
        tx,
        sensor_rx_framing,
        sensor_message_rx: sensor_mcu_rx,
        piezo_rx,
    }
}

fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(48)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// serialport opens TTYs with an exclusive `flock` by default — this error means another
/// process (e.g. a stale `lunaris` instance) still holds the device node.
fn is_exclusive_lock_error(e: &SensorLinkError) -> bool {
    match e {
        SensorLinkError::Port(p) => {
            matches!(p.kind(), serialport::ErrorKind::NoDevice)
                || p.to_string().contains("exclusive lock")
        }
        _ => false,
    }
}

/// Reconnect supervisor: runs [`run_once`] until the link ends (port error, RX task death,
/// or RX health timeout), then reconnects with exponential backoff. Never returns.
#[allow(clippy::too_many_arguments)]
async fn run(
    device: PathBuf,
    baud: u32,
    piezo_freq_hz: u32,
    force_bootloader_handshake: bool,
    vibrate_no_ack_wait: bool,
    mut rx: mpsc::Receiver<VibrationBatch>,
    capacitance_tx: Option<mpsc::Sender<crate::sensor_rx::SensorCapacitanceZones>>,
    capacitance_parse_diag: Option<Arc<PresenceCapDiag>>,
    sensor_mcu_tx: mpsc::Sender<String>,
    piezo_tx: mpsc::Sender<crate::sensor_rx::SensorPiezoSamples>,
    sensor_rx_framing: Arc<AtomicBool>,
) -> Result<(), SensorLinkError> {
    let path = device.to_string_lossy().to_string();
    let mut backoff = RECONNECT_BACKOFF_START;
    loop {
        let started = Instant::now();
        match run_once(
            &path,
            baud,
            piezo_freq_hz,
            force_bootloader_handshake,
            vibrate_no_ack_wait,
            &mut rx,
            capacitance_tx.clone(),
            capacitance_parse_diag.clone(),
            sensor_mcu_tx.clone(),
            piezo_tx.clone(),
            sensor_rx_framing.clone(),
        )
        .await
        {
            Ok(()) => {
                tracing::warn!("Sensor USART link stale — reconnecting");
            }
            Err(e) => {
                if is_exclusive_lock_error(&e) {
                    tracing::warn!(
                        error = %e,
                        "Sensor USART open failed: another process holds {} (exclusive lock) — `fuser -v {}` shows the owner; only one process may own each TTY; reconnecting",
                        path,
                        path
                    );
                } else {
                    tracing::warn!(error = %e, "Sensor USART link ended — reconnecting");
                }
            }
        }
        if started.elapsed() >= BACKOFF_RESET_AFTER {
            backoff = RECONNECT_BACKOFF_START;
        }
        tracing::info!(backoff_ms = backoff.as_millis(), "Sensor: reconnecting");
        sleep(backoff).await;
        backoff = next_backoff(backoff);
    }
}

fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(RECONNECT_BACKOFF_MAX)
}

/// Opens the Sensor USART and brings the MCU into a known state, following opensleep's
/// `run_discovery` order:
///
/// 1. **Bootloader at 38400**: ping ×3 (500 ms answer wait each — any valid frame answers,
///    opensleep `ping_device`). A pong reports the mode byte (`0x42` bootloader / `0x46`
///    firmware). When answered: `JumpToFirmware`, then wait at 38400 for the
///    `JumpingToFirmware` (`0x90`) confirmation — the jump is retried up to 3 times — before
///    switching the **same** port handle to firmware baud (`set_baud_rate`, no close/reopen:
///    each open of a `ttyS*` can toggle DTR/RTS and reset the MCU). Afterwards ping/pong is
///    retried for ~5 s to confirm the firmware is up.
/// 2. **Firmware baud probe** (opensleep: "happens if program was recently running"): same
///    ping ×3 at firmware baud. Skipped with `--sensor-force-bootloader-handshake`.
/// 3. **TX-only fallback**: nobody answered — the port stays open at firmware baud so
///    vibration (`SetAlarm`) can still be attempted blind — same spirit as
///    `--sensor-vibrate-no-ack-wait`.
///
/// Returns the open port plus any undecoded RX bytes left over from discovery, so the
/// caller's deframer continues seamlessly.
async fn connect_sensor_port(
    path: &str,
    fw_baud: u32,
    force_handshake: bool,
) -> Result<(tokio_serial::SerialStream, Vec<u8>), SensorLinkError> {
    if let Some((mut port, mut buf, ev)) = ping_for_answer(path, BOOTLOADER_BAUD).await? {
        tracing::info!(
            baud = BOOTLOADER_BAUD,
            "Sensor: MCU answered at bootloader baud"
        );
        if let Some(in_fw) = ev.pong_mode {
            tracing::info!(
                in_firmware = in_fw,
                "Sensor: pong mode seen at bootloader baud"
            );
        }
        let mut confirmed = false;
        for attempt in 1..=JUMP_ATTEMPTS {
            port.write_all(&jump_to_firmware_frame()).await?;
            port.flush().await?;
            let ev = read_until(&mut port, &mut buf, JUMP_ACK_WAIT_MS, |ev| {
                ev.jump_to_firmware
            })
            .await?;
            if ev.jump_to_firmware {
                confirmed = true;
                break;
            }
            tracing::debug!(
                attempt,
                "Sensor: no JumpingToFirmware (0x90) ack — re-ping + re-jump"
            );
            port.write_all(&ping_frame()).await?;
            port.flush().await?;
        }
        if !confirmed {
            drop(port);
            return Err(SensorLinkError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "MCU answered at bootloader baud but never confirmed JumpToFirmware (0x90)",
            )));
        }
        // Baud switch in place — a close/reopen here can reset the MCU (DTR/RTS toggle).
        port.set_baud_rate(fw_baud)?;
        port.clear(ClearBuffer::All)?;
        tracing::info!(
            baud = fw_baud,
            "Sensor: switched to firmware baud after confirmed JumpToFirmware (no port reopen)"
        );
        if wait_firmware_answer(&mut port, &mut buf).await? {
            tracing::info!(
                baud = fw_baud,
                "Sensor: MCU answered after jump (firmware running)"
            );
            return Ok((port, buf));
        }
        tracing::warn!(
            "Sensor: no valid frame after confirmed jump — keeping port open (TX-only; try `--sensor-vibrate-no-ack-wait`, or another `--sensor-baud` if RX never decodes 0x7E frames)"
        );
        return Ok((port, buf));
    }
    tracing::info!(
        baud = BOOTLOADER_BAUD,
        "Sensor: no answer at bootloader baud"
    );
    if !force_handshake {
        if let Some((port, buf, _ev)) = ping_for_answer(path, fw_baud).await? {
            tracing::info!(
                baud = fw_baud,
                "Sensor: MCU answered at firmware baud (already in firmware)"
            );
            return Ok((port, buf));
        }
        tracing::info!(baud = fw_baud, "Sensor: no answer at firmware baud");
    }
    let port = open_port(path, fw_baud).await?;
    tracing::warn!(
        "Sensor: no answer at bootloader or firmware baud — keeping port open (TX-only; try `--sensor-vibrate-no-ack-wait`, or another `--sensor-baud` if RX never decodes 0x7E frames). If a piezo stream was flowing earlier and then went silent while both bauds stay unanswered, the sensor MCU is likely hard-wedged — power-cycle it (unplug the mattress cover / pod power)"
    );
    Ok((port, Vec::new()))
}

async fn open_port(path: &str, baud: u32) -> Result<tokio_serial::SerialStream, SensorLinkError> {
    let port = tokio_serial::new(path.to_string(), baud).open_native_async()?;
    crate::serial_prime::set_serial_cloexec(&port)?;
    Ok(port)
}

/// opensleep `ping_device`: open at `baud`, ping up to [`DISCOVERY_PING_TRIES`] times,
/// waiting [`DISCOVERY_PING_WAIT_MS`] for any valid frame after each ping. Returns the
/// open port plus leftover RX bytes and the events seen when the MCU answered; `None`
/// (port dropped) when it stayed silent.
async fn ping_for_answer(
    path: &str,
    baud: u32,
) -> Result<Option<(tokio_serial::SerialStream, Vec<u8>, sensor_rx::DrainEvents)>, SensorLinkError>
{
    let mut port = open_port(path, baud).await?;
    let mut buf = Vec::with_capacity(512);
    let mut events = sensor_rx::DrainEvents::default();
    for _ in 0..DISCOVERY_PING_TRIES {
        port.write_all(&ping_frame()).await?;
        port.flush().await?;
        let ev = read_until(&mut port, &mut buf, DISCOVERY_PING_WAIT_MS, |ev| {
            ev.valid_frames > 0
        })
        .await?;
        events.merge(ev);
        if events.valid_frames > 0 {
            return Ok(Some((port, buf, events)));
        }
    }
    Ok(None)
}

/// Reads and deframes for up to `timeout_ms`, returning the events seen. `done` is checked
/// after every batch — return early on the condition you wait for.
async fn read_until(
    port: &mut tokio_serial::SerialStream,
    buf: &mut Vec<u8>,
    timeout_ms: u64,
    mut done: impl FnMut(&sensor_rx::DrainEvents) -> bool,
) -> Result<sensor_rx::DrainEvents, SensorLinkError> {
    let mut events = sensor_rx::DrainEvents::default();
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut chunk = [0u8; 512];
    loop {
        let now = Instant::now();
        if now >= deadline || done(&events) {
            return Ok(events);
        }
        let remaining = (deadline - now).as_millis() as u64;
        let slice = CONNECT_READ_SLICE_MS.min(remaining);
        match timeout(Duration::from_millis(slice), port.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                return Err(SensorLinkError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "EOF while waiting for Sensor frames",
                )));
            }
            Ok(Ok(n)) => {
                buf.extend_from_slice(&chunk[..n]);
                let ev = sensor_rx::drain_inbound(buf, None, None, None, None, None);
                events.merge(ev);
            }
            Ok(Err(e)) => return Err(SensorLinkError::Io(e)),
            Err(_) => {} // read slice timed out — keep looping
        }
    }
}

/// After the baud switch: ping + wait for any valid frame, re-pinging until
/// [`POST_HANDSHAKE_PONG_WAIT_MS`] elapsed. Returns `true` when the firmware answered.
async fn wait_firmware_answer(
    port: &mut tokio_serial::SerialStream,
    buf: &mut Vec<u8>,
) -> Result<bool, SensorLinkError> {
    let deadline = Instant::now() + Duration::from_millis(POST_HANDSHAKE_PONG_WAIT_MS);
    loop {
        port.write_all(&ping_frame()).await?;
        port.flush().await?;
        let ev = read_until(port, buf, POST_HANDSHAKE_PING_INTERVAL_MS, |ev| {
            ev.valid_frames > 0
        })
        .await?;
        if ev.valid_frames > 0 {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
    }
}

/// Opensleep’s sensor [`CommandScheduler`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)
/// spaces configuration commands by **`CONFIG_RES_TIME` ≈ 800 ms**. Sending all priming frames back-to-back
/// can yield `0xAC` **AlarmSet** acks without physical vibration on some Pods — stagger writes.
const SENSOR_PRIMING_INTER_FRAME_MS: u64 = 150;
/// Pause after the last priming frame (`EnablePiezo`) so the MCU can settle before we wait for `0xAE` / send `SetAlarm`.
const SENSOR_AFTER_PRIMING_MS: u64 = 400;
/// Pause between cover-button feedback pulses: each `SetAlarm` duration plus this gap,
/// so every button tap is a distinct short vibration (stock-firmware haptic feedback).
const SENSOR_FEEDBACK_PULSE_GAP_MS: u64 = 500;

/// `SetAlarm` duration (seconds) parsed from a framed command
/// (`7E len 2C side intensity pattern <duration u32 BE> crc crc`) — used to pace
/// feedback pulses. `None` when the frame is not a well-formed `SetAlarm`.
fn set_alarm_duration_secs(frame: &[u8]) -> Option<u32> {
    if frame.len() >= 10 && frame[0] == 0x7E && frame[2] == 0x2C {
        Some(u32::from_be_bytes([frame[6], frame[7], frame[8], frame[9]]))
    } else {
        None
    }
}

/// Writes trailing `SetAlarm` frames as **sequential pulses**: each frame is flushed and
/// followed by its own duration plus [`SENSOR_FEEDBACK_PULSE_GAP_MS`] (except after the
/// last pulse), so the MCU runs one short vibration per button tap.
async fn write_paced_alarm_frames(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    alarms: &[Vec<u8>],
) -> Result<(), SensorLinkError> {
    for (i, frame) in alarms.iter().enumerate() {
        write_half.write_all(frame).await?;
        write_half.flush().await?;
        if i + 1 < alarms.len() {
            let dur_ms = set_alarm_duration_secs(frame)
                .map(|secs| u64::from(secs) * 1000)
                .unwrap_or(0);
            sleep(Duration::from_millis(dur_ms + SENSOR_FEEDBACK_PULSE_GAP_MS)).await;
        }
    }
    Ok(())
}

/// Next opensleep-style piezo config command, or `None` when setup looks complete.
fn next_piezo_setup_frame(setup: &PiezoSetupState) -> Option<Vec<u8>> {
    if !setup.vibration_enabled() {
        return Some(enable_vibration_frame());
    }
    if !setup.piezo_gain_ok() {
        return Some(set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT));
    }
    if !setup.piezo_enabled() {
        return Some(enable_piezo_frame());
    }
    if !setup.piezo_freq_ok() {
        return Some(set_piezo_freq_frame(setup.target_freq_hz()));
    }
    None
}

async fn wait_for_vibration_ack(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    piezo_setup: &PiezoSetupState,
) -> Result<(), SensorLinkError> {
    if piezo_setup.vibration_enabled() {
        return Ok(());
    }
    // 120 × 50ms ≈ 6s — MCUs / framing can be slow; reassembly after split reads needs time.
    for attempt in 1..=120 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if piezo_setup.vibration_enabled() {
            tracing::debug!(attempt, "Sensor: 0xAE VibrationEnabled seen");
            return Ok(());
        }
        // Re-send EnableVibration only (do not blast full piezo priming — that restarts sampling).
        if attempt > 0 && attempt % 30 == 0 {
            write_half.write_all(&enable_vibration_frame()).await?;
            write_half.flush().await?;
            tracing::debug!(
                attempt,
                "Sensor: re-sent EnableVibration while waiting for 0xAE"
            );
        }
    }
    tracing::warn!(
        "Sensor: no 0xAE VibrationEnabled within ~6s — sending SetAlarm anyway (try `--sensor-vibrate-no-ack-wait`, or another `--sensor-baud` if RX never decodes 0x7E frames)"
    );
    Ok(())
}

async fn write_vibration_batch(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    batch: VibrationBatch,
    piezo_setup: &PiezoSetupState,
    vibrate_no_ack_wait: bool,
) -> Result<(), SensorLinkError> {
    let frames = batch.frames();
    // Known opensleep sequence shape: cancel(s) → 4 priming frames → alarm(s).
    let cancels = batch.cancel_count();
    let alarms = batch.alarm_count();
    let paced = batch.paced_alarms();
    let is_sequence = frames.len() == cancels + 4 + alarms;
    if vibrate_no_ack_wait {
        // Same pacing as opensleep-style staggering — back-to-back frames can overwhelm ttyS2 / firmware.
        let gap = Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS);
        if is_sequence {
            piezo_setup.set_vibration_enabled(false);
            // Vibration priming restarts MCU sampling — refresh stream state afterwards.
            piezo_setup.request_stream_recovery();
            // Head (cancels + priming) keeps the flat staggering — including the pause
            // after the last priming frame before the first alarm.
            let head = cancels + 4;
            for (i, frame) in frames[..head].iter().enumerate() {
                write_half.write_all(frame).await?;
                // Slightly longer pause after the cancel frame(s).
                let pause = if i < cancels {
                    Duration::from_millis(100)
                } else {
                    gap
                };
                sleep(pause).await;
            }
            if paced {
                // Feedback pulses: one short vibration per button tap, not one long one.
                write_paced_alarm_frames(write_half, &frames[head..]).await?;
            } else {
                for (i, frame) in frames[head..].iter().enumerate() {
                    write_half.write_all(frame).await?;
                    if i + 1 < frames[head..].len() {
                        sleep(gap).await;
                    }
                }
            }
        } else {
            for (i, frame) in frames.iter().enumerate() {
                write_half.write_all(frame).await?;
                if i + 1 < frames.len() {
                    sleep(gap).await;
                }
            }
        }
        write_half.flush().await?;
        tracing::debug!(
            "Sensor: vibration batch sent (--sensor-vibrate-no-ack-wait: no 0xAE wait)"
        );
        return Ok(());
    }
    if is_sequence {
        piezo_setup.set_vibration_enabled(false);
        piezo_setup.request_stream_recovery();
        for frame in &frames[..cancels] {
            write_half.write_all(frame).await?;
            sleep(Duration::from_millis(100)).await;
        }
        let prim = &frames[cancels..cancels + 4];
        let alarm = &frames[cancels + 4..];
        let gap = Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS);
        {
            for (i, frame) in prim.iter().enumerate() {
                write_half.write_all(frame).await?;
                if i + 1 < prim.len() {
                    sleep(gap).await;
                }
            }
            write_half.flush().await?;
            sleep(Duration::from_millis(SENSOR_AFTER_PRIMING_MS)).await;
            wait_for_vibration_ack(write_half, piezo_setup).await?;
        }
        if paced {
            // Feedback pulses: pace the SetAlarm frames (pulse duration + gap).
            write_paced_alarm_frames(write_half, alarm).await?;
        } else {
            // Simultaneous alarms (both sides) go back-to-back.
            for frame in alarm {
                write_half.write_all(frame).await?;
            }
            write_half.flush().await?;
        }
    } else {
        for frame in frames {
            write_half.write_all(frame).await?;
        }
        write_half.flush().await?;
    }
    Ok(())
}

/// One link session: connect, spawn the RX task, then [`session_loop`]. On exit the RX
/// task is aborted and awaited so the port (and its exclusive flock) is released before
/// the next connect attempt.
#[allow(clippy::too_many_arguments)]
async fn run_once(
    path: &str,
    baud: u32,
    piezo_freq_hz: u32,
    force_bootloader_handshake: bool,
    vibrate_no_ack_wait: bool,
    rx: &mut mpsc::Receiver<VibrationBatch>,
    capacitance_tx: Option<mpsc::Sender<crate::sensor_rx::SensorCapacitanceZones>>,
    capacitance_parse_diag: Option<Arc<PresenceCapDiag>>,
    sensor_mcu_tx: mpsc::Sender<String>,
    piezo_tx: mpsc::Sender<crate::sensor_rx::SensorPiezoSamples>,
    sensor_rx_framing: Arc<AtomicBool>,
) -> Result<(), SensorLinkError> {
    let (port, leftover) = connect_sensor_port(path, baud, force_bootloader_handshake).await?;
    let (mut read_half, mut write_half) = tokio::io::split(port);

    let piezo_setup = PiezoSetupState::new_arc(piezo_freq_hz);
    let last_valid_frame = Arc::new(Mutex::new(Instant::now()));
    let rx_alive = Arc::new(AtomicBool::new(true));
    let rx_task = {
        let setup_rx = piezo_setup.clone();
        let cap_flag = capacitance_tx;
        let parse_diag_for_rx = capacitance_parse_diag;
        let last_frame = last_valid_frame.clone();
        let rx_alive_task = rx_alive.clone();
        let framing = sensor_rx_framing.clone();
        tokio::spawn(async move {
            let mut buf = leftover;
            let mut chunk = [0u8; 512];
            let mut log_first_rx = true;
            loop {
                match read_half.read(&mut chunk).await {
                    Ok(0) => {
                        tracing::warn!("Sensor serial read EOF");
                        break;
                    }
                    Ok(n) => {
                        if chunk[..n].contains(&0x7E) {
                            framing.store(true, Ordering::SeqCst);
                        }
                        if n > 0 && log_first_rx {
                            log_first_rx = false;
                            let has_7e = chunk[..n].contains(&0x7E);
                            if has_7e {
                                tracing::info!(
                                    len = n,
                                    hex = %hex_prefix(&chunk[..n]),
                                    "Sensor: first RX shows `0x7E` (opensleep-style framing likely OK at this baud)"
                                );
                            } else {
                                tracing::info!(
                                    len = n,
                                    hex = %hex_prefix(&chunk[..n]),
                                    "Sensor: first RX has no 0x7E — line noise, wrong --sensor-baud, or half a frame; try --sensor-baud 115200 vs 38400 versus stock FW speed; --sensor-vibrate-no-ack-wait if RX never frames but TX might still drive the piezo"
                                );
                            }
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let diag = parse_diag_for_rx.as_ref().map(|a| a.as_ref());
                        let ev = sensor_rx::drain_inbound(
                            &mut buf,
                            Some(setup_rx.as_ref()),
                            cap_flag.as_ref(),
                            diag,
                            Some(&sensor_mcu_tx),
                            Some(&piezo_tx),
                        );
                        if ev.valid_frames > 0 {
                            if let Ok(mut lf) = last_frame.lock() {
                                *lf = Instant::now();
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Sensor serial read failed");
                        break;
                    }
                }
            }
            rx_alive_task.store(false, Ordering::SeqCst);
        })
    };

    let result = session_loop(
        &mut write_half,
        rx,
        vibrate_no_ack_wait,
        piezo_setup.as_ref(),
        &last_valid_frame,
        &rx_alive,
    )
    .await;

    // Release the port before returning: abort the RX task and wait for it, so the fd
    // (and its exclusive flock) is closed before the next connect attempt — otherwise a
    // blocked read() would keep the port locked and every reconnect open would fail with
    // `Unable to acquire exclusive lock on serial port`.
    rx_task.abort();
    let _ = rx_task.await;
    result
}

/// Session body: initial ping, then ack-driven piezo setup + keepalive until a port
/// error or a reconnect request (RX task death / RX health timeout).
async fn session_loop(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    rx: &mut mpsc::Receiver<VibrationBatch>,
    vibrate_no_ack_wait: bool,
    piezo_setup: &PiezoSetupState,
    last_valid_frame: &Mutex<Instant>,
    rx_alive: &AtomicBool,
) -> Result<(), SensorLinkError> {
    write_half.write_all(&ping_frame()).await?;
    write_half.flush().await?;
    tracing::info!(
        target_freq_hz = piezo_setup.target_freq_hz(),
        "Sensor link: ping sent; ack-driven piezo setup starting"
    );

    let mut config_tick = interval(CONFIG_RES_TIME);
    config_tick.tick().await;
    let mut keepalive_tick = interval(KEEPALIVE_INTERVAL);
    keepalive_tick.tick().await;

    let mut mqtt_channel_open = true;
    let mut piezo_was_ok = false;

    loop {
        if mqtt_channel_open {
            tokio::select! {
                biased;
                batch = rx.recv() => {
                    match batch {
                        Some(batch) => {
                            write_vibration_batch(
                                write_half,
                                batch,
                                piezo_setup,
                                vibrate_no_ack_wait,
                            )
                            .await?;
                            tracing::debug!("Sensor link: vibration batch completed");
                        }
                        None => {
                            tracing::warn!("Sensor MQTT→UART channel closed; piezo setup only");
                            mqtt_channel_open = false;
                        }
                    }
                }
                _ = config_tick.tick() => {
                    maybe_send_piezo_setup(write_half, piezo_setup, &mut piezo_was_ok).await?;
                }
                _ = keepalive_tick.tick() => {
                    if keepalive_and_health(
                        write_half,
                        last_valid_frame,
                        rx_alive,
                        vibrate_no_ack_wait,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                }
            }
        } else {
            tokio::select! {
                _ = config_tick.tick() => {
                    maybe_send_piezo_setup(write_half, piezo_setup, &mut piezo_was_ok).await?;
                }
                _ = keepalive_tick.tick() => {
                    if keepalive_and_health(
                        write_half,
                        last_valid_frame,
                        rx_alive,
                        vibrate_no_ack_wait,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Send at most one outstanding piezo config command (opensleep CommandScheduler style).
/// If setup was OK but the piezo stream stalled, clear stream flags so EnablePiezo / SetPiezoFreq re-run.
async fn maybe_send_piezo_setup(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    piezo_setup: &PiezoSetupState,
    piezo_was_ok: &mut bool,
) -> Result<(), SensorLinkError> {
    // A piezo sample stream already flowing (frame within the last config interval)
    // means the MCU retained its config from a previous session. Sending config
    // commands into a *sampling* MCU wedged it once (Pod 5: no acks, stream died
    // ~45 s later, only a power cycle revived it) — treat the live stream as setup
    // complete and send nothing. Checked before the ok-branch: the sample stream can
    // complete gain/enable/freq flags while EnableVibration is still pending.
    if next_piezo_setup_frame(piezo_setup).is_some()
        && piezo_setup
            .last_piezo_age()
            .is_some_and(|age| age < CONFIG_RES_TIME)
    {
        piezo_setup.mark_stream_configured();
        tracing::info!(
            "Sensor: piezo stream already flowing — MCU retained config; skipping setup commands (no re-config while sampling)"
        );
        return Ok(());
    }
    if piezo_setup.piezo_ok() {
        if !*piezo_was_ok {
            *piezo_was_ok = true;
            tracing::info!(
                freq = piezo_setup.target_freq_hz(),
                "Sensor: piezo setup OK (vibration + gain + enabled + freq)"
            );
        }
        let stalled = match piezo_setup.last_piezo_age() {
            None => true,
            Some(age) => age >= PIEZO_STREAM_STALL,
        };
        if stalled {
            tracing::warn!(
                stall_s = PIEZO_STREAM_STALL.as_secs(),
                "Sensor: no piezo samples for >{}s after setup OK — re-enabling piezo stream",
                PIEZO_STREAM_STALL.as_secs()
            );
            piezo_setup.request_stream_recovery();
            *piezo_was_ok = false;
        } else {
            return Ok(());
        }
    } else {
        *piezo_was_ok = false;
    }

    let Some(frame) = next_piezo_setup_frame(piezo_setup) else {
        return Ok(());
    };
    write_half.write_all(&frame).await?;
    write_half.flush().await?;
    tracing::debug!(
        vibration = piezo_setup.vibration_enabled(),
        gain_ok = piezo_setup.piezo_gain_ok(),
        enabled = piezo_setup.piezo_enabled(),
        freq_ok = piezo_setup.piezo_freq_ok(),
        "Sensor: piezo setup command sent"
    );
    Ok(())
}

/// Keepalive ping + RX health check. Returns `Ok(true)` when a reconnect was requested.
async fn keepalive_and_health(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    last_valid_frame: &Mutex<Instant>,
    rx_alive: &AtomicBool,
    vibrate_no_ack_wait: bool,
) -> Result<bool, SensorLinkError> {
    write_half.write_all(&ping_frame()).await?;
    write_half.flush().await?;
    tracing::trace!("Sensor keepalive ping");

    if !rx_alive.load(Ordering::SeqCst) {
        tracing::warn!("Sensor: RX task ended — reconnecting");
        return Ok(true);
    }
    if !vibrate_no_ack_wait {
        let stale = last_valid_frame
            .lock()
            .map(|lf| lf.elapsed() >= RX_HEALTH_TIMEOUT)
            .unwrap_or(true);
        if stale {
            tracing::warn!(
                timeout_s = RX_HEALTH_TIMEOUT.as_secs(),
                "Sensor: no valid RX frame for >{}s — reconnecting (probe/handshake)",
                RX_HEALTH_TIMEOUT.as_secs()
            );
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_alarm_duration_parsed_from_framed_command() {
        // 7E 08 2C side intensity pattern <duration u32 BE> crc crc
        let frame = crate::sensor_frame::set_alarm_frame(
            crate::frozen_frame::BedSide::Left,
            50,
            crate::sensor_frame::AlarmPattern::Single,
            1,
        );
        assert_eq!(set_alarm_duration_secs(&frame), Some(1));
        let frame = crate::sensor_frame::set_alarm_frame(
            crate::frozen_frame::BedSide::Right,
            64,
            crate::sensor_frame::AlarmPattern::Single,
            600,
        );
        assert_eq!(set_alarm_duration_secs(&frame), Some(600));
        // Not a SetAlarm / truncated → None.
        assert_eq!(
            set_alarm_duration_secs(&[0x7E, 0x01, 0x2E, 0x09, 0x30]),
            None
        );
        assert_eq!(set_alarm_duration_secs(&[0x7E, 0x08, 0x2C]), None);
    }

    #[test]
    fn backoff_doubles_until_cap() {
        assert_eq!(
            next_backoff(RECONNECT_BACKOFF_START),
            Duration::from_secs(4)
        );
        assert_eq!(next_backoff(Duration::from_secs(4)), Duration::from_secs(8));
        assert_eq!(next_backoff(Duration::from_secs(16)), RECONNECT_BACKOFF_MAX);
        assert_eq!(next_backoff(RECONNECT_BACKOFF_MAX), RECONNECT_BACKOFF_MAX);
    }

    #[test]
    fn exclusive_lock_error_detected() {
        // serialport flock failure: `ErrorKind::NoDevice` + "exclusive lock" description.
        let lock_err = SensorLinkError::Port(serialport::Error::new(
            serialport::ErrorKind::NoDevice,
            "Unable to acquire exclusive lock on serial port",
        ));
        assert!(is_exclusive_lock_error(&lock_err));
        let other = SensorLinkError::Port(serialport::Error::new(
            serialport::ErrorKind::Unknown,
            "some other port error",
        ));
        assert!(!is_exclusive_lock_error(&other));
        let io = SensorLinkError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "x"));
        assert!(!is_exclusive_lock_error(&io));
    }

    /// PTY-backed connect tests (Linux): a simulated Sensor MCU sits on the master side.
    /// PTY-backed connect tests (Linux): a simulated Sensor MCU sits on the master side.
    /// The sim reads the PTY line speed (TCGETS2 — master and slave share one termios) so it
    /// only "hears" the host at the baud matching its current mode, like the real MCU.
    #[cfg(target_os = "linux")]
    mod pty {
        use super::*;
        use std::io::{Read, Write};
        use std::os::unix::io::{AsRawFd, FromRawFd};

        /// Simulated Sensor MCU behaviour on the PTY master side.
        enum McuMode {
            /// Bootloader at 38400: answers pings, confirms jumps with `0x90`, then firmware at 921600.
            Bootloader,
            /// Bootloader at 38400 that answers pings but never sends the `0x90` jump ack.
            BootloaderNoAck,
            /// Firmware at 921600: answers pings with pong(0x46); ignores bootloader-baud traffic.
            Firmware,
            /// Never answers (TX-only path).
            Deaf,
        }

        fn open_pty_pair() -> (std::fs::File, String) {
            let fd = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
            assert!(fd >= 0, "posix_openpt failed");
            assert_eq!(unsafe { libc::grantpt(fd) }, 0, "grantpt failed");
            assert_eq!(unsafe { libc::unlockpt(fd) }, 0, "unlockpt failed");
            let mut buf = [0u8; 256];
            let rc = unsafe { libc::ptsname_r(fd, buf.as_mut_ptr().cast(), buf.len()) };
            assert_eq!(rc, 0, "ptsname_r failed");
            let end = buf.iter().position(|&b| b == 0).expect("ptsname NUL");
            let slave_path = String::from_utf8(buf[..end].to_vec()).expect("ptsname utf8");
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            (file, slave_path)
        }

        /// Current line speed of the PTY (lunaris sets the slave termios; master shares it).
        fn pty_line_speed(fd: std::os::unix::io::RawFd) -> u32 {
            let mut t2: libc::termios2 = unsafe { std::mem::zeroed() };
            let rc = unsafe { libc::ioctl(fd, libc::TCGETS2, &mut t2) };
            assert_eq!(rc, 0, "TCGETS2 failed");
            let cbaud = t2.c_cflag & libc::CBAUD;
            if cbaud == libc::BOTHER {
                t2.c_ospeed
            } else {
                match cbaud {
                    libc::B9600 => 9600,
                    libc::B19200 => 19200,
                    libc::B38400 => 38400,
                    libc::B57600 => 57600,
                    libc::B115200 => 115200,
                    libc::B230400 => 230400,
                    libc::B460800 => 460800,
                    libc::B921600 => 921600,
                    other => other,
                }
            }
        }

        /// The sim owns the PTY master. While the slave side is closed (phase changes in
        /// [`connect_sensor_port`] reopen the port), master reads fail with EIO — retry until
        /// `stop` is set; the test sets it before dropping the port so the thread can exit.
        fn spawn_mcu_sim(
            mut master: std::fs::File,
            mode: McuMode,
            stop: Arc<AtomicBool>,
        ) -> std::thread::JoinHandle<()> {
            std::thread::spawn(move || {
                let mut buf: Vec<u8> = Vec::new();
                let mut chunk = [0u8; 256];
                let mut in_firmware = matches!(mode, McuMode::Firmware);
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    match master.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            // The real MCU only hears the host at its current mode's baud.
                            let speed = pty_line_speed(master.as_raw_fd());
                            let hears = !matches!(mode, McuMode::Deaf)
                                && ((in_firmware && speed == 921600)
                                    || (!in_firmware && speed == 38400));
                            if !hears {
                                // Wrong baud for our mode — the MCU would not receive this.
                                continue;
                            }
                            buf.extend_from_slice(&chunk[..n]);
                            while let Some(pos) = buf.iter().position(|&b| b == 0x7E) {
                                if pos > 0 {
                                    buf.drain(..pos);
                                }
                                if buf.len() < 4 {
                                    break;
                                }
                                let plen = buf[1] as usize;
                                if buf.len() < 2 + plen + 2 {
                                    break;
                                }
                                let payload = buf[2..2 + plen].to_vec();
                                buf.drain(..2 + plen + 2);
                                match payload[0] {
                                    0x01 => {
                                        // Ping — pong with the mode byte (opensleep `parse_pong`).
                                        let mode_byte = if in_firmware { 0x46 } else { 0x42 };
                                        let pong = crate::frozen_frame::encode_command(&[
                                            0x81, 0x00, mode_byte,
                                        ]);
                                        if master.write_all(&pong).is_err() {
                                            return;
                                        }
                                        let _ = master.flush();
                                    }
                                    // JumpToFirmware — confirm with 0x90, then run firmware.
                                    0x10 if !in_firmware
                                        && !matches!(mode, McuMode::BootloaderNoAck) =>
                                    {
                                        let ack =
                                            crate::frozen_frame::encode_command(&[0x90, 0x01]);
                                        if master.write_all(&ack).is_err() {
                                            return;
                                        }
                                        let _ = master.flush();
                                        in_firmware = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(_) => {
                            // EIO while the slave side is closed — retry until stopped.
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    }
                }
            })
        }

        #[tokio::test]
        async fn bootloader_path_jump_confirmed_then_firmware() {
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::Bootloader, stop.clone());
            let (port, _leftover) = connect_sensor_port(&slave, 921600, false)
                .await
                .expect("bootloader path connect");
            stop.store(true, Ordering::Relaxed);
            drop(port);
            let _ = sim.join();
        }

        #[tokio::test]
        async fn firmware_path_probe_when_bootloader_silent() {
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::Firmware, stop.clone());
            let (port, _leftover) = connect_sensor_port(&slave, 921600, false)
                .await
                .expect("firmware probe connect");
            stop.store(true, Ordering::Relaxed);
            drop(port);
            let _ = sim.join();
        }

        #[tokio::test]
        async fn forced_handshake_skips_firmware_probe() {
            // Firmware-mode MCU + force flag: the bootloader phase stays silent, the firmware
            // probe is skipped → TX-only port at firmware baud.
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::Firmware, stop.clone());
            let (port, _leftover) = connect_sensor_port(&slave, 921600, true)
                .await
                .expect("forced handshake TX-only connect");
            stop.store(true, Ordering::Relaxed);
            drop(port);
            let _ = sim.join();
        }

        #[tokio::test]
        async fn tx_only_fallback_when_mcu_never_answers() {
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::Deaf, stop.clone());
            let (port, _leftover) = connect_sensor_port(&slave, 921600, false)
                .await
                .expect("TX-only fallback keeps the port open");
            stop.store(true, Ordering::Relaxed);
            drop(port);
            let _ = sim.join();
        }

        /// A piezo stream already flowing (retained MCU config) must gate the connect-time
        /// piezo setup: no config bytes on the wire, state marked complete. After a stream
        /// stall the normal ack-driven path resumes (EnableVibration goes out again).
        #[tokio::test]
        async fn piezo_setup_gate_skips_when_stream_flowing() {
            use std::io::Read;

            fn set_nonblock(fd: std::os::unix::io::RawFd, on: bool) {
                let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                assert_ne!(fl, -1, "F_GETFL failed");
                let fl = if on {
                    fl | libc::O_NONBLOCK
                } else {
                    fl & !libc::O_NONBLOCK
                };
                assert_ne!(unsafe { libc::fcntl(fd, libc::F_SETFL, fl) }, -1);
            }

            let (mut master, slave) = open_pty_pair();
            set_nonblock(master.as_raw_fd(), true);
            let port = open_port(&slave, 921600).await.expect("open slave port");
            let (_, mut write_half) = tokio::io::split(port);

            let setup = PiezoSetupState::new(500);
            // Retained, currently-flowing piezo stream (fresh sample batch just seen).
            setup.note_piezo_samples(&crate::sensor_rx::SensorPiezoSamples {
                tag: 0x35,
                freq: 0,
                sequence: 1000,
                gain: (400, 400),
                left_samples: vec![10, 20],
                right_samples: vec![30, 40],
                channel_variance: [0.0; 4],
                channel_stats: [crate::sensor_rx::PiezoChannelStats::default(); 4],
            });
            let mut was_ok = false;
            maybe_send_piezo_setup(&mut write_half, &setup, &mut was_ok)
                .await
                .expect("gate run");
            assert!(setup.piezo_ok(), "stream-alive gate marks setup complete");
            assert!(
                next_piezo_setup_frame(&setup).is_none(),
                "gate leaves no pending config command"
            );
            std::thread::sleep(Duration::from_millis(50));
            let mut buf = [0u8; 32];
            let n = master.read(&mut buf).unwrap_or(0);
            assert_eq!(n, 0, "no piezo config bytes may be sent while sampling");

            // Stalled stream (sample age cleared) → normal path resumes: recovery
            // re-sends EnablePiezo (`0x28`) for the cleared flags (vibration flag survived).
            setup.request_stream_recovery();
            let mut was_ok = false;
            maybe_send_piezo_setup(&mut write_half, &setup, &mut was_ok)
                .await
                .expect("post-stall run");
            let deadline = std::time::Instant::now() + Duration::from_millis(2000);
            let got = loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "no EnablePiezo byte seen on the wire after stall"
                );
                let mut chunk = [0u8; 32];
                match master.read(&mut chunk) {
                    Ok(0) | Err(_) => std::thread::sleep(Duration::from_millis(10)),
                    Ok(n) => break chunk[..n].to_vec(),
                }
            };
            assert!(
                got.contains(&0x28),
                "EnablePiezo must be sent after a stall: {got:?}"
            );
        }

        #[tokio::test]
        async fn jump_without_ack_confirmation_fails_after_retries() {
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::BootloaderNoAck, stop.clone());
            let err = connect_sensor_port(&slave, 921600, false)
                .await
                .expect_err("bootloader that never confirms the jump must fail discovery");
            assert!(
                err.to_string().contains("never confirmed JumpToFirmware"),
                "unexpected error: {err}"
            );
            stop.store(true, Ordering::Relaxed);
            let _ = sim.join();
        }

        /// Regression: a session's RX task must be aborted (and awaited) before the port is
        /// released — a blocked `read()` otherwise keeps the fd + exclusive flock open and
        /// every reconnect open fails with `Unable to acquire exclusive lock on serial port`.
        #[tokio::test]
        async fn port_reopens_after_rx_task_aborted() {
            let (master, slave) = open_pty_pair();
            let stop = Arc::new(AtomicBool::new(false));
            let sim = spawn_mcu_sim(master, McuMode::Deaf, stop.clone());

            let port = tokio_serial::new(slave.clone(), 921600)
                .open_native_async()
                .expect("session open");
            let (mut read_half, write_half) = tokio::io::split(port);
            let rx_task = tokio::spawn(async move {
                let mut chunk = [0u8; 64];
                loop {
                    match read_half.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
            drop(write_half);

            // While the RX task is blocked in read(), a second open hits the exclusive flock.
            let during = tokio_serial::new(slave.clone(), 921600).open_native_async();
            let err = during.expect_err("second open must fail while RX task holds port");
            assert!(is_exclusive_lock_error(&SensorLinkError::Port(err)));

            // Session end: abort + await releases the port.
            rx_task.abort();
            let _ = rx_task.await;
            let reopened = tokio_serial::new(slave.clone(), 921600).open_native_async();
            assert!(reopened.is_ok(), "reopen after RX task abort must succeed");
            drop(reopened);

            stop.store(true, Ordering::Relaxed);
            let _ = sim.join();
        }
    }
}
