//! Long-lived Sensor USART: optional **bootloader handshake** (opensleep: 38400 → jump → firmware baud),
//! RX parsing (`0xAE` VibrationEnabled), piezo priming, and queued vibration.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration};
use tokio_serial::SerialPortBuilderExt;

use crate::frozen_frame::jump_to_firmware_frame;
use crate::sensor_frame::{piezo_priming_frames, ping_frame};
use crate::sensor_rx::{self, PresenceCapDiag};

/// Bootloader serial speed — opensleep `BOOTLOADER_BAUD` (`src/sensor/manager.rs`).
const BOOTLOADER_BAUD: u32 = 38400;

#[derive(Debug, Error)]
pub enum SensorLinkError {
    #[error("serial I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("serial port: {0}")]
    Port(#[from] tokio_serial::Error),
}

#[derive(Debug)]
pub struct SensorLinkHandle {
    pub tx: mpsc::Sender<Vec<Vec<u8>>>,
    /// `true` after inbound Sensor bytes include `0x7E` (opensleep‑style frame sync on RX at this baud).
    pub sensor_rx_framing: Arc<AtomicBool>,
    /// UTF‑8 bodies of Sensor `0x07` MCU text (MQTT **Sensor Message** text sensor).
    pub sensor_message_rx: mpsc::Receiver<String>,
}

pub fn spawn(
    device: PathBuf,
    baud: u32,
    bootloader_handshake: bool,
    vibrate_no_ack_wait: bool,
    capacitance_tx: Option<mpsc::Sender<crate::sensor_rx::SensorCapacitanceZones>>,
    capacitance_parse_diag: Option<std::sync::Arc<PresenceCapDiag>>,
) -> SensorLinkHandle {
    let (tx, rx) = mpsc::channel::<Vec<Vec<u8>>>(32);
    let (sensor_mcu_tx, sensor_mcu_rx) = mpsc::channel::<String>(512);
    let sensor_rx_framing = Arc::new(AtomicBool::new(false));
    let sensor_rx_framing_reader = sensor_rx_framing.clone();
    tokio::spawn(async move {
        if let Err(e) = run(
            device,
            baud,
            bootloader_handshake,
            vibrate_no_ack_wait,
            rx,
            capacitance_tx,
            capacitance_parse_diag,
            sensor_mcu_tx,
            sensor_rx_framing_reader,
        )
        .await
        {
            tracing::error!(error = %e, "Sensor USART task ended");
        }
    });
    SensorLinkHandle {
        tx,
        sensor_rx_framing,
        sensor_message_rx: sensor_mcu_rx,
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

async fn open_sensor_port(
    path: &str,
    fw_baud: u32,
    bootloader_handshake: bool,
) -> Result<tokio_serial::SerialStream, SensorLinkError> {
    if bootloader_handshake {
        tracing::info!(
            bootloader_baud = BOOTLOADER_BAUD,
            firmware_baud = fw_baud,
            "Sensor: bootloader handshake (Ping + JumpToFirmware, opensleep order)"
        );
        let mut bl = tokio_serial::new(path.to_string(), BOOTLOADER_BAUD).open_native_async()?;
        crate::serial_prime::set_serial_cloexec(&bl)?;
        bl.write_all(&ping_frame()).await?;
        sleep(Duration::from_millis(200)).await;
        bl.write_all(&jump_to_firmware_frame()).await?;
        sleep(Duration::from_millis(700)).await;
        drop(bl);
    }
    let p = tokio_serial::new(path.to_string(), fw_baud)
        .open_native_async()
        .map_err(SensorLinkError::from)?;
    crate::serial_prime::set_serial_cloexec(&p)?;
    Ok(p)
}

/// Opensleep’s sensor [`CommandScheduler`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)
/// spaces configuration commands by **`CONFIG_RES_TIME` ≈ 800 ms**. Sending all priming frames back-to-back
/// can yield `0xAC` **AlarmSet** acks without physical vibration on some Pods — stagger writes.
const SENSOR_PRIMING_INTER_FRAME_MS: u64 = 150;
/// Pause after the last priming frame (`EnablePiezo`) so the MCU can settle before we wait for `0xAE` / send `SetAlarm`.
const SENSOR_AFTER_PRIMING_MS: u64 = 400;

async fn wait_for_vibration_ack(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    vibration_enabled: &Arc<AtomicBool>,
) -> Result<(), SensorLinkError> {
    if vibration_enabled.load(Ordering::SeqCst) {
        return Ok(());
    }
    // 120 × 50ms ≈ 6s — MCUs / framing can be slow; reassembly after split reads needs time.
    for attempt in 1..=120 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if vibration_enabled.load(Ordering::SeqCst) {
            tracing::debug!(attempt, "Sensor: 0xAE VibrationEnabled seen");
            return Ok(());
        }
        // Re-priming too often wedges some Sensor MCUs — opensleep uses ~800 ms spacing.
        if attempt > 0 && attempt % 30 == 0 {
            let prim = piezo_priming_frames();
            let gap = Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS);
            for (i, frame) in prim.iter().enumerate() {
                write_half.write_all(frame).await?;
                if i + 1 < prim.len() {
                    sleep(gap).await;
                }
            }
            write_half.flush().await?;
            tracing::debug!(attempt, "Sensor: re-primed while waiting for 0xAE");
        }
    }
    tracing::warn!(
        "Sensor: no 0xAE VibrationEnabled within ~6s — sending SetAlarm anyway (try `--sensor-vibrate-no-ack-wait`, or another `--sensor-baud` if RX never decodes 0x7E frames)"
    );
    Ok(())
}

async fn write_vibration_batch(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    frames: Vec<Vec<u8>>,
    vibration_enabled: &Arc<AtomicBool>,
    vibrate_no_ack_wait: bool,
) -> Result<(), SensorLinkError> {
    if vibrate_no_ack_wait {
        // Same pacing as opensleep-style staggering — back-to-back frames can overwhelm ttyS2 / firmware.
        let gap = Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS);
        if matches!(frames.len(), 5 | 6) {
            let has_cancel = frames.len() == 6;
            vibration_enabled.store(false, Ordering::SeqCst);
            if has_cancel {
                write_half.write_all(&frames[0]).await?;
                sleep(Duration::from_millis(100)).await;
            }
            let off = if has_cancel { 1 } else { 0 };
            let rest = &frames[off..];
            for (i, frame) in rest.iter().enumerate() {
                write_half.write_all(frame).await?;
                if i + 1 < rest.len() {
                    sleep(gap).await;
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
    if matches!(frames.len(), 5 | 6) {
        let has_cancel = frames.len() == 6;
        vibration_enabled.store(false, Ordering::SeqCst);
        if has_cancel {
            write_half.write_all(&frames[0]).await?;
            sleep(Duration::from_millis(100)).await;
        }
        let off = if has_cancel { 1 } else { 0 };
        let prim = &frames[off..off + 4];
        let alarm = &frames[off + 4..off + 5];
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
            wait_for_vibration_ack(write_half, vibration_enabled).await?;
        }
        for frame in alarm {
            write_half.write_all(frame).await?;
        }
        write_half.flush().await?;
    } else {
        for frame in frames {
            write_half.write_all(&frame).await?;
        }
        write_half.flush().await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run(
    device: PathBuf,
    baud: u32,
    bootloader_handshake: bool,
    vibrate_no_ack_wait: bool,
    mut rx: mpsc::Receiver<Vec<Vec<u8>>>,
    capacitance_tx: Option<mpsc::Sender<crate::sensor_rx::SensorCapacitanceZones>>,
    capacitance_parse_diag: Option<std::sync::Arc<PresenceCapDiag>>,
    sensor_mcu_tx: mpsc::Sender<String>,
    sensor_rx_framing: Arc<AtomicBool>,
) -> Result<(), SensorLinkError> {
    let path = device.to_string_lossy().to_string();
    let port = open_sensor_port(&path, baud, bootloader_handshake).await?;
    let (mut read_half, mut write_half) = tokio::io::split(port);

    let vibration_enabled = Arc::new(AtomicBool::new(false));
    let vib_flag = vibration_enabled.clone();
    let cap_flag = capacitance_tx;
    let parse_diag_for_rx = capacitance_parse_diag;
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(4096);
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
                        sensor_rx_framing.store(true, Ordering::SeqCst);
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
                    sensor_rx::drain_inbound(
                        &mut buf,
                        &vib_flag,
                        cap_flag.as_ref(),
                        diag,
                        Some(&sensor_mcu_tx),
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "Sensor serial read failed");
                    break;
                }
            }
        }
    });

    write_half.write_all(&ping_frame()).await?;
    let prim = piezo_priming_frames();
    let gap = Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS);
    for (i, frame) in prim.iter().enumerate() {
        write_half.write_all(frame).await?;
        if i + 1 < prim.len() {
            sleep(gap).await;
        }
    }
    write_half.flush().await?;
    tracing::info!("Sensor link: ping + initial piezo priming sent");

    let mut priming_tick = interval(Duration::from_secs(5));
    priming_tick.tick().await;

    let mut mqtt_channel_open = true;

    loop {
        if mqtt_channel_open {
            tokio::select! {
                biased;
                batch = rx.recv() => {
                    match batch {
                        Some(frames) => {
                            write_vibration_batch(
                                &mut write_half,
                                frames,
                                &vibration_enabled,
                                vibrate_no_ack_wait,
                            )
                            .await?;
                            tracing::debug!("Sensor link: vibration batch completed");
                        }
                        None => {
                            tracing::warn!("Sensor MQTT→UART channel closed; piezo priming only");
                            mqtt_channel_open = false;
                        }
                    }
                }
                _ = priming_tick.tick() => {
                    let prim = piezo_priming_frames();
                    for (i, frame) in prim.iter().enumerate() {
                        write_half.write_all(frame).await?;
                        if i + 1 < prim.len() {
                            sleep(Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS)).await;
                        }
                    }
                    write_half.flush().await?;
                    tracing::trace!("Sensor link: periodic piezo priming");
                }
            }
        } else {
            priming_tick.tick().await;
            let prim = piezo_priming_frames();
            for (i, frame) in prim.iter().enumerate() {
                write_half.write_all(frame).await?;
                if i + 1 < prim.len() {
                    sleep(Duration::from_millis(SENSOR_PRIMING_INTER_FRAME_MS)).await;
                }
            }
            write_half.flush().await?;
            tracing::trace!("Sensor link: periodic piezo priming");
        }
    }
}
