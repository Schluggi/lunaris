//! Long-lived Frozen USART task: opensleep-style wake (`Ping` + `JumpToFirmware`), RX parsing,
//! keepalive, and queued MQTT-driven frames (prime / climate). Replaces open/send/close per action.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_serial::SerialPortBuilderExt;

use crate::frozen_frame::{get_hardware_info_frame, jump_to_firmware_frame, ping_frame};
use crate::frozen_rx::{self, FrozenTemperatureUpdate};

#[derive(Debug, Error)]
pub enum FrozenLinkError {
    #[error("serial I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("serial port: {0}")]
    Port(#[from] tokio_serial::Error),
}

#[derive(Debug)]
pub struct FrozenLinkHandle {
    pub tx: mpsc::Sender<Vec<u8>>,
    /// `true` once the Frozen MCU answers with CRC‑valid frames in firmware mode (`0x81` pong, `0x90` jump ack, etc.).
    pub frozen_mcu_connected: Arc<AtomicBool>,
    /// Current water-side and heatsink temperatures from inbound Frozen frames (`0x41` / `0xC1`).
    pub temperature_rx: mpsc::Receiver<FrozenTemperatureUpdate>,
    /// Frozen `0x07` water tank present / removed (for MQTT **Water Tank**).
    pub water_tank_rx: mpsc::Receiver<bool>,
    /// UTF‑8 bodies of Frozen `0x07` messages (MQTT **Frozen Message** text sensor).
    pub firmware_message_rx: mpsc::Receiver<String>,
}

pub fn spawn(device: PathBuf, baud: u32) -> FrozenLinkHandle {
    let (tx, rx) = mpsc::channel::<Vec<u8>>(64);
    let (temp_tx, temp_rx) = mpsc::channel::<FrozenTemperatureUpdate>(64);
    let (water_tx, water_rx) = mpsc::channel::<bool>(16);
    let (fw_tx, fw_rx) = mpsc::channel::<String>(128);
    let awake = Arc::new(AtomicBool::new(false));
    let awake_clone = awake.clone();
    tokio::spawn(async move {
        if let Err(e) = run(device, baud, rx, awake_clone, temp_tx, water_tx, fw_tx).await {
            tracing::error!(error = %e, "Frozen USART task ended");
        }
    });
    FrozenLinkHandle {
        tx,
        frozen_mcu_connected: awake,
        temperature_rx: temp_rx,
        water_tank_rx: water_rx,
        firmware_message_rx: fw_rx,
    }
}

async fn run(
    device: PathBuf,
    baud: u32,
    mut cmd_rx: mpsc::Receiver<Vec<u8>>,
    awake: Arc<AtomicBool>,
    temp_tx: mpsc::Sender<FrozenTemperatureUpdate>,
    water_tank_tx: mpsc::Sender<bool>,
    firmware_message_tx: mpsc::Sender<String>,
) -> Result<(), FrozenLinkError> {
    let path = device.to_string_lossy().to_string();
    let port = tokio_serial::new(path, baud).open_native_async()?;
    crate::serial_prime::set_serial_cloexec(&port)?;
    let (mut read_half, mut write_half) = tokio::io::split(port);

    let awake_reader = awake.clone();
    let water_tank_for_reader = water_tank_tx.clone();
    let fw_for_reader = firmware_message_tx.clone();
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(4096);
        let mut chunk = [0u8; 512];
        loop {
            match read_half.read(&mut chunk).await {
                Ok(0) => {
                    tracing::warn!("Frozen serial read EOF");
                    break;
                }
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    frozen_rx::drain_inbound(
                        &mut buf,
                        &awake_reader,
                        Some(&temp_tx),
                        Some(&water_tank_for_reader),
                        Some(&fw_for_reader),
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "Frozen serial read failed");
                    break;
                }
            }
        }
    });

    write_half.write_all(&ping_frame()).await?;
    sleep(Duration::from_millis(200)).await;
    write_half.write_all(&get_hardware_info_frame()).await?;
    write_half.flush().await?;
    tracing::info!("Frozen link: boot Ping + GetHardwareInfo sent");

    let mut tick = interval(Duration::from_millis(20));
    let mut last_wake = Instant::now()
        .checked_sub(Duration::from_secs(3))
        .unwrap_or_else(Instant::now);
    let mut wake_attempts = 0u32;
    let mut last_keepalive = Instant::now();
    let mut mqtt_channel_open = true;

    loop {
        if mqtt_channel_open {
            tokio::select! {
                biased;
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(bytes) => {
                            write_half.write_all(&bytes).await?;
                            write_half.flush().await?;
                            tracing::debug!(len = bytes.len(), "Frozen link: outbound MQTT frame");
                        }
                        None => {
                            tracing::warn!("Frozen MQTT→UART channel closed; wake/keepalive only");
                            mqtt_channel_open = false;
                        }
                    }
                }
                _ = tick.tick() => {
                    tick_body(
                        &mut write_half,
                        baud,
                        &awake,
                        &mut last_wake,
                        &mut wake_attempts,
                        &mut last_keepalive,
                    )
                    .await?;
                }
            }
        } else {
            tick.tick().await;
            tick_body(
                &mut write_half,
                baud,
                &awake,
                &mut last_wake,
                &mut wake_attempts,
                &mut last_keepalive,
            )
            .await?;
        }
    }
}

async fn tick_body(
    write_half: &mut tokio::io::WriteHalf<tokio_serial::SerialStream>,
    baud: u32,
    awake: &Arc<AtomicBool>,
    last_wake: &mut Instant,
    wake_attempts: &mut u32,
    last_keepalive: &mut Instant,
) -> Result<(), FrozenLinkError> {
    let now = Instant::now();
    if awake.load(Ordering::Relaxed) {
        *wake_attempts = 0;
        if now.duration_since(*last_keepalive) >= Duration::from_secs(15) {
            *last_keepalive = now;
            write_half.write_all(&ping_frame()).await?;
            write_half.flush().await?;
            tracing::trace!("Frozen keepalive ping");
        }
    } else if now.duration_since(*last_wake) >= Duration::from_secs(2) {
        *last_wake = now;
        *wake_attempts += 1;
        tracing::debug!(
            attempt = *wake_attempts,
            "Frozen wake: ping + jump_to_firmware"
        );
        write_half.write_all(&ping_frame()).await?;
        sleep(Duration::from_millis(200)).await;
        write_half.write_all(&jump_to_firmware_frame()).await?;
        write_half.flush().await?;
        if *wake_attempts > 0 && *wake_attempts % 10 == 0 {
            tracing::warn!(
                attempt = *wake_attempts,
                "Frozen MCU still not reporting firmware (check RX wiring / baud {})",
                baud
            );
        }
    }
    Ok(())
}
