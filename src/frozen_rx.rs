//! Minimal Frozen inbound parsing for MCU wake detection (opensleep-style payloads).
//!
//! Temperature telemetry follows opensleep `FrozenPacket::TemperatureUpdate` (`0x41`) and
//! `GetTemperature` (`0xC1`) — see `src/frozen/packet.rs` in [opensleep](https://github.com/LiamSnow/opensleep).
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc;

use crate::frozen_frame::crc_ccitt;
use crate::wire_buffer::trim_when_no_sync_byte;

const MAX_INCOMPLETE_HOLD: usize = 4096;

/// Latest water-loop temperatures from Frozen `TemperatureUpdate` / `GetTemperature` (centidegrees °C).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrozenTemperatureUpdate {
    pub left_centi: u16,
    pub right_centi: u16,
    pub heatsink_centi: u16,
}

pub fn drain_inbound(
    buffer: &mut Vec<u8>,
    awake: &AtomicBool,
    temp_tx: Option<&mpsc::Sender<FrozenTemperatureUpdate>>,
) {
    loop {
        let start = match buffer.iter().position(|&b| b == 0x7E) {
            Some(i) => i,
            None => {
                trim_when_no_sync_byte(buffer);
                return;
            }
        };
        if start > 0 {
            buffer.drain(..start);
        }
        if buffer.len() < 4 {
            return;
        }
        let plen = buffer[1] as usize;
        let frame_len = 2 + plen + 2;
        if buffer.len() < frame_len {
            if buffer.len() > MAX_INCOMPLETE_HOLD {
                tracing::warn!(
                    held = buffer.len(),
                    need = frame_len,
                    "Frozen RX: incomplete frame buffer overflow — dropping leading 0x7E"
                );
                buffer.remove(0);
                continue;
            }
            return;
        }
        let payload = &buffer[2..2 + plen];
        let crc_rx = u16::from_be_bytes([buffer[2 + plen], buffer[2 + plen + 1]]);
        if crc_ccitt(payload) != crc_rx {
            buffer.remove(0);
            continue;
        }
        handle_payload(payload, awake, temp_tx);
        buffer.drain(..frame_len);
    }
}

fn send_temperature_update(
    tx: Option<&mpsc::Sender<FrozenTemperatureUpdate>>,
    update: FrozenTemperatureUpdate,
) {
    let Some(tx) = tx else {
        return;
    };
    if let Err(e) = tx.try_send(update) {
        tracing::trace!(
            ?e,
            "Frozen temperature update dropped (channel closed or full)"
        );
    }
}

fn handle_payload(
    payload: &[u8],
    awake: &AtomicBool,
    temp_tx: Option<&mpsc::Sender<FrozenTemperatureUpdate>>,
) {
    if payload.is_empty() {
        return;
    }
    match payload[0] {
        0x41 if payload.len() == 9 => {
            // opensleep `FrozenPacket::TemperatureUpdate`
            send_temperature_update(
                temp_tx,
                FrozenTemperatureUpdate {
                    left_centi: u16::from_be_bytes([payload[1], payload[2]]),
                    right_centi: u16::from_be_bytes([payload[3], payload[4]]),
                    heatsink_centi: u16::from_be_bytes([payload[5], payload[6]]),
                },
            );
        }
        0xC1 if payload.len() == 27 => {
            // opensleep `FrozenPacket::GetTemperature`
            let indices_valid = payload[1] == 0
                && payload[2] == 1
                && payload[5] == 2
                && payload[8] == 3
                && payload[11] == 4;
            if indices_valid {
                send_temperature_update(
                    temp_tx,
                    FrozenTemperatureUpdate {
                        left_centi: u16::from_be_bytes([payload[3], payload[4]]),
                        right_centi: u16::from_be_bytes([payload[6], payload[7]]),
                        heatsink_centi: u16::from_be_bytes([payload[12], payload[13]]),
                    },
                );
            }
        }
        0x81 if payload.len() >= 3 => match payload[2] {
            0x46 => {
                if !awake.swap(true, Ordering::SeqCst) {
                    tracing::info!("Frozen: firmware mode (pong)");
                }
            }
            0x42 => {
                awake.store(false, Ordering::SeqCst);
                tracing::debug!("Frozen: bootloader mode (pong)");
            }
            _ => tracing::trace!(code = payload[2], "Frozen pong unknown"),
        },
        0x90 => {
            if !awake.swap(true, Ordering::SeqCst) {
                tracing::info!("Frozen: jump-to-firmware acknowledged");
            }
        }
        _ => tracing::trace!(len = payload.len(), head = payload[0], "Frozen RX"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn temperature_update_opensleep_example() {
        let (tx, mut rx) = mpsc::channel(4);
        let awake = AtomicBool::new(false);
        let payload = [0x41, 0x09, 0xF6, 0x0A, 0x73, 0x08, 0xFC, 0x09, 0x00];
        let crc = crc_ccitt(&payload);
        let mut buf = Vec::new();
        buf.push(0x7E);
        buf.push(payload.len() as u8);
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&crc.to_be_bytes());

        drain_inbound(&mut buf, &awake, Some(&tx));
        let u = rx.recv().await.unwrap();
        assert_eq!(u.left_centi, 2550);
        assert_eq!(u.right_centi, 2675);
        assert_eq!(u.heatsink_centi, 2300);
        assert!(buf.is_empty());
    }
}
