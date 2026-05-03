//! Sensor inbound parsing — detects **`VibrationEnabled`** (`0xAE`) like opensleep’s
//! [`SensorPacket::VibrationEnabled`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/packet.rs).
//! The MCU often ignores `SetAlarm` until this ack arrives.
//!
//! **Capacitance** (`0x33`) matches opensleep [`SensorPacket::Capacitance`] — six zone values, ordered
//! left-to-right on the mattress — used for MQTT presence / occupancy.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc;

use crate::frozen_frame::crc_ccitt;
use crate::wire_buffer::trim_when_no_sync_byte;

/// If we hold more than this waiting for the rest of a putative frame, drop `0x7E` and resync (false length / truncated stream).
const MAX_INCOMPLETE_HOLD: usize = 4096;

/// Six capacitance samples from Sensor `0x33` (opensleep `CapacitanceData`), ordered **LTR** along the bed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SensorCapacitanceZones {
    pub sequence: u32,
    pub zones: [u16; 6],
}

fn send_cap_update(
    tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
    update: SensorCapacitanceZones,
) {
    let Some(tx) = tx else {
        return;
    };
    if let Err(e) = tx.try_send(update) {
        tracing::trace!(
            ?e,
            "Sensor capacitance update dropped (channel closed or full)"
        );
    }
}

pub fn drain_inbound(
    buffer: &mut Vec<u8>,
    vibration_enabled: &AtomicBool,
    capacitance_tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
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
                    "Sensor RX: incomplete frame buffer overflow — dropping leading 0x7E"
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
        handle_payload(payload, vibration_enabled, capacitance_tx);
        buffer.drain(..frame_len);
    }
}

fn parse_capacitance(payload: &[u8]) -> Option<SensorCapacitanceZones> {
    // opensleep `SensorPacket::parse_capacitance`: 27-byte payload starting with `0x33`.
    if payload.len() != 27 || payload[0] != 0x33 {
        return None;
    }
    let indices_valid = payload[9] == 0
        && payload[12] == 1
        && payload[15] == 2
        && payload[18] == 3
        && payload[21] == 4
        && payload[24] == 5;
    if !indices_valid {
        return None;
    }
    Some(SensorCapacitanceZones {
        sequence: u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]),
        zones: [
            u16::from_be_bytes([payload[10], payload[11]]),
            u16::from_be_bytes([payload[13], payload[14]]),
            u16::from_be_bytes([payload[16], payload[17]]),
            u16::from_be_bytes([payload[19], payload[20]]),
            u16::from_be_bytes([payload[22], payload[23]]),
            u16::from_be_bytes([payload[25], payload[26]]),
        ],
    })
}

fn handle_payload(
    payload: &[u8],
    vibration_enabled: &AtomicBool,
    capacitance_tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
) {
    if payload.is_empty() {
        return;
    }
    match payload[0] {
        // Pod 4 can send a **2-byte** payload (`0xAE` + 1 data byte, length `0x02`); opensleep
        // tests use 3 bytes — accept any well-CRC’d payload with at least the command byte.
        0xAE if !payload.is_empty() => {
            if !vibration_enabled.swap(true, Ordering::SeqCst) {
                tracing::info!("Sensor: vibration armed (MCU ack 0xAE VibrationEnabled)");
            }
        }
        0xAC if payload.len() >= 2 => {
            let status = payload[1];
            if status == 0x01 {
                tracing::debug!(
                    status,
                    "Sensor: AlarmSet status (0xAC), opensleep test expectation"
                );
            } else {
                tracing::warn!(
                    status,
                    "Sensor: AlarmSet status (0xAC) — opensleep tests expect status 1; with `--sensor-vibrate-cancel-preamble` the first `0xAC` can be the cancel-SetAlarm ack; non-1 on the real alarm may still mean the MCU did not start vibration — watch for `Sensor: MCU text`"
                );
            }
        }
        // Opensleep `parse_message`: `0x07`, spacer, UTF-8 body (MCU explains alarms / errors).
        0x07 if payload.len() >= 3 => match std::str::from_utf8(&payload[2..]) {
            Ok(text) => {
                let t = text.trim_end_matches('\0');
                if t.contains("alarm") || t.contains("FW:") {
                    tracing::warn!(msg = %t, "Sensor: MCU text (alarm / firmware — primary hint why vibration may not run)");
                } else {
                    tracing::info!(msg = %t, "Sensor: MCU text");
                }
            }
            Err(_) => tracing::trace!(len = payload.len(), "Sensor: MCU message (0x07) non-UTF8"),
        },
        0x81 if payload.len() >= 3 => {
            tracing::trace!(code = payload[2], "Sensor: pong");
        }
        0x33 => {
            if let Some(cap) = parse_capacitance(payload) {
                tracing::trace!(seq = cap.sequence, z = ?cap.zones, "Sensor: capacitance");
                send_cap_update(capacitance_tx, cap);
            } else {
                tracing::trace!(len = payload.len(), "Sensor: capacitance 0x33 parse skip");
            }
        }
        _ => tracing::trace!(len = payload.len(), head = payload[0], "Sensor RX"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vibration_enabled_sets_atomic() {
        let flag = AtomicBool::new(false);
        let payload = [0xAEu8, 0, 2];
        let crc = crate::frozen_frame::crc_ccitt(&payload);
        let mut frame = vec![0x7E, payload.len() as u8];
        frame.extend_from_slice(&payload);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);

        let mut buf = frame.clone();
        drain_inbound(&mut buf, &flag, None);
        assert!(flag.load(Ordering::SeqCst));
        assert!(buf.is_empty());
    }

    /// Noise without `0x7E` must not be fully discarded (regression: would break split reads).
    /// Pod 4-style frame from real UART: `7E 02 AE 02 [CRC]` (2-byte payload, not 3).
    #[test]
    fn vibration_enabled_two_byte_payload_matches_pod4_rx() {
        let flag = AtomicBool::new(false);
        let mut buf = vec![0x7Eu8, 0x02, 0xAE, 0x02, 0x9A, 0xF3];
        drain_inbound(&mut buf, &flag, None);
        assert!(flag.load(Ordering::SeqCst));
        assert!(buf.is_empty());
    }

    /// Decodes opensleep-style `0x07` text (see opensleep `parse_message`).
    #[test]
    fn mcu_message_parses_from_framed_payload() {
        let flag = AtomicBool::new(false);
        let mut inner = vec![0x07u8, 0x00];
        inner.extend_from_slice(b"FW: alarm [left] off");
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);
        let mut buf = frame;
        drain_inbound(&mut buf, &flag, None);
        assert!(buf.is_empty());
    }

    #[test]
    fn noise_prefix_then_vibration_enabled_frame() {
        let flag = AtomicBool::new(false);
        let payload = [0xAEu8, 0, 2];
        let crc = crate::frozen_frame::crc_ccitt(&payload);
        let mut frame = vec![0x7E, payload.len() as u8];
        frame.extend_from_slice(&payload);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);

        let mut buf = vec![0xE0, 0x1C, 0xE0, 0xFE, 0x00];
        drain_inbound(&mut buf, &flag, None);
        assert!(!flag.load(Ordering::SeqCst));
        buf.extend_from_slice(&frame);
        drain_inbound(&mut buf, &flag, None);
        assert!(
            flag.load(Ordering::SeqCst),
            "0xAE frame after leading noise"
        );
    }

    #[tokio::test]
    async fn capacitance_opensleep_example_framed() {
        let flag = AtomicBool::new(false);
        // opensleep `sensor/packet.rs` `test_capacitance`
        let mut inner = Vec::from([0x33u8, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00]);
        inner.extend_from_slice(&[
            0x00, 0x01, 0x02, 0x01, 0x03, 0x04, 0x02, 0x05, 0x06, 0x03, 0x07, 0x08, 0x04, 0x09,
            0x0A, 0x05, 0x0B, 0x0C,
        ]);
        assert_eq!(inner.len(), 27);
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.extend_from_slice(&crc.to_be_bytes());

        let (tx, mut rx) = mpsc::channel::<SensorCapacitanceZones>(4);
        let mut buf = frame;
        drain_inbound(&mut buf, &flag, Some(&tx));
        let c = rx.recv().await.unwrap();
        assert_eq!(c.sequence, 0x01020304);
        assert_eq!(c.zones, [0x0102, 0x0304, 0x0506, 0x0708, 0x090A, 0x0B0C]);
        assert!(buf.is_empty());
    }
}
