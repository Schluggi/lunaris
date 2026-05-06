//! Sensor inbound parsing — detects **`VibrationEnabled`** (`0xAE`) like opensleep’s
//! [`SensorPacket::VibrationEnabled`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/packet.rs).
//! The MCU often ignores `SetAlarm` until this ack arrives.
//!
//! **Capacitance** (`0x33`) matches opensleep [`SensorPacket::Capacitance`] — six zone values, ordered
//! left-to-right on the mattress — used for MQTT presence / occupancy.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::frozen_frame::crc_ccitt;
use crate::wire_buffer::trim_when_no_sync_byte;

/// If we hold more than this waiting for the rest of a putative frame, drop `0x7E` and resync (false length / truncated stream).
const MAX_INCOMPLETE_HOLD: usize = 4096;
/// MQTT text sensor truncation (MCU lines can be long; broker/HA still have practical limits).
const MAX_SENSOR_MESSAGE_BYTES: usize = 2048;

fn truncate_utf8_sensor_message(s: &str) -> String {
    if s.len() <= MAX_SENSOR_MESSAGE_BYTES {
        return s.to_string();
    }
    let mut n = MAX_SENSOR_MESSAGE_BYTES;
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    format!("{}…", &s[..n])
}

/// Parses Sensor MCU text lines that include **`[ambient] temp … humidity …`** (firmware diagnostics).
///
/// Returned strings are **`{:.2}`**-rounded for MQTT state topics ([`crate::mqtt_bridge`] publishes them).
#[must_use]
pub fn ambient_from_sensor_message_line(line: &str) -> Option<(String, String)> {
    let base = line.find("[ambient]")?;
    let sub = line[base..].trim_start();
    let (_, rest) = sub.split_once("temp ")?;
    let mut it = rest.split_whitespace();
    let temp_raw: f64 = it.next()?.parse().ok()?;
    let hum_kw = it.next()?;
    if hum_kw != "humidity" {
        return None;
    }
    let humidity_raw: f64 = it.next()?.parse().ok()?;
    Some((format!("{:.2}", temp_raw), format!("{:.2}", humidity_raw)))
}

/// Bed-side cover control (`[lisL]` / `[lisR]`) tap count from Sensor MCU dismissal text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverButtonTapSide {
    Left,
    Right,
}

/// Substring before the tap digit(s), matched **ASCII case-insensitive** (`Dismissing`, …).
const COVER_BTN_DISMISS_PHRASE: &[u8] = b"dismissing alarm (";

fn find_cover_button_lis_tag(line: &[u8]) -> Option<(CoverButtonTapSide, usize)> {
    /// Byte offset **after** the closing `]` of **`[lisL]` / `[lisR]`** (fixed **6** ASCII bytes, case-insensitive tag).
    const TAG_LEN: usize = 6;

    #[inline]
    fn find_window_ci(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len())
            .position(|w| w.eq_ignore_ascii_case(needle))
    }

    let left = find_window_ci(line, b"[lisl]").map(|i| (CoverButtonTapSide::Left, i));
    let right = find_window_ci(line, b"[lisr]").map(|i| (CoverButtonTapSide::Right, i));
    let (side, start) = match (left, right) {
        (Some(l), Some(r)) => {
            if l.1 <= r.1 {
                l
            } else {
                r
            }
        }
        (Some(x), None) | (None, Some(x)) => x,
        (None, None) => return None,
    };
    Some((side, start + TAG_LEN))
}

fn parse_cover_button_taps_after_open_paren(rest: &str) -> Option<u8> {
    let rest = rest.trim_start();
    let digit_end = rest
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit())
        .map(|(i, _)| i)
        .unwrap_or(rest.len());
    if digit_end == 0 {
        return None;
    }
    let n: u32 = rest.get(..digit_end)?.parse().ok()?;
    if !(1..=5).contains(&n) {
        return None;
    }
    let after_digits = rest.get(digit_end..)?.trim_start();
    let after_bytes = after_digits.as_bytes();
    let ok_taps = after_bytes.len() >= 5 && after_bytes[..5].eq_ignore_ascii_case(b"taps)");
    let ok_tap = after_bytes.len() >= 4 && after_bytes[..4].eq_ignore_ascii_case(b"tap)");
    if !ok_taps && !ok_tap {
        return None;
    }
    Some(n as u8)
}

/// Parsed tap count **`1..=5`** from Sensor MCU lines such as
/// `FW: 20628 [lisR] dismissing alarm (1 taps)` (phrase + digits are matched resiliently).
///
/// If both **`[lisL]`** and **`[lisR]`** appear, the **[leftmost]** tag wins.
#[must_use]
pub fn cover_button_dismiss_tap_count_from_sensor_message(
    line: &str,
) -> Option<(CoverButtonTapSide, u8)> {
    let bytes = line.as_bytes();
    let (side, after_tag) = find_cover_button_lis_tag(bytes)?;
    let rest = line.get(after_tag..)?.trim_start();
    if rest.len() < COVER_BTN_DISMISS_PHRASE.len() {
        return None;
    }
    if !rest.as_bytes()[..COVER_BTN_DISMISS_PHRASE.len()]
        .eq_ignore_ascii_case(COVER_BTN_DISMISS_PHRASE)
    {
        return None;
    }
    let after_phrase = rest.get(COVER_BTN_DISMISS_PHRASE.len()..)?;
    let taps = parse_cover_button_taps_after_open_paren(after_phrase)?;
    Some((side, taps))
}

fn maybe_send_sensor_message(tx: Option<&mpsc::Sender<String>>, text: &str) {
    let Some(t) = tx else {
        return;
    };
    let text = text.trim_end_matches('\0').trim_end();
    if text.is_empty() {
        return;
    }
    let body = truncate_utf8_sensor_message(text);
    if let Err(e) = t.try_send(body) {
        tracing::trace!(
            ?e,
            "Sensor MCU message (0x07) dropped (MQTT Sensor Message channel lag)"
        );
    }
}

/// Six capacitance samples from Sensor `0x33` (opensleep `CapacitanceData`), ordered **LTR** along the bed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SensorCapacitanceZones {
    pub sequence: u32,
    pub zones: [u16; 6],
}

fn hex_preview(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(48)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// With [`Cli::presence_debug`](crate::cli::Cli): log first CRC-valid **`0x33`** payload rejected by opensleep layout checks ([`parse_capacitance`] strict indices).
#[derive(Default)]
pub struct PresenceCapDiag {
    malformed_logged: AtomicBool,
}

impl PresenceCapDiag {
    #[must_use]
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self {
            malformed_logged: AtomicBool::new(false),
        })
    }

    pub fn malformed_once(&self, payload: &[u8]) {
        if !self.malformed_logged.swap(true, Ordering::SeqCst) {
            tracing::warn!(
                len = payload.len(),
                hex = %hex_preview(payload),
                "Sensor: `0x33` payload failed opensleep capacitance parse (--presence-debug) — MCU layout may differ from Pod 3; presence MQTT will stay unsupported until Parser matches firmware"
            );
        }
    }
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
    capacitance_diag: Option<&PresenceCapDiag>,
    sensor_message_tx: Option<&mpsc::Sender<String>>,
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
        handle_payload(
            payload,
            vibration_enabled,
            capacitance_tx,
            capacitance_diag,
            sensor_message_tx,
        );
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

/// `0x07` MCU strings: routine `FW:` status (sampling, therm, …) is noisy at WARN — reserve WARN for
/// lines that plausibly explain alarm / vibration behaviour.
fn mcu_text_is_vibration_hint(t: &str) -> bool {
    if cover_button_dismiss_tap_count_from_sensor_message(t).is_some() {
        return false;
    }
    let s = t.to_ascii_lowercase();
    let fw_failure_hint = s.contains("fw:") && (s.contains("error") || fw_line_fail_hint(&s));

    s.contains("alarm")
        || s.contains("vibrat")
        || s.contains("setalarm")
        || s.contains(" piezo ")
        || s.contains("piezo:")
        || fw_failure_hint
}

/// True if `fail` appears as a stem other than `fails` (MCU `[sampling] crc fails: …` is not a fault hint).
fn fw_line_fail_hint(s_lower: &str) -> bool {
    if !s_lower.contains("fw:") {
        return false;
    }
    for (idx, _) in s_lower.match_indices("fail") {
        if s_lower[idx.saturating_add(4)..].starts_with('s') {
            continue;
        }
        return true;
    }
    false
}

fn handle_payload(
    payload: &[u8],
    vibration_enabled: &AtomicBool,
    capacitance_tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
    capacitance_diag: Option<&PresenceCapDiag>,
    sensor_message_tx: Option<&mpsc::Sender<String>>,
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
                    "Sensor: AlarmSet status (0xAC) — opensleep tests expect status 1; with cancel preamble on (default) the first `0xAC` can be the cancel-SetAlarm ack; non-1 on the real alarm may still mean the MCU did not start vibration — watch for `Sensor: MCU text`"
                );
            }
        }
        // Opensleep `parse_message`: `0x07`, spacer, UTF-8 body (MCU explains alarms / errors).
        0x07 if payload.len() >= 3 => match std::str::from_utf8(&payload[2..]) {
            Ok(text) => {
                let t = text.trim_end_matches('\0');
                maybe_send_sensor_message(sensor_message_tx, t);
                if mcu_text_is_vibration_hint(t) {
                    tracing::warn!(msg = %t, "Sensor: MCU text (alarm / vibration hint)");
                } else if t.contains("FW:") {
                    tracing::debug!(msg = %t, "Sensor: MCU firmware line");
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
                if let Some(d) = capacitance_diag {
                    d.malformed_once(payload);
                }
            }
        }
        _ => tracing::trace!(len = payload.len(), head = payload[0], "Sensor RX"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcu_text_routine_fw_lines_not_vibration_hints() {
        assert!(!mcu_text_is_vibration_hint(
            "FW: 321519 [sampling] starting (flushed 3 samples)"
        ));
        assert!(!mcu_text_is_vibration_hint("FW: 321519 [sampling] sync"));
        assert!(!mcu_text_is_vibration_hint("FW: 325660 [therm] Tref=24.91"));
        assert!(!mcu_text_is_vibration_hint(
            "FW: 326217 [sampling] req gain 400 400"
        ));
        assert!(!mcu_text_is_vibration_hint(
            "FW: 16231446 [sampling] crc fails: 0 0"
        ));
    }

    #[test]
    fn mcu_text_fw_fail_still_hints_when_not_plural_fails() {
        assert!(mcu_text_is_vibration_hint("FW: 1 [err] uart fail"));
        assert!(mcu_text_is_vibration_hint("FW: 1 failed to init piezo"));
    }

    #[test]
    fn mcu_text_alarm_and_vibrate_are_hints() {
        assert!(mcu_text_is_vibration_hint("FW: alarm [left] off"));
        assert!(mcu_text_is_vibration_hint("something vibration disabled"));
        assert!(mcu_text_is_vibration_hint("SetAlarm rejected"));
    }

    #[test]
    fn mcu_text_cover_dismiss_line_is_not_vibration_hint() {
        assert!(!mcu_text_is_vibration_hint(
            "FW: 20628 [lisR] dismissing alarm (1 taps)"
        ));
    }

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
        drain_inbound(&mut buf, &flag, None, None, None);
        assert!(flag.load(Ordering::SeqCst));
        assert!(buf.is_empty());
    }

    /// Noise without `0x7E` must not be fully discarded (regression: would break split reads).
    /// Pod 4-style frame from real UART: `7E 02 AE 02 [CRC]` (2-byte payload, not 3).
    #[test]
    fn vibration_enabled_two_byte_payload_matches_pod4_rx() {
        let flag = AtomicBool::new(false);
        let mut buf = vec![0x7Eu8, 0x02, 0xAE, 0x02, 0x9A, 0xF3];
        drain_inbound(&mut buf, &flag, None, None, None);
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
        drain_inbound(&mut buf, &flag, None, None, None);
        assert!(buf.is_empty());
    }

    #[test]
    fn ambient_line_parsed_and_rounded() {
        let line = "changed to FW: 421463 [ambient] temp 25.5982 humidity 38.6536 percent ";
        assert_eq!(
            ambient_from_sensor_message_line(line),
            Some(("25.60".to_string(), "38.65".to_string()))
        );
        let line2 = " changed to FW: 331463 [ambient] temp 25.5261 humidity 38.6937 percent ";
        assert_eq!(
            ambient_from_sensor_message_line(line2),
            Some(("25.53".to_string(), "38.69".to_string()))
        );
    }

    #[test]
    fn ambient_line_requires_humidity_keyword() {
        assert!(ambient_from_sensor_message_line("[ambient] temp 1.234 junk 5").is_none());
    }

    #[test]
    fn cover_button_dismiss_tap_counts_parsed() {
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "changed to FW: 99 [lisL] dismissing alarm (1 taps)"
            ),
            Some((CoverButtonTapSide::Left, 1))
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "FW: 20628 [lisR] dismissing alarm (1 taps)"
            ),
            Some((CoverButtonTapSide::Right, 1)),
            "exact Pod log-style line",
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "FW: 26303 [lisR] dismissing alarm (4 taps)"
            ),
            Some((CoverButtonTapSide::Right, 4))
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisR] dismissing alarm (5 taps)"),
            Some((CoverButtonTapSide::Right, 5))
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisR] dismissing alarm (3 taps)x"),
            Some((CoverButtonTapSide::Right, 3))
        );
    }

    #[test]
    fn cover_button_dismiss_phrase_is_ascii_case_insensitive() {
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "FW: 1 [lisR] DiSmIsSiNg AlArM (3 taps)",
            ),
            Some((CoverButtonTapSide::Right, 3))
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "prefix [lisL] DISMISSING ALARM (2 taps)"
            ),
            Some((CoverButtonTapSide::Left, 2))
        );
    }

    #[test]
    fn cover_button_dismiss_tag_is_ascii_case_insensitive() {
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[LISR] dismissing alarm (1 taps)",),
            Some((CoverButtonTapSide::Right, 1))
        );
    }

    #[test]
    fn cover_button_dismiss_accepts_singular_tap() {
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisL] dismissing alarm (2 tap)",),
            Some((CoverButtonTapSide::Left, 2))
        );
    }

    #[test]
    fn cover_button_dismiss_rejects_bad_counts_and_other_lines() {
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisL] dismissing alarm (0 taps)"),
            None
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisL] dismissing alarm (6 taps)"),
            None
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message("[lisL] saw single tap"),
            None
        );
        assert_eq!(
            cover_button_dismiss_tap_count_from_sensor_message(
                "FW: 1 [ambient] temp 22 humidity 40"
            ),
            None
        );
    }

    #[tokio::test]
    async fn mcu_message_try_sends_utf8_body_to_mqtt_bridge_channel() {
        let flag = AtomicBool::new(false);
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let mut inner = vec![0x07u8, 0x00];
        inner.extend_from_slice(b"pump primed");
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);
        let mut buf = frame;
        drain_inbound(&mut buf, &flag, None, None, Some(&tx));
        assert!(buf.is_empty());
        assert_eq!(rx.recv().await, Some("pump primed".to_string()));
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
        drain_inbound(&mut buf, &flag, None, None, None);
        assert!(!flag.load(Ordering::SeqCst));
        buf.extend_from_slice(&frame);
        drain_inbound(&mut buf, &flag, None, None, None);
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
        drain_inbound(&mut buf, &flag, Some(&tx), None, None);
        let c = rx.recv().await.unwrap();
        assert_eq!(c.sequence, 0x01020304);
        assert_eq!(c.zones, [0x0102, 0x0304, 0x0506, 0x0708, 0x090A, 0x0B0C]);
        assert!(buf.is_empty());
    }
}
