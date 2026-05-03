//! Sensor subsystem USART framing — same CRC/framing as Frozen ([`crate::frozen_frame::encode_command`]).
//!
//! Vibration follows opensleep ([`src/sensor/command.rs`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/command.rs)):
//! the stock bridge enables vibration and configures the piezo **before** `SetAlarm` (see
//! [`sensor::manager`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)
//! `CommandScheduler`). Firmware mode uses **115200** baud (opensleep `FIRMWARE_BAUD`), not bootloader `38400`.
//! SPDX-License-Identifier: GPL-3.0-only

use crate::frozen_frame::{encode_command, BedSide};

/// Default piezo gain per channel (opensleep `sensor::state::PIEZO_GAIN`).
pub const PIEZO_GAIN_DEFAULT: u16 = 400;
/// Default piezo frequency Hz (opensleep `sensor::state::PIEZO_FREQ`).
pub const PIEZO_FREQ_DEFAULT: u32 = 1000;

/// `SensorCommand::Ping` (same opcode `0x01` as Frozen ping).
pub fn ping_frame() -> Vec<u8> {
    encode_command(&[0x01])
}

/// Piezo enable priming without alarm — opensleep sends these before `SetAlarm` works.
pub fn piezo_priming_frames() -> Vec<Vec<u8>> {
    vec![
        enable_vibration_frame(),
        set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT),
        set_piezo_freq_frame(PIEZO_FREQ_DEFAULT),
        enable_piezo_frame(),
    ]
}

/// Alarm / vibration pattern (opensleep `AlarmPattern`).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlarmPattern {
    Single = 0,
    Double = 1,
}

/// One-shot vibration / alarm on a mattress side (opensleep `SensorCommand::SetAlarm`).
///
/// - `intensity_pct`: 1–100 (opensleep: percentage).
/// - `duration_secs`: duration in seconds (u32 big-endian on the wire).
pub fn set_alarm_frame(
    side: BedSide,
    intensity_pct: u8,
    pattern: AlarmPattern,
    duration_secs: u32,
) -> Vec<u8> {
    encode_command(&[
        0x2C,
        side as u8,
        intensity_pct,
        pattern as u8,
        (duration_secs >> 24) as u8,
        (duration_secs >> 16) as u8,
        (duration_secs >> 8) as u8,
        duration_secs as u8,
    ])
}

/// Opensleep [`get_alarm_cmd`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)
/// stops an alarm with **intensity 0**, **duration 0**, **`Double`** — sent before a real alarm to reset MCU state on some beds.
pub fn cancel_alarm_frame(side: BedSide) -> Vec<u8> {
    set_alarm_frame(side, 0, AlarmPattern::Double, 0)
}

/// `SensorCommand::EnableVibration` — MCU often ignores `set_alarm_frame` until this runs (opensleep).
pub fn enable_vibration_frame() -> Vec<u8> {
    encode_command(&[0x2E])
}

pub fn set_piezo_gain_frame(gain_a: u16, gain_b: u16) -> Vec<u8> {
    encode_command(&[
        0x2B,
        (gain_a >> 8) as u8,
        gain_a as u8,
        (gain_b >> 8) as u8,
        gain_b as u8,
    ])
}

pub fn set_piezo_freq_frame(freq_hz: u32) -> Vec<u8> {
    encode_command(&[
        0x21,
        (freq_hz >> 24) as u8,
        (freq_hz >> 16) as u8,
        (freq_hz >> 8) as u8,
        freq_hz as u8,
    ])
}

pub fn enable_piezo_frame() -> Vec<u8> {
    encode_command(&[0x28])
}

/// **`cancel_preamble`:** optional opensleep-style alarm cancel (`SetAlarm` zeros), then enable vibration → gain →
/// frequency → enable piezo → real `SetAlarm` (**5** frames if `false`, **6** if `true`).
pub fn vibration_sequence_frames(
    side: BedSide,
    intensity_pct: u8,
    pattern: AlarmPattern,
    duration_secs: u32,
    cancel_preamble: bool,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(6);
    if cancel_preamble {
        out.push(cancel_alarm_frame(side));
    }
    out.extend([
        enable_vibration_frame(),
        set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT),
        set_piezo_freq_frame(PIEZO_FREQ_DEFAULT),
        enable_piezo_frame(),
        set_alarm_frame(side, intensity_pct, pattern, duration_secs),
    ]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_alarm_matches_opensleep_alarm1() {
        let frame = set_alarm_frame(BedSide::Right, 100, AlarmPattern::Single, 20);
        assert_eq!(
            frame,
            vec![0x7E, 0x08, 0x2C, 0x01, 0x64, 0x00, 0x00, 0x00, 0x00, 0x14, 0x38, 0x8B]
        );
    }

    #[test]
    fn set_alarm_matches_opensleep_alarm2() {
        let frame = set_alarm_frame(BedSide::Left, 50, AlarmPattern::Single, 50);
        assert_eq!(
            frame,
            vec![0x7E, 0x08, 0x2C, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x32, 0x39, 0x3B]
        );
    }

    #[test]
    fn set_alarm_matches_opensleep_alarm3() {
        let frame = set_alarm_frame(BedSide::Left, 50, AlarmPattern::Double, 0);
        assert_eq!(
            frame,
            vec![0x7E, 0x08, 0x2C, 0x00, 0x32, 0x01, 0x00, 0x00, 0x00, 0x00, 0x85, 0x7B]
        );
    }

    #[test]
    fn cancel_alarm_matches_opensleep_manager_cancel_shape() {
        assert_eq!(
            cancel_alarm_frame(BedSide::Right),
            set_alarm_frame(BedSide::Right, 0, AlarmPattern::Double, 0)
        );
    }

    #[test]
    fn sensor_aux_commands_match_opensleep_tests() {
        assert_eq!(enable_vibration_frame(), vec![0x7E, 0x01, 0x2E, 0x09, 0x30]);
        assert_eq!(
            set_piezo_gain_frame(400, 400),
            vec![0x7E, 0x05, 0x2B, 0x01, 0x90, 0x01, 0x90, 0xAB, 0x80]
        );
        assert_eq!(
            set_piezo_freq_frame(1000),
            vec![0x7E, 0x05, 0x21, 0x00, 0x00, 0x03, 0xE8, 0x7A, 0x5E]
        );
        assert_eq!(enable_piezo_frame(), vec![0x7E, 0x01, 0x28, 0x69, 0xF6]);
    }
}
