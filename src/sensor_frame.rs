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
/// Default piezo frequency Hz for Pod 3 / opensleep (`sensor::state::PIEZO_FREQ`).
pub const PIEZO_FREQ_POD3: u32 = 1000;
/// Pod 4 / 5 Sensor MCU samples at ~500 Hz (MCU `[sampling] avg rate ≈ 500Hz`).
pub const PIEZO_FREQ_POD4: u32 = 500;
/// Alias kept for callers that want the opensleep default (Pod 3).
pub const PIEZO_FREQ_DEFAULT: u32 = PIEZO_FREQ_POD3;

/// `SensorCommand::Ping` (same opcode `0x01` as Frozen ping).
pub fn ping_frame() -> Vec<u8> {
    encode_command(&[0x01])
}

/// Piezo enable priming without alarm — opensleep order: EnableVibration → SetPiezoGain →
/// EnablePiezo → SetPiezoFreq (freq only after piezo is enabled).
pub fn piezo_priming_frames(freq_hz: u32) -> Vec<Vec<u8>> {
    vec![
        enable_vibration_frame(),
        set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT),
        enable_piezo_frame(),
        set_piezo_freq_frame(freq_hz),
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

/// Native stock tap response: fixed power 30, 125 ms on / 125 ms off per tap.
/// Patterns 2/3/4 have the same waveform; the hub selects the matching gesture
/// code, capped at four taps. Single taps use one cycle of the same waveform.
/// See `docs/cover-button-feedback-analysis.md` for the static evidence.
pub fn tap_feedback_frame(side: BedSide, taps: u8) -> Vec<u8> {
    let count = taps.clamp(1, 4);
    encode_command(&[0x40, side as u8, 30, count.max(2), count])
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

/// **`cancel_preamble`:** optional opensleep-style alarm cancel (`SetAlarm` zeros), then enable
/// vibration → gain → enable piezo → frequency → real `SetAlarm` (**5** frames if `false`, **6** if `true`).
pub fn vibration_sequence_frames(
    side: BedSide,
    intensity_pct: u8,
    pattern: AlarmPattern,
    duration_secs: u32,
    cancel_preamble: bool,
    piezo_freq_hz: u32,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(6);
    if cancel_preamble {
        out.push(cancel_alarm_frame(side));
    }
    out.extend(piezo_priming_frames(piezo_freq_hz));
    out.push(set_alarm_frame(side, intensity_pct, pattern, duration_secs));
    out
}

/// Both mattress sides in **one** batch: optional cancels (left + right) → shared piezo
/// priming → `SetAlarm` left → `SetAlarm` right (**6** frames if `cancel_preamble` is
/// `false`, **8** if `true`). One batch so both sides vibrate at the same time.
pub fn vibration_sequence_frames_both(
    intensity_pct: u8,
    pattern: AlarmPattern,
    duration_secs: u32,
    cancel_preamble: bool,
    piezo_freq_hz: u32,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(8);
    if cancel_preamble {
        out.push(cancel_alarm_frame(BedSide::Left));
        out.push(cancel_alarm_frame(BedSide::Right));
    }
    out.extend(piezo_priming_frames(piezo_freq_hz));
    out.push(set_alarm_frame(
        BedSide::Left,
        intensity_pct,
        pattern,
        duration_secs,
    ));
    out.push(set_alarm_frame(
        BedSide::Right,
        intensity_pct,
        pattern,
        duration_secs,
    ));
    out
}

/// Optional cancel + piezo priming + one native stock tap response. The MCU
/// executes the entire pulse sequence; no host-side pacing or cancel is needed.
/// An armed, sampling link sends only the trailing feedback command.
pub fn cover_button_feedback_frames(
    side: BedSide,
    taps: u8,
    cancel_preamble: bool,
    piezo_freq_hz: u32,
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(usize::from(cancel_preamble) + 5);
    if cancel_preamble {
        out.push(cancel_alarm_frame(side));
    }
    out.extend(piezo_priming_frames(piezo_freq_hz));
    out.push(tap_feedback_frame(side, taps));
    out
}

/// A queued vibration batch for the Sensor UART link (`sensor_tx` channel).
///
/// The link's writer paces known sequence shapes (cancels → priming → alarm(s) with the
/// `0xAE` ack wait); the variant tells it how many cancel/alarm frames the batch carries,
/// because a both-sides batch without cancels (**6** frames) has the same length as a
/// one-side batch with cancel.
#[derive(Debug)]
pub enum VibrationBatch {
    /// One mattress side — opensleep sequence shape (optional cancel + 4 priming + `SetAlarm`).
    Side(Vec<Vec<u8>>),
    /// Both sides at once — optional cancels + 4 priming + `SetAlarm` left + `SetAlarm` right.
    Both(Vec<Vec<u8>>),
    /// Optional cancel + 4 priming + one MCU-paced stock tap response.
    Feedback(Vec<Vec<u8>>),
}

impl VibrationBatch {
    /// Raw frames (fallback path when no Sensor link task owns the UART).
    pub fn frames(&self) -> &[Vec<u8>] {
        match self {
            VibrationBatch::Side(frames)
            | VibrationBatch::Both(frames)
            | VibrationBatch::Feedback(frames) => frames,
        }
    }

    /// Cancel frames at the head of the batch (**0**/**1** for [`VibrationBatch::Side`],
    /// **0**/**2** for [`VibrationBatch::Both`], **0**/**1** for [`VibrationBatch::Feedback`]).
    pub fn cancel_count(&self) -> usize {
        match self {
            VibrationBatch::Side(frames) | VibrationBatch::Feedback(frames) => {
                usize::from(frames.len() == 6)
            }
            VibrationBatch::Both(frames) => usize::from(frames.len() == 8) * 2,
        }
    }

    /// Vibration frames at the tail of the batch (**1** for [`VibrationBatch::Side`], **2** for
    /// [`VibrationBatch::Both`], one native command for [`VibrationBatch::Feedback`]).
    pub fn alarm_count(&self) -> usize {
        match self {
            VibrationBatch::Side(_) | VibrationBatch::Feedback(_) => 1,
            VibrationBatch::Both(_) => 2,
        }
    }

    pub fn is_feedback(&self) -> bool {
        matches!(self, VibrationBatch::Feedback(_))
    }
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

    #[test]
    fn piezo_priming_order_enable_before_freq() {
        let frames = piezo_priming_frames(500);
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0], enable_vibration_frame());
        assert_eq!(
            frames[1],
            set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT)
        );
        assert_eq!(frames[2], enable_piezo_frame());
        assert_eq!(frames[3], set_piezo_freq_frame(500));
    }

    #[test]
    fn vibration_sequence_opensleep_order_with_freq() {
        let frames = vibration_sequence_frames(
            BedSide::Left,
            50,
            AlarmPattern::Single,
            10,
            false,
            PIEZO_FREQ_POD4,
        );
        assert_eq!(frames.len(), 5);
        assert_eq!(frames[2], enable_piezo_frame());
        assert_eq!(frames[3], set_piezo_freq_frame(PIEZO_FREQ_POD4));
        assert_eq!(
            frames[4],
            set_alarm_frame(BedSide::Left, 50, AlarmPattern::Single, 10)
        );
    }

    #[test]
    fn vibration_sequence_both_sets_alarms_on_both_sides() {
        let frames =
            vibration_sequence_frames_both(50, AlarmPattern::Double, 10, true, PIEZO_FREQ_POD4);
        assert_eq!(frames.len(), 8);
        assert_eq!(frames[0], cancel_alarm_frame(BedSide::Left));
        assert_eq!(frames[1], cancel_alarm_frame(BedSide::Right));
        assert_eq!(frames[2], enable_vibration_frame());
        assert_eq!(frames[5], set_piezo_freq_frame(PIEZO_FREQ_POD4));
        assert_eq!(
            frames[6],
            set_alarm_frame(BedSide::Left, 50, AlarmPattern::Double, 10)
        );
        assert_eq!(
            frames[7],
            set_alarm_frame(BedSide::Right, 50, AlarmPattern::Double, 10)
        );

        let no_cancel =
            vibration_sequence_frames_both(50, AlarmPattern::Single, 10, false, PIEZO_FREQ_POD4);
        assert_eq!(no_cancel.len(), 6);
        assert_eq!(no_cancel[0], enable_vibration_frame());
        assert_eq!(
            no_cancel[5],
            set_alarm_frame(BedSide::Right, 50, AlarmPattern::Single, 10)
        );

        let side = VibrationBatch::Side(vibration_sequence_frames(
            BedSide::Left,
            50,
            AlarmPattern::Single,
            10,
            true,
            PIEZO_FREQ_POD4,
        ));
        assert_eq!(side.cancel_count(), 1);
        assert_eq!(side.alarm_count(), 1);
        assert_eq!(side.frames().len(), 6);
        let both = VibrationBatch::Both(frames);
        assert_eq!(both.cancel_count(), 2);
        assert_eq!(both.alarm_count(), 2);
        assert_eq!(both.frames().len(), 8);
        // Same length, different shape: a both-sides batch without cancels (6)
        // must not be mistaken for a one-side batch with cancel (6).
        let both_no_cancel = VibrationBatch::Both(no_cancel);
        assert_eq!(both_no_cancel.cancel_count(), 0);
        assert_eq!(both_no_cancel.alarm_count(), 2);
    }

    #[test]
    fn native_tap_feedback_matches_stock_gesture_bytes() {
        assert_eq!(
            tap_feedback_frame(BedSide::Left, 2),
            vec![0x7E, 0x05, 0x40, 0x00, 0x1E, 0x02, 0x02, 0xFE, 0xE4]
        );
        assert_eq!(
            tap_feedback_frame(BedSide::Right, 3),
            vec![0x7E, 0x05, 0x40, 0x01, 0x1E, 0x03, 0x03, 0xAB, 0x40]
        );
        assert_eq!(
            &tap_feedback_frame(BedSide::Left, 1)[2..7],
            &[0x40, 0, 30, 2, 1]
        );
        assert_eq!(
            &tap_feedback_frame(BedSide::Left, 4)[2..7],
            &[0x40, 0, 30, 4, 4]
        );
        assert_eq!(
            tap_feedback_frame(BedSide::Left, 5),
            tap_feedback_frame(BedSide::Left, 4)
        );
        assert_eq!(
            tap_feedback_frame(BedSide::Left, 0),
            tap_feedback_frame(BedSide::Left, 1)
        );
    }

    #[test]
    fn cover_button_feedback_frames_shape() {
        // cancel + 4 priming + one MCU-paced native feedback command.
        let frames = cover_button_feedback_frames(BedSide::Right, 2, true, PIEZO_FREQ_POD4);
        assert_eq!(frames.len(), 6);
        assert_eq!(frames[0], cancel_alarm_frame(BedSide::Right));
        assert_eq!(frames[1], enable_vibration_frame());
        assert_eq!(
            frames[2],
            set_piezo_gain_frame(PIEZO_GAIN_DEFAULT, PIEZO_GAIN_DEFAULT)
        );
        assert_eq!(frames[3], enable_piezo_frame());
        assert_eq!(frames[4], set_piezo_freq_frame(PIEZO_FREQ_POD4));
        assert_eq!(frames[5], tap_feedback_frame(BedSide::Right, 2));

        let no_cancel = cover_button_feedback_frames(BedSide::Left, 1, false, PIEZO_FREQ_POD4);
        assert_eq!(no_cancel.len(), 5);
        assert_eq!(no_cancel[0], enable_vibration_frame());
        assert_eq!(no_cancel[4], tap_feedback_frame(BedSide::Left, 1));
    }

    #[test]
    fn feedback_batch_counts_and_bare_flag() {
        let with_cancel = VibrationBatch::Feedback(cover_button_feedback_frames(
            BedSide::Left,
            3,
            true,
            PIEZO_FREQ_POD4,
        ));
        assert_eq!(with_cancel.cancel_count(), 1);
        assert_eq!(with_cancel.alarm_count(), 1);
        assert_eq!(with_cancel.frames().len(), 6);
        assert!(with_cancel.is_feedback());

        let no_cancel = VibrationBatch::Feedback(cover_button_feedback_frames(
            BedSide::Left,
            2,
            false,
            PIEZO_FREQ_POD4,
        ));
        assert_eq!(no_cancel.cancel_count(), 0);
        assert_eq!(no_cancel.alarm_count(), 1);
        assert_eq!(no_cancel.frames().len(), 5);
        assert!(no_cancel.is_feedback());

        let single = VibrationBatch::Feedback(cover_button_feedback_frames(
            BedSide::Right,
            1,
            true,
            PIEZO_FREQ_POD4,
        ));
        assert_eq!(single.cancel_count(), 1);
        assert_eq!(single.alarm_count(), 1);
        assert!(single.is_feedback());

        let side = VibrationBatch::Side(vibration_sequence_frames(
            BedSide::Left,
            50,
            AlarmPattern::Single,
            10,
            true,
            PIEZO_FREQ_POD4,
        ));
        assert!(!side.is_feedback());
        let both = VibrationBatch::Both(vibration_sequence_frames_both(
            50,
            AlarmPattern::Double,
            10,
            true,
            PIEZO_FREQ_POD4,
        ));
        assert!(!both.is_feedback());
    }
}
