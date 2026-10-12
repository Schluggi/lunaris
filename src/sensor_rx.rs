//! Sensor inbound parsing — detects **`VibrationEnabled`** (`0xAE`) like opensleep’s
//! [`SensorPacket::VibrationEnabled`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/packet.rs).
//! The MCU often ignores `SetAlarm` until this ack arrives.
//!
//! Also tracks piezo setup ACKs (`0xAB` PiezoGainSet, `0xA8` PiezoEnabled, `0xA1` PiezoFreqSet)
//! and inbound piezo sample batches (`0x32` opensleep / Pod 3, or `0x35` Pod 4/5 — different
//! layout, see [`parse_piezo35`]) for HR/HRV ([`crate::hrv`]).
//!
//! **Capacitance** (`0x33`) matches opensleep [`SensorPacket::Capacitance`] — six zone values, ordered
//! left-to-right on the mattress — used for MQTT presence / occupancy.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;

use crate::frozen_frame::crc_ccitt;
use crate::sensor_frame::{PIEZO_FREQ_DEFAULT, PIEZO_GAIN_DEFAULT};
use crate::wire_buffer::trim_when_no_sync_byte;

/// opensleep `PIEZO_TOLERANCE` — requested gain vs MCU-reported gain may differ slightly.
const PIEZO_GAIN_TOLERANCE: i16 = 6;

/// If we hold more than this waiting for the rest of a putative frame, drop `0x7E` and resync (false length / truncated stream).
const MAX_INCOMPLETE_HOLD: usize = 4096;
/// MQTT text sensor truncation (MCU lines can be long; broker/HA still have practical limits).
const MAX_SENSOR_MESSAGE_BYTES: usize = 2048;

/// `--sensor-piezo-debug`: how many leading piezo sample payloads (`0x32` / `0x35`) to hex-dump
/// at **WARN** (raw payload, post-deframe, CRC-verified) — to verify the Pod 4/5 frame layout
/// against the opensleep `PiezoData` assumption. `0` = off. Set once at startup from the CLI.
pub static PIEZO_DEBUG_DUMP_LIMIT: AtomicUsize = AtomicUsize::new(0);

fn hex_dump(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

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

/// Piezo sample batch from Sensor **`0x32`** (opensleep `PiezoData`, Pod 3) or **`0x35`**
/// (Pod 4/5 sensor MCU — see [`parse_piezo35`]). Per mattress side the two piezo
/// channels are averaged into `left_samples` / `right_samples`.
/// Feeds [`crate::hrv`] (heart rate / HRV per side).
#[derive(Clone, Debug, PartialEq)]
pub struct SensorPiezoSamples {
    /// Payload opcode: `0x32` (opensleep / Pod 3) or `0x35` (Pod 4/5).
    pub tag: u8,
    pub freq: u32,
    /// `0x32`: opensleep sequence. Pod 5 `0x35`: MCU timestamp in **milliseconds**
    /// (increments by the frame interval) — the bridge derives the effective tick
    /// rate from consecutive deltas.
    pub sequence: u32,
    /// Reported gains `(left, right)` — diagnostic only (opensleep `0x32`: the two
    /// header gains; Pod 5 `0x35`: first header-u16 of each side's gain pair).
    pub gain: (u16, u16),
    pub left_samples: Vec<u16>,
    pub right_samples: Vec<u16>,
    /// Per-raw-channel variance `[c0, c1, c2, c3]` (Pod 5 `0x35` only; all zero
    /// for opensleep `0x32`) — used by `--sensor-piezo-debug` to verify the
    /// channel↔side pairing against real hardware.
    pub channel_variance: [f32; 4],
    /// Per-raw-channel min/max/mean over this frame `[c0, c1, c2, c3]` (Pod 5
    /// `0x35`; `0x32` fills channels 0/1 with left/right, 2/3 default). The
    /// `--sensor-piezo-debug` rolling 10 s window aggregates these — a slow
    /// ~1 Hz BCG wave is invisible per-frame but shows in the 10 s span.
    pub channel_stats: [PiezoChannelStats; 4],
}

/// Per-frame min/max/mean of one raw piezo channel (diagnostics).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PiezoChannelStats {
    pub min: u16,
    pub max: u16,
    pub mean: f32,
}

impl Default for PiezoChannelStats {
    fn default() -> Self {
        Self {
            min: u16::MAX,
            max: 0,
            mean: 0.0,
        }
    }
}

impl PiezoChannelStats {
    /// Stats over one channel's sample series.
    fn of_series(samples: &[u16]) -> Self {
        let mut min = u16::MAX;
        let mut max = 0u16;
        let mut sum = 0f64;
        for &s in samples {
            min = min.min(s);
            max = max.max(s);
            sum += f64::from(s);
        }
        Self {
            min,
            max,
            mean: if samples.is_empty() {
                0.0
            } else {
                (sum / samples.len() as f64) as f32
            },
        }
    }
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

fn send_piezo_update(tx: Option<&mpsc::Sender<SensorPiezoSamples>>, update: SensorPiezoSamples) {
    let Some(tx) = tx else {
        return;
    };
    if let Err(e) = tx.try_send(update) {
        tracing::trace!(?e, "Sensor piezo update dropped (channel closed or full)");
    }
}

/// Shared MCU piezo / vibration setup state for [`crate::sensor_link`] (opensleep-style
/// CommandScheduler: send each config command only until ACK’d / stream confirms).
#[derive(Debug)]
pub struct PiezoSetupState {
    /// Target sample rate requested via `SetPiezoFreq` (Pod 3: 1000 Hz; Pod 4/5: 500 Hz).
    target_freq_hz: u32,
    vibration_enabled: AtomicBool,
    piezo_enabled: AtomicBool,
    piezo_gain_ok: AtomicBool,
    piezo_freq_ok: AtomicBool,
    /// Wall-clock of the latest CRC-valid piezo sample batch (for stream-stall recovery).
    last_piezo_sample: Mutex<Option<Instant>>,
    first_piezo_logged: AtomicBool,
}

impl PiezoSetupState {
    #[must_use]
    pub fn new(target_freq_hz: u32) -> Self {
        Self {
            target_freq_hz: if target_freq_hz > 0 {
                target_freq_hz
            } else {
                PIEZO_FREQ_DEFAULT
            },
            vibration_enabled: AtomicBool::new(false),
            piezo_enabled: AtomicBool::new(false),
            piezo_gain_ok: AtomicBool::new(false),
            piezo_freq_ok: AtomicBool::new(false),
            last_piezo_sample: Mutex::new(None),
            first_piezo_logged: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn new_arc(target_freq_hz: u32) -> Arc<Self> {
        Arc::new(Self::new(target_freq_hz))
    }

    #[must_use]
    pub fn target_freq_hz(&self) -> u32 {
        self.target_freq_hz
    }

    #[must_use]
    pub fn vibration_enabled(&self) -> bool {
        self.vibration_enabled.load(Ordering::SeqCst)
    }

    pub fn set_vibration_enabled(&self, enabled: bool) {
        self.vibration_enabled.store(enabled, Ordering::SeqCst);
    }

    #[must_use]
    pub fn piezo_enabled(&self) -> bool {
        self.piezo_enabled.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn piezo_gain_ok(&self) -> bool {
        self.piezo_gain_ok.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn piezo_freq_ok(&self) -> bool {
        self.piezo_freq_ok.load(Ordering::SeqCst)
    }

    /// opensleep `SensorState::piezo_ok`: enabled + gain within tolerance + freq matches target.
    #[must_use]
    pub fn piezo_ok(&self) -> bool {
        self.piezo_enabled() && self.piezo_gain_ok() && self.piezo_freq_ok()
    }

    /// Age of the latest piezo sample batch, if any have been seen this session.
    #[must_use]
    pub fn last_piezo_age(&self) -> Option<std::time::Duration> {
        self.last_piezo_sample
            .lock()
            .ok()
            .and_then(|g| g.map(|t| t.elapsed()))
    }

    /// Clear piezo stream flags so the link re-sends EnablePiezo / SetPiezoFreq after a stall.
    pub fn request_stream_recovery(&self) {
        self.piezo_enabled.store(false, Ordering::SeqCst);
        self.piezo_freq_ok.store(false, Ordering::SeqCst);
        if let Ok(mut g) = self.last_piezo_sample.lock() {
            *g = None;
        }
    }

    /// Treat the piezo config as complete because a sample stream is already flowing
    /// (the MCU retained it from a previous session). Pending EnableVibration / gain /
    /// EnablePiezo / SetPiezoFreq commands are skipped — re-configuring a *sampling*
    /// MCU wedged it once (Pod 5: no acks, stream died ~45 s later, only a power cycle
    /// revived it).
    pub fn mark_stream_configured(&self) {
        self.vibration_enabled.store(true, Ordering::SeqCst);
        self.piezo_enabled.store(true, Ordering::SeqCst);
        self.piezo_gain_ok.store(true, Ordering::SeqCst);
        self.piezo_freq_ok.store(true, Ordering::SeqCst);
    }

    fn note_vibration_enabled(&self) {
        if !self.vibration_enabled.swap(true, Ordering::SeqCst) {
            tracing::info!("Sensor: vibration armed (MCU ack 0xAE VibrationEnabled)");
        }
    }

    fn note_piezo_gain(&self, left: u16, right: u16) {
        let ok = (PIEZO_GAIN_DEFAULT as i16 - left as i16).abs() < PIEZO_GAIN_TOLERANCE
            && (PIEZO_GAIN_DEFAULT as i16 - right as i16).abs() < PIEZO_GAIN_TOLERANCE;
        if ok {
            if !self.piezo_gain_ok.swap(true, Ordering::SeqCst) {
                tracing::info!(
                    left,
                    right,
                    "Sensor: piezo gain OK (MCU ack 0xAB PiezoGainSet)"
                );
            }
        } else {
            tracing::debug!(
                left,
                right,
                want = PIEZO_GAIN_DEFAULT,
                "Sensor: piezo gain ack outside tolerance"
            );
        }
    }

    fn note_piezo_enabled_ack(&self) {
        if !self.piezo_enabled.swap(true, Ordering::SeqCst) {
            tracing::info!("Sensor: piezo enabled (MCU ack 0xA8 PiezoEnabled)");
        }
    }

    fn note_piezo_freq_ack(&self) {
        // opensleep prefers stream confirmation; treating `0xA1` as freq OK stops SetPiezoFreq
        // spam when samples arrive under a different tag (Pod 4/5 `0x35`) or after a stall.
        if !self.piezo_freq_ok.swap(true, Ordering::SeqCst) {
            tracing::info!("Sensor: piezo freq OK (MCU ack 0xA1 PiezoFreqSet)");
        }
    }

    pub(crate) fn note_piezo_samples(&self, samples: &SensorPiezoSamples) {
        if let Ok(mut g) = self.last_piezo_sample.lock() {
            *g = Some(Instant::now());
        }
        self.piezo_enabled.store(true, Ordering::SeqCst);
        let (l, r) = samples.gain;
        let gain_ok = (PIEZO_GAIN_DEFAULT as i16 - l as i16).abs() < PIEZO_GAIN_TOLERANCE
            && (PIEZO_GAIN_DEFAULT as i16 - r as i16).abs() < PIEZO_GAIN_TOLERANCE;
        if gain_ok {
            self.piezo_gain_ok.store(true, Ordering::SeqCst);
        }
        // Exact target preferred; any live piezo stream also completes setup so we stop
        // re-sending SetPiezoFreq (Pod 4 may report a configured rate that differs slightly).
        if samples.freq == self.target_freq_hz || samples.freq > 0 || samples.tag == 0x35 {
            self.piezo_freq_ok.store(true, Ordering::SeqCst);
        }
        if !self.first_piezo_logged.swap(true, Ordering::SeqCst) {
            tracing::info!(
                tag = format_args!("0x{:02x}", samples.tag),
                seq = samples.sequence,
                freq = samples.freq,
                n = samples.left_samples.len(),
                gain_l = l,
                gain_r = r,
                "Sensor: first piezo sample batch (stream alive)"
            );
        }
    }
}

/// What [`drain_inbound`] decoded in one call — used by [`crate::sensor_link`] to confirm
/// the MCU is alive (any valid frame), to read the pong mode byte (bootloader vs firmware),
/// and to see the `JumpingToFirmware` (`0x90`) confirmation at bootloader baud.
///
/// opensleep `parse_pong`: `0x81` payload `81 00 42` = bootloader, `81 00 46` = firmware.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainEvents {
    /// CRC-valid frames decoded in this call.
    pub valid_frames: usize,
    /// Latest `0x81` pong mode byte: `Some(true)` = firmware (`0x46`), `Some(false)` = bootloader (`0x42`).
    pub pong_mode: Option<bool>,
    /// `0x90` JumpingToFirmware seen (MCU confirmed the jump — sent at bootloader baud).
    pub jump_to_firmware: bool,
}

impl DrainEvents {
    pub fn merge(&mut self, other: DrainEvents) {
        self.valid_frames += other.valid_frames;
        if other.pong_mode.is_some() {
            self.pong_mode = other.pong_mode;
        }
        self.jump_to_firmware |= other.jump_to_firmware;
    }
}

/// Deframes and handles buffered inbound bytes. Returns the [`DrainEvents`] decoded in this
/// call — used by [`crate::sensor_link`] to confirm the MCU is alive (discovery / RX health
/// tracking) and to follow the bootloader→firmware mode switch.
pub fn drain_inbound(
    buffer: &mut Vec<u8>,
    piezo_setup: Option<&PiezoSetupState>,
    capacitance_tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
    capacitance_diag: Option<&PresenceCapDiag>,
    sensor_message_tx: Option<&mpsc::Sender<String>>,
    piezo_tx: Option<&mpsc::Sender<SensorPiezoSamples>>,
) -> DrainEvents {
    let mut events = DrainEvents::default();
    loop {
        let start = match buffer.iter().position(|&b| b == 0x7E) {
            Some(i) => i,
            None => {
                trim_when_no_sync_byte(buffer);
                return events;
            }
        };
        if start > 0 {
            buffer.drain(..start);
        }
        if buffer.len() < 4 {
            return events;
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
            return events;
        }
        let payload = &buffer[2..2 + plen];
        let crc_rx = u16::from_be_bytes([buffer[2 + plen], buffer[2 + plen + 1]]);
        if crc_ccitt(payload) != crc_rx {
            buffer.remove(0);
            continue;
        }
        events.valid_frames += 1;
        events.merge(handle_payload(
            payload,
            piezo_setup,
            capacitance_tx,
            capacitance_diag,
            sensor_message_tx,
            piezo_tx,
        ));
        buffer.drain(..frame_len);
    }
}

/// opensleep `SensorPacket::parse_piezo`: 14-byte header (`tag`, `0x02`, freq `u32` BE,
/// sequence `u32` BE, two gains `u16` BE) + interleaved left/right `u16` BE samples
/// (4 bytes per sample). Pod 3 / opensleep layout — unchanged.
fn parse_piezo32_opensleep(payload: &[u8]) -> Option<SensorPiezoSamples> {
    let tag = *payload.first()?;
    if payload.len() < 18 || tag != 0x32 {
        return None;
    }
    if payload[1] != 0x02 {
        tracing::trace!(
            tag = format_args!("0x{tag:02x}"),
            byte = payload[1],
            "Sensor: piezo header byte != 0x02"
        );
    }
    let freq = u32::from_be_bytes([payload[2], payload[3], payload[4], payload[5]]);
    let sequence = u32::from_be_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let gain = (
        u16::from_be_bytes([payload[10], payload[11]]),
        u16::from_be_bytes([payload[12], payload[13]]),
    );
    let num_samples = (payload.len() - 14) / 4;
    let mut left_samples = Vec::with_capacity(num_samples);
    let mut right_samples = Vec::with_capacity(num_samples);
    for i in 0..num_samples {
        let idx = 14 + i * 4;
        left_samples.push(u16::from_be_bytes([payload[idx], payload[idx + 1]]));
        right_samples.push(u16::from_be_bytes([payload[idx + 2], payload[idx + 3]]));
    }
    let channel_stats = [
        PiezoChannelStats::of_series(&left_samples),
        PiezoChannelStats::of_series(&right_samples),
        PiezoChannelStats::default(),
        PiezoChannelStats::default(),
    ];
    Some(SensorPiezoSamples {
        tag,
        freq,
        sequence,
        gain,
        left_samples,
        right_samples,
        channel_variance: [0.0; 4],
        channel_stats,
    })
}

/// Pod 4/5 sensor-MCU piezo sample frame, tag `0x35` — layout verified from real wire
/// dumps (Pod 5, sensor MCU FW v5.1.11.0, `--sensor-piezo-debug`, all frames `len=192`):
///
/// - **16-byte header**: `tag` u8 (`0x35`), marker u8 (`0x02`), four `u16` BE fields
///   (zero before `SetPiezoGain` — likely the per-channel gains), one `u16` BE
///   (constant `200` observed — unidentified), and a `u32` BE timestamp at offset 12
///   in **milliseconds since MCU boot** (increments by the frame interval and matches
///   the MCU's own `FW: <ms>` log clock: 1446 → 1500 → 1555 for three consecutive
///   frames 55 ms apart).
/// - **Sample groups**: every 16 bytes = **four `u32` BE channels** `[c0, c1, c2, c3]`
///   (one tick). The hub firmware buffers these as `piezo-dual` lanes and uploads
///   `left1`/`left2`/`right1`/`right2` arrays; on the observed Pod 5 only `c0`/`c1`
///   carry signal (idle ≈ 32720/32752 — the hub `piezo-dual` chunks are 32-bit), and
///   `c2`/`c3` are constantly 0 (unpopulated).
///
/// This is **not** the opensleep Pod 3 `PiezoData` layout (14-byte header, two
/// interleaved `u16` channels) — parsing `0x35` with it misaligns the stream (off by
/// 3 bytes, wrong widths) and makes the heart-rate detector lock onto frame-periodic
/// artifacts (bpm pinned at exact multiples of the frame period, RMSSD ≡ 0).
///
/// Side pairing is **wire-verified via settled DC shifts** (Pod 5: lying on the left
/// lifts `c1` by ~+27 LSB, lying on the right lowers `c0` by ~−40 LSB, empty bed
/// baseline unchanged; `c2`/`c3` constantly 0): **left pair = `(c1, c3)`, right pair =
/// `(c0, c2)`**. Both partners are averaged when both are live; when one partner is
/// dead (all-zero across the frame — the case on observed Pod 5 hardware) the live
/// channel is fed at **full scale**, because the BCG wave is only ±1–2 LSB and
/// halving it would push it below the detector's resolution. With only `c0`/`c1`
/// wired this gives `left = c1`, `right = c0`; `--sensor-piezo-swap-lr` and the
/// single-side auto-routing in `mqtt_bridge` cover a swapped wiring
/// (`--sensor-piezo-debug` logs per-channel rolling windows to verify — the
/// per-frame variance cannot show a slow ~1 Hz BCG wave, the 10 s min/max/mean
/// window can).
///
/// The frame carries **no usable sample-rate field**. The hub firmware defaults to
/// **200 Hz** (5 ms per tick — its own fallback when its freq field reads 0), and the
/// wire confirms it: 11 ticks per ~55 ms frame = 200 Hz. Feeding the tracker the
/// 500 Hz `SetPiezoFreq` target instead would inflate every bpm by 500/200 = 2.5×.
/// [`SensorPiezoSamples::freq`] is therefore always 0 for `0x35`; the MQTT bridge
/// derives the effective tick rate from consecutive [`SensorPiezoSamples::sequence`]
/// (ms) deltas and falls back to the SetPiezoFreq target until the first derivation
/// lands.
const PIEZO35_HEADER_LEN: usize = 16;

/// Raw channel value of piezo35 group `g` (each channel is one u32 BE).
fn piezo35_channel(payload: &[u8], g: usize, ch: usize) -> u32 {
    let base = PIEZO35_HEADER_LEN + g * 16 + ch * 4;
    u32::from_be_bytes([
        payload[base],
        payload[base + 1],
        payload[base + 2],
        payload[base + 3],
    ])
}

fn parse_piezo35(payload: &[u8]) -> Option<SensorPiezoSamples> {
    if payload.len() < PIEZO35_HEADER_LEN + 16 || payload.first() != Some(&0x35) {
        return None;
    }
    if payload[1] != 0x02 {
        let tag = payload[0];
        tracing::trace!(
            tag = format_args!("0x{tag:02x}"),
            byte = payload[1],
            "Sensor: piezo35 marker byte != 0x02"
        );
    }
    let gains = [
        u16::from_be_bytes([payload[2], payload[3]]),
        u16::from_be_bytes([payload[4], payload[5]]),
        u16::from_be_bytes([payload[6], payload[7]]),
        u16::from_be_bytes([payload[8], payload[9]]),
    ];
    let sequence = u32::from_be_bytes([payload[12], payload[13], payload[14], payload[15]]);
    let num_groups = (payload.len() - PIEZO35_HEADER_LEN) / 16;
    // Dead-partner handling: on observed Pod 5 hardware c2/c3 read constantly 0.
    // Averaging with a dead partner halves the live channel's amplitude and pushes
    // a ±1–2 LSB BCG wave below the heart-rate detector's resolution — feed the
    // live channel at full scale when its partner is dead across the whole frame.
    let c2_live = (0..num_groups).any(|g| piezo35_channel(payload, g, 2) != 0);
    let c3_live = (0..num_groups).any(|g| piezo35_channel(payload, g, 3) != 0);
    let mut left_samples = Vec::with_capacity(num_groups);
    let mut right_samples = Vec::with_capacity(num_groups);
    let mut sum = [0f64; 4];
    let mut sum_sq = [0f64; 4];
    let mut ch_min = [u16::MAX; 4];
    let mut ch_max = [0u16; 4];
    for g in 0..num_groups {
        let ch = [
            piezo35_channel(payload, g, 0),
            piezo35_channel(payload, g, 1),
            piezo35_channel(payload, g, 2),
            piezo35_channel(payload, g, 3),
        ];
        for (i, &c) in ch.iter().enumerate() {
            let c = f64::from(c);
            sum[i] += c;
            sum_sq[i] += c * c;
            ch_min[i] = ch_min[i].min(c as u16);
            ch_max[i] = ch_max[i].max(c as u16);
        }
        // Wire-verified pairing (Pod 5, settled DC shifts: lying left lifts c1 by
        // ~+27, lying right lowers c0 by ~−40; c2/c3 constantly 0):
        // left = c1 (+ c3), right = c0 (+ c2) — averaged only when both partners live.
        let left = if c3_live { (ch[1] + ch[3]) / 2 } else { ch[1] };
        let right = if c2_live { (ch[0] + ch[2]) / 2 } else { ch[0] };
        left_samples.push(u16::try_from(left).unwrap_or(u16::MAX));
        right_samples.push(u16::try_from(right).unwrap_or(u16::MAX));
    }
    let n = num_groups as f64;
    let mut channel_variance = [0f32; 4];
    let mut channel_stats = [PiezoChannelStats::default(); 4];
    for (i, v) in channel_variance.iter_mut().enumerate() {
        let mean = sum[i] / n;
        *v = (sum_sq[i] / n - mean * mean).max(0.0) as f32;
        channel_stats[i] = PiezoChannelStats {
            min: ch_min[i],
            max: ch_max[i],
            mean: (sum[i] / n) as f32,
        };
    }
    Some(SensorPiezoSamples {
        tag: 0x35,
        freq: 0,
        sequence,
        gain: (gains[1], gains[0]),
        left_samples,
        right_samples,
        channel_variance,
        channel_stats,
    })
}

/// Dispatch piezo sample payloads: `0x32` = opensleep Pod 3 layout,
/// `0x35` = Pod 4/5 four-channel layout (see [`parse_piezo35`]).
fn parse_piezo(payload: &[u8]) -> Option<SensorPiezoSamples> {
    match payload.first() {
        Some(0x32) => parse_piezo32_opensleep(payload),
        Some(0x35) => parse_piezo35(payload),
        _ => None,
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

/// Handles one CRC-valid payload. Returns the [`DrainEvents`] this packet contributes
/// (pong mode byte / jump confirmation); `valid_frames` is counted by [`drain_inbound`].
fn handle_payload(
    payload: &[u8],
    piezo_setup: Option<&PiezoSetupState>,
    capacitance_tx: Option<&mpsc::Sender<SensorCapacitanceZones>>,
    capacitance_diag: Option<&PresenceCapDiag>,
    sensor_message_tx: Option<&mpsc::Sender<String>>,
    piezo_tx: Option<&mpsc::Sender<SensorPiezoSamples>>,
) -> DrainEvents {
    if payload.is_empty() {
        return DrainEvents::default();
    }
    match payload[0] {
        // Pod 4 can send a **2-byte** payload (`0xAE` + 1 data byte, length `0x02`); opensleep
        // tests use 3 bytes — accept any well-CRC’d payload with at least the command byte.
        0xAE if !payload.is_empty() => {
            if let Some(s) = piezo_setup {
                s.note_vibration_enabled();
            }
            DrainEvents::default()
        }
        // opensleep `PiezoGainSet` (`0xAB`): `AB 00` + two `u16` BE gains.
        0xAB if payload.len() >= 6 => {
            let left = u16::from_be_bytes([payload[2], payload[3]]);
            let right = u16::from_be_bytes([payload[4], payload[5]]);
            if let Some(s) = piezo_setup {
                s.note_piezo_gain(left, right);
            }
            DrainEvents::default()
        }
        // opensleep `PiezoEnabled` (`0xA8`): `A8` + status byte.
        0xA8 if payload.len() >= 2 => {
            if let Some(s) = piezo_setup {
                s.note_piezo_enabled_ack();
            }
            DrainEvents::default()
        }
        // opensleep `PiezoFreqSet` (`0xA1`): `A1` + status byte — marks freq OK (stream still preferred).
        0xA1 if payload.len() >= 2 => {
            if let Some(s) = piezo_setup {
                s.note_piezo_freq_ack();
            }
            DrainEvents::default()
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
            DrainEvents::default()
        }
        // Opensleep `parse_message`: `0x07`, spacer, UTF-8 body (MCU explains alarms / errors).
        0x07 if payload.len() >= 3 => {
            match std::str::from_utf8(&payload[2..]) {
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
                Err(_) => {
                    tracing::trace!(len = payload.len(), "Sensor: MCU message (0x07) non-UTF8")
                }
            }
            DrainEvents::default()
        }
        // opensleep `parse_pong`: mode byte 0x42 = bootloader, 0x46 = firmware — tells the
        // host which mode the MCU answered in (logged at INFO during connect discovery).
        0x81 if payload.len() >= 3 => {
            let mode = match payload[2] {
                0x46 => Some(true),
                0x42 => Some(false),
                _ => None,
            };
            match mode {
                Some(true) => tracing::info!("Sensor: pong — MCU in firmware"),
                Some(false) => tracing::info!("Sensor: pong — MCU in bootloader"),
                None => tracing::warn!(
                    code = payload[2],
                    "Sensor: pong with unexpected mode byte (opensleep: 0x42 bootloader / 0x46 firmware)"
                ),
            }
            DrainEvents {
                pong_mode: mode,
                ..DrainEvents::default()
            }
        }
        // opensleep `parse_jumping_to_firmware`: the MCU confirms JumpToFirmware at the
        // bootloader baud before switching to firmware baud.
        0x90 if payload.len() >= 2 => {
            tracing::info!(
                code = payload[1],
                "Sensor: MCU jumping to firmware (JumpingToFirmware)"
            );
            DrainEvents {
                jump_to_firmware: true,
                ..DrainEvents::default()
            }
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
            DrainEvents::default()
        }
        0x32 | 0x35 => {
            let dumps_left = PIEZO_DEBUG_DUMP_LIMIT.load(Ordering::Relaxed);
            let dump = dumps_left > 0;
            if dump {
                PIEZO_DEBUG_DUMP_LIMIT.store(dumps_left - 1, Ordering::Relaxed);
            }
            if let Some(p) = parse_piezo(payload) {
                if dump {
                    tracing::warn!(
                        len = payload.len(),
                        hex = %hex_dump(payload),
                        ch_var = ?p.channel_variance,
                        "Sensor: piezo payload dump (--sensor-piezo-debug) — 0x35: 16-byte header (tag, marker 0x02, four u16 fields, u16, ts u32 BE ms @12) + 16-byte groups of 4x u32 BE channels [c0, c1, c2, c3] (wire-verified DC pairing: left=(c1,c3), right=(c0,c2)); ch_var = per-frame variance — slow BCG waves only show in the 10s rolling window log"
                    );
                }
                tracing::trace!(
                    tag = format_args!("0x{:02x}", p.tag),
                    seq = p.sequence,
                    freq = p.freq,
                    n = p.left_samples.len(),
                    "Sensor: piezo samples"
                );
                if let Some(s) = piezo_setup {
                    s.note_piezo_samples(&p);
                }
                send_piezo_update(piezo_tx, p);
            } else {
                tracing::trace!(
                    len = payload.len(),
                    tag = format_args!("0x{:02x}", payload[0]),
                    "Sensor: piezo parse skip"
                );
            }
            DrainEvents::default()
        }
        _ => {
            tracing::trace!(len = payload.len(), head = payload[0], "Sensor RX");
            DrainEvents::default()
        }
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

    fn framed(payload: &[u8]) -> Vec<u8> {
        let crc = crate::frozen_frame::crc_ccitt(payload);
        let mut frame = vec![0x7E, payload.len() as u8];
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&crc.to_be_bytes());
        frame
    }

    #[test]
    fn vibration_enabled_sets_atomic() {
        let setup = PiezoSetupState::new(1000);
        let mut buf = framed(&[0xAEu8, 0, 2]);
        assert_eq!(
            drain_inbound(&mut buf, Some(&setup), None, None, None, None).valid_frames,
            1
        );
        assert!(setup.vibration_enabled());
        assert!(buf.is_empty());
    }

    #[test]
    fn piezo_gain_enabled_freq_acks_update_setup_state() {
        let setup = PiezoSetupState::new(500);
        // opensleep PiezoGainSet example shape: AB 00 + two u16 BE (405,405) within tolerance of 400
        let mut buf = framed(&[0xAB, 0x00, 0x01, 0x95, 0x01, 0x95]);
        drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert!(setup.piezo_gain_ok());

        let mut buf = framed(&[0xA8, 0x00]);
        drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert!(setup.piezo_enabled());

        let mut buf = framed(&[0xA1, 0x00]);
        drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        // `0xA1` marks freq OK so SetPiezoFreq stops; stream still preferred for stall recovery.
        assert!(setup.piezo_freq_ok());
        assert!(setup.piezo_ok());
    }

    #[tokio::test]
    async fn piezo_samples_complete_setup_and_mark_stream() {
        let setup = PiezoSetupState::new(500);
        let mut inner = Vec::from([
            0x32u8, 0x02, 0x00, 0x00, 0x01, 0xF4, 0x00, 0x00, 0x00, 0x01, 0x01, 0x90, 0x01, 0x90,
        ]);
        inner.extend_from_slice(&[0x00, 0x0A, 0x00, 0x14]);
        let mut buf = framed(&inner);
        let (tx, mut rx) = mpsc::channel::<SensorPiezoSamples>(4);
        drain_inbound(&mut buf, Some(&setup), None, None, None, Some(&tx));
        let p = rx.recv().await.unwrap();
        assert_eq!(p.freq, 500);
        assert!(setup.piezo_enabled());
        assert!(setup.piezo_gain_ok());
        assert!(setup.piezo_freq_ok());
        assert!(setup.piezo_ok());
        assert!(setup.last_piezo_age().is_some());
    }

    /// Noise without `0x7E` must not be fully discarded (regression: would break split reads).
    /// Pod 4-style frame from real UART: `7E 02 AE 02 [CRC]` (2-byte payload, not 3).
    #[test]
    fn vibration_enabled_two_byte_payload_matches_pod4_rx() {
        let setup = PiezoSetupState::new(1000);
        let mut buf = vec![0x7Eu8, 0x02, 0xAE, 0x02, 0x9A, 0xF3];
        drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert!(setup.vibration_enabled());
        assert!(buf.is_empty());
    }

    /// Decodes opensleep-style `0x07` text (see opensleep `parse_message`).
    #[test]
    fn mcu_message_parses_from_framed_payload() {
        let setup = PiezoSetupState::new(1000);
        let mut inner = vec![0x07u8, 0x00];
        inner.extend_from_slice(b"FW: alarm [left] off");
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);
        let mut buf = frame;
        drain_inbound(&mut buf, Some(&setup), None, None, None, None);
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

    #[test]
    fn pong_mode_byte_reports_bootloader_and_firmware() {
        let setup = PiezoSetupState::new(1000);
        // opensleep `test_pong`: `81 00 42` = bootloader, `81 00 46` = firmware.
        let mut buf = framed(&[0x81, 0x00, 0x42]);
        let ev = drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert_eq!(ev.valid_frames, 1);
        assert_eq!(ev.pong_mode, Some(false));
        assert!(!ev.jump_to_firmware);

        let mut buf = framed(&[0x81, 0x00, 0x46]);
        let ev = drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert_eq!(ev.valid_frames, 1);
        assert_eq!(ev.pong_mode, Some(true));

        let mut buf = framed(&[0x81, 0x00, 0x01]);
        let ev = drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert_eq!(ev.valid_frames, 1);
        assert_eq!(ev.pong_mode, None);
    }

    #[test]
    fn jumping_to_firmware_ack_sets_event() {
        let setup = PiezoSetupState::new(1000);
        // opensleep `test_jumping_to_firmware`: `90 01`.
        let mut buf = framed(&[0x90, 0x01]);
        let ev = drain_inbound(&mut buf, Some(&setup), None, None, None, None);
        assert_eq!(ev.valid_frames, 1);
        assert!(ev.jump_to_firmware);
        assert_eq!(ev.pong_mode, None);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn mcu_message_try_sends_utf8_body_to_mqtt_bridge_channel() {
        let setup = PiezoSetupState::new(1000);
        let (tx, mut rx) = mpsc::channel::<String>(8);
        let mut inner = vec![0x07u8, 0x00];
        inner.extend_from_slice(b"pump primed");
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);
        let mut buf = frame;
        drain_inbound(&mut buf, Some(&setup), None, None, Some(&tx), None);
        assert!(buf.is_empty());
        assert_eq!(rx.recv().await, Some("pump primed".to_string()));
    }

    #[test]
    fn noise_prefix_then_vibration_enabled_frame() {
        let setup = PiezoSetupState::new(1000);
        let payload = [0xAEu8, 0, 2];
        let crc = crate::frozen_frame::crc_ccitt(&payload);
        let mut frame = vec![0x7E, payload.len() as u8];
        frame.extend_from_slice(&payload);
        frame.push((crc >> 8) as u8);
        frame.push(crc as u8);

        let mut buf = vec![0xE0, 0x1C, 0xE0, 0xFE, 0x00];
        assert_eq!(
            drain_inbound(&mut buf, Some(&setup), None, None, None, None).valid_frames,
            0
        );
        assert!(!setup.vibration_enabled());
        buf.extend_from_slice(&frame);
        assert_eq!(
            drain_inbound(&mut buf, Some(&setup), None, None, None, None).valid_frames,
            1
        );
        assert!(setup.vibration_enabled(), "0xAE frame after leading noise");
    }

    #[tokio::test]
    async fn capacitance_opensleep_example_framed() {
        let setup = PiezoSetupState::new(1000);
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
        drain_inbound(&mut buf, Some(&setup), Some(&tx), None, None, None);
        let c = rx.recv().await.unwrap();
        assert_eq!(c.sequence, 0x01020304);
        assert_eq!(c.zones, [0x0102, 0x0304, 0x0506, 0x0708, 0x090A, 0x0B0C]);
        assert!(buf.is_empty());
    }

    #[test]
    fn piezo_opensleep_example_payload() {
        // opensleep `sensor/packet.rs` `test_piezo`
        let inner = [
            0x32u8, 0x02, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04,
        ];
        let p = parse_piezo(&inner).expect("opensleep piezo example");
        assert_eq!(p.tag, 0x32);
        assert_eq!(p.freq, 1000);
        assert_eq!(p.sequence, 1);
        assert_eq!(p.gain, (1, 1));
        assert_eq!(p.left_samples, vec![1, 3]);
        assert_eq!(p.right_samples, vec![2, 4]);
    }

    #[test]
    fn piezo_pod5_0x35_dump_shape() {
        // Real Pod 5 wire shape (from --sensor-piezo-debug dumps, MCU FW v5.1.11.0):
        // 16-byte header — tag, marker 0x02, four zero u16 fields (pre-gain), u16 200,
        // ts u32 BE ms — followed by groups of 4x u32 BE channels; c2/c3 unpopulated.
        let mut inner = vec![
            0x35u8, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00,
            0x05, 0xDC, // ts = 1500 ms
        ];
        for g in 0..11u32 {
            // Idle wobble: c0 ≈ 0x7fd0/0x7fd1, c1 ≈ 0x7ff0/0x7ff1, c2 = c3 = 0.
            let c0: u32 = 0x7FD0 + (g % 2);
            let c1: u32 = 0x7FF0 + (g % 2);
            inner.extend_from_slice(&c0.to_be_bytes());
            inner.extend_from_slice(&c1.to_be_bytes());
            inner.extend_from_slice(&[0u8; 8]);
        }
        assert_eq!(inner.len(), 192);
        let p = parse_piezo(&inner).expect("Pod 5 piezo 0x35");
        assert_eq!(p.tag, 0x35);
        // No usable rate field — bridge derives the effective tick rate (200 Hz) itself.
        assert_eq!(p.freq, 0);
        assert_eq!(p.sequence, 1500);
        assert_eq!(p.left_samples.len(), 11);
        // Wire-verified DC pairing with dead partners (c2/c3 = 0): the live channel
        // is fed at FULL scale — left = c1 = 0x7ff0, right = c0 = 0x7fd0 (halving a
        // ±1–2 LSB BCG wave would hide it from the heart-rate detector).
        assert_eq!(p.left_samples[0], 0x7FF0);
        assert_eq!(p.right_samples[0], 0x7FD0);
        // c2/c3 constant zero → zero variance; c0/c1 wobble slightly.
        assert_eq!(p.channel_variance[2], 0.0);
        assert_eq!(p.channel_variance[3], 0.0);
        assert!(p.channel_variance[0] > 0.0);
        assert!(p.channel_variance[1] > 0.0);
    }

    #[test]
    fn piezo35_dead_partner_feeds_live_channel_at_full_scale() {
        // Header: tag, marker, four zero u16 fields, u16 200, ts u32 BE ms.
        let mut inner = vec![
            0x35u8, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00,
            0x05, 0xDC,
        ];
        // One group [c0, c1, c2, c3] — only c0/c1 populated (observed Pod 5: c2/c3
        // constantly 0).
        for c in [0x7FD0u32, 0x7FF0, 0, 0] {
            inner.extend_from_slice(&c.to_be_bytes());
        }
        let p = parse_piezo(&inner).expect("Pod 5 piezo 0x35");
        // Dead partner → live channel at full scale, not halved: the BCG wave is
        // only ±1–2 LSB and halving it would push it below the detector's resolution.
        assert_eq!(p.left_samples, vec![0x7FF0]);
        assert_eq!(p.right_samples, vec![0x7FD0]);
        // Both partners live → averaged (covered by piezo_0x35_framed_delivers_...).
    }

    #[test]
    fn piezo35_both_partners_live_are_averaged() {
        let mut inner = vec![
            0x35u8, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00,
            0x05, 0xDC,
        ];
        // One group [c0, c1, c2, c3] = [100, 200, 300, 400] — all four live.
        for c in [100u32, 200, 300, 400] {
            inner.extend_from_slice(&c.to_be_bytes());
        }
        let p = parse_piezo(&inner).expect("Pod 5 piezo 0x35");
        // left = (c1 + c3)/2, right = (c0 + c2)/2.
        assert_eq!(p.left_samples, vec![300]);
        assert_eq!(p.right_samples, vec![200]);
    }

    #[test]
    fn piezo_rejects_short_and_wrong_tag() {
        assert!(parse_piezo(&[0x32, 0x02, 0x00, 0x00]).is_none());
        assert!(parse_piezo(&[0x35, 0x02, 0x00, 0x00]).is_none());
        assert!(parse_piezo(&[
            0x33, 0x02, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01,
            0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04
        ])
        .is_none());
    }

    #[tokio::test]
    async fn piezo_framed_delivers_left_and_right_to_channel() {
        let setup = PiezoSetupState::new(1000);
        let mut inner = Vec::from([
            0x32u8, 0x02, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x2A, 0x01, 0x90, 0x01, 0x90,
        ]);
        // 3 samples: (L,R) = (10,20), (30,40), (50,60)
        inner.extend_from_slice(&[0x00, 0x0A, 0x00, 0x14]);
        inner.extend_from_slice(&[0x00, 0x1E, 0x00, 0x28]);
        inner.extend_from_slice(&[0x00, 0x32, 0x00, 0x3C]);
        let crc = crate::frozen_frame::crc_ccitt(&inner);
        let mut frame = vec![0x7E, inner.len() as u8];
        frame.extend_from_slice(&inner);
        frame.extend_from_slice(&crc.to_be_bytes());

        let (tx, mut rx) = mpsc::channel::<SensorPiezoSamples>(4);
        let mut buf = frame;
        drain_inbound(&mut buf, Some(&setup), None, None, None, Some(&tx));
        let p = rx.recv().await.unwrap();
        assert_eq!(p.tag, 0x32);
        assert_eq!(p.freq, 1000);
        assert_eq!(p.sequence, 0x2A);
        assert_eq!(p.gain, (400, 400));
        assert_eq!(p.left_samples, vec![10, 30, 50]);
        assert_eq!(p.right_samples, vec![20, 40, 60]);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn piezo_0x35_framed_delivers_and_marks_piezo_ok() {
        let setup = PiezoSetupState::new(500);
        let mut inner = Vec::from([
            0x35u8, 0x02, 0x01, 0x90, 0x01, 0x90, 0x01, 0x90, 0x01,
            0x90, // tag, marker, gains (400 x4)
            0x00, 0xC8, // u16 field
            0x00, 0x00, 0x0B, 0xB8, // ts = 3000 ms
        ]);
        // 2 groups: [c0, c1, c2, c3] = [100, 200, 300, 400] and [140, 240, 340, 440]
        inner.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00, 0x01, 0x2C, 0x00, 0x00,
            0x01, 0x90,
        ]);
        inner.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x8C, 0x00, 0x00, 0x00, 0xF0, 0x00, 0x00, 0x01, 0x54, 0x00, 0x00,
            0x01, 0xB8,
        ]);
        let mut buf = framed(&inner);
        let (tx, mut rx) = mpsc::channel::<SensorPiezoSamples>(4);
        drain_inbound(&mut buf, Some(&setup), None, None, None, Some(&tx));
        let p = rx.recv().await.unwrap();
        assert_eq!(p.tag, 0x35);
        assert_eq!(p.freq, 0);
        assert_eq!(p.sequence, 3000);
        // left = (c1 + c3)/2 = (200+400)/2, right = (c0 + c2)/2 = (100+300)/2.
        assert_eq!(p.left_samples, vec![300, 340]);
        assert_eq!(p.right_samples, vec![200, 240]);
        assert!(setup.piezo_enabled());
        assert!(setup.piezo_gain_ok());
        assert!(setup.piezo_freq_ok());
        assert!(setup.piezo_ok());
        assert!(setup.last_piezo_age().is_some());
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn piezo_debug_dump_limit_counts_down_and_still_parses() {
        let mut inner = Vec::from([
            0x35u8, 0x02, 0x01, 0x90, 0x01, 0x90, 0x01, 0x90, 0x01,
            0x90, // tag, marker, gains (400 x4)
            0x00, 0xC8, // u16 field
            0x00, 0x00, 0x0B, 0xB8, // ts = 3000 ms
        ]);
        inner.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00, 0x01, 0x2C, 0x00, 0x00,
            0x01, 0x90,
        ]);
        let frame = framed(&inner);
        PIEZO_DEBUG_DUMP_LIMIT.store(2, Ordering::SeqCst);
        let (tx, mut rx) = mpsc::channel::<SensorPiezoSamples>(4);
        for _ in 0..3 {
            let mut buf = frame.clone();
            drain_inbound(&mut buf, None, None, None, None, Some(&tx));
            assert!(buf.is_empty());
        }
        // Two dumps counted down, third frame not dumped; all three parsed.
        assert_eq!(PIEZO_DEBUG_DUMP_LIMIT.load(Ordering::SeqCst), 0);
        for _ in 0..3 {
            assert!(rx.recv().await.is_some());
        }
        PIEZO_DEBUG_DUMP_LIMIT.store(0, Ordering::SeqCst);
    }
}
