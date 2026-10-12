//! Heart rate (HR) and heart-rate variability (HRV, **RMSSD**) from Sensor piezo
//! samples (opensleep `SensorPacket::Piezo`, inbound `0x32` frames — see
//! [`crate::sensor_rx`]). The MCU streams left/right piezo channels while the
//! piezo is enabled (`EnablePiezo` `0x28`, sent by [`crate::sensor_link`]
//! priming); this module turns that raw ballistocardiogram stream into beats.
//!
//! Detection is deliberately simple and dependency-free: DC removal over a
//! sliding window, a peak detector above an adaptive threshold
//! (`mean + k·σ` of the window), and a refractory period so one heartbeat
//! yields exactly one beat. Inter-beat intervals (IBI) feed two metrics:
//!
//! - **HR** in beats per minute (`60000 / mean(IBI)`)
//! - **HRV** as **RMSSD** in milliseconds (root mean square of successive
//!   IBI differences — the standard short-term HRV metric)
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::collections::VecDeque;

/// Minimum IBI in milliseconds (≈ 200 bpm) — refractory period between beats.
/// Center frequency of the heart-rate bandpass (Hz) — passband ≈ 0.65–1.95 Hz
/// (39–117 bpm) with two cascaded stages.
const BANDPASS_CENTER_HZ: f64 = 1.3;
/// Moving-average smoothing after the bandpass (s). The bandpassed BCG wave
/// carries several sub-peaks per beat (the I/J/K deflections of the
/// ballistocardiogram plus bandpass ringing, ~0.3–0.5 s apart) — the peak
/// detector would fire on whichever sub-peak wins, jittering the beat-to-beat
/// intervals and inflating the RMSSD far beyond real heart-rate variability
/// (measured: 300–700 ms instead of 20–80 ms). A ~450 ms average merges the
/// sub-peaks into one hump per beat; the ~1 Hz heart wave passes at ~70 % gain
/// while 2–3 Hz sub-peak content drops to ~10–20 %. Reliable range ~40–80 bpm.
const SMOOTHING_SECS: f32 = 0.45;
/// Bandpass Q per stage (two cascaded stages double the rolloff).
const BANDPASS_Q: f64 = 1.0;

/// Minimum inter-beat interval. Real piezo BCG waves show a systolic peak plus a
/// smaller dicrotic wave ~300–400 ms later; hardware logs showed bpm ≈ 2× the true
/// rate (126 vs ~63) with RMSSD ≈ 350 ms — the classic alternating [~300, ~650] ms
/// IBI pattern of the dicrotic peak slipping past a 300 ms refractory. 500 ms blocks
/// the dicrotic second peak and caps the measurable range at 120 bpm.
const MIN_IBI_MS: f32 = 500.0;
/// Maximum plausible IBI in milliseconds (≈ 24 bpm); longer gaps reset the window (missed beats).
const MAX_IBI_MS: f32 = 2500.0;
/// Sliding window (seconds of samples) for DC removal and the adaptive threshold.
const WINDOW_SECS: f32 = 2.0;
/// Peak must exceed `mean + PEAK_SIGMA_K · σ` of the window.
const PEAK_SIGMA_K: f64 = 0.6;
/// A beat completes when the signal falls this far below the pending peak (hysteresis).
/// A beat candidate within this window after the previous beat must reach
/// `DICROTIC_PEAK_RATIO` of the previous beat's peak height. The bandpassed BCG
/// wave carries a smaller dicrotic/secondary wave ~0.4–0.75 s after each
/// systolic peak (hardware: 40–60 % of the systolic height); without the gate it
/// registers as an extra beat ~0.5–0.7 s after the real one, which then also
/// blocks the next systolic peak via the refractory period — inflating RMSSD to
/// 200–450 ms and jittering the bpm. Real tachycardia beats at comparable
/// amplitude still pass; beyond the window the gate does not apply at all.
const DICROTIC_WINDOW_MS: f32 = 750.0;
const DICROTIC_PEAK_RATIO: f64 = 0.75;
/// Number of recent accepted beats whose peak heights form the dicrotic-gate
/// reference (the maximum of them — robust against one underestimated peak).
const DICROTIC_PEAK_REF_BEATS: usize = 4;

/// A beat is confirmed only when the detrended signal falls back below this
/// fraction of σ (hysteresis). Requiring a *relative* drop below the pending peak
/// (the old rule) let noise dips on a wave crest register as beats.
const FALL_SIGMA_K: f64 = 0.25;
/// A pending peak older than this without a clear fall is drift/plateau, not a beat.
const MAX_PEAK_AGE_SECS: f32 = 0.6;
/// How many recent IBIs the reading is computed over.
const MAX_IBIS: usize = 32;
/// Minimum IBIs before a reading is reported.
const MIN_IBIS: usize = 4;
/// Fallback sample rate when a frame reports `freq == 0` (opensleep default is 1000 Hz).
const DEFAULT_FREQ_HZ: u32 = 1000;

/// One HR/HRV reading for a mattress side.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HeartRateReading {
    /// Heart rate in beats per minute.
    pub bpm: f32,
    /// RMSSD heart-rate variability in milliseconds.
    pub rmssd_ms: f32,
}

/// Mean heart rate (bpm) from inter-beat intervals (ms). `None` when empty.
#[must_use]
pub fn mean_bpm(ibis_ms: &[f32]) -> Option<f32> {
    if ibis_ms.is_empty() {
        return None;
    }
    let sum: f32 = ibis_ms.iter().sum();
    Some(60_000.0 / (sum / ibis_ms.len() as f32))
}

/// RMSSD (ms): root mean square of successive IBI differences. `None` with < 2 IBIs.
#[must_use]
pub fn rmssd_ms(ibis_ms: &[f32]) -> Option<f32> {
    if ibis_ms.len() < 2 {
        return None;
    }
    let mut sum_sq = 0.0f32;
    for w in ibis_ms.windows(2) {
        let d = w[1] - w[0];
        sum_sq += d * d;
    }
    Some((sum_sq / (ibis_ms.len() - 1) as f32).sqrt())
}

/// Cascaded two-stage RBJ biquad bandpass isolating the heart band (~0.65–1.95 Hz).
/// The piezo BCG wave is only ±1–2 LSB on Pod 4/5 hardware; without the bandpass the
/// peak detector fires on slow drift/respiration waves and quantization wobble
/// (observed on hardware: bpm ~20–30 % too high, RMSSD 200–500 ms instead of tens
/// of ms). Designed for the tracker's current sample rate.
#[derive(Debug)]
struct Bandpass {
    /// Per-stage coefficients (normalized by a0; b1 = 0).
    b0: [f64; 2],
    b2: [f64; 2],
    a1: [f64; 2],
    a2: [f64; 2],
    /// Per-stage delay lines (stage input / output history).
    x1: [f64; 2],
    x2: [f64; 2],
    y1: [f64; 2],
    y2: [f64; 2],
    /// Sample rate the coefficients were designed for.
    freq_hz: u32,
}

impl Bandpass {
    fn new(freq_hz: u32) -> Self {
        let mut bp = Self {
            b0: [0.0; 2],
            b2: [0.0; 2],
            a1: [0.0; 2],
            a2: [0.0; 2],
            x1: [0.0; 2],
            x2: [0.0; 2],
            y1: [0.0; 2],
            y2: [0.0; 2],
            freq_hz: 0,
        };
        bp.design(freq_hz);
        bp
    }

    fn design(&mut self, freq_hz: u32) {
        let fs = f64::from(freq_hz.max(1));
        let w0 = 2.0 * std::f64::consts::PI * BANDPASS_CENTER_HZ / fs;
        let alpha = w0.sin() / (2.0 * BANDPASS_Q);
        let a0 = 1.0 + alpha;
        // RBJ bandpass (constant 0 dB peak gain), normalized by a0.
        let (b0, b2) = (alpha / a0, -alpha / a0);
        let (a1, a2) = (-2.0 * w0.cos() / a0, (1.0 - alpha) / a0);
        for s in 0..2 {
            self.b0[s] = b0;
            self.b2[s] = b2;
            self.a1[s] = a1;
            self.a2[s] = a2;
        }
        self.freq_hz = freq_hz;
    }

    fn set_freq(&mut self, freq_hz: u32) {
        if freq_hz > 0 && freq_hz != self.freq_hz {
            self.design(freq_hz);
        }
    }

    fn push(&mut self, x: f64) -> f64 {
        let mut v = x;
        for s in 0..2 {
            let y = self.b0[s] * v + self.b2[s] * self.x2[s]
                - self.a1[s] * self.y1[s]
                - self.a2[s] * self.y2[s];
            self.x2[s] = self.x1[s];
            self.x1[s] = v;
            self.y2[s] = self.y1[s];
            self.y1[s] = y;
            v = y;
        }
        v
    }
}

/// Incremental HR/HRV tracker for **one** mattress side (one piezo channel).
#[derive(Debug)]
pub struct HeartRateTracker {
    /// Sample rate from the latest `0x32` frame (`DEFAULT_FREQ_HZ` until known).
    freq_hz: u32,
    /// Heart-band bandpass in front of the window statistics.
    bandpass: Bandpass,
    /// Short moving-average window for post-bandpass smoothing (merges the
    /// sub-peaks of each BCG complex into one hump per beat).
    ma_window: VecDeque<f64>,
    ma_sum: f64,
    /// Bandpass settling time left (s). Samples in this window only settle the
    /// filter: starting from zero state, the DC step at the input makes the
    /// bandpass ring for ~0.5 s with an amplitude of thousands of LSB, which
    /// would otherwise pollute the window statistics (and the beat detection)
    /// for the first ~2 s after the tracker starts seeing samples.
    warmup_secs: f64,
    /// Sliding window of bandpassed samples (for mean/σ). **f64 on purpose**: the piezo
    /// channels sit at a DC of ~32740, so the window sum-of-squares reaches ~4e11 —
    /// with f32 (one ULP = 32768 there) the variance of a ±2 LSB BCG wave (≈ 2)
    /// cancels away entirely and σ reads as ~0, so no beat could ever be detected.
    window: VecDeque<f64>,
    /// Running sum / sum-of-squares of `window` (O(1) stats per sample).
    sum: f64,
    sum_sq: f64,
    /// Total samples pushed (global timestamp base for IBI math).
    sample_idx: u64,
    /// Highest detrended value since the last beat (pending peak) + its sample index.
    pending_peak: Option<(f64, u64)>,
    /// Sample index of the last accepted beat.
    last_beat_idx: Option<u64>,
    /// Peak heights (detrended values) of the last few accepted beats; their
    /// maximum is the reference for the dicrotic gate (`DICROTIC_PEAK_RATIO`).
    recent_peaks: VecDeque<f64>,
    /// Recent inter-beat intervals (ms), oldest first.
    ibis_ms: VecDeque<f32>,
}

impl Default for HeartRateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartRateTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            freq_hz: DEFAULT_FREQ_HZ,
            bandpass: Bandpass::new(DEFAULT_FREQ_HZ),
            ma_window: VecDeque::new(),
            ma_sum: 0.0,
            warmup_secs: f64::from(WINDOW_SECS),
            window: VecDeque::new(),
            sum: 0.0,
            sum_sq: 0.0,
            sample_idx: 0,
            pending_peak: None,
            last_beat_idx: None,
            recent_peaks: VecDeque::new(),
            ibis_ms: VecDeque::with_capacity(MAX_IBIS),
        }
    }

    /// Sample rate reported by the MCU (Hz). `0` keeps the previous/default rate.
    pub fn set_freq(&mut self, freq_hz: u32) {
        if freq_hz > 0 {
            self.freq_hz = freq_hz;
            self.bandpass.set_freq(freq_hz);
        }
    }

    #[must_use]
    pub fn freq_hz(&self) -> u32 {
        self.freq_hz
    }

    /// Total samples ingested (diagnostics).
    #[must_use]
    pub fn samples_seen(&self) -> u64 {
        self.sample_idx
    }

    /// Number of IBIs currently held (diagnostics).
    #[must_use]
    pub fn ibi_count(&self) -> usize {
        self.ibis_ms.len()
    }

    /// Current IBI window in ms (oldest first) — diagnostics.
    #[must_use]
    pub fn ibis_ms(&self) -> Vec<f32> {
        self.ibis_ms.iter().copied().collect()
    }

    /// Detrended view of the sliding window (oldest first), decimated by `step` —
    /// waveform dumps for `--sensor-piezo-debug`.
    #[must_use]
    pub fn detrended_window(&self, step: usize) -> Vec<f32> {
        let n = self.window.len().max(1) as f64;
        let mean = self.sum / n;
        self.window
            .iter()
            .step_by(step.max(1))
            .map(|&x| (x - mean) as f32)
            .collect()
    }

    /// Span (max − min) of the current IBI window in milliseconds; `None` when empty.
    ///
    /// IBIs are quantized to whole samples (`1000 / freq_hz` ms), so a span below one
    /// sample period means every IBI in the window is **identical**. No real heart beats
    /// metronome-exactly over many beats — a constant window means the detector locked
    /// onto a periodic artifact (e.g. per-frame structure of a misparsed piezo stream),
    /// not a pulse. Used by the MQTT bridge as an artifact guard before publishing.
    #[must_use]
    pub fn ibi_span_ms(&self) -> Option<f32> {
        let first = *self.ibis_ms.front()?;
        let mut min = first;
        let mut max = first;
        for &v in self.ibis_ms.iter().skip(1) {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
        Some(max - min)
    }

    /// Clear beat state so the next reading requires a fresh lock (IBIs + pending peak).
    pub fn clear_lock(&mut self) {
        self.ibis_ms.clear();
        self.last_beat_idx = None;
        self.pending_peak = None;
        self.recent_peaks.clear();
    }

    /// Post-bandpass moving average: merges the sub-peaks of each BCG complex
    /// into one hump per beat so beat timing — and the RMSSD — reflects the real
    /// heart-rate variability.
    fn smooth(&mut self, x: f64) -> f64 {
        let len = (self.freq_hz as f32 * SMOOTHING_SECS).max(1.0) as usize;
        self.ma_window.push_back(x);
        self.ma_sum += x;
        while self.ma_window.len() > len {
            let old = self.ma_window.pop_front().unwrap_or(0.0);
            self.ma_sum -= old;
        }
        self.ma_sum / self.ma_window.len() as f64
    }

    /// Feed one batch of raw piezo samples (one `0x32` / `0x35` frame's channel).
    pub fn push_samples(&mut self, samples: &[u16]) {
        if samples.is_empty() {
            return;
        }
        let win_len = (self.freq_hz as f32 * WINDOW_SECS).max(8.0) as usize;
        let min_ibi_samples = (self.freq_hz as f64 * MIN_IBI_MS as f64 / 1000.0).max(1.0) as u64;
        let dicrotic_window_samples =
            (self.freq_hz as f64 * DICROTIC_WINDOW_MS as f64 / 1000.0).max(1.0) as u64;
        let max_peak_age_samples = (self.freq_hz as f64 * MAX_PEAK_AGE_SECS as f64).max(1.0) as u64;
        let max_gap_samples = (self.freq_hz as f64 * MAX_IBI_MS as f64 / 1000.0).max(1.0) as u64;
        for &raw in samples {
            // Heart-band bandpass first: removes slow drift/respiration waves and
            // quantization wobble so the peak detector sees the BCG wave only.
            let xf = self.bandpass.push(f64::from(raw));
            let x = self.smooth(xf);
            if self.warmup_secs > 0.0 {
                // Settle the bandpass (DC-step ringing); no detection yet.
                self.warmup_secs -= 1.0 / f64::from(self.freq_hz.max(1));
                self.sample_idx += 1;
                continue;
            }
            // Sliding-window DC removal + σ.
            self.window.push_back(x);
            self.sum += x;
            self.sum_sq += x * x;
            while self.window.len() > win_len {
                let old = self.window.pop_front().unwrap_or(0.0);
                self.sum -= old;
                self.sum_sq -= old * old;
            }
            let n = self.window.len() as f64;
            let mean = self.sum / n;
            let var = (self.sum_sq / n - mean * mean).max(0.0);
            let sigma = var.sqrt();
            let det = x - mean;
            let threshold = PEAK_SIGMA_K * sigma;

            // Track the highest detrended value since the last beat (pending peak).
            // Noise blips on a flank never win against the real peak because the
            // pending peak only ever moves up; a beat completes on a clear fall.
            match self.pending_peak {
                Some((val, _)) if det > val => self.pending_peak = Some((det, self.sample_idx)),
                None if det > threshold => self.pending_peak = Some((det, self.sample_idx)),
                _ => {}
            }

            if let Some((pval, pidx)) = self.pending_peak {
                let refractory_ok = self
                    .last_beat_idx
                    .is_none_or(|lb| pidx.saturating_sub(lb) >= min_ibi_samples);
                let clearly_fallen = det < FALL_SIGMA_K * sigma;
                let stale = self.sample_idx.saturating_sub(pidx) > max_peak_age_samples;
                // Dicrotic gate: within `DICROTIC_WINDOW_MS` of the previous beat,
                // the candidate must be nearly as tall as that beat — the smaller
                // secondary wave of each BCG complex must not register as a beat.
                let dicrotic_gate = match self.last_beat_idx {
                    Some(lb) if pidx.saturating_sub(lb) <= dicrotic_window_samples => {
                        let peak_ref = self.recent_peaks.iter().copied().fold(0.0f64, f64::max);
                        pval >= DICROTIC_PEAK_RATIO * peak_ref
                    }
                    _ => true,
                };
                if clearly_fallen
                    && pval > threshold
                    && sigma > 1e-3
                    && refractory_ok
                    && dicrotic_gate
                {
                    self.register_beat(pidx);
                    self.recent_peaks.push_back(pval);
                    while self.recent_peaks.len() > DICROTIC_PEAK_REF_BEATS {
                        self.recent_peaks.pop_front();
                    }
                    self.pending_peak = None;
                } else if stale {
                    // Plateau / drift — not a beat.
                    self.pending_peak = None;
                }
            }
            self.sample_idx += 1;
        }
        // No accepted beat for longer than MAX_IBI_MS → drop stale lock (empty bed / leave).
        if let Some(lb) = self.last_beat_idx {
            if self.sample_idx.saturating_sub(lb) > max_gap_samples {
                self.clear_lock();
            }
        }
    }

    fn register_beat(&mut self, beat_idx: u64) {
        if let Some(last) = self.last_beat_idx {
            let ibi_ms = (beat_idx - last) as f32 * 1000.0 / self.freq_hz as f32;
            if (MIN_IBI_MS..=MAX_IBI_MS).contains(&ibi_ms) {
                self.ibis_ms.push_back(ibi_ms);
                while self.ibis_ms.len() > MAX_IBIS {
                    self.ibis_ms.pop_front();
                }
            } else {
                // Gap too long/short — likely a missed or spurious beat: start over.
                self.ibis_ms.clear();
            }
        }
        self.last_beat_idx = Some(beat_idx);
    }

    /// Latest HR/HRV reading, once enough beats have been seen and the lock is fresh.
    #[must_use]
    pub fn reading(&self) -> Option<HeartRateReading> {
        if self.ibis_ms.len() < MIN_IBIS {
            return None;
        }
        // Guard if caller inspects without pushing more samples after a long gap.
        if let Some(lb) = self.last_beat_idx {
            let max_gap_samples =
                (self.freq_hz as f64 * MAX_IBI_MS as f64 / 1000.0).max(1.0) as u64;
            if self.sample_idx.saturating_sub(lb) > max_gap_samples {
                return None;
            }
        }
        let ibis: Vec<f32> = self.ibis_ms.iter().copied().collect();
        Some(HeartRateReading {
            bpm: mean_bpm(&ibis)?,
            rmssd_ms: rmssd_ms(&ibis)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine_samples(freq_hz: u32, beat_hz: f32, secs: f32, noise: f32) -> Vec<u16> {
        let n = (freq_hz as f32 * secs) as usize;
        let mut out = Vec::with_capacity(n);
        // Deterministic pseudo-noise (LCG) so tests are reproducible.
        let mut state: u32 = 0x1234_5678;
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let noise_amp = noise * (state as f32 / u32::MAX as f32 - 0.5) * 2.0;
            let t = i as f32 / freq_hz as f32;
            let v = 32_768.0 + 800.0 * (2.0 * std::f32::consts::PI * beat_hz * t).sin() + noise_amp;
            out.push(v.clamp(0.0, 65_535.0) as u16);
        }
        out
    }

    #[test]
    fn mean_bpm_and_rmssd_helpers() {
        assert_eq!(mean_bpm(&[]), None);
        // 800 ms IBI → 75 bpm.
        assert_eq!(mean_bpm(&[800.0, 800.0, 800.0]), Some(75.0));
        assert_eq!(rmssd_ms(&[800.0]), None);
        // Constant IBIs → RMSSD 0.
        assert_eq!(rmssd_ms(&[800.0, 800.0, 800.0]), Some(0.0));
        // Alternating 700/900 ms → diffs ±200 → RMSSD 200.
        let r = rmssd_ms(&[700.0, 900.0, 700.0, 900.0]).unwrap();
        assert!((r - 200.0).abs() < 1e-3, "rmssd {r}");
    }

    #[test]
    fn detects_heart_rate_from_synthetic_piezo() {
        // 1.2 Hz beat → 72 bpm, clean sine → RMSSD ≈ 0.
        let samples = sine_samples(1000, 1.2, 30.0, 0.0);
        let mut tracker = HeartRateTracker::new();
        tracker.push_samples(&samples);
        let r = tracker.reading().expect("reading after 30 s of 1.2 Hz");
        assert!((r.bpm - 72.0).abs() < 3.0, "bpm {} should be ≈ 72", r.bpm);
        // Clean sine: RMSSD is pure detector jitter (sample quantization), still
        // far below any physiological HRV (tens of ms).
        assert!(
            r.rmssd_ms < 10.0,
            "clean sine rmssd {} should be tiny",
            r.rmssd_ms
        );
    }

    /// Realistic Pod 5 `0x35` stream: ~204 Hz ticks, the BCG wave measured on
    /// hardware is only ±1–2 LSB around a ~32740 DC level (10 s window spans of
    /// 2–5 LSB while lying still; body movement spans 40–118). Fed in 11-sample
    /// frame chunks like the bridge does. The old tests used amplitude 800 — that
    /// is why the detector passed tests but never locked on hardware.
    #[test]
    fn locks_realistic_pod5_bcg_amplitude() {
        let freq = 204u32;
        let beat_hz = 1.05f32; // ≈ 63 bpm
        let dc = 32_740.0f32;
        let amp = 2.0f32;
        let mut state: u32 = 0x2545_F491;
        let n = (freq as f32 * 40.0) as usize;
        let mut samples: Vec<u16> = Vec::with_capacity(n);
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let noise = 0.6 * (state as f32 / u32::MAX as f32 - 0.5) * 2.0;
            let t = i as f32 / freq as f32;
            let v = dc + amp * (2.0 * std::f32::consts::PI * beat_hz * t).sin() + noise;
            samples.push(v.round().clamp(0.0, 65_535.0) as u16);
        }
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(freq);
        for chunk in samples.chunks(11) {
            tracker.push_samples(chunk);
        }
        let r = tracker
            .reading()
            .expect("reading for ±2 LSB BCG at 204 Hz in 11-sample chunks");
        assert!(
            (r.bpm - 63.0).abs() < 4.0,
            "bpm {} should be ≈ 63 for a ±2 LSB BCG",
            r.bpm
        );
    }

    /// BCG-like pulse train: one systolic pulse per beat plus a smaller dicrotic
    /// pulse `dicrotic_delay_ms` later (real piezo waves show both).
    fn bcg_pulse_samples(
        freq: u32,
        beat_hz: f32,
        secs: f32,
        main_amp: f32,
        dicrotic_amp: f32,
        dicrotic_delay_ms: f32,
        noise: f32,
    ) -> Vec<u16> {
        let n = (freq as f32 * secs) as usize;
        let mut out = Vec::with_capacity(n);
        let mut state: u32 = 0x5EED_1234;
        let period = 1.0 / beat_hz;
        let pulse = |t: f32, center_s: f32, amp: f32| {
            let d_ms = (t - center_s) * 1000.0;
            amp * (-d_ms * d_ms / (2.0 * 30.0 * 30.0)).exp()
        };
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let nz = noise * (state as f32 / u32::MAX as f32 - 0.5) * 2.0;
            let t = (i as f32 / freq as f32) % period;
            let v = 32_740.0
                + pulse(t, 0.0, main_amp)
                + pulse(t, dicrotic_delay_ms / 1000.0, dicrotic_amp)
                + nz;
            out.push(v.round().clamp(0.0, 65_535.0) as u16);
        }
        out
    }

    #[test]
    fn dicrotic_second_peak_is_not_counted_twice() {
        // Hardware showed bpm ≈ 2× the true rate (126 vs ~63) with RMSSD ≈ 350 ms
        // — the dicrotic peak ~300 ms after each systolic peak slipped past the old
        // 300 ms refractory. With the 500 ms refractory only the systolic peak counts.
        let samples = bcg_pulse_samples(204, 1.05, 40.0, 1.5, 0.9, 300.0, 0.3);
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(204);
        for chunk in samples.chunks(11) {
            tracker.push_samples(chunk);
        }
        let r = tracker
            .reading()
            .expect("reading for systolic+dicrotic BCG at ~63 bpm");
        assert!(
            (r.bpm - 63.0).abs() < 4.0,
            "bpm {} should be ≈ 63 (dicrotic peak must not double-count)",
            r.bpm
        );
        assert!(
            r.rmssd_ms < 100.0,
            "rmssd {} should be small for a regular pulse train",
            r.rmssd_ms
        );
    }

    /// Realistic Pod 5 quiet-lying signal (from `--sensor-piezo-debug` wave dumps):
    /// a smooth ±1.5 LSB BCG wave at ~1 Hz, a slow ±4 LSB respiration/drift wave at
    /// ~0.35 Hz, and ±0.5 LSB high-frequency wobble — at 204 Hz in 11-sample frame
    /// chunks. Without the heart-band bandpass the detector fired on the drift and
    /// wobble (bpm ~20–30 % too high, RMSSD 200–500 ms instead of tens of ms).
    #[test]
    fn bandpass_yields_true_rate_for_realistic_noisy_bcg() {
        let freq = 204u32;
        let dc = 32_740.0f64;
        let n = (freq as f64 * 40.0) as usize;
        let mut state: u32 = 0xC0FF_EE11;
        let mut samples: Vec<u16> = Vec::with_capacity(n);
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let wobble = 0.5 * (state as f64 / u32::MAX as f64 - 0.5) * 2.0;
            let t = i as f64 / freq as f64;
            let v = dc
                + 1.5 * (2.0 * std::f64::consts::PI * 1.0 * t).sin() // heart, 60 bpm
                + 4.0 * (2.0 * std::f64::consts::PI * 0.35 * t).sin() // respiration/drift
                + wobble;
            samples.push(v.round().clamp(0.0, 65_535.0) as u16);
        }
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(freq);
        for chunk in samples.chunks(11) {
            tracker.push_samples(chunk);
        }
        let r = tracker
            .reading()
            .expect("reading for realistic noisy BCG at 60 bpm");
        assert!(
            (r.bpm - 60.0).abs() < 4.0,
            "bpm {} should be ≈ 60 (bandpass must reject drift/wobble)",
            r.bpm
        );
        assert!(
            r.rmssd_ms < 100.0,
            "rmssd {} should be small for a regular 60 bpm wave",
            r.rmssd_ms
        );
    }

    #[test]
    fn secondary_peak_within_gate_window_is_blocked_by_peak_gate() {
        // Continuous oscillation with a big systolic peak and a smaller secondary
        // peak 600 ms later (fundamental + 2nd harmonic — the shape the bandpassed
        // hardware wave shows: big peak, deep trough, small peak, deep trough). The
        // secondary sits past the 500 ms refractory but within the 750 ms dicrotic
        // gate window. Without the peak-height gate every beat counts twice
        // (measured: bpm 100.0 with alternating ~560/640 ms IBIs); with the gate
        // only the systolic peak counts (bpm ≈ 50).
        let freq = 204u32;
        let beat_hz = 0.833f32; // 50 bpm, period 1200 ms → secondary at 600 ms
        let dc = 32_740.0f32;
        let n = (freq as f32 * 40.0) as usize;
        let mut state: u32 = 0x5eed_600d;
        let mut samples: Vec<u16> = Vec::with_capacity(n);
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let nz = 0.2 * (state as f32 / u32::MAX as f32 - 0.5) * 2.0;
            let t = i as f32 / freq as f32;
            let v = dc
                + 0.8 * (2.0 * std::f32::consts::PI * beat_hz * t).cos()
                + 1.5 * (2.0 * std::f32::consts::PI * 2.0 * beat_hz * t).cos()
                + nz;
            samples.push(v.round().clamp(0.0, 65_535.0) as u16);
        }
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(freq);
        for chunk in samples.chunks(11) {
            tracker.push_samples(chunk);
        }
        let r = tracker
            .reading()
            .expect("reading for 50 bpm oscillation with a secondary peak 600 ms late");
        assert!(
            (r.bpm - 50.0).abs() < 3.0,
            "bpm {} should be ≈ 50 (secondary peak must not double-count)",
            r.bpm
        );
        assert!(
            r.rmssd_ms < 150.0,
            "rmssd {} should be small once the secondary peak is gated out",
            r.rmssd_ms
        );
    }

    /// Same fundamental+2nd-harmonic oscillation as the dicrotic-gate test, but
    /// with a strong secondary (harm = 2.0, ~85 % of the systolic height — the
    /// gate alone cannot block it: measured bpm 86.5 without smoothing). The
    /// post-bandpass moving average attenuates the 2nd harmonic (~1.7 Hz) to
    /// ~30 % while keeping the fundamental, so the sub-peaks merge into one hump
    /// per beat and the IBIs become stable.
    #[test]
    fn smoothing_merges_bcg_subpeaks_for_stable_ibis() {
        let freq = 204u32;
        let beat_hz = 0.833f32; // 50 bpm, period 1200 ms
        let dc = 32_740.0f32;
        let n = (freq as f32 * 40.0) as usize;
        let mut state: u32 = 0x5eed_600d;
        let mut samples: Vec<u16> = Vec::with_capacity(n);
        for i in 0..n {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let nz = 0.2 * (state as f32 / u32::MAX as f32 - 0.5) * 2.0;
            let t = i as f32 / freq as f32;
            let v = dc
                + 0.8 * (2.0 * std::f32::consts::PI * beat_hz * t).cos()
                + 2.0 * (2.0 * std::f32::consts::PI * 2.0 * beat_hz * t).cos()
                + nz;
            samples.push(v.round().clamp(0.0, 65_535.0) as u16);
        }
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(freq);
        for chunk in samples.chunks(11) {
            tracker.push_samples(chunk);
        }
        let r = tracker
            .reading()
            .expect("reading for 50 bpm oscillation with strong sub-peaks");
        assert!(
            (r.bpm - 50.0).abs() < 3.0,
            "bpm {} should be ≈ 50 (sub-peaks must merge into one beat)",
            r.bpm
        );
        assert!(
            r.rmssd_ms < 150.0,
            "rmssd {} should be small once sub-peaks are smoothed out",
            r.rmssd_ms
        );
    }

    #[test]
    fn detects_heart_rate_with_noise() {
        // 1.0 Hz beat → 60 bpm with noise.
        let samples = sine_samples(1000, 1.0, 30.0, 150.0);
        let mut tracker = HeartRateTracker::new();
        tracker.push_samples(&samples);
        let r = tracker.reading().expect("reading with noise");
        assert!((r.bpm - 60.0).abs() < 4.0, "bpm {} should be ≈ 60", r.bpm);
    }

    /// 1100/1300 ms beat-pattern samples (mean 1200 ms → 50 bpm, RMSSD 200 ms).
    /// One smooth 150 ms raised-cosine hump per beat — realistic BCG hump width.
    /// The 2nd-harmonic secondary peak (~T/2 later) stays inside the dicrotic gate
    /// window, where the moving average has attenuated it below the gate ratio.
    fn jittered_beat_samples(freq: u32) -> Vec<u16> {
        let mut samples: Vec<u16> = Vec::new();
        let mut t = 0.0f32;
        let pattern = [1100.0f32, 1300.0];
        let mut pi = 0usize;
        let hump_width_s = 0.15f32;
        // 60 s so the startup IBI (window settling) flushes out of the 32-IBI
        // window and does not inflate the RMSSD.
        while t < 60.0f32 {
            let period_ms = pattern[pi % pattern.len()];
            pi += 1;
            // One beat per period: smooth hump at the start of each period.
            let beat_samples = (period_ms / 1000.0 * freq as f32) as usize;
            for j in 0..beat_samples {
                let d = (j as f32 / freq as f32) / (hump_width_s / 2.0);
                let hump = if d.abs() < 1.0 {
                    900.0 * (std::f32::consts::FRAC_PI_2 * d).cos().powi(2)
                } else {
                    0.0
                };
                samples.push((32_768.0 + hump).clamp(0.0, 65_535.0) as u16);
            }
            t += period_ms / 1000.0;
        }
        samples
    }

    #[test]
    fn reports_hrv_from_jittered_beats() {
        // Alternate beat spacing: 1100/1300 ms pattern → mean 1200 ms (50 bpm),
        // RMSSD 200 ms.
        let mut tracker = HeartRateTracker::new();
        tracker.push_samples(&jittered_beat_samples(1000));
        let r = tracker.reading().expect("reading from jittered beats");
        assert!((r.bpm - 50.0).abs() < 3.0, "bpm {} should be ≈ 50", r.bpm);
        assert!(
            (r.rmssd_ms - 200.0).abs() < 60.0,
            "rmssd {} should be ≈ 200 ms",
            r.rmssd_ms
        );
    }

    #[test]
    fn silence_yields_no_reading() {
        let mut tracker = HeartRateTracker::new();
        tracker.push_samples(&vec![32_768u16; 5000]);
        assert!(tracker.reading().is_none());
        assert_eq!(tracker.ibi_count(), 0);
    }

    #[test]
    fn too_few_beats_yields_no_reading() {
        // 3 s at 1 Hz → only ~2 beats → below MIN_IBIS.
        let samples = sine_samples(1000, 1.0, 3.0, 0.0);
        let mut tracker = HeartRateTracker::new();
        tracker.push_samples(&samples);
        assert!(tracker.reading().is_none());
    }

    #[test]
    fn chunked_pushes_match_single_push() {
        let samples = sine_samples(1000, 1.2, 20.0, 50.0);
        let mut a = HeartRateTracker::new();
        a.push_samples(&samples);
        let mut b = HeartRateTracker::new();
        for chunk in samples.chunks(47) {
            b.push_samples(chunk);
        }
        assert_eq!(a.reading(), b.reading());
        assert_eq!(a.samples_seen(), b.samples_seen());
    }

    #[test]
    fn freq_zero_keeps_default_and_set_freq_updates() {
        let mut tracker = HeartRateTracker::new();
        assert_eq!(tracker.freq_hz(), 1000);
        tracker.set_freq(0);
        assert_eq!(tracker.freq_hz(), 1000);
        tracker.set_freq(500);
        assert_eq!(tracker.freq_hz(), 500);
        // 500 Hz, 1.2 Hz → same bpm.
        let samples = sine_samples(500, 1.2, 30.0, 0.0);
        tracker.push_samples(&samples);
        let r = tracker.reading().expect("reading at 500 Hz");
        assert!((r.bpm - 72.0).abs() < 3.0, "bpm {} should be ≈ 72", r.bpm);
    }

    #[test]
    fn chunked_500hz_sine_locks_like_pod4_stream() {
        // Pod 4 MCU streams ~500 Hz; frames arrive as small interleaved batches.
        let samples = sine_samples(500, 1.0, 25.0, 20.0);
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(500);
        for chunk in samples.chunks(40) {
            tracker.push_samples(chunk);
        }
        let r = tracker.reading().expect("reading from chunked 500 Hz");
        assert!((r.bpm - 60.0).abs() < 4.0, "bpm {} should be ≈ 60", r.bpm);
    }

    #[test]
    fn silence_after_lock_clears_reading() {
        let samples = sine_samples(500, 1.0, 20.0, 0.0);
        let mut tracker = HeartRateTracker::new();
        tracker.set_freq(500);
        tracker.push_samples(&samples);
        assert!(tracker.reading().is_some(), "should lock on sine");
        // > MAX_IBI_MS (2.5 s) of flatline — lock must expire.
        tracker.push_samples(&vec![32_768u16; 500 * 3]);
        assert!(tracker.reading().is_none());
        assert_eq!(tracker.ibi_count(), 0);
    }

    #[test]
    fn ibi_span_ms_detects_constant_window() {
        let mut tracker = HeartRateTracker::new();
        assert_eq!(tracker.ibi_span_ms(), None);
        // Clean 1.25 Hz sine at 500 Hz: exactly 400 samples per beat → every IBI is
        // exactly 800 ms → span 0, well within one sample period (2 ms) — a
        // metronome window. (The tracker rate must match the stream rate, otherwise
        // the refractory period is mis-scaled.)
        tracker.set_freq(500);
        let samples = sine_samples(500, 1.25, 30.0, 0.0);
        tracker.push_samples(&samples);
        let span = tracker.ibi_span_ms().expect("span after lock");
        // The post-bandpass moving average widens the peaks slightly, so the
        // natural IBI jitter floor of a perfectly periodic signal is a few ms.
        assert!(
            span < 10.0,
            "clean sine span {span} should be a few ms at most"
        );
        // Jittered beats (1100/1300 ms pattern) → span far above one sample period.
        let mut t2 = HeartRateTracker::new();
        t2.push_samples(&jittered_beat_samples(1000));
        let span2 = t2.ibi_span_ms().expect("span after jittered lock");
        assert!(
            span2 > 100.0,
            "jittered span {span2} should be far above one sample period"
        );
    }
}
