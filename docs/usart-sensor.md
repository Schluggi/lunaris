# Sensor subsystem USART (vibration + capacitance)

This document describes how `lunaris` uses the Sensor USART: piezo vibration (`SetAlarm`) and optional capacitance‑based presence MQTT entities. Behaviour and opcodes trace to [opensleep](https://github.com/LiamSnow/opensleep) (`src/sensor/command.rs`, `src/sensor/packet.rs`, `src/sensor/manager.rs`) under GPL‑3.0.

Pod 4 warning: opensleep is validated on Pod 3. Baud, device node, and payloads may differ on newer pods. Verify with captures from your mattress if behaviour is unclear.

## Role vs Frozen USART

| Link | Responsibility |
|------|----------------|
| Frozen (`--serial-device`) | Prime, climate, temperature polling — required at startup |
| Sensor (`--sensor-device`) | Vibration pipeline, capacitance `0x33` for occupancy — optional; if the port does not open, `lunaris` continues without vibrate buttons and without presence |

Only one process may own each TTY.

## Device path and speed

| Setting | Default (CLI) | Override on Pod OS |
|--------|----------------|---------------------|
| Path | `/dev/ttyS2` | `sensorPort` in `/opt/eight/config/machine.json` when `--sensor-device` is not passed |
| Baud | From `--pod`: 3 → 115200; 4 / 5 → 921600 | `--sensor-baud` always wins |

Implementation: [`src/cli.rs`](../src/cli.rs) (`effective_sensor_baud`), [`src/machine_config.rs`](../src/machine_config.rs).

## Frame format

Same as Frozen: `0x7E` start, length byte, payload, big‑endian CRC16‑CCITT over the payload only. Encoding helpers: [`src/frozen_frame.rs`](../src/frozen_frame.rs) (`encode_command`).

## Connect sequence (startup + reconnect)

`sensor_link` brings the Sensor MCU into a known state following opensleep's `run_discovery` order ([`src/sensor/manager.rs`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)):

1. **Bootloader at 38400**: ping ×3 (500 ms answer wait each — any CRC‑valid frame answers, opensleep `ping_device`). A pong reports the mode byte (`81 00 42` = bootloader, `81 00 46` = firmware — opensleep `parse_pong`), logged at INFO. When answered: send `JumpToFirmware` (`0x10` payload — [`frozen_frame::jump_to_firmware_frame`](../src/frozen_frame.rs), same byte sequence opensleep uses from the Sensor path), then wait at 38400 for the `JumpingToFirmware` (`0x90`) confirmation (opensleep `parse_jumping_to_firmware`) — the jump is retried up to 3 times — before switching the **same** port handle to firmware baud (`set_baud_rate` — no close/reopen; each open of a `ttyS*` can toggle DTR/RTS and reset the MCU). Afterwards ping/pong is retried for ~5 s to confirm the firmware is up.
2. **Firmware baud probe** (opensleep: "happens if program was recently running"): same ping ×3 at firmware baud (`--sensor-baud` / pod default). Skipped with `--sensor-force-bootloader-handshake`.
3. **TX‑only fallback**: nobody answered — the port stays open at firmware baud so vibration (`SetAlarm`) can still be attempted blind — same spirit as `--sensor-vibrate-no-ack-wait`.

**Reconnect**: the link task never gives up. Port errors, RX task death, or no CRC‑valid frame for 45 s (checked every 15 s together with a keepalive ping; skipped with `--sensor-vibrate-no-ack-wait`) trigger a reconnect with exponential backoff (2 s doubling to 30 s, reset after a link ran ≥ 60 s). A MCU wedged in its bootloader is therefore healed automatically: bootloader answers → jump → `0x90` → firmware.

**Exclusive lock**: the port is opened with an exclusive `flock` (serialport default). If another process — e.g. a stale `lunaris` instance — still holds the device, every open fails with `Unable to acquire exclusive lock on serial port`; `fuser -v /dev/ttyS2` shows the owner. Only one process may own each TTY; the reconnect loop picks the port up as soon as it is free. When a session ends, its RX task is aborted and awaited first, so the port (and its `flock`) is always released before the next connect — a blocked `read()` must not keep the device locked.

If this sequence is wrong for your hardware, you would adjust [`src/sensor_link.rs`](../src/sensor_link.rs) only with evidence from serial captures.

**Stream-alive gate (wedge protection):** a Piezo stream already flowing on connect (the MCU retained its config from a previous session) skips the connect-time setup commands entirely — re-configuring a *sampling* MCU wedged it once on a Pod 5 (no acks, stream died ~45 s later, then the MCU ignored both bauds and only a power cycle revived it). A live sample stream is treated as "config complete"; after a stream stall the normal ack-driven recovery (EnablePiezo / SetPiezoFreq) resumes.

## Host → MCU: vibration command bytes (payload, after framing)

Documented in [`src/sensor_frame.rs`](../src/sensor_frame.rs); payload opcodes:

| Concept | Opcode (first payload byte) | Notes |
|--------|-----------------------------|--------|
| Ping | `0x01` | Bootloader / link check |
| SetPiezoFrequency | `0x21` | `u32` BE Hz (Pod 3 default 1000; Pod 4/5 default 500) |
| EnablePiezo | `0x28` | After gain; before freq (opensleep order) |
| SetPiezoGain | `0x2B` | Two `u16` BE (default 400 per channel) |
| SetAlarm | `0x2C` | Side `u8`, intensity 1–100, pattern `u8`, duration `u32` BE seconds |
| EnableVibration | `0x2E` | Often required before `SetAlarm` is honored |

SetAlarm side: `0x00` left, `0x01` right (same `BedSide` as Frozen). AlarmPattern: Single `0`, Double `1`.

Cancel preamble (HA Vibration Cancel Preamble switch, default on): send `SetAlarm` with intensity 0, duration 0, pattern Double before the real sequence — opensleep `get_alarm_cmd` style.

Full vibrate batch: optional cancel, then EnableVibration → SetPiezoGain → EnablePiezo → SetPiezoFrequency → SetAlarm (5 or 6 frames). See `vibration_sequence_frames`.

Cover-button feedback (HA `Cover Button Vibration Feedback` switch, default on): a `[lisL]`/`[lisR]` dismiss message queues one native Sensor `0x40` command. Payload is `[0x40, side, 30, pattern, count]`: stock strength 30 %, alternating 125 ms vibration / 125 ms pause, 250 ms total per tap. Two/three/four taps select pattern/count 2/3/4; higher counts are capped at four like the hub. Single taps use one cycle of pattern 2. The MCU executes the whole sequence without a power ramp, host-side gaps, or early cancels. Feedback strength and duration are fixed: only the HA on/off switch remains, with retained state restored. Empty retained discovery messages remove the former feedback intensity/duration numbers, and their saved values are ignored. Armed, sampling links send the response bare; otherwise the existing initialization fallback runs. Ordinary vibration buttons still use `SetAlarm` (`0x2C`) and keep their separate settings. The native path has worked in hardware testing; this shorter stock waveform still needs confirmation. Tap aggregation can still delay the report. See `docs/cover-button-feedback-analysis.md`.

## TX pacing (why not burst everything at once)

opensleep’s sensor manager spaces configuration commands by ~800 ms (`CONFIG_RES_TIME`) and sends each piezo command only until ACK’d (`0xAE` / `0xAB` / `0xA8` / `0xA1` / live piezo stream). `lunaris` does the same for continuous piezo setup — it does not re‑blast the full priming sequence every few seconds (that restarts MCU sampling and flushes samples). Vibration batches still stagger sub‑frames (~150 ms) and wait ~400 ms after priming before `SetAlarm`.

[`src/sensor_link.rs`](../src/sensor_link.rs): `CONFIG_RES_TIME`, `SENSOR_PRIMING_INTER_FRAME_MS`, `SENSOR_AFTER_PRIMING_MS`, `wait_for_vibration_ack`, stream‑stall recovery (`PIEZO_STREAM_STALL`).

## MCU → host: parses used by `lunaris`

Inbound deframer and handlers: [`src/sensor_rx.rs`](../src/sensor_rx.rs).

| Payload tag | Meaning |
|-------------|---------|
| `0xAE` | VibrationEnabled — piezo path armed; `SetAlarm` typically waits until this has been seen (unless bypassed below) |
| `0xAB` | PiezoGainSet — gain ACK (opensleep; within ±6 of requested 400) |
| `0xA8` | PiezoEnabled — enable ACK |
| `0xA1` | PiezoFreqSet — freq ACK (marks setup freq OK; live stream still preferred for stall recovery) |
| `0xAC` | AlarmSet status (logged; status `0x01` matches opensleep test expectations; first ack may belong to cancel preamble) |
| `0x33` | Capacitance: 27‑byte opensleep layout, six `u16` BE zone samples (L→R along the bed) + sequence — drives MQTT presence when parse succeeds |
| `0x32` / `0x35` | Piezo samples — feeds the MQTT Heart Rate sensor per side (the HRV RMSSD is computed and logged but not exposed to Home Assistant for now). **`0x32`** (Pod 3 / opensleep `PiezoData`): 14‑byte header (`tag`, `0x02`, freq `u32` BE, sequence `u32` BE, two gains `u16` BE) + interleaved left/right `u16` BE samples. **`0x35`** (Pod 4/5 sensor MCU, layout verified from real wire dumps): **16‑byte header** — `tag` u8, marker `0x02`, four `u16` BE fields (zero before SetPiezoGain — likely per‑channel gains), one `u16` BE (constant `200` observed), `u32` BE timestamp at offset 12 in **milliseconds since MCU boot** — then sample groups of **16 bytes = four `u32` BE channels** `[c0, c1, c2, c3]` (one tick; hub uploads four piezo lanes as `piezo-dual` arrays; on an observed Pod 5 only `c0`/`c1` are wired (`c0` = right side, `c1` = left side per DC shifts), `c2`/`c3` constant 0). The frame has **no usable rate field** — the effective stream rate is ~**200 ticks/s** (11 ticks per ~55 ms frame; hub firmware also defaults to 200 Hz for freq 0), so lunaris derives it from the ms‑timestamp deltas. `--sensor-piezo-debug` hex‑dumps the first payloads (re‑armed on every presence flip: lie on a side, watch which channels move) and logs a rolling **10 s per‑channel min/max/mean window** — the per‑frame variance cannot show a slow ~1 Hz BCG wave, the 10 s span can (amplitude + per‑side DC shifts) |
| `0x07` | MCU UTF‑8 text (debug / alarm explanations) — MQTT Sensor Message when the Sensor TTY is open |
| `0x81` | Pong‑style reply (logged at trace) |

`0x33` caveat: Strict index checks mirror opensleep Pod 3. Pod 4 firmware may emit a different layout — presence stays disabled until parsing matches; with `--presence-debug`, the first malformed `0x33` emits a one‑shot WARN.

### Waiting for `0xAE`

Normal path: after the vibrate priming frames, `sensor_link` polls up to ~6 s for `0xAE`, re‑sending EnableVibration only (not a full piezo re‑blast). If it never arrives, `SetAlarm` is still sent and a WARN suggests `--sensor-vibrate-no-ack-wait` or a different `--sensor-baud`.

`--sensor-vibrate-no-ack-wait`: skips the wait; sends the vibrate batch with the same inter‑frame gaps but no `0xAE` gate — for lines where RX is not opensleep‑framed at the chosen baud but TX might still work.

### First RX heuristic

After connect, `sensor_link` logs whether the first read contains `0x7E` — quick signal that baud/framing likely match.

## Heart rate / HRV from piezo samples

While the piezo is enabled (`EnablePiezo` `0x28` via ack‑driven setup), the MCU streams piezo sample frames — Pod 3: opensleep `PiezoData` tag `0x32`; Pod 4/5: tag `0x35` with the four‑channel layout above (11 groups of four `u32` BE channels per ~55 ms frame ≈ 200 ticks/s). `lunaris` averages the two channels per side when both are live (wire‑verified DC pairing: left = `c1`+`c3`, right = `c0`+`c2`) and feeds the live channel at **full scale** when its partner is dead (`c2`/`c3` constant 0 on observed hardware — the BCG wave is only ±1–2 LSB, halving it would hide it), then feeds each side into a beat detector ([`src/hrv.rs`](../src/hrv.rs)): a cascaded-biquad **heart-band bandpass (~0.65–1.95 Hz = 39–117 bpm)** first — the BCG wave is only ±1–2 LSB and without the bandpass the detector fires on slow respiration/drift waves and quantization wobble (measured: bpm ~20–30 % too high, RMSSD 200–500 ms instead of tens of ms) — a **~450 ms moving-average smoothing** stage (merges the several sub-peaks of each BCG complex — the I/J/K deflections plus bandpass ringing — into one hump per beat; without it the beat timing jittered between sub-peaks and the RMSSD read 300–700 ms instead of 20–80 ms), then sliding-window DC removal with an adaptive threshold, pending-peak detection with hysteresis, a **500 ms refractory period** and a **dicrotic gate** (a beat candidate within 750 ms of the previous beat must reach 75 % of the recent peak heights) — real BCG waves carry a smaller secondary/dicrotic wave ~0.5–0.75 s after each systolic peak; without these guards it registered as an extra beat (measured: bpm ≈ 2× the true rate, RMSSD 200–450 ms instead of tens of ms). Reliable range ~40–80 bpm; the bandpass passes up to ~117 bpm. The window statistics run in f64: the channels sit at a DC of ~32740, where an f32 sum-of-squares (one ULP = 32768) would cancel the ±2 LSB BCG variance away entirely. The effective tick rate is derived from the frame ms‑timestamps (the SetPiezoFreq 500 Hz target would inflate bpm by 2.5×). From the inter-beat intervals (IBI) it publishes, throttled to one reading per side per 5 s:

- Heart Rate Left / Right — `…/sensor/heart_rate_left|right/state`, unit `bpm` (when nobody is in bed — Presence OFF — the state is cleared with an empty payload, so HA shows the sensor as `unknown` and "not in bed" stays distinguishable from "in bed, no reading"; a `0` is published only when the side is occupied but has had no valid reading for ~10 s — sporadic zeros are held back so they do not pollute the statistics)
- HRV RMSSD Left / Right — computed (root mean square of successive IBI differences) and logged with `--sensor-piezo-debug`, but **not exposed to Home Assistant for now** (values not calibrated yet; previously discovered entities are removed via empty discovery payloads)

States are not retained; Home Assistant shows `0` whenever there is no valid reading: no Presence on that side, the lock expired (no beat accepted for ~2.5 s), or the reading was rejected as an artifact. **Artifact guard:** IBIs are quantized to whole samples, so an IBI window of ≥ 8 identical IBIs (span below one sample period — no real heart beats metronome-exactly) means the detector locked onto a periodic artifact, e.g. per-frame structure of a misparsed piezo stream; the reading is dropped, the lock cleared, and `0` is published (one WARN per side). When capacitance presence is available, HR/HRV publish only while that side’s Presence is ON (presence OFF also sets `0`). If only one side is occupied and the opposite piezo wire has much higher variance (common on Pod 4), that lively channel is routed to the occupied side automatically; `--sensor-piezo-swap-lr` still forces a static L/R swap. Pod 4/5 tick rate: derived from frame timestamps (≈200 Hz), with the SetPiezoFreq target only as startup fallback; Pod 3 uses the frame `freq` (opensleep: 1000 Hz). Look for log `Sensor: first piezo sample batch` then `heart rate / HRV tracking started`. The detector is heuristic — treat the values as wellness-grade, not medical.

## MQTT / Home Assistant (summary)

When the Sensor serial opens successfully, discovery includes vibrate buttons (intensity, duration, pattern as runtime entities elsewhere in the bridge), Vibration Cancel Preamble, Cover Button Vibration Feedback (one short vibration per cover-button tap, like the original firmware), diagnostic Sensor Message (`0x07` text), optional presence binary sensors (Left / Right / Any), calibration button, Presence Cap Threshold, Presence Baseline Delta, retained Presence Baseline Zones, Heart Rate sensor (from piezo `0x32` / `0x35` samples; HRV RMSSD is computed but not exposed for now), etc. Full topic list: [`AGENTS.md`](../AGENTS.md).

## Source map

| File | Role |
|------|------|
| [`sensor_frame.rs`](../src/sensor_frame.rs) | Build framed bytes: ping, priming, `SetAlarm`, `vibration_sequence_frames` |
| [`sensor_link.rs`](../src/sensor_link.rs) | Owns the Sensor TTY task: handshake, ack‑driven piezo setup, keepalive, queued vibration |
| [`sensor_rx.rs`](../src/sensor_rx.rs) | RX deframe + `0xAE` / `0xAB` / `0xA8` / `0xA1` / `0x32` / `0x35` / `0x33` / `0x07` handling |
| [`hrv.rs`](../src/hrv.rs) | Beat detection + HR (bpm) / HRV (RMSSD ms) from piezo `0x32` / `0x35` samples |
| [`mqtt_bridge.rs`](../src/mqtt_bridge.rs) | HA discovery + enqueue to `sensor_tx` |

## Practical checks on Pod hardware

1. Confirm `sensorPort` / `/dev/tty…` matches stock or known‑good mappings (see [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11) for Pod 4 community notes).
2. Align `--sensor-baud` with firmware speed after bootloader (often 921600 on Pod 4/5; 115200 on Pod 3).
3. Prefer a capture when vibration or presence fails: absence of `0x7E` suggests wrong speed or stray binary; malformed `0x33` suggests a layout change versus opensleep Pod 3.
