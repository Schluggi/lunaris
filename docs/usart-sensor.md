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

## Bootloader handshake (startup)

Unlike Frozen, `sensor_link` optionally runs opensleep‑style wake‑up:

1. Open port at `38400` (opensleep bootloader baud).
2. Send Sensor Ping (`0x01` payload — `ping_frame()` in [`src/sensor_frame.rs`](../src/sensor_frame.rs)).
3. Send Frozen `JumpToFirmware` frame (reuse from [`frozen_frame::jump_to_firmware_frame`](../src/frozen_frame.rs)) — same byte sequence opensleep uses from the Sensor path.
4. Short delays, drop the bootloader handle, then open the port again at firmware baud (`--sensor-baud` / pod default).

If this sequence is wrong for your hardware, you would adjust [`src/sensor_link.rs`](../src/sensor_link.rs) only with evidence from serial captures.

## Host → MCU: vibration command bytes (payload, after framing)

Documented in [`src/sensor_frame.rs`](../src/sensor_frame.rs); payload opcodes:

| Concept | Opcode (first payload byte) | Notes |
|--------|-----------------------------|--------|
| Ping | `0x01` | Bootloader / link check |
| SetPiezoFrequency | `0x21` | `u32` BE Hz (default 1000) |
| EnablePiezo | `0x28` | After gain/freq |
| SetPiezoGain | `0x2B` | Two `u16` BE (default 400 per channel) |
| SetAlarm | `0x2C` | Side `u8`, intensity 1–100, pattern `u8`, duration `u32` BE seconds |
| EnableVibration | `0x2E` | Often required before `SetAlarm` is honored |

SetAlarm side: `0x00` left, `0x01` right (same `BedSide` as Frozen). AlarmPattern: Single `0`, Double `1`.

Cancel preamble (HA Vibration Cancel Preamble switch, default on): send `SetAlarm` with intensity 0, duration 0, pattern Double before the real sequence — opensleep `get_alarm_cmd` style.

Full vibrate batch: optional cancel, then EnableVibration → SetPiezoGain → SetPiezoFrequency → EnablePiezo → SetAlarm (5 or 6 frames). See `vibration_sequence_frames`.

## TX pacing (why not burst everything at once)

opensleep’s sensor manager spaces configuration commands by ~800 ms (`CONFIG_RES_TIME`). `lunaris` staggers priming sub‑frames (~150 ms between frames) and waits ~400 ms after the last priming frame before `SetAlarm`, otherwise some pods report `0xAC` without physical vibration. Periodic piezo priming repeats every 5 s to stay close to opensleep’s long‑running supervisor.

[`src/sensor_link.rs`](../src/sensor_link.rs): `SENSOR_PRIMING_INTER_FRAME_MS`, `SENSOR_AFTER_PRIMING_MS`, `wait_for_vibration_ack`.

## MCU → host: parses used by `lunaris`

Inbound deframer and handlers: [`src/sensor_rx.rs`](../src/sensor_rx.rs).

| Payload tag | Meaning |
|-------------|---------|
| `0xAE` | VibrationEnabled — piezo path armed; `SetAlarm` typically waits until this has been seen (unless bypassed below) |
| `0xAC` | AlarmSet status (logged; status `0x01` matches opensleep test expectations; first ack may belong to cancel preamble) |
| `0x33` | Capacitance: 27‑byte opensleep layout, six `u16` BE zone samples (L→R along the bed) + sequence — drives MQTT presence when parse succeeds |
| `0x07` | MCU UTF‑8 text (debug / alarm explanations) — MQTT Sensor Message when the Sensor TTY is open |
| `0x81` | Pong‑style reply (logged at trace) |

`0x33` caveat: Strict index checks mirror opensleep Pod 3. Pod 4 firmware may emit a different layout — presence stays disabled until parsing matches; with `--presence-debug`, the first malformed `0x33` emits a one‑shot WARN.

### Waiting for `0xAE`

Normal path: after sending the four priming frames, `sensor_link` polls up to ~6 s for `0xAE`, with occasional re‑priming. If it never arrives, `SetAlarm` is still sent and a WARN suggests `--sensor-vibrate-no-ack-wait` or a different `--sensor-baud`.

`--sensor-vibrate-no-ack-wait`: skips the wait; sends the vibrate batch with the same inter‑frame gaps but no `0xAE` gate — for lines where RX is not opensleep‑framed at the chosen baud but TX might still work.

### First RX heuristic

After connect, `sensor_link` logs whether the first read contains `0x7E` — quick signal that baud/framing likely match.

## MQTT / Home Assistant (summary)

When the Sensor serial opens successfully, discovery includes vibrate buttons (intensity, duration, pattern as runtime entities elsewhere in the bridge), Vibration Cancel Preamble, diagnostic Sensor Message (`0x07` text), optional presence binary sensors (Left / Right / Any), calibration button, Presence Cap Threshold, Presence Baseline Delta, retained Presence Baseline Zones, etc. Full topic list: [`AGENTS.md`](../AGENTS.md).

## Source map

| File | Role |
|------|------|
| [`sensor_frame.rs`](../src/sensor_frame.rs) | Build framed bytes: ping, priming, `SetAlarm`, `vibration_sequence_frames` |
| [`sensor_link.rs`](../src/sensor_link.rs) | Owns the Sensor TTY task: handshake, periodic priming, queued batches |
| [`sensor_rx.rs`](../src/sensor_rx.rs) | RX deframe + `0xAE` / `0x33` / `0x07` handling |
| [`mqtt_bridge.rs`](../src/mqtt_bridge.rs) | HA discovery + enqueue to `sensor_tx` |

## Practical checks on Pod hardware

1. Confirm `sensorPort` / `/dev/tty…` matches stock or known‑good mappings (see [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11) for Pod 4 community notes).
2. Align `--sensor-baud` with firmware speed after bootloader (often 921600 on Pod 4/5; 115200 on Pod 3).
3. Prefer a capture when vibration or presence fails: absence of `0x7E` suggests wrong speed or stray binary; malformed `0x33` suggests a layout change versus opensleep Pod 3.
