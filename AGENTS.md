# AGENTS.md — Context for working on this repo

This file is for **AI and human agents** working on the project. **Please update this section when behavior, architecture, or conventions change materially**, so context stays accurate for later sessions.

## Purpose

**narcolepsy** is a **single Rust binary** that sends **Frozen** USART commands locally (without the Eight Sleep cloud): **Prime**, **SetTargetTemperature** left/right, and optionally **Vibration** on the **Sensor** UART (**SetAlarm** per side). Wire format **opensleep** ([LiamSnow/opensleep](https://github.com/LiamSnow/opensleep): CRC + `0x7E` framing).

Target hardware: **Eight Sleep Pod 4** — the protocol is modeled in code/repos after **Pod 3 / opensleep** and is **experimental on Pod 4**; differences (TTY, baud, commands) are possible ([`docs/usart-frozen.md`](docs/usart-frozen.md)).

**Deploy target:** Pod OS is **Linux aarch64** (e.g. “Eight Layer”, `uname -m` → `aarch64`). Releases must be built with **`--target aarch64-unknown-linux-gnu`** (or similar); see **README** and [`.cargo/config.toml`](.cargo/config.toml). Running native `x86_64` builds on the Pod → `Exec format error`.

## Architecture (brief)

| Module / path | Role |
|---------------|------|
| [`src/main.rs`](src/main.rs) | CLI, logging; **Frozen serial required** (`exit(1)` if not open); **Sensor serial optional** (vibration — warning if not open); LED I²C optional. Spawns **`frozen_link`** + optional **`sensor_link`** (long-lived USART owners), then `mqtt_bridge::run`. |
| [`src/cli.rs`](src/cli.rs) | **clap**: MQTT, discovery IDs, Frozen + Sensor + I²C. **No config file** in v1. |
| [`src/frozen_frame.rs`](src/frozen_frame.rs) | CRC + framing; Frozen: `prime_frame()`, `ping_frame()`, `jump_to_firmware_frame()`, `set_target_temperature_frame()`, … |
| [`src/frozen_rx.rs`](src/frozen_rx.rs) | Parses inbound Frozen payloads (`0x81` pong, `0x90` jump ack) so wake state tracks firmware vs bootloader. |
| [`src/frozen_link.rs`](src/frozen_link.rs) | **Opens Frozen UART once** — opensleep-style **wake** (`Ping` + `JumpToFirmware` every 2s until pong shows firmware), **keepalive** ping ~15s, queues MQTT frames (`mpsc`). |
| [`src/sensor_frame.rs`](src/sensor_frame.rs) | Sensor: `vibration_sequence_frames`, `piezo_priming_frames`, `set_alarm_frame()` (= opensleep `SetAlarm`). |
| [`src/sensor_rx.rs`](src/sensor_rx.rs) | Parses inbound **`0xAE` VibrationEnabled** (and related acks); required before `SetAlarm` works like opensleep’s state machine. |
| [`src/wire_buffer.rs`](src/wire_buffer.rs) | If no `0x7E` yet, **retain** RX tail (do not `clear()`); avoids losing frames split across `read()` calls. |
| [`src/sensor_link.rs`](src/sensor_link.rs) | **Opens Sensor UART once** — RX decode, periodic **piezo priming**, queues vibration: **priming → wait for `0xAE` → SetAlarm**. |
| [`src/serial_prime.rs`](src/serial_prime.rs) | `check_device_accessible` at startup; **`send_frame` / `send_frames`** only as **fallback** when link queues are unset (tests). |
| [`src/is31fl3194.rs`](src/is31fl3194.rs) | IS31FL3194 over Linux **I²C** (`i2cdev`), solid RGB — logic from opensleep `led/controller.rs` (GPL). |
| [`src/mqtt_bridge.rs`](src/mqtt_bridge.rs) | **rumqttc**: discovery Prime + optional **Vibrate** + climate + optional light; enqueues bytes to **`frozen_tx` / `sensor_tx`**; outbound in **`tokio::spawn`** (avoid deadlock with `poll()`). |

Typical MQTT topics: `…/button/prime/set`, `…/button/vibrate_left|vibrate_right/set`, `…/climate/…`, `…/result`.

**Standalone operation:** **`frozen_link`** / **`sensor_link`** replicate opensleep’s continuous MCU handshake and priming so Prime/climate/vibration work **without** another service (e.g. frankenfirmware) on the same TTY — **only one process may own each UART**.

**Frozen serial:** default **`/dev/ttyS1`** @ 38400 (Pod 4). **Sensor (vibration):** default **`/dev/ttyS2`** @ **`38400`** (Pod 4 RX framing; use **`115200`** for opensleep Pod 3–style firmware) — `--no-vibration` or open failure → no vibration buttons. On startup, **`sensor_link`** runs an opensleep-style **bootloader handshake** (38400 Ping + JumpToFirmware, then opens firmware baud) unless **`--no-sensor-bootloader-handshake`**. **`--sensor-vibrate-no-ack-wait`** skips waiting for inbound **`0xAE`** (for Pods where RX is not opensleep-framed at the chosen baud). Each vibrate sends EnableVibration + piezo setup + SetAlarm (matches opensleep sensor manager ordering).

**LED:** I²C **`/dev/i2c-1`**; `--no-led` or error → no light.

## Conventions for changes

- **Language:** User-facing text (Home Assistant discovery names, README, `docs/`, CLI help) is **English only**.
- **License:** GPL-3.0 ([`LICENSE`](LICENSE)). Code/framing traceable to opensleep → keep attribution/source notices in affected files.
- **Scope:** Stay focused (no large drive-by refactors). Add new features (TLS, more entities) deliberately and document them.
- **Tests:** **Unit-test** critical logic (CRC, frame bytes); CI: [`/.github/workflows/ci.yml`](.github/workflows/ci.yml) (`fmt`, `test`, `clippy`, `release` build).
- **Toolchain:** [`rust-toolchain.toml`](rust-toolchain.toml) — **clap** is pinned to `=4.5.27` (compatibility with Rust 1.84; newer clap may require Rust 1.85+/Edition 2024).

## Commands

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
# Pod / aarch64:
# rustup target add aarch64-unknown-linux-gnu  # + cross-linker package for the host OS
cargo build --release --target aarch64-unknown-linux-gnu
```

Logging: `RUST_LOG`, plus `--log-level` (see `--help`).

## Maintaining this file

When you change any of the following, **update AGENTS.md here**:

- New CLI flags, default topics, or discovery fields.
- Serial parameters, protocol bytes, or Pod-specific assumptions.
- New dependencies or intentional pins (e.g. clap).
- New public doc paths or CI steps.

---

*Cursor typically picks up `AGENTS.md` at the repo root; the contents apply to all automated agents.*
