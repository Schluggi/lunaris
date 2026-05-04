# narcolepsy

Single-binary **local-only** bridge: **Frozen** USART (prime, per-side temperature), optional **Sensor** USART (per-side **vibration** / `SetAlarm`), optional **IS31FL3194** LED on I²C — **Home Assistant** via [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) (buttons, climates, light).

## Disclaimer

This project is for **personal, educational, and research** use. It is **not** affiliated with, endorsed by, or sponsored by Eight Sleep. “Eight Sleep” and related names are trademarks of Eight Sleep, Inc.

Using custom firmware or low-level hardware control **may void warranties**, **break the vendor app**, or in rare cases **damage hardware**. **Use at your own risk.**

Protocol details for **Pod 4** are **not** officially documented here and are **assumed compatible** with the opensleep Pod 3 Frozen framing until verified on real hardware.

## Requirements

- A reachable **MQTT broker** (for example Mosquitto on your LAN).
- Network path from the machine running `narcolepsy` to that broker.
- **Serial access** to the Pod Frozen USART from that same machine (typical when running **on the Pod SOM** after SSH/access setup; see opensleep [SETUP.md](https://github.com/LiamSnow/opensleep/blob/main/SETUP.md) for background — Pod 3 oriented).

No cloud services are required.

On startup, `narcolepsy` **opens `--serial-device` once** to verify it exists and is accessible. If that fails (missing path, permission, busy port), the process **exits before** connecting to MQTT.

## Quick start

### Run on the Pod (aarch64) — cross-compile on your PC

The Pod OS (**Eight Layer**, Yocto Kirkstone) reports **`aarch64`** (`uname -m`). A binary built on a typical PC is **`x86_64`** — copying it to the Pod yields:

`cannot execute binary file: Exec format error`

Build for ARM64 on your dev machine:

```bash
rustup target add aarch64-unknown-linux-gnu
# Debian/Ubuntu — provides linker aarch64-linux-gnu-gcc (see [.cargo/config.toml](.cargo/config.toml))
sudo apt install gcc-aarch64-linux-gnu

cargo build --release --target aarch64-unknown-linux-gnu
```

Deploy:

```bash
scp target/aarch64-unknown-linux-gnu/release/narcolepsy eight-pod:/tmp/
```

Then on the Pod (Pod **4** example — see [Examples](#examples-pod-3-pod-4-and-pod-5) for **`--pod`** and Pod **3**):

```bash
chmod +x /tmp/narcolepsy
/tmp/narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --pod 4 \
  --serial-device /dev/ttyS1 \
  --sensor-device /dev/ttyS2
```

If the binary fails with **GLIBC / version `GLIBC_x.y` not found**, your linker used a **newer** glibc than the Pod’s rootfs. Options: build on an older distro/container closer to Kirkstone, use a **[musl](https://musl.cc/) static** target (e.g. `aarch64-unknown-linux-musl` with a suitable toolchain), or tools like [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) / [cross](https://github.com/cross-rs/cross).

### Run on the same machine as `cargo build` (native)

```bash
cargo build --release
./target/release/narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --pod 4 \
  --serial-device /dev/ttyS1 \
  --sensor-device /dev/ttyS2
```

**`--pod`** (**3**, **4**, or **5**) is **required** — it selects default USART speeds (**5** matches **4**); see [Examples](#examples-pod-3-pod-4-and-pod-5). Paths default to Pod **4**/ **5** style (`ttyS1` / `ttyS2`).

## Examples: Pod 3, Pod 4, and Pod 5

These examples assume `narcolepsy` runs **on the bed’s Linux SoM** (same machine that owns `/dev/tty…`). Adjust **`--mqtt-host`**, port, and credentials for your broker.

### Pod 4 and Pod 5 (Eight Sleep Pod 4 / 5)

**Pod 5** uses the **same** CLI USART defaults as Pod **4** — pass **`--pod 5`** instead of **`--pod 4`** when that matches your hardware.

Community mapping ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)): **Frozen** → **`/dev/ttyS1`**, **Sensor** (vibration / capacitance) → **`/dev/ttyS2`**. Frozen uses **38400** (default **`--serial-baud`**).

**Typical start** (defaults: Frozen **38400**, Sensor **921600** — matches stock / Frankenfirmware after the bootloader jump).

```bash
./narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --pod 4 \
  --serial-device /dev/ttyS1 \
  --sensor-device /dev/ttyS2 \
  --topic-prefix narcolepsy/pod4
```

Use **`--sensor-baud 38400`** only if your Sensor link still runs at **38400** and the default **921600** fails.

If **`/opt/eight/config/machine.json`** defines **`frozenPort`** / **`sensorPort`**, you can omit **`--serial-device`** / **`--sensor-device`** unless you want to override the file.

### Pod 3 (opensleep-validated layout)

Opensleep upstream uses **Frozen** on **`/dev/ttymxc2`** @ **38400** and **Sensor** on **`/dev/ttymxc0`** with firmware **115200** after the bootloader handshake (`opensleep` [`src/frozen/manager.rs`](https://github.com/LiamSnow/opensleep/blob/main/src/frozen/manager.rs), [`src/sensor/manager.rs`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/manager.rs)).

```bash
./narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --pod 3 \
  --serial-device /dev/ttymxc2 \
  --sensor-device /dev/ttymxc0 \
  --topic-prefix narcolepsy/pod3
```

Your hardware may still use different `tty` names — confirm with **`dmesg`**, vendor logs, or opensleep [SETUP.md](https://github.com/LiamSnow/opensleep/blob/main/SETUP.md).

## CLI arguments

All settings are **CLI-only** (no config file in v1). `narcolepsy --help` lists the same flags. Optional environment variables override only the marked options when set.

| Option | Default | Environment | Description |
|--------|---------|-------------|-------------|
| `--mqtt-host` | `localhost` | — | MQTT broker hostname or IP. |
| `--mqtt-port` | `1883` | — | MQTT broker TCP port. |
| `--mqtt-username` | *(empty)* | `MQTT_USERNAME` | Broker username (optional). |
| `--mqtt-password` | *(empty)* | `MQTT_PASSWORD` | Broker password (optional). |
| `--mqtt-client-id` | `narcolepsy` | — | MQTT client id. |
| `--topic-prefix` | `narcolepsy/pod4` | — | Prefix for device topics: `{prefix}/availability`, `{prefix}/button/prime/set`, `{prefix}/button/vibrate_left|vibrate_right/set`, `{prefix}/climate/...`, `{prefix}/light/...`, `{prefix}/result`, etc. |
| `--discovery-prefix` | `homeassistant` | — | Home Assistant [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) prefix. |
| `--device-name` | `Eight Sleep` | — | Friendly device name in discovery (`device.name`). |
| `--device-identifier` | `narcolepsy_pod` | — | Stable id for the HA device registry (`device.identifiers`). |
| `--serial-device` | `/dev/ttyS1` | — | Frozen subsystem UART. Pod 4 / 5: often `ttyS1` ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)); Pod 3: often `ttymxc2`. Must open at startup or the process exits. |
| `--pod` | — | — | **Required.** `3`, `4`, or `5` (**5** = same baud defaults as **4**): default **`--serial-baud`** / **`--sensor-baud`** when those flags are omitted (**3** → 38400 / 115200; **4** / **5** → 38400 / 921600). Explicit baud flags override per line. |
| `--serial-baud` | *(from `--pod`; Pod 4 / 5 → `38400`)* | — | Frozen line speed (bits/s). |
| `--payload-press` | `PRESS` | — | Payload Home Assistant publishes when the button is pressed (must match discovery `payload_press`). |
| `--i2c-device` | `/dev/i2c-1` | — | Linux I²C bus where the IS31FL3194 LED driver sits (address **0x53**). Probed at startup; if it cannot be opened, MQTT **Prime** still runs but no **Light** entity is advertised. |
| `--climate-min-temp` | `13` | — | Minimum target temperature (°C) in climate discovery. |
| `--climate-max-temp` | `47` | — | Maximum target temperature (°C) in climate discovery. |
| `--climate-temp-step` | `0.5` | — | Slider step (°C) in Home Assistant. |
| `--sensor-device` | `/dev/ttyS2` | — | **Sensor** subsystem UART (vibration / `SetAlarm`). Pod 4 / 5: often `ttyS2` while Frozen is `ttyS1` ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)). Probed at startup; if it cannot be opened, the bridge continues **without** vibrate buttons. |
| `--sensor-baud` | *(from `--pod`; Pod 4 / 5 → `921600`)* | — | Sensor serial speed (bits/s). Override when your hardware differs. See [Examples](#examples-pod-3-pod-4-and-pod-5). |
| `--sensor-vibrate-no-ack-wait` | *(off)* | — | Send vibration frames **without** waiting for `0xAE` between priming and SetAlarm. For Pods where Sensor RX is not opensleep-shaped at your baud but TX may still work. |
| `--vibration-intensity` | `64` | — | Default intensity 1–100 for vibrate buttons (Sensor `SetAlarm`). |
| `--vibration-duration-sec` | `15` | — | Default duration in seconds (clamped 1…600). |
| `--vibration-pattern` | `single` | — | `single` or `double` (opensleep `AlarmPattern`). |
| `--log-level` | `info` | — | Default `tracing` filter if `RUST_LOG` is unset (e.g. `debug`, `info,narcolepsy=debug`). |

If **`RUST_LOG`** is set in the environment, it takes precedence over `--log-level` (standard `tracing_subscriber` behaviour).

## USART protocol

See [docs/usart-frozen.md](docs/usart-frozen.md).

## Climate (Home Assistant, left / right)

Two MQTT **[climate](https://www.home-assistant.io/integrations/climate.mqtt/)** entities send opensleep-compatible **`SetTargetTemperature`** frames on the Frozen UART (`0x40` + side + enable + target in **centidegree** Celsius). Modes: **`off`** and **`heat_cool`**. There is **no** MQTT-published **current** temperature yet (serial RX not implemented).

## Vibration (per side, Sensor UART)

Vibration uses the **Sensor** MCU’s USART (not Frozen). Each button press sends the same **primed sequence** as opensleep’s sensor scheduler — **`EnableVibration`**, **`SetPiezoGain`**, **`SetPiezoFreq`**, **`EnablePiezo`**, then **`SetAlarm`** (`0x2C` + side + intensity + pattern + duration) — so the piezo path is enabled before the alarm. MQTT exposes two **[buttons](https://www.home-assistant.io/integrations/button.mqtt/)** (**Vibrate mattress (left)** / **Vibrate mattress (right)**); parameters come from **`--vibration-intensity`**, **`--vibration-duration-sec`**, and **`--vibration-pattern`**. Device paths and **`--sensor-baud`** per hardware: [Examples](#examples-pod-3-pod-4-and-pod-5).

## LED (Home Assistant light)

See [docs/led-is31fl3194.md](docs/led-is31fl3194.md).

## License

SPDX: **GPL-3.0-only**. See [LICENSE](LICENSE). CRC/framing for Frozen frames derives from [opensleep](https://github.com/LiamSnow/opensleep) (same license).
