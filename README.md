# narcolepsy

Single-binary **local-only** bridge: triggers the Eight Sleep Pod **Frozen** subsystem **prime** command over USART using the same framing as [opensleep](https://github.com/LiamSnow/opensleep), and exposes a **Home Assistant MQTT button** via [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery).

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

Then on the Pod (example):

```bash
chmod +x /tmp/narcolepsy
/tmp/narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --serial-device /dev/ttyS1
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
  --serial-device /dev/ttyS1
```

Default **`/dev/ttyS1`** matches Pod **4** Frozen paths from [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11). Pod **3** often needs **`--serial-device /dev/ttymxc2`**. Baud **38400** unless you prove otherwise.

## CLI arguments

All settings are **CLI-only** (no config file in v1). `narcolepsy --help` lists the same flags. Optional environment variables override only the marked options when set.

| Option | Default | Environment | Description |
|--------|---------|-------------|-------------|
| `--mqtt-host` | `localhost` | — | MQTT broker hostname or IP. |
| `--mqtt-port` | `1883` | — | MQTT broker TCP port. |
| `--mqtt-username` | *(empty)* | `MQTT_USERNAME` | Broker username (optional). |
| `--mqtt-password` | *(empty)* | `MQTT_PASSWORD` | Broker password (optional). |
| `--mqtt-client-id` | `narcolepsy` | — | MQTT client id. |
| `--topic-prefix` | `narcolepsy/pod4` | — | Prefix for device topics: `{prefix}/availability`, `{prefix}/button/prime/set`, `{prefix}/light/led/set`, `{prefix}/light/led/state`, `{prefix}/result`, etc. |
| `--discovery-prefix` | `homeassistant` | — | Home Assistant [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) prefix. |
| `--discovery-object-id` | `narcolepsy_prime` | — | `<object_id>` in `homeassistant/button/<object_id>/config`. |
| `--device-name` | `Eight Sleep` | — | Friendly device name in discovery (`device.name`). |
| `--device-identifier` | `narcolepsy_pod` | — | Stable id for the HA device registry (`device.identifiers`). |
| `--serial-device` | `/dev/ttyS1` | — | Frozen subsystem UART. Pod 4: often `ttyS1` ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)); Pod 3: often `ttymxc2`. Must open at startup or the process exits. |
| `--serial-baud` | `38400` | — | Serial line speed (bits/s). |
| `--payload-press` | `PRESS` | — | Payload Home Assistant publishes when the button is pressed (must match discovery `payload_press`). |
| `--i2c-device` | `/dev/i2c-1` | — | Linux I²C bus where the IS31FL3194 LED driver sits (address **0x53**). Probed at startup; if it cannot be opened, MQTT **Prime** still runs but no **Light** entity is advertised. |
| `--no-led` | *(off)* | — | Disables LED/I²C entirely (no probe, no `homeassistant/light/...` discovery). |
| `--discovery-object-id-led` | `narcolepsy_led` | — | `<object_id>` for `homeassistant/light/<object_id>/config`. |
| `--log-level` | `info` | — | Default `tracing` filter if `RUST_LOG` is unset (e.g. `debug`, `info,narcolepsy=debug`). |

If **`RUST_LOG`** is set in the environment, it takes precedence over `--log-level` (standard `tracing_subscriber` behaviour).

## USART protocol

See [docs/usart-frozen.md](docs/usart-frozen.md).

## LED (Home Assistant light)

See [docs/led-is31fl3194.md](docs/led-is31fl3194.md).

## License

SPDX: **GPL-3.0-only**. See [LICENSE](LICENSE). CRC/framing for Frozen frames derives from [opensleep](https://github.com/LiamSnow/opensleep) (same license).
