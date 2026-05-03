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

## Quick start

```bash
cargo build --release
./target/release/narcolepsy \
  --mqtt-host 192.168.1.10 \
  --mqtt-port 1883 \
  --mqtt-username ha \
  --mqtt-password secret \
  --serial-device /dev/ttymxc2
```

Defaults match opensleep’s Pod 3 reference: **`/dev/ttymxc2`** @ **38400** baud. Override if your Pod 4 differs.

## CLI overview

Run `narcolepsy --help` for full options. MQTT connection parameters and serial device settings are **CLI-only** (no config file in v1).

## USART protocol

See [docs/usart-frozen.md](docs/usart-frozen.md).

## License

SPDX: **GPL-3.0-only**. See [LICENSE](LICENSE). CRC/framing for Frozen frames derives from [opensleep](https://github.com/LiamSnow/opensleep) (same license).
