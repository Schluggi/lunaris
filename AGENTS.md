# AGENTS.md — Kontext für Bearbeitung dieses Repos

Diese Datei ist für **KI- und Human-Agenten** gedacht, die am Projekt arbeiten. **Bitte bei relevanten Änderungen am Verhalten, der Architektur oder den Konventionen diesen Abschnitt aktualisieren**, damit der Kontext für spätere Sessions stimmt.

## Zweck

**narcolepsy** ist eine **einzige Rust-Binary**, die lokal (ohne Eight-Sleep-Cloud) **Frozen**-USART-Befehle sendet (**Prime**, **SetTargetTemperature** links/rechts). Das Wire-Format ist **opensleep-kompatibel** ([LiamSnow/opensleep](https://github.com/LiamSnow/opensleep): CRC + `0x7E`-Rahmen).

Zielhardware des Nutzers: **Eight Sleep Pod 4** — das Protokoll ist im Code/Repositories nach **Pod 3 / opensleep** modelliert und **auf Pod 4 experimentell**; Abweichungen (TTY, Baud, Befehl) sind möglich ([`docs/usart-frozen.md`](docs/usart-frozen.md)).

**Deploy-Ziel:** Das Pod-OS ist **Linux aarch64** (z. B. „Eight Layer“, `uname -m` → `aarch64`). Releases müssen mit **`--target aarch64-unknown-linux-gnu`** (o. ä.) gebaut werden; siehe **README** und [`.cargo/config.toml`](.cargo/config.toml). Native `x86_64`-Builds auf dem Pod ausführen → `Exec format error`.

## Architektur (kurz)

| Modul / Pfad | Rolle |
|--------------|--------|
| [`src/main.rs`](src/main.rs) | Einstieg: CLI parsen, Logging, **Serial-Port-Startup-Check** (`check_device_accessible`); bei Fehler **sofort `exit(1)`**, kein MQTT. Danach `mqtt_bridge::run`. |
| [`src/cli.rs`](src/cli.rs) | **clap**: MQTT-Broker, Topic-Prefix, Discovery-IDs, Serielle Parameter (`--serial-device`, `--serial-baud`). **Kein Config-File** in v1. |
| [`src/frozen_frame.rs`](src/frozen_frame.rs) | CRC-CCITT + Frame-Encoding; `prime_frame()`, `set_target_temperature_frame()` — Tests mit festem Hex (opensleep). |
| [`src/serial_prime.rs`](src/serial_prime.rs) | `check_device_accessible` (Start), `send_frame` beim Button (`tokio-serial`). |
| [`src/is31fl3194.rs`](src/is31fl3194.rs) | IS31FL3194 über Linux **I²C** (`i2cdev`), solid RGB — Logik aus opensleep `led/controller.rs` (GPL). |
| [`src/mqtt_bridge.rs`](src/mqtt_bridge.rs) | **rumqttc**: LWT/Availability, Discovery **Button** + **Climate** (links/rechts) + optional **Light** (JSON), Prime-/Climate-/LED-Commands, `homeassistant/status`, `{prefix}/result`. |

Typische MQTT-Topics (Default-Präfix `narcolepsy/pod4` in CLI): `…/availability`, `…/button/prime/set`, `…/climate/left|right/…`, `…/result`; Discovery unter `homeassistant/button|climate|light/<object_id>/config`.

**Frozen-Seriell:** Default **`/dev/ttyS1`** (Pod 4 laut [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)). Pod 3: meist **`/dev/ttymxc2`**.

**LED:** I²C default **`/dev/i2c-1`**, Chip **0x53**; bei Problemen nur Warnung, dann ohne Light-Entity. `--no-led` schaltet LED komplett ab.

## Konventionen für Änderungen

- **Lizenz:** GPL-3.0 ([`LICENSE`](LICENSE)). Code/Framing aus opensleep ableitbar → Urheber-/Herkunftshinweise in betroffenen Dateien wahren.
- **Scope:** Fokussiert bleiben (keine großen Refactors „nebenbei“). Neue Features (TLS, weitere Entities) gezielt und dokumentiert ergänzen.
- **Tests:** Kritische Logik (CRC, Frame-Bytes) **unit-testen**; CI: [`/.github/workflows/ci.yml`](.github/workflows/ci.yml) (`fmt`, `test`, `clippy`, `release` build).
- **Toolchain:** [`rust-toolchain.toml`](rust-toolchain.toml) — **clap** ist auf `=4.5.27` gepinnt (Kompatibilität mit Rust 1.84; neuere clap-Versionen können Rust 1.85+/Edition 2024 verlangen).

## Befehle

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
# Pod / aarch64:
# rustup target add aarch64-unknown-linux-gnu  # + Kreuz-Linker-Paket des Host-OS
cargo build --release --target aarch64-unknown-linux-gnu
```

Logging: `RUST_LOG`, zusätzlich `--log-level` (siehe `--help`).

## Pflege dieser Datei

Wenn du folgendes änderst, **AGENTS.md hier anpassen**:

- Neue CLI-Flags, Default-Topics oder Discovery-Felder.
- Serielle Parameter, Protokollbytes oder Pod-spezifische Annahmen.
- Neue Abhängigkeiten oder bewusste Pins (z. B. clap).
- Neue öffentliche Doku-Pfade oder CI-Schritte.

---

*Cursor erkennt üblicherweise `AGENTS.md` im Repo-Root; Inhalt gilt für alle automatisierten Agenten.*
