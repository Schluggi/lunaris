# AGENTS.md — Kontext für Bearbeitung dieses Repos

Diese Datei ist für **KI- und Human-Agenten** gedacht, die am Projekt arbeiten. **Bitte bei relevanten Änderungen am Verhalten, der Architektur oder den Konventionen diesen Abschnitt aktualisieren**, damit der Kontext für spätere Sessions stimmt.

## Zweck

**narcolepsy** ist eine **einzige Rust-Binary**, die lokal (ohne Eight-Sleep-Cloud) den **Prime**-Befehl an das **Frozen**-Subsystem per **USART** sendet. Das Wire-Format ist **opensleep-kompatibel** ([LiamSnow/opensleep](https://github.com/LiamSnow/opensleep): CRC + `0x7E`-Rahmen, Prime-Payload `0x52`).

Zielhardware des Nutzers: **Eight Sleep Pod 4** — das Protokoll ist im Code/Repositories nach **Pod 3 / opensleep** modelliert und **auf Pod 4 experimentell**; Abweichungen (TTY, Baud, Befehl) sind möglich ([`docs/usart-frozen.md`](docs/usart-frozen.md)).

**Deploy-Ziel:** Das Pod-OS ist **Linux aarch64** (z. B. „Eight Layer“, `uname -m` → `aarch64`). Releases müssen mit **`--target aarch64-unknown-linux-gnu`** (o. ä.) gebaut werden; siehe **README** und [`.cargo/config.toml`](.cargo/config.toml). Native `x86_64`-Builds auf dem Pod ausführen → `Exec format error`.

## Architektur (kurz)

| Modul / Pfad | Rolle |
|--------------|--------|
| [`src/main.rs`](src/main.rs) | Einstieg: CLI parsen, Logging, **Serial-Port-Startup-Check** (`check_device_accessible`); bei Fehler **sofort `exit(1)`**, kein MQTT. Danach `mqtt_bridge::run`. |
| [`src/cli.rs`](src/cli.rs) | **clap**: MQTT-Broker, Topic-Prefix, Discovery-IDs, Serielle Parameter (`--serial-device`, `--serial-baud`). **Kein Config-File** in v1. |
| [`src/frozen_frame.rs`](src/frozen_frame.rs) | CRC-CCITT + Frame-Encoding; `prime_frame()` muss mit opensleep übereinstimmen (Tests mit festem Hex). |
| [`src/serial_prime.rs`](src/serial_prime.rs) | `check_device_accessible` (Start), `send_frame` beim Button (`tokio-serial`). |
| [`src/mqtt_bridge.rs`](src/mqtt_bridge.rs) | **rumqttc**: LWT/Availability, **MQTT Discovery** für Home-Assistant-**Button**, Subscribe auf Command + optional `homeassistant/status`, Ergebnis auf `{prefix}/result`. |

Typische MQTT-Topics (Default-Präfix `narcolepsy/pod4` in CLI): `…/availability`, `…/button/prime/set`, `…/result`; Discovery unter `homeassistant/button/<object_id>/config`.

**Frozen-Seriell:** Default **`/dev/ttyS1`** (Pod 4 laut [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)). Pod 3: meist **`/dev/ttymxc2`**.

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
