# Frozen subsystem USART (opensleep-compatible framing)

This document describes the **wire format** for the **Frozen** USART subsystem that `lunaris` uses for mandatory pod serial control beyond a single command: **`0x7E`-framed** payloads with CRC, including **Prime**, **SetTargetTemperature** (climate left/right), **GetTemperatures** (**`0x41`**), wake traffic (**Ping** / **JumpToFirmware**), and inbound packets that the bridge decodes (**temperature**, **MCU text** **`0x07`**, pong **`0x81`**, jump ack **`0x90`**). For behavior and MQTT mapping, see the source tree and **[AGENTS.md](../AGENTS.md)**. Opensleep references: [opensleep](https://github.com/LiamSnow/opensleep) (GPL-3.0) `src/common/codec.rs`, `src/common/checksum.rs`, `src/frozen/command.rs`.

**Pod 4 warning:** opensleep is validated on **Pod 3** only. Eight Sleep does not publish this protocol. **Baud rate, device node (`/dev/tty…`), and command bytes may differ on Pod 4.** Treat the values below as a **working hypothesis** until confirmed with a capture from your hardware.

## Electrical / serial parameters

| Variant | Frozen UART (`lunaris --serial-device`) | Notes |
|--------|---------------------------------------------|--------|
| Pod 3 (opensleep tested) | `/dev/ttymxc2` @ 38400 | opensleep `src/frozen/manager.rs` (`PORT`) |
| Pod 4 (community) | **`/dev/ttyS1`** @ 38400 | frankenfirmware: Frozen=`ttyS1`, Sensor=`ttyS2` — [opensleep#11](https://github.com/LiamSnow/opensleep/issues/11) |

Same framing is being tried on Pod 4; the MCU may emit extra/different packets compared to Pod 3 — see the issue log.

## Frame format (host → Frozen MCU)

All multi-byte integers on the wire are **big-endian** where applicable. Each **command** is:

1. **Start byte:** `0x7E`
2. **Length:** one byte `N` = number of **payload** bytes that follow (not counting start, length, or CRC).
3. **Payload:** `N` bytes (the Frozen command body).
4. **CRC:** two bytes, **big-endian** `CRC16`, computed only over the **payload** bytes (not over start or length).

CRC algorithm (same as opensleep `checksum::compute`):

- Polynomial CCITT: `0x1021`
- Initial CRC value: `0x1D0F`
- For each payload byte, update per usual CRC-CCITT table/shift implementation.

## Frozen commands referenced by `lunaris`

| Name (concept) | Payload bytes (hex) | Full framed example (hex, spaces) | Notes |
|------------------|----------------------|-------------------------------------|-------|
| **Prime**        | `52`                 | `7E 01 52 B6 2B`                    | Opensleep `FrozenCommand::Prime`; starts/marks priming on the Frozen subsystem. |
| **SetTargetTemperature** | `40` *side* *en* *T_hi* *T_lo* | `7E 05 40 00 01 0E 10 E6 A8` (example) | Side `00` = left, `01` = right; *en* `01` = enabled; target `T` is **u16 BE** in **centidegrees** °C (`0x0E10` → 36.00 °C). Opensleep `FrozenCommand::SetTargetTemperature`. |

The **Prime** example matches opensleep’s unit test output byte-for-byte (`FrozenCommand::Prime.to_bytes()`). The **SetTargetTemperature** example matches opensleep’s `test_temp` (`Left`, 36 °C, enabled).

## Responses

The Frozen MCU sends frames using the **same** `0x7E` / length / CRC wrapping for outbound packets; opensleep models these in `FrozenPacket` (`src/frozen/packet.rs`). **lunaris** decodes a **subset** inbound in [`src/frozen_rx.rs`](../src/frozen_rx.rs): **temperature** (`0x41` / `0xC1`), MCU **text messages** (`0x07` UTF‑8 → MQTT **Firmware message** sensor + optional **Water Tank** MQTT sensor for two reservoir strings), pong **0x81**, and jump ack **0x90** (wake state). Other opcodes are ignored for MQTT (trace logging only).

## Verifying on Pod 4

1. Identify the correct serial device and baud (stock firmware logs, `dmesg`, or UART probe).
2. Prefer a **passive capture** of stock traffic around a prime cycle, then compare payload/CRC rules.
3. If lengths or CRC differ, adjust `src/frozen_frame.rs` only after you have evidence from captures.
