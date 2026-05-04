# Pod LED (IS31FL3194)

The cover status LEDs are driven by an **IS31FL3194** 3-channel LED driver on I²C (**7-bit address `0x53`**), same stack as [opensleep](https://github.com/LiamSnow/opensleep) (`src/led/`).

## Linux interface

- Default bus device: **`/dev/i2c-1`** (override with `--i2c-device`).
- Process runs **read/write as the same user** that owns `lunaris`; you may need root or membership in the right group if udev restricts `/dev/i2c-*`.

## Behaviour in `lunaris`

- At startup the bus is **opened** once (with slave `0x53`). Failure logs a **warning** and the bridge continues **without** the MQTT **Light** entity (Frozen **Prime** still works).
- Home Assistant sees an MQTT **Light** with **JSON schema**, **RGB**, and **brightness** (`schema` / `supported_color_modes` / `brightness` in discovery).

## Protocol note

Register writes follow opensleep’s **solid RGB (“current level”)** path; PCB channel order matches opensleep (`BRG` at the register level). Pod **4** vs **3** bus numbering may differ — adjust `--i2c-device` if needed.
