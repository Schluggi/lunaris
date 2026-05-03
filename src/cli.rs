//! Command-line interface (MQTT broker + serial port). No config file.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// Vibration pattern for [`Cli::vibration_pattern`] (Sensor `SetAlarm` / opensleep `AlarmPattern`).
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum VibrationPatternArg {
    #[default]
    Single,
    Double,
}

#[derive(Debug, Parser)]
#[command(name = "narcolepsy")]
#[command(
    about = "Local MQTT bridge: Frozen USART (prime, climate) + Sensor vibration (per side) + Pod LED (I²C), Home Assistant MQTT discovery."
)]
pub struct Cli {
    /// MQTT broker hostname or IP.
    #[arg(long, default_value = "localhost")]
    pub mqtt_host: String,

    #[arg(long, default_value_t = 1883)]
    pub mqtt_port: u16,

    #[arg(long, env = "MQTT_USERNAME")]
    pub mqtt_username: Option<String>,

    #[arg(long, env = "MQTT_PASSWORD")]
    pub mqtt_password: Option<String>,

    #[arg(long, default_value = "narcolepsy")]
    pub mqtt_client_id: String,

    /// Prefix for device topics (`{prefix}/availability`, `{prefix}/button/prime/set`, `{prefix}/climate/...`, …).
    #[arg(long, default_value = "narcolepsy/pod4")]
    pub topic_prefix: String,

    /// Home Assistant MQTT discovery prefix (usually `homeassistant`).
    #[arg(long, default_value = "homeassistant")]
    pub discovery_prefix: String,

    /// `<object_id>` in `homeassistant/button/<object_id>/config`.
    #[arg(long, default_value = "narcolepsy_prime")]
    pub discovery_object_id: String,

    #[arg(long, default_value = "Eight Sleep")]
    pub device_name: String,

    /// Stable ID for Home Assistant device registry (`device.identifiers`).
    #[arg(long, default_value = "narcolepsy_pod")]
    pub device_identifier: String,

    /// Serial device for the **Frozen** subsystem.
    ///
    /// Pod **4** (reported by frankenfirmware / community): Frozen on **`/dev/ttyS1`**, sensor on `/dev/ttyS2`
    /// ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11)).
    /// Pod **3** / stock opensleep paths often use **`/dev/ttymxc2`** for Frozen instead.
    ///
    /// Must be openable at startup; otherwise the process exits before MQTT connects.
    #[arg(long, default_value = "/dev/ttyS1")]
    pub serial_device: PathBuf,

    #[arg(long, default_value_t = 38400)]
    pub serial_baud: u32,

    /// Payload Home Assistant sends when the button is pressed (see discovery `payload_press`).
    #[arg(long, default_value = "PRESS")]
    pub payload_press: String,

    /// Linux I²C bus device for the IS31FL3194 LED controller (address `0x53`).
    #[arg(long, default_value = "/dev/i2c-1")]
    pub i2c_device: PathBuf,

    /// Skip LED support: no I²C open at startup, no MQTT light discovery.
    #[arg(long, default_value_t = false)]
    pub no_led: bool,

    /// `<object_id>` for `homeassistant/light/<object_id>/config`.
    #[arg(long, default_value = "narcolepsy_led")]
    pub discovery_object_id_led: String,

    /// `<object_id>` for `homeassistant/switch/<object_id>/config` (startup LED on/off preference).
    #[arg(long, default_value = "narcolepsy_startup_led")]
    pub discovery_object_id_startup_led: String,

    /// `<object_id>` for `homeassistant/climate/<object_id>/config` (left mattress side).
    #[arg(long, default_value = "narcolepsy_climate_left")]
    pub discovery_object_id_climate_left: String,

    /// `<object_id>` for `homeassistant/climate/<object_id>/config` (right mattress side).
    #[arg(long, default_value = "narcolepsy_climate_right")]
    pub discovery_object_id_climate_right: String,

    /// Minimum target temperature (°C) published in MQTT climate discovery.
    #[arg(long, default_value_t = 13.0)]
    pub climate_min_temp: f64,

    /// Maximum target temperature (°C) published in MQTT climate discovery.
    #[arg(long, default_value_t = 47.0)]
    pub climate_max_temp: f64,

    /// Step between temperature adjustments in Home Assistant (°C).
    #[arg(long, default_value_t = 0.5)]
    pub climate_temp_step: f64,

    /// Serial device for the **Sensor** subsystem (vibration / piezo). Pod **4**: often **`/dev/ttyS2`** (Frozen is `ttyS1`).
    #[arg(long, default_value = "/dev/ttyS2")]
    pub sensor_device: PathBuf,

    /// Sensor line speed. Opensleep Pod **3** firmware uses **115200**; Pod **4** often matches **38400** on `ttyS2` for coherent `0x7E` RX — override if needed (`115200`, etc.).
    #[arg(long, default_value_t = 38400)]
    pub sensor_baud: u32,

    /// Do not open the Sensor UART — no vibration MQTT buttons, no capacitance presence.
    #[arg(long, default_value_t = false)]
    pub no_vibration: bool,

    /// Do not publish MQTT **occupancy** from Sensor capacitance (`0x33`). Vibration is unchanged.
    #[arg(long, default_value_t = false)]
    pub no_presence_detection: bool,

    /// Do not publish MQTT **Water Tank** from Frozen `0x07` messages (`FW: water …`, opensleep parity).
    #[arg(long, default_value_t = false)]
    pub no_water_tank_sensor: bool,

    /// Minimum **maximum** raw capacitance among the three Sensor zones on one mattress side to report that side as **occupied**. Lower if presence never triggers; raise if it sticks ON when empty (`RUST_LOG` `trace` shows zone values). Unused after successful **MQTT calibrate presence** (opensleep baseline + Δ + debounce applies).
    #[arg(long, default_value_t = 800)]
    pub presence_cap_threshold: u16,

    /// Duration (seconds) to average capacitance **with the mattress empty**, after MQTT **Calibrate presence** (opensleep `CALIBRATION_DURATION` = 10 s).
    #[arg(long, default_value_t = 10)]
    pub presence_calibrate_secs: u64,

    /// `<object_id>` for `homeassistant/button/...` (presence baseline calibration — empty bed during the calibration window).
    #[arg(long, default_value = "narcolepsy_calibrate_presence")]
    pub discovery_object_id_calibrate_presence: String,

    /// Skip Sensor **bootloader handshake** (38400: Ping + JumpToFirmware, then `--sensor-baud`). Opensleep does this before firmware traffic; disable only if your MCU is already in firmware-only mode and the handshake causes trouble.
    #[arg(long, default_value_t = false)]
    pub no_sensor_bootloader_handshake: bool,

    /// Do **not** wait for `0xAE` VibrationEnabled between piezo priming and SetAlarm — send all five frames back-to-back. Use when Sensor RX never shows opensleep `0x7E` framing (wrong baud / Pod 4 differences) but TX might still drive the piezo.
    #[arg(long, default_value_t = false)]
    pub sensor_vibrate_no_ack_wait: bool,

    /// Prepend opensleep-style **alarm cancel** (`SetAlarm` intensity/duration 0) before piezo priming. Try **`false`** (default) when debugging **`AlarmSet` status 2** — the first `0xAC` line in logs may be the cancel frame’s ack, not the real alarm.
    #[arg(long, default_value_t = false)]
    pub sensor_vibrate_cancel_preamble: bool,

    /// `<object_id>` for `homeassistant/button/<object_id>/config` (vibrate left).
    #[arg(long, default_value = "narcolepsy_vibrate_left")]
    pub discovery_object_id_vibrate_left: String,

    /// `<object_id>` for `homeassistant/button/<object_id>/config` (vibrate right).
    #[arg(long, default_value = "narcolepsy_vibrate_right")]
    pub discovery_object_id_vibrate_right: String,

    /// `<object_id>` for `homeassistant/binary_sensor/<object_id>/config` (Frozen **Prime** window + optional vibrate piezo priming).
    #[arg(long, default_value = "narcolepsy_priming")]
    pub discovery_object_id_priming: String,

    /// `<object_id>` for occupancy (left side), from Sensor capacitance zones 0–2.
    #[arg(long, default_value = "narcolepsy_presence_left")]
    pub discovery_object_id_presence_left: String,

    /// `<object_id>` for occupancy (right side), from Sensor capacitance zones 3–5.
    #[arg(long, default_value = "narcolepsy_presence_right")]
    pub discovery_object_id_presence_right: String,

    /// `<object_id>` for `homeassistant/binary_sensor/.../config` (Frozen reservoir present).
    #[arg(long, default_value = "narcolepsy_water_tank")]
    pub discovery_object_id_water_tank: String,

    /// `<object_id>` for `homeassistant/sensor/<object_id>/config` (Frozen current temperature, left).
    #[arg(long, default_value = "narcolepsy_temp_left")]
    pub discovery_object_id_temp_left: String,

    /// `<object_id>` for `homeassistant/sensor/<object_id>/config` (Frozen current temperature, right).
    #[arg(long, default_value = "narcolepsy_temp_right")]
    pub discovery_object_id_temp_right: String,

    /// `<object_id>` for `homeassistant/sensor/<object_id>/config` (Frozen heatsink temperature).
    #[arg(long, default_value = "narcolepsy_heatsink_temp")]
    pub discovery_object_id_heatsink_temp: String,

    /// `<object_id>` for `homeassistant/sensor/<object_id>/config` (climate target temperature, left).
    #[arg(long, default_value = "narcolepsy_target_temp_left")]
    pub discovery_object_id_target_temp_left: String,

    /// `<object_id>` for `homeassistant/sensor/<object_id>/config` (climate target temperature, right).
    #[arg(long, default_value = "narcolepsy_target_temp_right")]
    pub discovery_object_id_target_temp_right: String,

    /// Default vibration intensity (1–100) for MQTT vibrate buttons.
    #[arg(long, default_value_t = 64)]
    pub vibration_intensity: u8,

    /// Default duration (seconds) for MQTT vibrate buttons.
    #[arg(long, default_value_t = 15)]
    pub vibration_duration_sec: u32,

    /// Default vibration pattern for MQTT vibrate buttons.
    #[arg(long, value_enum, default_value_t = VibrationPatternArg::Single)]
    pub vibration_pattern: VibrationPatternArg,

    /// `tracing` filter (e.g. `debug`, `info,narcolepsy=debug`).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}
