//! Command-line interface (MQTT broker + serial port). No standalone config **file** —
//! on Pod OS, Frozen/Sensor **`/dev/tty…`** defaults can be patched from **`/opt/eight/config/machine.json`**
//! when [`crate::machine_config`] applies (CLI flags still win).

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// Vibration pattern for [`Cli::vibration_pattern`] (Sensor `SetAlarm` / opensleep `AlarmPattern`).
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum VibrationPatternArg {
    #[default]
    Single,
    Double,
}

/// Pod generation for default USART speeds ([`Cli::effective_serial_baud`] / [`Cli::effective_sensor_baud`]) when baud flags are omitted.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PodModel {
    /// Opensleep-style defaults: Frozen **38400**, Sensor firmware **115200** after bootloader jump.
    #[value(name = "3")]
    Three,
    /// Community Pod **4**: Frozen **38400**, Sensor **921600** (stock / Frankenfirmware firmware speed after bootloader jump). Use **`--sensor-baud`** to override (e.g. **38400** when testing).
    #[value(name = "4")]
    Four,
    /// Pod **5**: same default USART speeds as Pod **4** (**38400** / **921600**).
    #[value(name = "5")]
    Five,
}

impl PodModel {
    fn default_frozen_baud(self) -> u32 {
        38400
    }

    fn default_sensor_baud(self) -> u32 {
        match self {
            PodModel::Three => 115200,
            PodModel::Four | PodModel::Five => 921600,
        }
    }

    /// MQTT discovery `device.model` (matches [`--pod`](Cli::pod)).
    pub fn homeassistant_device_model(self) -> &'static str {
        match self {
            PodModel::Three => "Eight Sleep Pod 3",
            PodModel::Four => "Eight Sleep Pod 4",
            PodModel::Five => "Eight Sleep Pod 5",
        }
    }
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

    #[arg(long, default_value = "Eight Sleep")]
    pub device_name: String,

    /// Stable ID for Home Assistant device registry (`device.identifiers`).
    #[arg(long, default_value = "narcolepsy_pod")]
    pub device_identifier: String,

    /// Serial device for the **Frozen** subsystem.
    ///
    /// Default **`/dev/ttyS1`** is overridden by **`frozenPort`** in **`/opt/eight/config/machine.json`**
    /// when present and **`--serial-device`** is **not** on the command line.
    ///
    /// Pod **4** / **5** (community): often **`ttyS1`/`ttyS2`** ([opensleep#11](https://github.com/LiamSnow/opensleep/issues/11));
    /// Pod **3** / opensleep paths often **`/dev/ttymxc2`** for Frozen.
    ///
    /// Must be openable at startup; otherwise the process exits before MQTT connects.
    #[arg(long, default_value = "/dev/ttyS1")]
    pub serial_device: PathBuf,

    /// Pod generation (**required**): sets default **`--serial-baud`** / **`--sensor-baud`** when those flags are omitted (**4** / **5** = **38400** / **921600**; **3** = **38400** / **115200** opensleep). Explicit **`--serial-baud`** or **`--sensor-baud`** always override.
    #[arg(long, value_name = "MODEL")]
    pub pod: PodModel,

    /// Serial line speed for Frozen (bits/s). Omit to use **`--pod`** default (**38400** for Pod **3**, **4**, and **5**).
    #[arg(long)]
    pub serial_baud: Option<u32>,

    /// Payload Home Assistant sends when the button is pressed (see discovery `payload_press`).
    #[arg(long, default_value = "PRESS")]
    pub payload_press: String,

    /// Linux I²C bus device for the IS31FL3194 LED controller (address `0x53`).
    #[arg(long, default_value = "/dev/i2c-1")]
    pub i2c_device: PathBuf,

    /// Minimum target temperature (°C) published in MQTT climate discovery.
    #[arg(long, default_value_t = 13.0)]
    pub climate_min_temp: f64,

    /// Maximum target temperature (°C) published in MQTT climate discovery.
    #[arg(long, default_value_t = 47.0)]
    pub climate_max_temp: f64,

    /// Step between temperature adjustments in Home Assistant (°C).
    #[arg(long, default_value_t = 0.5)]
    pub climate_temp_step: f64,

    /// Serial device for the **Sensor** subsystem (vibration / piezo).
    ///
    /// Default **`/dev/ttyS2`** is overridden by **`sensorPort`** in **`/opt/eight/config/machine.json`**
    /// when present and **`--sensor-device`** is **not** on the command line.
    #[arg(long, default_value = "/dev/ttyS2")]
    pub sensor_device: PathBuf,

    /// Sensor line speed (bits/s). Omit to use **`--pod`** defaults. Pod **3**: **115200**; Pod **4** / **5**: **921600**; pass **`--sensor-baud`** (e.g. **38400**) to override.
    #[arg(long)]
    pub sensor_baud: Option<u32>,

    /// Minimum **maximum** raw capacitance among the three Sensor zones on one mattress side to report that side as **occupied**. Lower if presence never triggers; raise if it sticks ON when empty (`RUST_LOG` `trace` shows zone values). Unused after successful **MQTT calibrate presence** (opensleep baseline + Δ + debounce applies). **Runtime:** Home Assistant MQTT **Presence Cap Threshold** number (same bounds as opensleep tuning).
    #[arg(long, default_value_t = 800)]
    pub presence_cap_threshold: u16,

    /// Δ above calibrated per-zone baseline before a zone counts toward occupancy (opensleep `DEFAULT_THRESHOLD` = **50**). Only used **after** **MQTT Calibrate presence**. **Runtime:** Home Assistant MQTT **Presence Baseline Delta** number.
    #[arg(long, default_value_t = 50)]
    pub presence_baseline_delta: u16,

    /// Duration (seconds) to average capacitance **with the mattress empty**, after MQTT **Calibrate presence** (opensleep `CALIBRATION_DURATION` = 10 s).
    #[arg(long, default_value_t = 10)]
    pub presence_calibrate_secs: u64,

    /// Log every capacitance sample used for occupancy at **INFO** (zones, threshold / baselines, debounce, computed side occupancy). Implies noise if many `0x33` frames; disable after debugging.
    #[arg(long, default_value_t = false)]
    pub presence_debug: bool,

    /// Do **not** wait for `0xAE` VibrationEnabled between piezo priming and SetAlarm — send all five frames back-to-back. Use when Sensor RX never shows opensleep `0x7E` framing (wrong baud / Pod 4 differences) but TX might still drive the piezo.
    #[arg(long, default_value_t = false)]
    pub sensor_vibrate_no_ack_wait: bool,

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

impl Cli {
    /// Effective Frozen USART baud: **`--serial-baud`** if set, else **`--pod`** default (**38400** for all pod models).
    pub fn effective_serial_baud(&self) -> u32 {
        self.serial_baud
            .unwrap_or_else(|| self.pod.default_frozen_baud())
    }

    /// Effective Sensor USART baud: **`--sensor-baud`** if set, else **`--pod`** default (**3** → **115200**, **4** / **5** → **921600**).
    pub fn effective_sensor_baud(&self) -> u32 {
        self.sensor_baud
            .unwrap_or_else(|| self.pod.default_sensor_baud())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_bauds_default_match_pod4() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "4"]);
        assert_eq!(cli.effective_serial_baud(), 38400);
        assert_eq!(cli.effective_sensor_baud(), 921600);
    }

    #[test]
    fn pod3_sets_sensor_115200() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "3"]);
        assert_eq!(cli.effective_serial_baud(), 38400);
        assert_eq!(cli.effective_sensor_baud(), 115200);
    }

    #[test]
    fn explicit_baud_overrides_pod3() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "3", "--sensor-baud", "38400"]);
        assert_eq!(cli.effective_sensor_baud(), 38400);
    }

    #[test]
    fn pod4_explicit_same_as_default() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "4"]);
        assert_eq!(cli.effective_serial_baud(), 38400);
        assert_eq!(cli.effective_sensor_baud(), 921600);
    }

    #[test]
    fn pod4_sensor_baud_can_be_overridden_to_38400() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "4", "--sensor-baud", "38400"]);
        assert_eq!(cli.effective_sensor_baud(), 38400);
    }

    #[test]
    fn pod5_matches_pod4_bauds() {
        let cli = Cli::parse_from(["narcolepsy", "--pod", "5"]);
        assert_eq!(cli.effective_serial_baud(), 38400);
        assert_eq!(cli.effective_sensor_baud(), 921600);
    }

    #[test]
    fn pod_model_homeassistant_device_model() {
        assert_eq!(PodModel::Three.homeassistant_device_model(), "Eight Sleep Pod 3");
        assert_eq!(PodModel::Four.homeassistant_device_model(), "Eight Sleep Pod 4");
        assert_eq!(PodModel::Five.homeassistant_device_model(), "Eight Sleep Pod 5");
    }
}
