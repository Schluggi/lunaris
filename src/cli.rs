//! Command-line interface (MQTT broker + serial port). No config file.

use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "narcolepsy")]
#[command(
    about = "Local MQTT bridge: Frozen USART (prime, mattress climate left/right) + Pod LED (IS31FL3194 I²C), Home Assistant MQTT discovery."
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

    /// `tracing` filter (e.g. `debug`, `info,narcolepsy=debug`).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}
