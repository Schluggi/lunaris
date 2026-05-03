//! Command-line interface (MQTT broker + serial port). No config file.

use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "narcolepsy")]
#[command(
    about = "Local MQTT bridge: Home Assistant button → Frozen USART prime (opensleep-compatible)."
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

    /// Prefix for device topics (`{prefix}/availability`, `{prefix}/button/prime/set`, …).
    #[arg(long, default_value = "eight/pod4")]
    pub topic_prefix: String,

    /// Home Assistant MQTT discovery prefix (usually `homeassistant`).
    #[arg(long, default_value = "homeassistant")]
    pub discovery_prefix: String,

    /// `<object_id>` in `homeassistant/button/<object_id>/config`.
    #[arg(long, default_value = "narcolepsy_prime")]
    pub discovery_object_id: String,

    #[arg(long, default_value = "Eight Pod")]
    pub device_name: String,

    /// Stable ID for Home Assistant device registry (`device.identifiers`).
    #[arg(long, default_value = "narcolepsy_pod")]
    pub device_identifier: String,

    /// Serial device for the Frozen subsystem (Pod 3 opensleep default).
    #[arg(long, default_value = "/dev/ttymxc2")]
    pub serial_device: PathBuf,

    #[arg(long, default_value_t = 38400)]
    pub serial_baud: u32,

    /// Payload Home Assistant sends when the button is pressed (see discovery `payload_press`).
    #[arg(long, default_value = "PRESS")]
    pub payload_press: String,

    /// `tracing` filter (e.g. `debug`, `info,narcolepsy=debug`).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}
