//! MQTT: Home Assistant discovery, prime, per-side vibration (Sensor), optional capacitance **presence**,
//! optional Frozen **water tank** status, mattress climate, JSON light (I²C).
//!
//! **rumqttc:** subscribe/publish must not block the task that runs [`rumqttc::EventLoop::poll`].
//! Outbound work runs in [`tokio::spawn`] so the event loop keeps draining requests (see upstream docs on [`AsyncClient`]).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, LastWill, MqttOptions, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{interval, sleep, Duration, Instant};

use crate::cli::Cli;
use crate::deviceinfo;
use crate::frozen_frame::{get_temperatures_frame, set_target_temperature_frame, BedSide};
use crate::frozen_rx::FrozenTemperatureUpdate;
use crate::is31fl3194::{shutdown_led, Is31fl3194};
use crate::sensor_frame::{vibration_sequence_frames, AlarmPattern};
use crate::sensor_link::{PrimingCounts, PrimingEvent};
use crate::sensor_rx::SensorCapacitanceZones;
use crate::serial_prime;

const HA_STATUS_TOPIC: &str = "homeassistant/status";
/// MQTT text sensor state topics (leading `/` — not under [`BridgeConfig::topic_prefix`]).
const DEVICEINFO_DEVICE_LABEL_STATE_TOPIC: &str = "/deviceinfo/device-label";
const DEVICEINFO_DEVICE_ID_STATE_TOPIC: &str = "/deviceinfo/device-id";
const DISCOVERY_OBJECT_ID_DEVICEINFO_LABEL: &str = "narcolepsy_deviceinfo_device_label";
const DISCOVERY_OBJECT_ID_DEVICEINFO_ID: &str = "narcolepsy_deviceinfo_device_id";
/// After HA **Prime**, MQTT **Priming** stays **ON** this long (no priming-complete frame on USART; aligns with ~8 min 30 s hub prime).
const FROZEN_PRIME_SHOWN_AFTER_PRESS: Duration = Duration::from_secs(8 * 60 + 30);
/// Home Assistant [HVACMode](https://developers.home-assistant.io/docs/core/entity/climate#hvac-modes) for active regulation.
const CLIMATE_MODE_HEAT_COOL: &str = "heat_cool";
const CLIMATE_MODE_OFF: &str = "off";

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_username: Option<String>,
    pub mqtt_password: Option<String>,
    pub mqtt_client_id: String,
    pub topic_prefix: String,
    pub discovery_prefix: String,
    pub discovery_object_id: String,
    pub discovery_object_id_request_temperatures: String,
    pub discovery_object_id_led: String,
    pub discovery_object_id_startup_led: String,
    pub discovery_object_id_climate_left: String,
    pub discovery_object_id_climate_right: String,
    pub climate_min_temp: f64,
    pub climate_max_temp: f64,
    pub climate_temp_step: f64,
    pub device_name: String,
    pub device_identifier: String,
    pub sw_version: String,
    pub payload_press: String,
    pub serial_device: std::path::PathBuf,
    pub serial_baud: u32,
    /// `None` → LED feature disabled (`--no-led` or I²C probe failed).
    pub i2c_device: Option<PathBuf>,
    /// `None` → vibration MQTT buttons disabled (`--no-vibration` or Sensor UART probe failed).
    pub sensor_device: Option<PathBuf>,
    pub sensor_baud: u32,
    pub discovery_object_id_vibrate_left: String,
    pub discovery_object_id_vibrate_right: String,
    pub discovery_object_id_priming: String,
    pub discovery_object_id_temp_left: String,
    pub discovery_object_id_temp_right: String,
    pub discovery_object_id_heatsink_temp: String,
    pub discovery_object_id_target_temp_left: String,
    pub discovery_object_id_target_temp_right: String,
    pub discovery_object_id_presence_left: String,
    pub discovery_object_id_presence_right: String,
    pub discovery_object_id_calibrate_presence: String,
    pub discovery_object_id_water_tank: String,
    pub discovery_object_id_firmware_message: String,
    /// Uncalibrated: side occupied if **`max`** of three zones `>= threshold`. After MQTT calibration: opensleep baseline + Δ + debounce.
    pub presence_cap_threshold: u16,
    /// Window for MQTT **Calibrate presence** (average samples → baseline per zone; opensleep default 10.).
    pub presence_calibrate_secs: u64,
    /// MQTT occupancy from Sensor capacitance (needs open Sensor UART; see [`crate::main`]).
    pub presence_discovery: bool,
    /// Log each `0x33` presence inference step at INFO ([`Cli::presence_debug`](crate::cli::Cli::presence_debug)).
    pub presence_debug: bool,
    pub vibration_intensity: u8,
    pub vibration_duration_sec: u32,
    pub vibration_pattern: AlarmPattern,
    /// Prepends cancel `SetAlarm` before piezo + alarm (`--sensor-vibrate-cancel-preamble`).
    pub sensor_vibrate_cancel_preamble: bool,
    /// When set, Frozen frames are queued to [`crate::frozen_link`] instead of opening the port per command.
    pub frozen_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// When set, vibration batches go to [`crate::sensor_link`].
    pub sensor_tx: Option<mpsc::Sender<Vec<Vec<u8>>>>,
    /// Present when [`crate::sensor_link`] runs — interactive (vibrate) piezo priming refcount (combined with Frozen Prime for MQTT **Priming**).
    pub sensor_priming_counts: Option<Arc<PrimingCounts>>,
    /// Publish MQTT sensors for Frozen inbound temperatures (`0x41` / `0xC1`); set by [`crate::main`] with [`crate::frozen_link`].
    pub frozen_temperature_discovery: bool,
    /// MQTT **Water Tank** binary_sensor from Frozen `0x07` messages ([`crate::frozen_rx`]).
    pub frozen_water_tank_discovery: bool,
    /// MQTT text **sensor** from Frozen `0x07` UTF‑8 message bodies (all lines, not only tank).
    pub frozen_firmware_message_discovery: bool,
}

impl BridgeConfig {
    pub fn from_cli(cli: &Cli) -> Self {
        Self {
            mqtt_host: cli.mqtt_host.clone(),
            mqtt_port: cli.mqtt_port,
            mqtt_username: cli.mqtt_username.clone(),
            mqtt_password: cli.mqtt_password.clone(),
            mqtt_client_id: cli.mqtt_client_id.clone(),
            topic_prefix: cli.topic_prefix.clone(),
            discovery_prefix: cli.discovery_prefix.clone(),
            discovery_object_id: cli.discovery_object_id.clone(),
            discovery_object_id_request_temperatures: cli
                .discovery_object_id_request_temperatures
                .clone(),
            discovery_object_id_led: cli.discovery_object_id_led.clone(),
            discovery_object_id_startup_led: cli.discovery_object_id_startup_led.clone(),
            discovery_object_id_climate_left: cli.discovery_object_id_climate_left.clone(),
            discovery_object_id_climate_right: cli.discovery_object_id_climate_right.clone(),
            climate_min_temp: cli.climate_min_temp,
            climate_max_temp: cli.climate_max_temp,
            climate_temp_step: cli.climate_temp_step,
            device_name: cli.device_name.clone(),
            device_identifier: cli.device_identifier.clone(),
            sw_version: env!("CARGO_PKG_VERSION").to_string(),
            payload_press: cli.payload_press.clone(),
            serial_device: cli.serial_device.clone(),
            serial_baud: cli.serial_baud,
            i2c_device: None,
            sensor_device: None,
            sensor_baud: cli.sensor_baud,
            discovery_object_id_vibrate_left: cli.discovery_object_id_vibrate_left.clone(),
            discovery_object_id_vibrate_right: cli.discovery_object_id_vibrate_right.clone(),
            discovery_object_id_priming: cli.discovery_object_id_priming.clone(),
            discovery_object_id_temp_left: cli.discovery_object_id_temp_left.clone(),
            discovery_object_id_temp_right: cli.discovery_object_id_temp_right.clone(),
            discovery_object_id_heatsink_temp: cli.discovery_object_id_heatsink_temp.clone(),
            discovery_object_id_target_temp_left: cli.discovery_object_id_target_temp_left.clone(),
            discovery_object_id_target_temp_right: cli
                .discovery_object_id_target_temp_right
                .clone(),
            discovery_object_id_presence_left: cli.discovery_object_id_presence_left.clone(),
            discovery_object_id_presence_right: cli.discovery_object_id_presence_right.clone(),
            discovery_object_id_calibrate_presence: cli
                .discovery_object_id_calibrate_presence
                .clone(),
            discovery_object_id_water_tank: cli.discovery_object_id_water_tank.clone(),
            discovery_object_id_firmware_message: cli.discovery_object_id_firmware_message.clone(),
            presence_cap_threshold: cli.presence_cap_threshold,
            presence_calibrate_secs: cli.presence_calibrate_secs.max(3),
            presence_discovery: false,
            presence_debug: cli.presence_debug,
            vibration_intensity: cli.vibration_intensity.clamp(1, 100),
            vibration_duration_sec: cli.vibration_duration_sec.clamp(1, 600),
            vibration_pattern: match cli.vibration_pattern {
                crate::cli::VibrationPatternArg::Single => AlarmPattern::Single,
                crate::cli::VibrationPatternArg::Double => AlarmPattern::Double,
            },
            sensor_vibrate_cancel_preamble: cli.sensor_vibrate_cancel_preamble,
            frozen_tx: None,
            sensor_tx: None,
            sensor_priming_counts: None,
            frozen_temperature_discovery: false,
            frozen_water_tank_discovery: false,
            frozen_firmware_message_discovery: false,
        }
    }

    pub fn availability_topic(&self) -> String {
        format!("{}/availability", self.topic_prefix)
    }

    pub fn command_topic(&self) -> String {
        format!("{}/button/prime/set", self.topic_prefix)
    }

    pub fn discovery_topic(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, self.discovery_object_id
        )
    }

    pub fn request_get_temperatures_command_topic(&self) -> String {
        format!("{}/button/request_get_temperatures/set", self.topic_prefix)
    }

    pub fn discovery_topic_request_get_temperatures(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, self.discovery_object_id_request_temperatures
        )
    }

    pub fn discovery_topic_light(&self) -> String {
        format!(
            "{}/light/{}/config",
            self.discovery_prefix, self.discovery_object_id_led
        )
    }

    pub fn light_command_topic(&self) -> String {
        format!("{}/light/led/set", self.topic_prefix)
    }

    pub fn light_state_topic(&self) -> String {
        format!("{}/light/led/state", self.topic_prefix)
    }

    pub fn discovery_topic_startup_led(&self) -> String {
        format!(
            "{}/switch/{}/config",
            self.discovery_prefix, self.discovery_object_id_startup_led
        )
    }

    pub fn startup_led_command_topic(&self) -> String {
        format!("{}/switch/startup_led/set", self.topic_prefix)
    }

    pub fn startup_led_state_topic(&self) -> String {
        format!("{}/switch/startup_led/state", self.topic_prefix)
    }

    pub fn climate_discovery_topic(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => &self.discovery_object_id_climate_left,
            BedSide::Right => &self.discovery_object_id_climate_right,
        };
        format!("{}/climate/{}/config", self.discovery_prefix, id)
    }

    pub fn climate_mode_command_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/climate/{}/mode/set", self.topic_prefix, s)
    }

    pub fn climate_mode_state_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/climate/{}/mode/state", self.topic_prefix, s)
    }

    pub fn climate_temperature_command_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/climate/{}/temperature/set", self.topic_prefix, s)
    }

    pub fn climate_temperature_state_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/climate/{}/temperature/state", self.topic_prefix, s)
    }

    pub fn vibrate_discovery_topic(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => &self.discovery_object_id_vibrate_left,
            BedSide::Right => &self.discovery_object_id_vibrate_right,
        };
        format!("{}/button/{}/config", self.discovery_prefix, id)
    }

    pub fn vibrate_command_topic(&self, side: BedSide) -> String {
        let suffix = match side {
            BedSide::Left => "vibrate_left",
            BedSide::Right => "vibrate_right",
        };
        format!("{}/button/{}/set", self.topic_prefix, suffix)
    }

    pub fn discovery_topic_priming(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, self.discovery_object_id_priming
        )
    }

    pub fn priming_state_topic(&self) -> String {
        format!("{}/binary_sensor/priming/state", self.topic_prefix)
    }

    /// Inbound Frozen `0x41` / `0xC1` centidegrees — current water temperature (one side of the cover).
    pub fn frozen_current_temp_state_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/sensor/cover_temperature_{}/state", self.topic_prefix, s)
    }

    /// Inbound Frozen `0x41` / `0xC1` — thermoelectric heatsink temperature.
    pub fn frozen_heatsink_temp_state_topic(&self) -> String {
        format!("{}/sensor/heatsink_temp/state", self.topic_prefix)
    }

    pub fn discovery_topic_frozen_current_temp(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => &self.discovery_object_id_temp_left,
            BedSide::Right => &self.discovery_object_id_temp_right,
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn discovery_topic_frozen_heatsink_temp(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, self.discovery_object_id_heatsink_temp
        )
    }

    /// MQTT bridge climate target (°C), mirrors `temperature_state_topic` for dashboards.
    pub fn target_temperature_state_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!(
            "{}/sensor/target_temperature_{}/state",
            self.topic_prefix, s
        )
    }

    pub fn discovery_topic_target_temperature(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => &self.discovery_object_id_target_temp_left,
            BedSide::Right => &self.discovery_object_id_target_temp_right,
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn presence_state_topic(&self, side: BedSide) -> String {
        let s = match side {
            BedSide::Left => "left",
            BedSide::Right => "right",
        };
        format!("{}/binary_sensor/presence_{}/state", self.topic_prefix, s)
    }

    pub fn discovery_topic_presence(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => &self.discovery_object_id_presence_left,
            BedSide::Right => &self.discovery_object_id_presence_right,
        };
        format!("{}/binary_sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn calibrate_presence_command_topic(&self) -> String {
        format!("{}/button/calibrate_presence/set", self.topic_prefix)
    }

    pub fn discovery_topic_calibrate_presence(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, self.discovery_object_id_calibrate_presence
        )
    }

    pub fn water_tank_state_topic(&self) -> String {
        format!("{}/binary_sensor/water_tank/state", self.topic_prefix)
    }

    pub fn discovery_topic_water_tank(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, self.discovery_object_id_water_tank
        )
    }

    pub fn firmware_message_state_topic(&self) -> String {
        format!("{}/sensor/firmware_message/state", self.topic_prefix)
    }

    pub fn discovery_topic_firmware_message(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, self.discovery_object_id_firmware_message
        )
    }

    pub fn discovery_topic_deviceinfo_device_label(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_DEVICEINFO_LABEL
        )
    }

    pub fn discovery_topic_deviceinfo_device_id(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_DEVICEINFO_ID
        )
    }

    pub fn result_topic(&self) -> String {
        format!("{}/result", self.topic_prefix)
    }

    fn device_json(&self) -> serde_json::Value {
        json!({
            "identifiers": [self.device_identifier.clone()],
            "name": self.device_name,
            "model": "Eight Sleep Pod",
            "sw_version": self.sw_version,
        })
    }

    fn availability_json(&self) -> serde_json::Value {
        json!([{
            "topic": self.availability_topic(),
            "payload_available": "online",
            "payload_not_available": "offline",
        }])
    }
}

async fn enqueue_frozen_frame(config: &BridgeConfig, frame: Vec<u8>) -> Result<(), String> {
    if let Some(tx) = &config.frozen_tx {
        tx.send(frame)
            .await
            .map_err(|e| format!("frozen UART task disconnected: {e:?}"))
    } else {
        serial_prime::send_frame(&config.serial_device, config.serial_baud, &frame)
            .await
            .map_err(|e| e.to_string())
    }
}

async fn enqueue_sensor_vibration(
    config: &BridgeConfig,
    frames: Vec<Vec<u8>>,
) -> Result<(), String> {
    if let Some(tx) = &config.sensor_tx {
        tx.send(frames)
            .await
            .map_err(|e| format!("sensor UART task disconnected: {e:?}"))
    } else if let Some(ref path) = config.sensor_device {
        serial_prime::send_frames(path, config.sensor_baud, &frames)
            .await
            .map_err(|e| e.to_string())
    } else {
        Err("sensor UART disabled".into())
    }
}

fn discovery_payload_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Prime",
        "command_topic": config.command_topic(),
        "payload_press": config.payload_press,
        "unique_id": format!("{}_prime_button", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_request_get_temperatures_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Request Temperatures",
        "command_topic": config.request_get_temperatures_command_topic(),
        "payload_press": config.payload_press,
        "entity_category": "diagnostic",
        "unique_id": format!("{}_request_get_temperatures", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_startup_led_switch(config: &BridgeConfig) -> String {
    json!({
        "name": "Startup LED",
        "command_topic": config.startup_led_command_topic(),
        "state_topic": config.startup_led_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "unique_id": format!("{}_startup_led", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_light(config: &BridgeConfig) -> String {
    json!({
        "name": "LED",
        "unique_id": format!("{}_led", config.device_identifier),
        "schema": "json",
        "supported_color_modes": ["rgb"],
        "brightness": true,
        "brightness_scale": 255,
        "command_topic": config.light_command_topic(),
        "state_topic": config.light_state_topic(),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_climate(config: &BridgeConfig, side: BedSide) -> String {
    let name = match side {
        BedSide::Left => "Cover Left",
        BedSide::Right => "Cover Right",
    };
    let unique_suffix = match side {
        BedSide::Left => "climate_left",
        BedSide::Right => "climate_right",
    };
    json!({
        "name": name,
        "unique_id": format!("{}_{}", config.device_identifier, unique_suffix),
        "temperature_unit": "C",
        "min_temp": config.climate_min_temp,
        "max_temp": config.climate_max_temp,
        "temp_step": config.climate_temp_step,
        "precision": 0.1,
        "modes": [CLIMATE_MODE_OFF, CLIMATE_MODE_HEAT_COOL],
        "mode_command_topic": config.climate_mode_command_topic(side),
        "mode_state_topic": config.climate_mode_state_topic(side),
        "temperature_command_topic": config.climate_temperature_command_topic(side),
        "temperature_state_topic": config.climate_temperature_state_topic(side),
        "current_temperature_topic": config.frozen_current_temp_state_topic(side),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibrate_button(config: &BridgeConfig, side: BedSide) -> String {
    let name = match side {
        BedSide::Left => "Vibrate mattress (left)",
        BedSide::Right => "Vibrate mattress (right)",
    };
    let unique_suffix = match side {
        BedSide::Left => "vibrate_left",
        BedSide::Right => "vibrate_right",
    };
    json!({
        "name": name,
        "command_topic": config.vibrate_command_topic(side),
        "payload_press": config.payload_press,
        "unique_id": format!("{}_{}", config.device_identifier, unique_suffix),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_priming(config: &BridgeConfig) -> String {
    json!({
        "name": "Priming",
        "state_topic": config.priming_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "unique_id": format!("{}_priming", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_water_tank(config: &BridgeConfig) -> String {
    json!({
        "name": "Water Tank",
        "state_topic": config.water_tank_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "plug",
        "icon": "mdi:water",
        "entity_category": "diagnostic",
        "unique_id": format!("{}_water_tank", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_firmware_message(config: &BridgeConfig) -> String {
    json!({
        "name": "Firmware Message",
        "state_topic": config.firmware_message_state_topic(),
        "icon": "mdi:message-text",
        "entity_category": "diagnostic",
        "enabled_by_default": false,
        "unique_id": format!("{}_firmware_message", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_deviceinfo_device_label(config: &BridgeConfig) -> String {
    json!({
        "name": "Device Label",
        "state_topic": DEVICEINFO_DEVICE_LABEL_STATE_TOPIC,
        "icon": "mdi:label",
        "entity_category": "diagnostic",
        "enabled_by_default": false,
        "unique_id": format!("{}_deviceinfo_device_label", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_deviceinfo_device_id(config: &BridgeConfig) -> String {
    json!({
        "name": "Device ID",
        "state_topic": DEVICEINFO_DEVICE_ID_STATE_TOPIC,
        "icon": "mdi:identifier",
        "entity_category": "diagnostic",
        "enabled_by_default": false,
        "unique_id": format!("{}_deviceinfo_device_id", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_presence(config: &BridgeConfig, side: BedSide) -> String {
    let (name, unique_suffix) = match side {
        BedSide::Left => ("Presence Left", "presence_left"),
        BedSide::Right => ("Presence Right", "presence_right"),
    };
    json!({
        "name": name,
        "state_topic": config.presence_state_topic(side),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "occupancy",
        "unique_id": format!("{}_{}", config.device_identifier, unique_suffix),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_calibrate_presence_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Calibrate presence",
        "command_topic": config.calibrate_presence_command_topic(),
        "payload_press": config.payload_press,
        "unique_id": format!("{}_calibrate_presence", config.device_identifier),
        "entity_category": "config",
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// opensleep [`PresenceConfig.threshold`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/presence.rs) (`DEFAULT_THRESHOLD`).
const PRESENCE_BASELINE_DELTA: u16 = 50;

/// opensleep [`PresenceConfig.debounce_count`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/presence.rs) (`DEFAULT_DEBOUNCE`).
const PRESENCE_DEBOUNCE_FRAMES: u8 = 5;

#[derive(Clone, Copy, Debug, Default)]
struct PresenceInferenceState {
    baselines: Option<[u16; 6]>,
    debounce: [u8; 6],
}

impl PresenceInferenceState {
    fn set_baselines(&mut self, b: [u16; 6]) {
        self.baselines = Some(b);
        self.debounce = [0; 6];
    }
}

fn mean_baselines_from_samples(samples: &[[u16; 6]]) -> Option<[u16; 6]> {
    if samples.is_empty() {
        return None;
    }
    let mut sums = [0u32; 6];
    for s in samples {
        for i in 0..6 {
            sums[i] += s[i] as u32;
        }
    }
    let n = samples.len() as u32;
    Some(sums.map(|sum| (sum / n) as u16))
}

/// Uncalibrated: max-of-three zones vs absolute threshold (`--presence-cap-threshold`).
fn occupancy_uncalibrated_max(z: &SensorCapacitanceZones, abs_threshold: u16) -> (bool, bool) {
    let left_max = z.zones[..3].iter().copied().max().unwrap_or(0);
    let right_max = z.zones[3..].iter().copied().max().unwrap_or(0);
    (left_max >= abs_threshold, right_max >= abs_threshold)
}

/// Calibrated: opensleep [`PresenseManager::update_presence`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/presence.rs).
fn occupancy_calibrated(
    z: &SensorCapacitanceZones,
    inference: &mut PresenceInferenceState,
) -> (bool, bool) {
    let Some(ref baselines) = inference.baselines else {
        return (false, false);
    };
    for (i, &b) in baselines.iter().enumerate() {
        if z.zones[i] > b.saturating_add(PRESENCE_BASELINE_DELTA) {
            inference.debounce[i] = inference.debounce[i].saturating_add(1);
        } else {
            inference.debounce[i] = 0;
        }
    }
    let left = inference.debounce[..3]
        .iter()
        .any(|&c| c >= PRESENCE_DEBOUNCE_FRAMES);
    let right = inference.debounce[3..]
        .iter()
        .any(|&c| c >= PRESENCE_DEBOUNCE_FRAMES);
    (left, right)
}

fn inference_occupancy(
    z: &SensorCapacitanceZones,
    inference: &mut PresenceInferenceState,
    fallback_abs_threshold: u16,
) -> (bool, bool) {
    if inference.baselines.is_some() {
        occupancy_calibrated(z, inference)
    } else {
        occupancy_uncalibrated_max(z, fallback_abs_threshold)
    }
}

/// Per-sample diagnostic for [`BridgeConfig::presence_debug`].
fn log_presence_debug(
    z: &SensorCapacitanceZones,
    inference: &PresenceInferenceState,
    fallback_abs_threshold: u16,
    left_on: bool,
    right_on: bool,
    calibrating: bool,
) {
    let left_max = z.zones[..3].iter().copied().max().unwrap_or(0);
    let right_max = z.zones[3..].iter().copied().max().unwrap_or(0);
    match &inference.baselines {
        None => {
            tracing::info!(
                seq = z.sequence,
                zones = ?z.zones,
                left_max,
                right_max,
                threshold = fallback_abs_threshold,
                occupied_left = left_on,
                occupied_right = right_on,
                calibrating,
                "presence debug (uncalibrated: left_max/right_max vs threshold — use MQTT Calibrate presence or lower --presence-cap-threshold)"
            );
        }
        Some(b) => {
            tracing::info!(
                seq = z.sequence,
                zones = ?z.zones,
                baseline = ?b,
                debounce_per_zone = ?inference.debounce,
                delta = PRESENCE_BASELINE_DELTA,
                debounce_need = PRESENCE_DEBOUNCE_FRAMES,
                occupied_left = left_on,
                occupied_right = right_on,
                calibrating,
                "presence debug (calibrated: side ON when any zone on that side exceeds baseline+Δ for debounce_need consecutive frames)"
            );
        }
    }
}

async fn handle_presence_calibrate_press(
    client: &AsyncClient,
    config: &BridgeConfig,
    notify: Option<&mpsc::Sender<()>>,
) {
    let Some(tx) = notify else {
        tracing::warn!("presence calibration: no Sensor/presence bridge (ignored)");
        return;
    };
    if let Err(e) = tx.try_send(()) {
        tracing::warn!(?e, "presence calibration notify dropped (MQTT handler lag)");
        publish_json_result(
            client,
            config,
            "calibrate_presence",
            "error",
            &format!("calibration channel saturated: {e:?}"),
        )
        .await;
        return;
    }
    tracing::info!(
        secs = config.presence_calibrate_secs,
        "presence calibration requested (leave mattress empty until window completes; opensleep-style baseline capture)"
    );
    publish_json_result(
        client,
        config,
        "calibrate_presence",
        "success",
        "calibration sampling started",
    )
    .await;
}

fn discovery_payload_frozen_temperature(
    config: &BridgeConfig,
    name: &str,
    unique_suffix: &str,
    state_topic: String,
) -> String {
    json!({
        "name": name,
        "state_topic": state_topic,
        "unit_of_measurement": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "unique_id": format!("{}_{}", config.device_identifier, unique_suffix),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

async fn publish_presence_readings(
    client: &AsyncClient,
    config: &BridgeConfig,
    left_on: bool,
    right_on: bool,
) {
    let qos = QoS::AtLeastOnce;
    let publishes = [
        (
            config.presence_state_topic(BedSide::Left),
            if left_on { "ON" } else { "OFF" },
        ),
        (
            config.presence_state_topic(BedSide::Right),
            if right_on { "ON" } else { "OFF" },
        ),
    ];
    for (topic, payload) in publishes {
        if let Err(e) = client.publish(topic, qos, true, payload).await {
            tracing::error!(?e, "publish presence occupancy state");
        }
    }
}

async fn publish_water_tank_state(client: &AsyncClient, config: &BridgeConfig, present: bool) {
    let qos = QoS::AtLeastOnce;
    let payload = if present { "ON" } else { "OFF" };
    if let Err(e) = client
        .publish(config.water_tank_state_topic(), qos, true, payload)
        .await
    {
        tracing::error!(?e, "publish Frozen water tank binary_sensor state");
    }
}

async fn publish_firmware_message_state(client: &AsyncClient, config: &BridgeConfig, msg: &str) {
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.firmware_message_state_topic(),
            qos,
            false,
            msg.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish Frozen firmware message sensor state");
    }
}

async fn publish_frozen_temperature_readings(
    client: &AsyncClient,
    config: &BridgeConfig,
    u: FrozenTemperatureUpdate,
) {
    let qos = QoS::AtLeastOnce;
    let left = format!("{:.2}", u.left_centi as f64 / 100.0);
    let right = format!("{:.2}", u.right_centi as f64 / 100.0);
    let hs = format!("{:.2}", u.heatsink_centi as f64 / 100.0);
    let publishes = [
        (config.frozen_current_temp_state_topic(BedSide::Left), left),
        (
            config.frozen_current_temp_state_topic(BedSide::Right),
            right,
        ),
        (config.frozen_heatsink_temp_state_topic(), hs),
    ];
    for (topic, payload) in publishes {
        if let Err(e) = client.publish(topic, qos, true, payload).await {
            tracing::error!(?e, "publish Frozen temperature sensor state");
        }
    }
}

#[derive(Deserialize, Default)]
struct HaLightCommand {
    state: Option<String>,
    brightness: Option<u8>,
    color: Option<HaRgb>,
}

#[derive(Deserialize)]
struct HaRgb {
    r: u8,
    g: u8,
    b: u8,
}

/// Logical LED state: HA “base” RGB × brightness → chip PWM levels.
#[derive(Clone)]
struct LightStateSnapshot {
    on: bool,
    brightness: u8,
    base_r: u8,
    base_g: u8,
    base_b: u8,
}

impl Default for LightStateSnapshot {
    fn default() -> Self {
        Self {
            on: false,
            brightness: 255,
            base_r: 255,
            base_g: 255,
            base_b: 255,
        }
    }
}

impl LightStateSnapshot {
    fn chip_rgb(&self) -> (u8, u8, u8) {
        let f = self.brightness as u32;
        let scale = |c: u8| ((c as u32 * f) / 255).min(255) as u8;
        (scale(self.base_r), scale(self.base_g), scale(self.base_b))
    }

    fn to_json(&self) -> String {
        let (r, g, b) = self.chip_rgb();
        json!({
            "state": if self.on && self.brightness > 0 { "ON" } else { "OFF" },
            "brightness": self.brightness,
            "color": {"r": r, "g": g, "b": b},
            "color_mode": "rgb",
        })
        .to_string()
    }
}

/// Target temperature for one mattress side (Frozen `SetTargetTemperature`).
#[derive(Clone)]
struct ClimateSideState {
    enabled: bool,
    target_centi: u16,
}

impl Default for ClimateSideState {
    fn default() -> Self {
        Self {
            enabled: false,
            target_centi: 3000,
        }
    }
}

fn climate_action_label(side: BedSide) -> &'static str {
    match side {
        BedSide::Left => "climate_left",
        BedSide::Right => "climate_right",
    }
}

fn vibrate_action_label(side: BedSide) -> &'static str {
    match side {
        BedSide::Left => "vibrate_left",
        BedSide::Right => "vibrate_right",
    }
}

#[derive(Clone)]
struct PublishHandlerState {
    light_state: Arc<Mutex<LightStateSnapshot>>,
    startup_led_on: Arc<Mutex<bool>>,
    /// Set when an inbound `state_topic` message was sent with the MQTT retain flag (broker-stored preference), not a live publish.
    startup_led_broker_retain_seen: Arc<AtomicBool>,
    climate_left: Arc<Mutex<ClimateSideState>>,
    climate_right: Arc<Mutex<ClimateSideState>>,
    /// Notify presence task to start baseline sampling (MQTT **Calibrate presence**).
    presence_calibrate_tx: Option<mpsc::Sender<()>>,
    /// `Some(until)` while MQTT **Priming** should show Frozen **Prime** as active.
    frozen_prime_shown_until: Arc<Mutex<Option<Instant>>>,
    /// incremented on each Prime; stale expiry tasks ignore superseded primes.
    frozen_prime_generation: Arc<AtomicU64>,
}

async fn priming_binary_sensor_active(
    sensor_counts: Option<&Arc<PrimingCounts>>,
    frozen_prime_shown_until: &Mutex<Option<Instant>>,
) -> bool {
    if let Some(c) = sensor_counts {
        if c.mqtt_priming_active() {
            return true;
        }
    }
    let lock = frozen_prime_shown_until.lock().await;
    lock.map(|u| Instant::now() < u).unwrap_or(false)
}

async fn on_frozen_prime_pressed(
    client: &AsyncClient,
    config: &BridgeConfig,
    hs: &PublishHandlerState,
) {
    let my_gen = hs.frozen_prime_generation.fetch_add(1, Ordering::SeqCst) + 1;
    let until = Instant::now() + FROZEN_PRIME_SHOWN_AFTER_PRESS;
    {
        let mut g = hs.frozen_prime_shown_until.lock().await;
        *g = Some(until);
    }
    let active = priming_binary_sensor_active(
        config.sensor_priming_counts.as_ref(),
        &hs.frozen_prime_shown_until,
    )
    .await;
    publish_priming_state(client, config, active).await;

    let c = client.clone();
    let cfg = config.clone();
    let counts = config.sensor_priming_counts.clone();
    let fz_until = hs.frozen_prime_shown_until.clone();
    let fz_gen = hs.frozen_prime_generation.clone();
    tokio::spawn(async move {
        tokio::time::sleep_until(until).await;
        if fz_gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        {
            let mut g = fz_until.lock().await;
            if fz_gen.load(Ordering::SeqCst) == my_gen {
                *g = None;
            }
        }
        let active = priming_binary_sensor_active(counts.as_ref(), &fz_until).await;
        publish_priming_state(&c, &cfg, active).await;
    });
}

async fn handle_vibrate_press(client: &AsyncClient, config: &BridgeConfig, side: BedSide) {
    if config.sensor_device.is_none() {
        return;
    }
    let intensity = config.vibration_intensity.clamp(1, 100);
    let duration = config.vibration_duration_sec.clamp(1, 600);
    let frames = vibration_sequence_frames(
        side,
        intensity,
        config.vibration_pattern,
        duration,
        config.sensor_vibrate_cancel_preamble,
    );
    let frame_count = frames.len();
    match enqueue_sensor_vibration(config, frames).await {
        Ok(()) => {
            tracing::info!(
                ?side,
                intensity,
                duration,
                pattern = ?config.vibration_pattern,
                frame_count,
                "Sensor vibration sequence sent (opensleep: optional cancel + EnableVibration + piezo + SetAlarm)"
            );
            publish_json_result(
                client,
                config,
                vibrate_action_label(side),
                "success",
                "vibration sequence",
            )
            .await;
        }
        Err(e) => {
            tracing::error!(%e, ?side, "Sensor vibration enqueue failed");
            publish_json_result(client, config, vibrate_action_label(side), "error", &e).await;
        }
    }
}

fn parse_light_command(payload: &[u8]) -> Option<HaLightCommand> {
    serde_json::from_slice(payload).ok()
}

fn compute_light_state(cmd: &HaLightCommand, prev: &LightStateSnapshot) -> LightStateSnapshot {
    if cmd.state.as_deref() == Some("OFF") {
        let mut s = prev.clone();
        s.on = false;
        s.brightness = 0;
        return s;
    }

    let mut next = prev.clone();

    if let Some(c) = &cmd.color {
        next.base_r = c.r;
        next.base_g = c.g;
        next.base_b = c.b;
    }

    if let Some(b) = cmd.brightness {
        next.brightness = b;
    }

    if cmd.state.as_deref() == Some("ON") {
        next.on = true;
        if cmd.color.is_none() && cmd.brightness.is_none() {
            // Explicit ON without payload: keep previous color/brightness
        }
    }

    if next.brightness == 0 {
        next.on = false;
    } else {
        let implies_on = cmd.state.as_deref() == Some("ON")
            || cmd.color.is_some()
            || (cmd.brightness.is_some()
                && (cmd.state.is_none() && cmd.color.is_none() || prev.on));
        if implies_on {
            next.on = true;
        }
    }

    if next.on && next.base_r == 0 && next.base_g == 0 && next.base_b == 0 {
        next.base_r = 255;
        next.base_g = 255;
        next.base_b = 255;
    }

    next
}

async fn publish_json_result(
    client: &AsyncClient,
    config: &BridgeConfig,
    action: &str,
    status: &str,
    message: &str,
) {
    let body = json!({
        "action": action,
        "status": status,
        "message": message,
    })
    .to_string();
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(config.result_topic(), qos, false, body)
        .await
    {
        tracing::error!(?e, "publish result");
    }
}

fn parse_mqtt_on_off(payload: &[u8]) -> Option<bool> {
    let s = std::str::from_utf8(payload).ok()?.trim();
    match s {
        "ON" => Some(true),
        "OFF" => Some(false),
        _ => None,
    }
}

async fn publish_startup_led_state(client: &AsyncClient, config: &BridgeConfig, on: bool) {
    let payload = if on { "ON" } else { "OFF" };
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(config.startup_led_state_topic(), qos, true, payload)
        .await
    {
        tracing::error!(?e, "publish startup LED state");
    }
}

async fn publish_priming_state(client: &AsyncClient, config: &BridgeConfig, active: bool) {
    let payload = if active { "ON" } else { "OFF" };
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(config.priming_state_topic(), qos, true, payload)
        .await
    {
        tracing::error!(?e, "publish priming state");
    }
}

/// Apply [`LightStateSnapshot`] to the IS31FL3194 and mirror it to MQTT state.
async fn commit_light_snapshot(
    client: &AsyncClient,
    config: &BridgeConfig,
    light_state: &Arc<Mutex<LightStateSnapshot>>,
    snap: LightStateSnapshot,
) -> Result<(), String> {
    let Some(i2c_path) = config.i2c_device.clone() else {
        return Err("LED disabled".into());
    };
    let (cr, cg, cb) = snap.chip_rgb();
    let path = i2c_path.clone();
    let on_hw = snap.on && snap.brightness > 0;
    let set_res = tokio::task::spawn_blocking(move || {
        let mut dev = Is31fl3194::open(&path)?;
        dev.set_solid_rgb(on_hw, cr, cg, cb)
    })
    .await;

    match set_res {
        Ok(Ok(())) => {
            *light_state.lock().await = snap.clone();
            publish_light_state(client, config, &snap).await;
            Ok(())
        }
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("LED task join failed: {e:?}")),
    }
}

async fn publish_light_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    snap: &LightStateSnapshot,
) {
    let qos = QoS::AtLeastOnce;
    let payload = snap.to_json();
    if let Err(e) = client
        .publish(config.light_state_topic(), qos, true, payload)
        .await
    {
        tracing::error!(?e, "publish light state");
    }
}

async fn publish_climate_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    side: BedSide,
    snap: &ClimateSideState,
) {
    let mode = if snap.enabled {
        CLIMATE_MODE_HEAT_COOL
    } else {
        CLIMATE_MODE_OFF
    };
    let temp_c = snap.target_centi as f64 / 100.0;
    let qos = QoS::AtLeastOnce;
    let temp_str = format!("{temp_c:.2}");
    if let Err(e) = client
        .publish(config.climate_mode_state_topic(side), qos, true, mode)
        .await
    {
        tracing::error!(?e, "publish climate mode state");
    }
    if let Err(e) = client
        .publish(
            config.climate_temperature_state_topic(side),
            qos,
            true,
            temp_str.as_str(),
        )
        .await
    {
        tracing::error!(?e, "publish climate temperature state");
    }
    if let Err(e) = client
        .publish(
            config.target_temperature_state_topic(side),
            qos,
            true,
            temp_str.as_str(),
        )
        .await
    {
        tracing::error!(?e, "publish target temperature sensor state");
    }
}

async fn handle_climate_mode_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    side: BedSide,
    state: &Arc<Mutex<ClimateSideState>>,
    payload: &[u8],
) {
    let Ok(mode_str) = std::str::from_utf8(payload) else {
        tracing::warn!("climate mode: invalid UTF-8");
        return;
    };
    let mode_str = mode_str.trim();
    let enabled = match mode_str {
        CLIMATE_MODE_OFF => false,
        CLIMATE_MODE_HEAT_COOL => true,
        _ => {
            tracing::warn!(%mode_str, "ignored unknown climate mode");
            return;
        }
    };

    let mut st = state.lock().await;
    let backup = st.clone();
    st.enabled = enabled;
    let frame = set_target_temperature_frame(side, st.enabled, st.target_centi);
    let snap = st.clone();
    drop(st);

    match enqueue_frozen_frame(config, frame).await {
        Ok(()) => {
            tracing::info!(
                ?side,
                enabled,
                target_centi = snap.target_centi,
                "climate mode"
            );
            publish_climate_state(client, config, side, &snap).await;
            publish_json_result(
                client,
                config,
                climate_action_label(side),
                "success",
                "set target temperature",
            )
            .await;
        }
        Err(e) => {
            tracing::error!(%e, ?side, "climate mode Frozen UART enqueue failed");
            *state.lock().await = backup;
            publish_json_result(client, config, climate_action_label(side), "error", &e).await;
        }
    }
}

async fn handle_climate_temperature_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    side: BedSide,
    state: &Arc<Mutex<ClimateSideState>>,
    payload: &[u8],
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        return;
    };
    let Ok(temp_c) = text.trim().parse::<f64>() else {
        tracing::warn!("ignored non-numeric climate temperature");
        return;
    };
    let lo = config.climate_min_temp;
    let hi = config.climate_max_temp;
    let clamped = temp_c.clamp(lo, hi);
    let centi = (clamped * 100.0).round() as u16;

    let mut st = state.lock().await;
    let backup = st.clone();
    st.target_centi = centi;
    if st.enabled {
        let frame = set_target_temperature_frame(side, true, centi);
        let snap = st.clone();
        drop(st);
        match enqueue_frozen_frame(config, frame).await {
            Ok(()) => {
                tracing::info!(?side, target_centi = centi, "climate temperature");
                publish_climate_state(client, config, side, &snap).await;
                publish_json_result(
                    client,
                    config,
                    climate_action_label(side),
                    "success",
                    "set target temperature",
                )
                .await;
            }
            Err(e) => {
                tracing::error!(%e, ?side, "climate temperature Frozen UART enqueue failed");
                *state.lock().await = backup;
                publish_json_result(client, config, climate_action_label(side), "error", &e).await;
            }
        }
    } else {
        let snap = st.clone();
        drop(st);
        publish_climate_state(client, config, side, &snap).await;
    }
}

async fn publish_discovery_and_online(
    client: &AsyncClient,
    config: &BridgeConfig,
    hs: &PublishHandlerState,
) {
    let qos = QoS::AtLeastOnce;
    let disc_btn = discovery_payload_button(config);
    if let Err(e) = client
        .publish(config.discovery_topic(), qos, true, disc_btn)
        .await
    {
        tracing::error!(?e, "publish button discovery");
    }
    let disc_req_temp = discovery_payload_request_get_temperatures_button(config);
    if let Err(e) = client
        .publish(
            config.discovery_topic_request_get_temperatures(),
            qos,
            true,
            disc_req_temp,
        )
        .await
    {
        tracing::error!(?e, "publish request GetTemperatures button discovery");
    }
    {
        let disc = discovery_payload_deviceinfo_device_label(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_deviceinfo_device_label(),
                qos,
                true,
                disc,
            )
            .await
        {
            tracing::error!(?e, "publish deviceinfo device label discovery");
        }
        let disc = discovery_payload_deviceinfo_device_id(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_deviceinfo_device_id(),
                qos,
                true,
                disc,
            )
            .await
        {
            tracing::error!(?e, "publish deviceinfo device id discovery");
        }
        let (label_payload, id_payload) = deviceinfo::device_label_and_id_payloads();
        if let Err(e) = client
            .publish(
                DEVICEINFO_DEVICE_LABEL_STATE_TOPIC,
                qos,
                true,
                label_payload,
            )
            .await
        {
            tracing::error!(?e, "publish deviceinfo device-label state");
        }
        if let Err(e) = client
            .publish(
                DEVICEINFO_DEVICE_ID_STATE_TOPIC,
                qos,
                true,
                id_payload,
            )
            .await
        {
            tracing::error!(?e, "publish deviceinfo device-id state");
        }
    }
    if config.i2c_device.is_some() {
        let disc_led = discovery_payload_light(config);
        if let Err(e) = client
            .publish(config.discovery_topic_light(), qos, true, disc_led)
            .await
        {
            tracing::error!(?e, "publish light discovery");
        }
        let disc_sw = discovery_payload_startup_led_switch(config);
        if let Err(e) = client
            .publish(config.discovery_topic_startup_led(), qos, true, disc_sw)
            .await
        {
            tracing::error!(?e, "publish startup LED switch discovery");
        }
    }
    for side in [BedSide::Left, BedSide::Right] {
        let disc_climate = discovery_payload_climate(config, side);
        if let Err(e) = client
            .publish(
                config.climate_discovery_topic(side),
                qos,
                true,
                disc_climate,
            )
            .await
        {
            tracing::error!(?e, side = ?side, "publish climate discovery");
        }
        let (name, suffix) = match side {
            BedSide::Left => ("Target Temperature Left", "target_temp_left"),
            BedSide::Right => ("Target Temperature Right", "target_temp_right"),
        };
        let disc_target = discovery_payload_frozen_temperature(
            config,
            name,
            suffix,
            config.target_temperature_state_topic(side),
        );
        if let Err(e) = client
            .publish(
                config.discovery_topic_target_temperature(side),
                qos,
                true,
                disc_target,
            )
            .await
        {
            tracing::error!(?e, side = ?side, "publish target temperature sensor discovery");
        }
    }
    if config.sensor_device.is_some() {
        for side in [BedSide::Left, BedSide::Right] {
            let disc_v = discovery_payload_vibrate_button(config, side);
            if let Err(e) = client
                .publish(config.vibrate_discovery_topic(side), qos, true, disc_v)
                .await
            {
                tracing::error!(?e, side = ?side, "publish vibrate discovery");
            }
        }
    }
    {
        let disc_p = discovery_payload_priming(config);
        if let Err(e) = client
            .publish(config.discovery_topic_priming(), qos, true, disc_p)
            .await
        {
            tracing::error!(?e, "publish priming binary_sensor discovery");
        }
        let active = priming_binary_sensor_active(
            config.sensor_priming_counts.as_ref(),
            &hs.frozen_prime_shown_until,
        )
        .await;
        publish_priming_state(client, config, active).await;
    }
    if config.sensor_device.is_some() && config.presence_discovery {
        for side in [BedSide::Left, BedSide::Right] {
            let disc = discovery_payload_presence(config, side);
            if let Err(e) = client
                .publish(config.discovery_topic_presence(side), qos, true, disc)
                .await
            {
                tracing::error!(?e, side = ?side, "publish presence discovery");
            }
        }
        // HA shows "Unknown" until a retained state arrives; capacitance `0x33` may be rare/absent at boot.
        publish_presence_readings(client, config, false, false).await;
        let disc_cal = discovery_payload_calibrate_presence_button(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_calibrate_presence(),
                qos,
                true,
                disc_cal,
            )
            .await
        {
            tracing::error!(?e, "publish presence calibration button discovery");
        }
    }
    if config.frozen_temperature_discovery {
        for (side, name, suffix) in [
            (BedSide::Left, "Current Temperature Left", "cover_temp_left"),
            (
                BedSide::Right,
                "Current Temperature Right",
                "cover_temp_right",
            ),
        ] {
            let disc = discovery_payload_frozen_temperature(
                config,
                name,
                suffix,
                config.frozen_current_temp_state_topic(side),
            );
            if let Err(e) = client
                .publish(
                    config.discovery_topic_frozen_current_temp(side),
                    qos,
                    true,
                    disc,
                )
                .await
            {
                tracing::error!(?e, side = ?side, "publish Frozen current temperature discovery");
            }
        }
        let disc_hs = discovery_payload_frozen_temperature(
            config,
            "Heatsink Temperature",
            "heatsink_temp",
            config.frozen_heatsink_temp_state_topic(),
        );
        if let Err(e) = client
            .publish(
                config.discovery_topic_frozen_heatsink_temp(),
                qos,
                true,
                disc_hs,
            )
            .await
        {
            tracing::error!(?e, "publish Frozen heatsink temperature discovery");
        }
    }
    if config.frozen_water_tank_discovery {
        let disc = discovery_payload_water_tank(config);
        if let Err(e) = client
            .publish(config.discovery_topic_water_tank(), qos, true, disc)
            .await
        {
            tracing::error!(?e, "publish Frozen water tank discovery");
        }
    }
    if config.frozen_firmware_message_discovery {
        let disc = discovery_payload_firmware_message(config);
        if let Err(e) = client
            .publish(config.discovery_topic_firmware_message(), qos, true, disc)
            .await
        {
            tracing::error!(?e, "publish Frozen firmware message discovery");
        }
    }
    if let Err(e) = client
        .publish(config.availability_topic(), qos, true, "online")
        .await
    {
        tracing::error!(?e, "publish availability online");
    }
}

async fn setup_session(client: &AsyncClient, config: &BridgeConfig, hs: &PublishHandlerState) {
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client.subscribe(config.command_topic(), qos).await {
        tracing::error!(?e, "subscribe prime command topic");
    }
    if let Err(e) = client
        .subscribe(config.request_get_temperatures_command_topic(), qos)
        .await
    {
        tracing::error!(?e, "subscribe request_get_temperatures command topic");
    }
    if config.i2c_device.is_some() {
        if let Err(e) = client.subscribe(config.light_command_topic(), qos).await {
            tracing::error!(?e, "subscribe light command topic");
        }
        if let Err(e) = client
            .subscribe(config.startup_led_command_topic(), qos)
            .await
        {
            tracing::error!(?e, "subscribe startup LED command topic");
        }
        if let Err(e) = client
            .subscribe(config.startup_led_state_topic(), qos)
            .await
        {
            tracing::error!(?e, "subscribe startup LED state topic (retained sync)");
        }
    }
    for side in [BedSide::Left, BedSide::Right] {
        if let Err(e) = client
            .subscribe(config.climate_mode_command_topic(side), qos)
            .await
        {
            tracing::error!(?e, side = ?side, "subscribe climate mode topic");
        }
        if let Err(e) = client
            .subscribe(config.climate_temperature_command_topic(side), qos)
            .await
        {
            tracing::error!(?e, side = ?side, "subscribe climate temperature topic");
        }
    }
    if config.sensor_device.is_some() {
        for side in [BedSide::Left, BedSide::Right] {
            if let Err(e) = client
                .subscribe(config.vibrate_command_topic(side), qos)
                .await
            {
                tracing::error!(?e, side = ?side, "subscribe vibrate command topic");
            }
        }
        if config.presence_discovery {
            if let Err(e) = client
                .subscribe(config.calibrate_presence_command_topic(), qos)
                .await
            {
                tracing::error!(?e, "subscribe calibrate_presence button topic");
            }
        }
    }
    if let Err(e) = client.subscribe(HA_STATUS_TOPIC, qos).await {
        tracing::error!(?e, "subscribe Home Assistant birth topic");
    }
    publish_discovery_and_online(client, config, hs).await;
}

async fn handle_light_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    light_state: &Arc<Mutex<LightStateSnapshot>>,
    payload: &[u8],
) {
    let Some(cmd) = parse_light_command(payload) else {
        tracing::warn!("ignored non-JSON light command");
        return;
    };
    if config.i2c_device.is_none() {
        return;
    }

    let prev = light_state.lock().await.clone();
    let snap = compute_light_state(&cmd, &prev);
    let (cr, cg, cb) = snap.chip_rgb();

    match commit_light_snapshot(client, config, light_state, snap.clone()).await {
        Ok(()) => {
            tracing::debug!(
                base_r = snap.base_r,
                base_g = snap.base_g,
                base_b = snap.base_b,
                brightness = snap.brightness,
                chip_r = cr,
                chip_g = cg,
                chip_b = cb,
                on = snap.on,
                "LED updated"
            );
        }
        Err(e) => {
            tracing::error!(%e, "I²C LED write failed");
            publish_json_result(client, config, "led", "error", &e).await;
        }
    }
}

async fn handle_startup_led_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    startup_led_on: &Arc<Mutex<bool>>,
    payload: &[u8],
) {
    let Some(on) = parse_mqtt_on_off(payload) else {
        tracing::warn!("startup LED switch: ignored payload (expected ON or OFF)");
        return;
    };
    *startup_led_on.lock().await = on;
    publish_startup_led_state(client, config, on).await;
    if on {
        tracing::info!("Startup LED preference ON (green LED applies on next narcolepsy start)");
    } else {
        tracing::info!("Startup LED preference OFF");
    }
}

/// Inbound sync on `state_topic` — updates internal preference only; hardware is driven after connect from broker-retained payloads.
async fn handle_startup_led_state_message(
    startup_led_on: &Arc<Mutex<bool>>,
    startup_led_broker_retain_seen: &Arc<AtomicBool>,
    payload: &[u8],
    mqtt_retain: bool,
) {
    let Some(on) = parse_mqtt_on_off(payload) else {
        return;
    };
    *startup_led_on.lock().await = on;
    if mqtt_retain {
        startup_led_broker_retain_seen.store(true, Ordering::SeqCst);
    }
}

async fn handle_publish(
    client: &AsyncClient,
    config: &BridgeConfig,
    prime_frame: &[u8],
    st: &PublishHandlerState,
    p: Publish,
) {
    if p.topic == config.command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            match enqueue_frozen_frame(config, prime_frame.to_vec()).await {
                Ok(()) => {
                    tracing::info!("prime frame queued to Frozen USART task");
                    on_frozen_prime_pressed(client, config, st).await;
                    publish_json_result(client, config, "prime", "success", "prime frame sent")
                        .await;
                }
                Err(e) => {
                    tracing::error!(%e, "Frozen UART enqueue failed");
                    publish_json_result(client, config, "prime", "error", &e).await;
                }
            }
        }
        return;
    }

    if p.topic == config.request_get_temperatures_command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            match enqueue_frozen_frame(config, get_temperatures_frame()).await {
                Ok(()) => {
                    tracing::info!("GetTemperatures frame queued to Frozen USART task");
                    publish_json_result(
                        client,
                        config,
                        "request_get_temperatures",
                        "success",
                        "get_temperatures frame sent",
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!(%e, "Frozen UART enqueue failed (GetTemperatures)");
                    publish_json_result(client, config, "request_get_temperatures", "error", &e)
                        .await;
                }
            }
        }
        return;
    }

    if config.sensor_device.is_some()
        && config.presence_discovery
        && p.topic == config.calibrate_presence_command_topic()
        && p.payload.as_ref() == config.payload_press.as_bytes()
    {
        handle_presence_calibrate_press(client, config, st.presence_calibrate_tx.as_ref()).await;
        return;
    }

    if config.sensor_device.is_some() {
        let expected = config.payload_press.as_bytes();
        if p.topic == config.vibrate_command_topic(BedSide::Left) && p.payload.as_ref() == expected
        {
            handle_vibrate_press(client, config, BedSide::Left).await;
            return;
        }
        if p.topic == config.vibrate_command_topic(BedSide::Right) && p.payload.as_ref() == expected
        {
            handle_vibrate_press(client, config, BedSide::Right).await;
            return;
        }
    }

    if p.topic == config.climate_mode_command_topic(BedSide::Left) {
        handle_climate_mode_command(client, config, BedSide::Left, &st.climate_left, &p.payload)
            .await;
        return;
    }
    if p.topic == config.climate_mode_command_topic(BedSide::Right) {
        handle_climate_mode_command(
            client,
            config,
            BedSide::Right,
            &st.climate_right,
            &p.payload,
        )
        .await;
        return;
    }
    if p.topic == config.climate_temperature_command_topic(BedSide::Left) {
        handle_climate_temperature_command(
            client,
            config,
            BedSide::Left,
            &st.climate_left,
            &p.payload,
        )
        .await;
        return;
    }
    if p.topic == config.climate_temperature_command_topic(BedSide::Right) {
        handle_climate_temperature_command(
            client,
            config,
            BedSide::Right,
            &st.climate_right,
            &p.payload,
        )
        .await;
        return;
    }

    if config.i2c_device.is_some() && p.topic == config.startup_led_command_topic() {
        handle_startup_led_command(client, config, &st.startup_led_on, &p.payload).await;
        return;
    }
    if config.i2c_device.is_some() && p.topic == config.startup_led_state_topic() {
        handle_startup_led_state_message(
            &st.startup_led_on,
            &st.startup_led_broker_retain_seen,
            &p.payload,
            p.retain,
        )
        .await;
        return;
    }

    if config.i2c_device.is_some() && p.topic == config.light_command_topic() {
        handle_light_command(client, config, &st.light_state, &p.payload).await;
        return;
    }

    if p.topic == HA_STATUS_TOPIC && p.payload.as_ref() == b"online" {
        tracing::debug!("Home Assistant online; republishing discovery");
        publish_discovery_and_online(client, config, st).await;
    }
}

/// Run the MQTT event loop until a fatal error (or process kill).
pub async fn run(
    config: BridgeConfig,
    prime_frame: Arc<[u8]>,
    sensor_priming_events: Option<mpsc::Receiver<PrimingEvent>>,
    frozen_temperature_rx: Option<mpsc::Receiver<FrozenTemperatureUpdate>>,
    frozen_water_tank_rx: Option<mpsc::Receiver<bool>>,
    frozen_firmware_message_rx: Option<mpsc::Receiver<String>>,
    capacitance_rx: Option<mpsc::Receiver<SensorCapacitanceZones>>,
) {
    let (presence_calibrate_tx, presence_calibrate_rx) =
        if config.presence_discovery && capacitance_rx.is_some() {
            let (t, r) = mpsc::channel::<()>(8);
            (Some(t), Some(r))
        } else {
            (None, None)
        };

    let handler_state = PublishHandlerState {
        light_state: Arc::new(Mutex::new(LightStateSnapshot::default())),
        startup_led_on: Arc::new(Mutex::new(false)),
        startup_led_broker_retain_seen: Arc::new(AtomicBool::new(false)),
        climate_left: Arc::new(Mutex::new(ClimateSideState::default())),
        climate_right: Arc::new(Mutex::new(ClimateSideState::default())),
        presence_calibrate_tx: presence_calibrate_tx.clone(),
        frozen_prime_shown_until: Arc::new(Mutex::new(None)),
        frozen_prime_generation: Arc::new(AtomicU64::new(0)),
    };

    let startup_led_hw_once_per_process = Arc::new(AtomicBool::new(false));

    let mut opts = MqttOptions::new(
        config.mqtt_client_id.clone(),
        config.mqtt_host.as_str(),
        config.mqtt_port,
    );
    opts.set_keep_alive(Duration::from_secs(60));
    if let (Some(u), Some(p)) = (&config.mqtt_username, &config.mqtt_password) {
        opts.set_credentials(u, p);
    }
    opts.set_last_will(LastWill {
        topic: config.availability_topic(),
        message: "offline".into(),
        qos: QoS::AtLeastOnce,
        retain: true,
    });

    let (client, mut eventloop) = AsyncClient::new(opts, 16);

    if let Some(mut frozen_temp_rx) = frozen_temperature_rx {
        let c = client.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            while let Some(u) = frozen_temp_rx.recv().await {
                publish_frozen_temperature_readings(&c, &cfg, u).await;
            }
        });
    }

    if let Some(mut water_rx) = frozen_water_tank_rx {
        let c = client.clone();
        let cfg = config.clone();
        let enabled = config.frozen_water_tank_discovery;
        tokio::spawn(async move {
            if !enabled {
                return;
            }
            let mut last: Option<bool> = None;
            while let Some(present) = water_rx.recv().await {
                if last == Some(present) {
                    continue;
                }
                last = Some(present);
                publish_water_tank_state(&c, &cfg, present).await;
            }
        });
    }

    if let Some(mut fw_rx) = frozen_firmware_message_rx {
        let c = client.clone();
        let cfg = config.clone();
        let enabled = config.frozen_firmware_message_discovery;
        tokio::spawn(async move {
            if !enabled {
                return;
            }
            while let Some(msg) = fw_rx.recv().await {
                publish_firmware_message_state(&c, &cfg, &msg).await;
            }
        });
    }

    if let Some(mut cap_rx) = capacitance_rx {
        if let Some(mut cal_rx) = presence_calibrate_rx {
            let c = client.clone();
            let cfg = config.clone();
            let cal_wait = Duration::from_secs(cfg.presence_calibrate_secs.max(3));
            let fallback_abs_threshold = cfg.presence_cap_threshold;
            let presence_debug = cfg.presence_debug;
            tokio::spawn(async move {
                let mut inference = PresenceInferenceState::default();
                // Matches `publish_discovery_and_online` seed (`OFF`/`OFF`).
                let mut prev_publish = Some((false, false));
                let mut cal_until: Option<Instant> = None;
                let mut cal_samples = Vec::<[u16; 6]>::new();

                let mut saw_cap_sample = false;
                let mut cap_stall_check = if presence_debug {
                    let mut i = interval(Duration::from_secs(60));
                    i.tick().await;
                    Some(i)
                } else {
                    None
                };

                if presence_debug {
                    tracing::info!(
                        "presence debug: enabled — each Sensor capacitance `0x33` frame will log zones and inference; if MCU sends none or parse fails, WARN every 60s here and once from Sensor on bad `0x33` payload"
                    );
                }

                loop {
                    tokio::select! {
                        biased;
                        cmd = cal_rx.recv() => {
                            if cmd.is_none() {
                                tracing::warn!("presence calibrate MQTT channel closed; stopping occupancy task");
                                return;
                            }
                            tracing::info!(
                                secs = cal_wait.as_secs(),
                                "presence calibration window started — keep mattress empty (opensleep parity)"
                            );
                            cal_until = Some(Instant::now() + cal_wait);
                            cal_samples.clear();
                        }
                        z_opt = cap_rx.recv() => {
                            let Some(z) = z_opt else {
                                return;
                            };

                            saw_cap_sample = true;

                            if cal_until.is_some() {
                                cal_samples.push(z.zones);
                            }
                            if let Some(end) = cal_until {
                                if Instant::now() >= end {
                                    cal_until = None;
                                    match mean_baselines_from_samples(&cal_samples) {
                                        Some(nb) => {
                                            inference.set_baselines(nb);
                                            tracing::info!(
                                                baseline = ?nb,
                                                frames = cal_samples.len(),
                                                delta = PRESENCE_BASELINE_DELTA,
                                                debounce_frames = PRESENCE_DEBOUNCE_FRAMES,
                                                "presence baseline calibrated (sensor/presence parity)"
                                            );
                                            prev_publish = None;
                                        }
                                        None => {
                                            tracing::error!(
                                                "presence calibration window ended without capacitance samples — MCU quiet or wrong baud; baseline unchanged"
                                            );
                                        }
                                    }
                                    cal_samples.clear();
                                }
                            }

                            let (left_on, right_on) =
                                inference_occupancy(&z, &mut inference, fallback_abs_threshold);
                            let calibrating = cal_until.is_some();
                            if presence_debug {
                                log_presence_debug(
                                    &z,
                                    &inference,
                                    fallback_abs_threshold,
                                    left_on,
                                    right_on,
                                    calibrating,
                                );
                            }
                            if prev_publish == Some((left_on, right_on)) {
                                continue;
                            }
                            tracing::trace!(
                                left_on,
                                right_on,
                                calibrated = inference.baselines.is_some(),
                                zones = ?z.zones,
                                "Sensor presence (capacitance)"
                            );
                            prev_publish = Some((left_on, right_on));
                            publish_presence_readings(&c, &cfg, left_on, right_on).await;
                        }

                        _ = async {
                            if let Some(i) = cap_stall_check.as_mut() {
                                i.tick().await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        }, if presence_debug && cap_stall_check.is_some() => {
                            if !saw_cap_sample {
                                tracing::warn!(
                                    "presence debug: no capacitance `0x33` occupancy sample in 60s — MCU likely not sending usable `0x33`, or narcolepsy rejected layout (Sensor logs first bad `0x33` WARNING once)"
                                );
                            }
                        }
                    }
                }
            });
        } else {
            tracing::error!("presence_discovery without calibration channel (bridge bug)");
        }
    }

    let hs_priming = handler_state.clone();
    match (sensor_priming_events, config.sensor_priming_counts.clone()) {
        (Some(mut events_rx), Some(counts)) => {
            let c = client.clone();
            let cfg = config.clone();
            tokio::spawn(async move {
                let mut last: Option<bool> = None;
                while let Some(_ev) = events_rx.recv().await {
                    let active = priming_binary_sensor_active(
                        Some(&counts),
                        &hs_priming.frozen_prime_shown_until,
                    )
                    .await;
                    if last == Some(active) {
                        continue;
                    }
                    last = Some(active);
                    publish_priming_state(&c, &cfg, active).await;
                }
            });
        }
        (Some(mut rx), None) => {
            tracing::error!(
                "sensor Priming MQTT events present without PrimingCounts (misconfigured)"
            );
            rx.close();
        }
        _ => {}
    }

    tracing::info!(
        host = %config.mqtt_host,
        port = config.mqtt_port,
        led = config.i2c_device.is_some(),
        vibrate = config.sensor_device.is_some(),
        presence = config.sensor_device.is_some() && config.presence_discovery,
        "MQTT client created; polling event loop"
    );

    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install SIGINT handler");
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");

    loop {
        #[cfg(unix)]
        let evt = tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("SIGINT: shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM: shutting down");
                break;
            }
            evt = eventloop.poll() => evt,
        };
        #[cfg(not(unix))]
        let evt = eventloop.poll().await;

        match evt {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                if ack.code == rumqttc::ConnectReturnCode::Success {
                    tracing::info!("MQTT connected");
                    // rumqttc: `AsyncClient` requests are only processed while `eventloop.poll()` runs.
                    // Awaiting `publish`/`subscribe` on the same task that calls `poll()` deadlocks the
                    // event loop — spawn so the broker actually receives discovery and availability.
                    let c = client.clone();
                    let cfg = config.clone();
                    let hs = handler_state.clone();
                    let hw_once = startup_led_hw_once_per_process.clone();
                    tokio::spawn(async move {
                        hs.startup_led_broker_retain_seen
                            .store(false, Ordering::SeqCst);
                        setup_session(&c, &cfg, &hs).await;
                        if cfg.i2c_device.is_some() {
                            // Wait for broker-retained state (MQTT retain=1) or assume default OFF.
                            const RETAIN_WAIT: Duration = Duration::from_millis(800);
                            const POLL: Duration = Duration::from_millis(20);
                            let mut waited = Duration::ZERO;
                            while waited < RETAIN_WAIT
                                && !hs.startup_led_broker_retain_seen.load(Ordering::SeqCst)
                            {
                                sleep(POLL).await;
                                waited += POLL;
                            }
                            let on = *hs.startup_led_on.lock().await;
                            if on
                                && hw_once
                                    .compare_exchange(
                                        false,
                                        true,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                            {
                                let green = LightStateSnapshot {
                                    on: true,
                                    brightness: 255,
                                    base_r: 0,
                                    base_g: 255,
                                    base_b: 0,
                                };
                                match commit_light_snapshot(&c, &cfg, &hs.light_state, green).await {
                                    Ok(()) => tracing::info!(
                                        "Startup LED preference ON: green LED applied (this narcolepsy start)"
                                    ),
                                    Err(e) => {
                                        tracing::error!(%e, "Startup LED on boot: I²C LED write failed");
                                        publish_json_result(
                                            &c,
                                            &cfg,
                                            "startup_led",
                                            "error",
                                            &e,
                                        )
                                        .await;
                                        hw_once.store(false, Ordering::SeqCst);
                                    }
                                }
                            }
                            let snap = hs.light_state.lock().await.clone();
                            publish_light_state(&c, &cfg, &snap).await;
                        }
                        let left = hs.climate_left.lock().await.clone();
                        publish_climate_state(&c, &cfg, BedSide::Left, &left).await;
                        let right = hs.climate_right.lock().await.clone();
                        publish_climate_state(&c, &cfg, BedSide::Right, &right).await;
                    });
                } else {
                    tracing::error!(code = ?ack.code, "MQTT connection refused");
                }
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                let c = client.clone();
                let cfg = config.clone();
                let pf = prime_frame.clone();
                let hs = handler_state.clone();
                tokio::spawn(async move {
                    handle_publish(&c, &cfg, &pf, &hs, p).await;
                });
            }
            Ok(_) => {}
            Err(ConnectionError::RequestsDone) => {
                tracing::warn!("MQTT requests channel closed; exiting");
                break;
            }
            Err(e) => {
                tracing::warn!(?e, "MQTT event loop error; backing off");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if let Some(ref path) = config.i2c_device {
        match shutdown_led(path) {
            Ok(()) => tracing::info!("LED turned off on exit"),
            Err(e) => tracing::warn!(error = %e, "LED turn-off on exit failed (I²C)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn chip_rgb_scales_brightness() {
        let s = LightStateSnapshot {
            on: true,
            brightness: 128,
            base_r: 255,
            base_g: 0,
            base_b: 0,
        };
        let (r, g, b) = s.chip_rgb();
        assert_eq!((r, g, b), (128, 0, 0));
    }

    #[test]
    fn compute_brightness_only_turns_on() {
        let prev = LightStateSnapshot {
            on: false,
            brightness: 255,
            base_r: 255,
            base_g: 255,
            base_b: 255,
        };
        let cmd = HaLightCommand {
            brightness: Some(200),
            ..Default::default()
        };
        let next = compute_light_state(&cmd, &prev);
        assert!(next.on);
        assert_eq!(next.brightness, 200);
    }

    #[test]
    fn parse_mqtt_on_off_trimmed() {
        assert_eq!(parse_mqtt_on_off(b"ON"), Some(true));
        assert_eq!(parse_mqtt_on_off(b"OFF"), Some(false));
        assert_eq!(parse_mqtt_on_off(b" ON \n"), Some(true));
        assert!(parse_mqtt_on_off(b"maybe").is_none());
    }

    #[test]
    fn climate_discovery_uses_current_temperature_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let left: serde_json::Value =
            serde_json::from_str(&discovery_payload_climate(&cfg, BedSide::Left)).unwrap();
        assert_eq!(
            left["current_temperature_topic"].as_str(),
            Some(cfg.frozen_current_temp_state_topic(BedSide::Left).as_str()),
        );
        let right: serde_json::Value =
            serde_json::from_str(&discovery_payload_climate(&cfg, BedSide::Right)).unwrap();
        assert_eq!(
            right["current_temperature_topic"].as_str(),
            Some(cfg.frozen_current_temp_state_topic(BedSide::Right).as_str()),
        );
    }

    #[test]
    fn target_temperature_sensor_discovery_matches_state_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let cfg = BridgeConfig::from_cli(&cli);
        for side in [BedSide::Left, BedSide::Right] {
            let (name, suffix) = match side {
                BedSide::Left => ("Target Temperature Left", "target_temp_left"),
                BedSide::Right => ("Target Temperature Right", "target_temp_right"),
            };
            let disc = discovery_payload_frozen_temperature(
                &cfg,
                name,
                suffix,
                cfg.target_temperature_state_topic(side),
            );
            let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
            assert_eq!(
                v["state_topic"].as_str(),
                Some(cfg.target_temperature_state_topic(side).as_str()),
            );
        }
    }

    #[test]
    fn presence_discovery_is_occupancy() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let mut cfg = BridgeConfig::from_cli(&cli);
        cfg.presence_discovery = true;
        cfg.sensor_device = Some(std::path::PathBuf::from("/dev/null"));
        for side in [BedSide::Left, BedSide::Right] {
            let disc = discovery_payload_presence(&cfg, side);
            let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
            assert_eq!(v["device_class"].as_str(), Some("occupancy"));
            assert_eq!(
                v["state_topic"].as_str(),
                Some(cfg.presence_state_topic(side).as_str()),
            );
        }
    }

    #[test]
    fn firmware_message_discovery_matches_state_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_firmware_message(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.firmware_message_state_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(
            v["enabled_by_default"].as_bool(),
            Some(false),
            "disabled in HA registry by default — enable under device entities if desired"
        );
    }

    #[test]
    fn deviceinfo_discovery_matches_state_topics() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_l = discovery_payload_deviceinfo_device_label(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_l).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(DEVICEINFO_DEVICE_LABEL_STATE_TOPIC),
        );
        let disc_i = discovery_payload_deviceinfo_device_id(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_i).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(DEVICEINFO_DEVICE_ID_STATE_TOPIC),
        );
        for disc in [disc_l, disc_i] {
            let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
            assert_eq!(
                v["enabled_by_default"].as_bool(),
                Some(false),
                "disabled in HA by default — enable under device entities if desired"
            );
        }
    }

    #[test]
    fn request_get_temperatures_button_discovery_matches_command_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_request_get_temperatures_button(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.request_get_temperatures_command_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
    }

    #[test]
    fn calibrate_presence_discovery_has_entity_category_config() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy"]);
        let mut cfg = BridgeConfig::from_cli(&cli);
        cfg.presence_discovery = true;
        let disc = discovery_payload_calibrate_presence_button(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.calibrate_presence_command_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("config"));
    }

    #[test]
    fn presence_mean_baseline_average_per_zone() {
        let samples = [[10u16, 20, 30, 40, 50, 60], [20, 30, 40, 50, 60, 70]];
        assert_eq!(
            mean_baselines_from_samples(&samples).unwrap(),
            [15, 25, 35, 45, 55, 65]
        );
        assert!(mean_baselines_from_samples(&[]).is_none());
    }

    #[test]
    fn presence_calibrated_opensleep_five_frame_debounce() {
        let z = SensorCapacitanceZones {
            sequence: 1,
            zones: [200, 100, 100, 200, 100, 100],
        };
        let mut inference = PresenceInferenceState::default();
        inference.set_baselines([100; 6]);
        for _ in 0..4 {
            assert_eq!(
                inference_occupancy(&z, &mut inference, 9999),
                (false, false)
            );
        }
        assert_eq!(inference_occupancy(&z, &mut inference, 9999), (true, true));
    }
}
