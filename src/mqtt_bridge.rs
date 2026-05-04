//! MQTT: Home Assistant discovery, prime, per-side vibration (Sensor) + retained **number/select/switch**
//! vibration tuning, optional capacitance **presence**,
//! Frozen **water tank** + **Firmware message** from **`0x07`**, mattress climate, JSON light (I²C).
//!
//! **rumqttc:** subscribe/publish must not block the task that runs [`rumqttc::EventLoop::poll`].
//! Outbound work runs in [`tokio::spawn`] so the event loop keeps draining requests (see upstream docs on [`AsyncClient`]).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
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
use crate::sensor_rx::SensorCapacitanceZones;
use crate::serial_prime;

const HA_STATUS_TOPIC: &str = "homeassistant/status";
const DISCOVERY_OBJECT_ID_DEVICEINFO_LABEL: &str = "narcolepsy_deviceinfo_device_label";
const DISCOVERY_OBJECT_ID_DEVICEINFO_ID: &str = "narcolepsy_deviceinfo_device_id";
const DISCOVERY_OBJECT_ID_PRIME: &str = "narcolepsy_prime";
const DISCOVERY_OBJECT_ID_REQUEST_TEMPERATURES: &str = "narcolepsy_request_temperatures";
const DISCOVERY_OBJECT_ID_LED: &str = "narcolepsy_led";
const DISCOVERY_OBJECT_ID_STARTUP_LED: &str = "narcolepsy_startup_led";
const DISCOVERY_OBJECT_ID_CLIMATE_LEFT: &str = "narcolepsy_climate_left";
const DISCOVERY_OBJECT_ID_CLIMATE_RIGHT: &str = "narcolepsy_climate_right";
const DISCOVERY_OBJECT_ID_VIBRATE_LEFT: &str = "narcolepsy_vibrate_left";
const DISCOVERY_OBJECT_ID_VIBRATE_RIGHT: &str = "narcolepsy_vibrate_right";
const DISCOVERY_OBJECT_ID_TEMP_LEFT: &str = "narcolepsy_temp_left";
const DISCOVERY_OBJECT_ID_TEMP_RIGHT: &str = "narcolepsy_temp_right";
const DISCOVERY_OBJECT_ID_HEATSINK_TEMP: &str = "narcolepsy_heatsink_temp";
const DISCOVERY_OBJECT_ID_VIBRATION_INTENSITY: &str = "narcolepsy_vibration_intensity";
const DISCOVERY_OBJECT_ID_VIBRATION_DURATION: &str = "narcolepsy_vibration_duration";
const DISCOVERY_OBJECT_ID_VIBRATION_PATTERN: &str = "narcolepsy_vibration_pattern";
const DISCOVERY_OBJECT_ID_VIBRATION_CANCEL_PREAMBLE: &str = "narcolepsy_vibration_cancel_preamble";
const DISCOVERY_OBJECT_ID_TARGET_TEMP_LEFT: &str = "narcolepsy_target_temp_left";
const DISCOVERY_OBJECT_ID_TARGET_TEMP_RIGHT: &str = "narcolepsy_target_temp_right";
const DISCOVERY_OBJECT_ID_PRESENCE_LEFT: &str = "narcolepsy_presence_left";
const DISCOVERY_OBJECT_ID_PRESENCE_RIGHT: &str = "narcolepsy_presence_right";
const DISCOVERY_OBJECT_ID_PRESENCE_ANY: &str = "narcolepsy_presence_any";
const DISCOVERY_OBJECT_ID_CALIBRATE_PRESENCE: &str = "narcolepsy_calibrate_presence";
const DISCOVERY_OBJECT_ID_PRESENCE_CAP_THRESHOLD: &str = "narcolepsy_presence_cap_threshold";
const DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_DELTA: &str = "narcolepsy_presence_baseline_delta";
const DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_ZONES: &str = "narcolepsy_presence_baseline_zones";
const DISCOVERY_OBJECT_ID_PRESENCE_CALIBRATION: &str = "narcolepsy_presence_calibration";
const DISCOVERY_OBJECT_ID_WATER_TANK: &str = "narcolepsy_water_tank";
const DISCOVERY_OBJECT_ID_FIRMWARE_MESSAGE: &str = "narcolepsy_firmware_message";
/// Home Assistant [HVACMode](https://developers.home-assistant.io/docs/core/entity/climate#hvac-modes) for active regulation.
const CLIMATE_MODE_HEAT_COOL: &str = "heat_cool";
const CLIMATE_MODE_OFF: &str = "off";

/// Runtime vibration / `SetAlarm` parameters (Home Assistant **number** / **select** / **switch**; retained state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VibrationSettings {
    pub intensity: u8,
    pub duration_sec: u32,
    pub pattern: AlarmPattern,
    pub cancel_preamble: bool,
}

impl Default for VibrationSettings {
    fn default() -> Self {
        Self {
            intensity: 64,
            duration_sec: 15,
            pattern: AlarmPattern::Single,
            cancel_preamble: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_username: Option<String>,
    pub mqtt_password: Option<String>,
    pub mqtt_client_id: String,
    pub topic_prefix: String,
    pub discovery_prefix: String,
    pub climate_min_temp: f64,
    pub climate_max_temp: f64,
    pub climate_temp_step: f64,
    pub device_name: String,
    pub device_identifier: String,
    /// Home Assistant `device.model` from [`Cli::pod`](crate::cli::Cli::pod).
    pub device_model: String,
    pub sw_version: String,
    pub payload_press: String,
    pub serial_device: std::path::PathBuf,
    pub serial_baud: u32,
    /// `None` → LED feature disabled (I²C probe failed at startup).
    pub i2c_device: Option<PathBuf>,
    /// `None` → vibration MQTT buttons disabled (Sensor UART probe failed at startup).
    pub sensor_device: Option<PathBuf>,
    pub sensor_baud: u32,
    /// Uncalibrated: side occupied if **`max`** of three zones `>= threshold` (MQTT **Presence Cap Threshold** updates this at runtime).
    pub presence_cap_threshold: Arc<AtomicU16>,
    /// Calibrated: zone counts when raw `>` baseline + Δ (MQTT **Presence Baseline Delta**; opensleep default **50**).
    pub presence_baseline_delta: Arc<AtomicU16>,
    /// Calibrated baseline grid from MQTT (**retained**) or runtime **Calibrate presence**; synced into the occupancy task each `0x33` sample.
    pub presence_baselines_mtx: Arc<Mutex<Option<[u16; 6]>>>,
    /// Window for MQTT **Calibrate presence** (average samples → baseline per zone; opensleep default 10.).
    pub presence_calibrate_secs: u64,
    /// MQTT occupancy from Sensor capacitance (needs open Sensor UART; see [`crate::main`]).
    pub presence_discovery: bool,
    /// Log each `0x33` presence inference step at INFO ([`Cli::presence_debug`](crate::cli::Cli::presence_debug)).
    pub presence_debug: bool,
    pub vibration_settings: Arc<Mutex<VibrationSettings>>,
    /// When set, Frozen frames are queued to [`crate::frozen_link`] instead of opening the port per command.
    pub frozen_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// When set, vibration batches go to [`crate::sensor_link`].
    pub sensor_tx: Option<mpsc::Sender<Vec<Vec<u8>>>>,
    /// Publish MQTT sensors for Frozen inbound temperatures (`0x41` / `0xC1`); set by [`crate::main`] with [`crate::frozen_link`].
    pub frozen_temperature_discovery: bool,
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
            climate_min_temp: cli.climate_min_temp,
            climate_max_temp: cli.climate_max_temp,
            climate_temp_step: cli.climate_temp_step,
            device_name: cli.device_name.clone(),
            device_identifier: cli.device_identifier.clone(),
            device_model: cli.pod.homeassistant_device_model().to_string(),
            sw_version: env!("CARGO_PKG_VERSION").to_string(),
            payload_press: cli.payload_press.clone(),
            serial_device: cli.serial_device.clone(),
            serial_baud: cli.effective_serial_baud(),
            i2c_device: None,
            sensor_device: None,
            sensor_baud: cli.effective_sensor_baud(),
            presence_cap_threshold: Arc::new(AtomicU16::new(
                cli.presence_cap_threshold
                    .clamp(PRESENCE_CAP_THRESHOLD_MIN, PRESENCE_CAP_THRESHOLD_MAX),
            )),
            presence_baseline_delta: Arc::new(AtomicU16::new(
                cli.presence_baseline_delta
                    .clamp(PRESENCE_BASELINE_DELTA_MIN, PRESENCE_BASELINE_DELTA_MAX),
            )),
            presence_baselines_mtx: Arc::new(Mutex::new(None)),
            presence_calibrate_secs: cli.presence_calibrate_secs.max(3),
            presence_discovery: false,
            presence_debug: cli.presence_debug,
            vibration_settings: Arc::new(Mutex::new(VibrationSettings {
                intensity: cli.vibration_intensity.clamp(1, 100),
                duration_sec: cli.vibration_duration_sec.clamp(1, 600),
                pattern: match cli.vibration_pattern {
                    crate::cli::VibrationPatternArg::Single => AlarmPattern::Single,
                    crate::cli::VibrationPatternArg::Double => AlarmPattern::Double,
                },
                cancel_preamble: true,
            })),
            frozen_tx: None,
            sensor_tx: None,
            frozen_temperature_discovery: false,
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
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRIME
        )
    }

    pub fn request_get_temperatures_command_topic(&self) -> String {
        format!("{}/button/request_get_temperatures/set", self.topic_prefix)
    }

    pub fn discovery_topic_request_get_temperatures(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_REQUEST_TEMPERATURES
        )
    }

    pub fn discovery_topic_light(&self) -> String {
        format!(
            "{}/light/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_LED
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
            self.discovery_prefix, DISCOVERY_OBJECT_ID_STARTUP_LED
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
            BedSide::Left => DISCOVERY_OBJECT_ID_CLIMATE_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_CLIMATE_RIGHT,
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
            BedSide::Left => DISCOVERY_OBJECT_ID_VIBRATE_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_VIBRATE_RIGHT,
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

    pub fn vibration_intensity_command_topic(&self) -> String {
        format!("{}/number/vibration_intensity/set", self.topic_prefix)
    }

    pub fn vibration_intensity_state_topic(&self) -> String {
        format!("{}/number/vibration_intensity/state", self.topic_prefix)
    }

    pub fn discovery_topic_vibration_intensity(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_VIBRATION_INTENSITY
        )
    }

    pub fn vibration_duration_command_topic(&self) -> String {
        format!("{}/number/vibration_duration/set", self.topic_prefix)
    }

    pub fn vibration_duration_state_topic(&self) -> String {
        format!("{}/number/vibration_duration/state", self.topic_prefix)
    }

    pub fn discovery_topic_vibration_duration(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_VIBRATION_DURATION
        )
    }

    pub fn vibration_pattern_command_topic(&self) -> String {
        format!("{}/select/vibration_pattern/set", self.topic_prefix)
    }

    pub fn vibration_pattern_state_topic(&self) -> String {
        format!("{}/select/vibration_pattern/state", self.topic_prefix)
    }

    pub fn discovery_topic_vibration_pattern(&self) -> String {
        format!(
            "{}/select/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_VIBRATION_PATTERN
        )
    }

    pub fn vibration_cancel_preamble_command_topic(&self) -> String {
        format!("{}/switch/vibration_cancel_preamble/set", self.topic_prefix)
    }

    pub fn vibration_cancel_preamble_state_topic(&self) -> String {
        format!(
            "{}/switch/vibration_cancel_preamble/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_vibration_cancel_preamble(&self) -> String {
        format!(
            "{}/switch/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_VIBRATION_CANCEL_PREAMBLE
        )
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
            BedSide::Left => DISCOVERY_OBJECT_ID_TEMP_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_TEMP_RIGHT,
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn discovery_topic_frozen_heatsink_temp(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_HEATSINK_TEMP
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
            BedSide::Left => DISCOVERY_OBJECT_ID_TARGET_TEMP_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_TARGET_TEMP_RIGHT,
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

    pub fn presence_any_state_topic(&self) -> String {
        format!("{}/binary_sensor/presence_any/state", self.topic_prefix)
    }

    pub fn discovery_topic_presence(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => DISCOVERY_OBJECT_ID_PRESENCE_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_PRESENCE_RIGHT,
        };
        format!("{}/binary_sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn discovery_topic_presence_any(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRESENCE_ANY
        )
    }

    pub fn calibrate_presence_command_topic(&self) -> String {
        format!("{}/button/calibrate_presence/set", self.topic_prefix)
    }

    pub fn discovery_topic_calibrate_presence(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_CALIBRATE_PRESENCE
        )
    }

    pub fn presence_cap_threshold_command_topic(&self) -> String {
        format!("{}/number/presence_cap_threshold/set", self.topic_prefix)
    }

    pub fn presence_cap_threshold_state_topic(&self) -> String {
        format!("{}/number/presence_cap_threshold/state", self.topic_prefix)
    }

    pub fn discovery_topic_presence_cap_threshold(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRESENCE_CAP_THRESHOLD
        )
    }

    pub fn presence_baseline_delta_command_topic(&self) -> String {
        format!("{}/number/presence_baseline_delta/set", self.topic_prefix)
    }

    pub fn presence_baseline_delta_state_topic(&self) -> String {
        format!("{}/number/presence_baseline_delta/state", self.topic_prefix)
    }

    pub fn discovery_topic_presence_baseline_delta(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_DELTA
        )
    }

    pub fn presence_baseline_zones_state_topic(&self) -> String {
        format!("{}/sensor/presence_baseline_zones/state", self.topic_prefix)
    }

    pub fn discovery_topic_presence_baseline_zones(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_ZONES
        )
    }

    pub fn presence_calibration_state_topic(&self) -> String {
        format!(
            "{}/binary_sensor/presence_calibration/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_presence_calibration(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_PRESENCE_CALIBRATION
        )
    }

    pub fn water_tank_state_topic(&self) -> String {
        format!("{}/binary_sensor/water_tank/state", self.topic_prefix)
    }

    pub fn discovery_topic_water_tank(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_WATER_TANK
        )
    }

    pub fn firmware_message_state_topic(&self) -> String {
        format!("{}/sensor/firmware_message/state", self.topic_prefix)
    }

    pub fn discovery_topic_firmware_message(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_FIRMWARE_MESSAGE
        )
    }

    pub fn deviceinfo_device_label_state_topic(&self) -> String {
        format!("{}/sensor/deviceinfo_device_label/state", self.topic_prefix)
    }

    pub fn deviceinfo_device_id_state_topic(&self) -> String {
        format!("{}/sensor/deviceinfo_device_id/state", self.topic_prefix)
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
            "model": self.device_model.clone(),
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
        "entity_category": "config",
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
        BedSide::Left => "Vibrate Cover Left",
        BedSide::Right => "Vibrate Cover Right",
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
        "icon": "mdi:vibrate",
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_intensity_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Intensity",
        "command_topic": config.vibration_intensity_command_topic(),
        "state_topic": config.vibration_intensity_state_topic(),
        "min": 1,
        "max": 100,
        "step": 1,
        "mode": "slider",
        "entity_category": "config",
        "unique_id": format!("{}_vibration_intensity", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_duration_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Duration",
        "command_topic": config.vibration_duration_command_topic(),
        "state_topic": config.vibration_duration_state_topic(),
        "min": 1,
        "max": 600,
        "step": 1,
        "unit_of_measurement": "s",
        "mode": "box",
        "entity_category": "config",
        "unique_id": format!("{}_vibration_duration", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_pattern_select(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Pattern",
        "command_topic": config.vibration_pattern_command_topic(),
        "state_topic": config.vibration_pattern_state_topic(),
        "options": ["single", "double"],
        "entity_category": "config",
        "unique_id": format!("{}_vibration_pattern", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_cancel_preamble_switch(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Cancel Preamble",
        "command_topic": config.vibration_cancel_preamble_command_topic(),
        "state_topic": config.vibration_cancel_preamble_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "entity_category": "config",
        "enabled_by_default": false,
        "unique_id": format!("{}_vibration_cancel_preamble", config.device_identifier),
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
        "state_topic": config.deviceinfo_device_label_state_topic(),
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
        "state_topic": config.deviceinfo_device_id_state_topic(),
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

fn discovery_payload_presence_any(config: &BridgeConfig) -> String {
    json!({
        "name": "Presence Any",
        "state_topic": config.presence_any_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "occupancy",
        "unique_id": format!("{}_presence_any", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_presence_calibration_binary_sensor(config: &BridgeConfig) -> String {
    json!({
        "name": "Presence Calibration",
        "state_topic": config.presence_calibration_state_topic(),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "running",
        "icon": "mdi:leak",
        "unique_id": format!("{}_presence_calibration", config.device_identifier),
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
        "name": "Calibrate Presence",
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

fn discovery_payload_presence_cap_threshold_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Presence Cap Threshold",
        "command_topic": config.presence_cap_threshold_command_topic(),
        "state_topic": config.presence_cap_threshold_state_topic(),
        "min": PRESENCE_CAP_THRESHOLD_MIN,
        "max": PRESENCE_CAP_THRESHOLD_MAX,
        "step": 1,
        "mode": "box",
        "entity_category": "config",
        "enabled_by_default": false,
        "unique_id": format!("{}_presence_cap_threshold", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_presence_baseline_zones_sensor(config: &BridgeConfig) -> String {
    json!({
        "name": "Presence Baseline Zones",
        "state_topic": config.presence_baseline_zones_state_topic(),
        "unique_id": format!("{}_presence_baseline_zones", config.device_identifier),
        "enabled_by_default": false,
        "entity_category": "diagnostic",
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_presence_baseline_delta_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Presence Baseline Delta",
        "command_topic": config.presence_baseline_delta_command_topic(),
        "state_topic": config.presence_baseline_delta_state_topic(),
        "min": PRESENCE_BASELINE_DELTA_MIN,
        "max": PRESENCE_BASELINE_DELTA_MAX,
        "step": 1,
        "mode": "box",
        "entity_category": "config",
        "enabled_by_default": false,
        "unique_id": format!("{}_presence_baseline_delta", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// Raw capacitance MQTT number bounds (uncalibrated max‑per‑side vs threshold).
const PRESENCE_CAP_THRESHOLD_MIN: u16 = 1;
const PRESENCE_CAP_THRESHOLD_MAX: u16 = u16::MAX;

/// opensleep [`PresenceConfig.threshold`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/presence.rs) (`DEFAULT_THRESHOLD` = **50**) — HA **Presence Baseline Delta** default; clamped to this range at runtime.
const PRESENCE_BASELINE_DELTA_MIN: u16 = 1;
const PRESENCE_BASELINE_DELTA_MAX: u16 = 2000;

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

    fn sync_baselines_from_shared(&mut self, shared: &Option<[u16; 6]>) {
        if &self.baselines == shared {
            return;
        }
        match shared {
            Some(b) => self.set_baselines(*b),
            None => {
                self.baselines = None;
                self.debounce = [0; 6];
            }
        }
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

/// Slack above strongest per‑mattress‑side capacitance during the calibration window → **Presence Cap Threshold**
/// (single threshold for left and right vs max‑of‑zones in uncalibrated mode).
const PRESENCE_CALIB_CAP_PADDING: u16 = 48;

/// Slack above strongest |sample − baseline| per zone → **Presence Baseline Delta** (Δ over mean baseline).
const PRESENCE_CALIB_DELTA_PADDING: u16 = 16;

/// Derive MQTT **Presence Cap Threshold** / **Presence Baseline Delta** from empty‑bed **`0x33`** samples (`cal_samples` window).
///
/// Threshold: max(left three zones, right three zones) across samples plus padding — empty bed stays below occupancy in **uncalibrated** mode next run.
///
/// Δ: strongest absolute deviation from the computed baseline grid plus padding — noise headroom vs opensleep‑style fixed **50**.
fn presence_tune_from_calibration_samples(
    samples: &[[u16; 6]],
    baselines: &[u16; 6],
) -> (u16, u16) {
    let mut peak_abs_dev: u16 = 0;
    for s in samples {
        for i in 0..6 {
            let dev = if s[i] > baselines[i] {
                s[i] - baselines[i]
            } else {
                baselines[i] - s[i]
            };
            peak_abs_dev = peak_abs_dev.max(dev);
        }
    }
    let mut peak_left_side = 0u16;
    let mut peak_right_side = 0u16;
    for s in samples {
        peak_left_side = peak_left_side.max(s[0].max(s[1]).max(s[2]));
        peak_right_side = peak_right_side.max(s[3].max(s[4]).max(s[5]));
    }
    let peak_side_signal = peak_left_side.max(peak_right_side);
    let cap = peak_side_signal
        .saturating_add(PRESENCE_CALIB_CAP_PADDING)
        .clamp(PRESENCE_CAP_THRESHOLD_MIN, PRESENCE_CAP_THRESHOLD_MAX);
    let delta = peak_abs_dev
        .saturating_add(PRESENCE_CALIB_DELTA_PADDING)
        .clamp(PRESENCE_BASELINE_DELTA_MIN, PRESENCE_BASELINE_DELTA_MAX);
    (cap, delta)
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
    baseline_delta: u16,
) -> (bool, bool) {
    let Some(ref baselines) = inference.baselines else {
        return (false, false);
    };
    for (i, &b) in baselines.iter().enumerate() {
        if z.zones[i] > b.saturating_add(baseline_delta) {
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
    baseline_delta: u16,
) -> (bool, bool) {
    if inference.baselines.is_some() {
        occupancy_calibrated(z, inference, baseline_delta)
    } else {
        occupancy_uncalibrated_max(z, fallback_abs_threshold)
    }
}

/// Per-sample diagnostic for [`BridgeConfig::presence_debug`].
fn log_presence_debug(
    z: &SensorCapacitanceZones,
    inference: &PresenceInferenceState,
    fallback_abs_threshold: u16,
    baseline_delta: u16,
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
                delta = baseline_delta,
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

async fn publish_presence_calibration_running_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    running: bool,
) {
    let qos = QoS::AtLeastOnce;
    let payload = if running { "ON" } else { "OFF" };
    if let Err(e) = client
        .publish(
            config.presence_calibration_state_topic(),
            qos,
            true,
            payload.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish presence calibration running state");
    }
}

async fn publish_presence_readings(
    client: &AsyncClient,
    config: &BridgeConfig,
    left_on: bool,
    right_on: bool,
) {
    let qos = QoS::AtLeastOnce;
    let any_on = left_on || right_on;
    let publishes = [
        (
            config.presence_state_topic(BedSide::Left),
            if left_on { "ON" } else { "OFF" },
        ),
        (
            config.presence_state_topic(BedSide::Right),
            if right_on { "ON" } else { "OFF" },
        ),
        (
            config.presence_any_state_topic(),
            if any_on { "ON" } else { "OFF" },
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

fn ingest_climate_mode_from_state_payload(st: &mut ClimateSideState, payload: &[u8]) -> bool {
    let Ok(mode_str) = std::str::from_utf8(payload) else {
        return false;
    };
    match mode_str.trim() {
        CLIMATE_MODE_OFF => st.enabled = false,
        CLIMATE_MODE_HEAT_COOL => st.enabled = true,
        _ => return false,
    };
    true
}

fn ingest_climate_temperature_from_state_payload(
    st: &mut ClimateSideState,
    config: &BridgeConfig,
    payload: &[u8],
) -> bool {
    let Ok(text) = std::str::from_utf8(payload) else {
        return false;
    };
    let Ok(temp_c) = text.trim().parse::<f64>() else {
        return false;
    };
    let clamped = temp_c.clamp(config.climate_min_temp, config.climate_max_temp);
    st.target_centi = (clamped * 100.0).round() as u16;
    true
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
    /// While **true**, inbound retained **`state_topic`** payloads repopulate UI-facing settings so broker-stored HA choices survive narcolepsy restarts (climate, MQTT light snapshot, vibration, presence). Ignore after drain — later `…/state` echoes must not overwrite values set via `…/set` (no `unsubscribe`).
    mqtt_ha_state_bootstrap: Arc<AtomicBool>,
    climate_left: Arc<Mutex<ClimateSideState>>,
    climate_right: Arc<Mutex<ClimateSideState>>,
    /// Notify presence task to start baseline sampling (MQTT **Calibrate presence**).
    presence_calibrate_tx: Option<mpsc::Sender<()>>,
}

async fn apply_vibration_intensity_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(v) = parse_and_clamp_u8_intensity(payload) else {
        tracing::trace!("ignored vibration intensity MQTT payload");
        return;
    };
    {
        let mut g = config.vibration_settings.lock().await;
        if g.intensity == v {
            return;
        }
        g.intensity = v;
    }
    publish_vibration_intensity_state(client, config, v).await;
}

async fn apply_vibration_duration_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(secs) = parse_and_clamp_duration_secs(payload) else {
        tracing::trace!("ignored vibration duration MQTT payload");
        return;
    };
    {
        let mut g = config.vibration_settings.lock().await;
        if g.duration_sec == secs {
            return;
        }
        g.duration_sec = secs;
    }
    publish_vibration_duration_state(client, config, secs).await;
}

async fn apply_vibration_pattern_mqtt(client: &AsyncClient, config: &BridgeConfig, payload: &[u8]) {
    let Some(p) = parse_vibration_pattern_str(payload) else {
        tracing::trace!("ignored vibration pattern MQTT payload");
        return;
    };
    {
        let mut g = config.vibration_settings.lock().await;
        if g.pattern == p {
            return;
        }
        g.pattern = p;
    }
    publish_vibration_pattern_state(client, config, p).await;
}

async fn apply_vibration_cancel_preamble_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(on) = parse_mqtt_on_off(payload) else {
        tracing::trace!("ignored vibration cancel preamble MQTT payload");
        return;
    };
    {
        let mut g = config.vibration_settings.lock().await;
        if g.cancel_preamble == on {
            return;
        }
        g.cancel_preamble = on;
    }
    publish_vibration_cancel_preamble_state(client, config, on).await;
}

async fn handle_vibrate_press(client: &AsyncClient, config: &BridgeConfig, side: BedSide) {
    if config.sensor_device.is_none() {
        return;
    }
    let (intensity, duration, pattern, cancel_preamble) = {
        let g = config.vibration_settings.lock().await;
        (g.intensity, g.duration_sec, g.pattern, g.cancel_preamble)
    };
    let intensity = intensity.clamp(1, 100);
    let duration = duration.clamp(1, 600);
    let frames = vibration_sequence_frames(side, intensity, pattern, duration, cancel_preamble);
    let frame_count = frames.len();
    match enqueue_sensor_vibration(config, frames).await {
        Ok(()) => {
            tracing::info!(
                ?side,
                intensity,
                duration,
                pattern = ?pattern,
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

fn vibration_pattern_as_str(p: AlarmPattern) -> &'static str {
    match p {
        AlarmPattern::Single => "single",
        AlarmPattern::Double => "double",
    }
}

fn parse_vibration_pattern_str(payload: &[u8]) -> Option<AlarmPattern> {
    let s = std::str::from_utf8(payload).ok()?.trim().to_lowercase();
    match s.as_str() {
        "single" => Some(AlarmPattern::Single),
        "double" => Some(AlarmPattern::Double),
        _ => None,
    }
}

fn parse_and_clamp_u8_intensity(payload: &[u8]) -> Option<u8> {
    let t = std::str::from_utf8(payload).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    let v: f64 = t.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    let v = v.round() as i64;
    Some((v.clamp(1, 100)) as u8)
}

fn parse_and_clamp_duration_secs(payload: &[u8]) -> Option<u32> {
    let t = std::str::from_utf8(payload).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    let v: f64 = t.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    let v = v.round() as i64;
    Some((v.clamp(1, 600)) as u32)
}

fn parse_and_clamp_presence_cap_threshold_mqtt(payload: &[u8]) -> Option<u16> {
    let t = std::str::from_utf8(payload).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    let v: f64 = t.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    let v = v.round() as i64;
    Some(v.clamp(
        i64::from(PRESENCE_CAP_THRESHOLD_MIN),
        i64::from(PRESENCE_CAP_THRESHOLD_MAX),
    ) as u16)
}

fn parse_and_clamp_presence_baseline_delta_mqtt(payload: &[u8]) -> Option<u16> {
    let t = std::str::from_utf8(payload).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    let v: f64 = t.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    let v = v.round() as i64;
    Some(v.clamp(
        i64::from(PRESENCE_BASELINE_DELTA_MIN),
        i64::from(PRESENCE_BASELINE_DELTA_MAX),
    ) as u16)
}

async fn publish_presence_cap_threshold_state(client: &AsyncClient, config: &BridgeConfig, v: u16) {
    let qos = QoS::AtLeastOnce;
    let body = format!("{v}");
    if let Err(e) = client
        .publish(
            config.presence_cap_threshold_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish presence cap threshold state");
    }
}

async fn publish_presence_baseline_delta_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    v: u16,
) {
    let qos = QoS::AtLeastOnce;
    let body = format!("{v}");
    if let Err(e) = client
        .publish(
            config.presence_baseline_delta_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish presence baseline delta state");
    }
}

async fn publish_presence_sensitivity_states(client: &AsyncClient, config: &BridgeConfig) {
    let thr = config.presence_cap_threshold.load(Ordering::Relaxed);
    let delta = config.presence_baseline_delta.load(Ordering::Relaxed);
    publish_presence_cap_threshold_state(client, config, thr).await;
    publish_presence_baseline_delta_state(client, config, delta).await;
}

/// JSON **`[z0,z1,…,z5]`** or `{"zones":[…]}` — broker-retained copy of calibrated baselines.
#[derive(Deserialize)]
struct PresenceBaselineZonesJson {
    zones: [u16; 6],
}

fn parse_presence_baseline_zones_payload(payload: &[u8]) -> Option<[u16; 6]> {
    let t = std::str::from_utf8(payload).ok()?.trim();
    if t.is_empty() {
        return None;
    }
    serde_json::from_str::<[u16; 6]>(t).ok().or_else(|| {
        serde_json::from_str::<PresenceBaselineZonesJson>(t)
            .ok()
            .map(|w| w.zones)
    })
}

async fn publish_presence_baseline_zones_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    zones: &[u16; 6],
) {
    let qos = QoS::AtLeastOnce;
    let body = match serde_json::to_string(zones) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(?e, "serialize presence baseline zones");
            return;
        }
    };
    if let Err(e) = client
        .publish(
            config.presence_baseline_zones_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish presence baseline zones state");
    }
}

/// After broker retain drain: republish numbers + optional zones (refreshes HA without clobbering retain during discovery).
async fn publish_presence_bootstrap_finalize(client: &AsyncClient, config: &BridgeConfig) {
    publish_presence_sensitivity_states(client, config).await;
    let snap = *config.presence_baselines_mtx.lock().await;
    if let Some(ref z) = snap {
        publish_presence_baseline_zones_state(client, config, z).await;
    }
}

async fn apply_presence_cap_threshold_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(v) = parse_and_clamp_presence_cap_threshold_mqtt(payload) else {
        tracing::trace!("ignored presence cap threshold MQTT payload");
        return;
    };
    let prev = config.presence_cap_threshold.load(Ordering::SeqCst);
    if prev == v {
        return;
    }
    config.presence_cap_threshold.store(v, Ordering::SeqCst);
    publish_presence_cap_threshold_state(client, config, v).await;
}

async fn apply_presence_baseline_delta_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(v) = parse_and_clamp_presence_baseline_delta_mqtt(payload) else {
        tracing::trace!("ignored presence baseline delta MQTT payload");
        return;
    };
    let prev = config.presence_baseline_delta.load(Ordering::SeqCst);
    if prev == v {
        return;
    }
    config.presence_baseline_delta.store(v, Ordering::SeqCst);
    publish_presence_baseline_delta_state(client, config, v).await;
}

async fn publish_vibration_intensity_state(client: &AsyncClient, config: &BridgeConfig, v: u8) {
    let qos = QoS::AtLeastOnce;
    let body = format!("{v}");
    if let Err(e) = client
        .publish(
            config.vibration_intensity_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish vibration intensity state");
    }
}

async fn publish_vibration_duration_state(client: &AsyncClient, config: &BridgeConfig, secs: u32) {
    let qos = QoS::AtLeastOnce;
    let body = format!("{secs}");
    if let Err(e) = client
        .publish(
            config.vibration_duration_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish vibration duration state");
    }
}

async fn publish_vibration_pattern_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    p: AlarmPattern,
) {
    let qos = QoS::AtLeastOnce;
    let body = vibration_pattern_as_str(p);
    if let Err(e) = client
        .publish(
            config.vibration_pattern_state_topic(),
            qos,
            true,
            body.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish vibration pattern state");
    }
}

async fn publish_vibration_cancel_preamble_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    on: bool,
) {
    let payload = if on { "ON" } else { "OFF" };
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.vibration_cancel_preamble_state_topic(),
            qos,
            true,
            payload,
        )
        .await
    {
        tracing::error!(?e, "publish vibration cancel preamble state");
    }
}

async fn publish_vibration_mqtt_states(client: &AsyncClient, config: &BridgeConfig) {
    let vs = *config.vibration_settings.lock().await;
    publish_vibration_intensity_state(client, config, vs.intensity).await;
    publish_vibration_duration_state(client, config, vs.duration_sec).await;
    publish_vibration_pattern_state(client, config, vs.pattern).await;
    publish_vibration_cancel_preamble_state(client, config, vs.cancel_preamble).await;
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

async fn publish_discovery_and_online(client: &AsyncClient, config: &BridgeConfig) {
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
                config.deviceinfo_device_label_state_topic(),
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
                config.deviceinfo_device_id_state_topic(),
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
        let disc_i = discovery_payload_vibration_intensity_number(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_vibration_intensity(),
                qos,
                true,
                disc_i,
            )
            .await
        {
            tracing::error!(?e, "publish vibration intensity discovery");
        }
        let disc_d = discovery_payload_vibration_duration_number(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_vibration_duration(),
                qos,
                true,
                disc_d,
            )
            .await
        {
            tracing::error!(?e, "publish vibration duration discovery");
        }
        let disc_p = discovery_payload_vibration_pattern_select(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_vibration_pattern(),
                qos,
                true,
                disc_p,
            )
            .await
        {
            tracing::error!(?e, "publish vibration pattern discovery");
        }
        let disc_c = discovery_payload_vibration_cancel_preamble_switch(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_vibration_cancel_preamble(),
                qos,
                true,
                disc_c,
            )
            .await
        {
            tracing::error!(?e, "publish vibration cancel preamble discovery");
        }
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
        let disc_any = discovery_payload_presence_any(config);
        if let Err(e) = client
            .publish(config.discovery_topic_presence_any(), qos, true, disc_any)
            .await
        {
            tracing::error!(?e, "publish presence_any discovery");
        }
        // HA shows "Unknown" until a retained state arrives; capacitance `0x33` may be rare/absent at boot.
        publish_presence_readings(client, config, false, false).await;
        publish_presence_calibration_running_state(client, config, false).await;
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
        let disc_thr = discovery_payload_presence_cap_threshold_number(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_presence_cap_threshold(),
                qos,
                true,
                disc_thr,
            )
            .await
        {
            tracing::error!(?e, "publish presence cap threshold discovery");
        }
        let disc_delta = discovery_payload_presence_baseline_delta_number(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_presence_baseline_delta(),
                qos,
                true,
                disc_delta,
            )
            .await
        {
            tracing::error!(?e, "publish presence baseline delta discovery");
        }
        let disc_z = discovery_payload_presence_baseline_zones_sensor(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_presence_baseline_zones(),
                qos,
                true,
                disc_z,
            )
            .await
        {
            tracing::error!(?e, "publish presence baseline zones discovery");
        }
        let disc_cal_run = discovery_payload_presence_calibration_binary_sensor(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_presence_calibration(),
                qos,
                true,
                disc_cal_run,
            )
            .await
        {
            tracing::error!(?e, "publish presence calibration running discovery");
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
    let disc = discovery_payload_water_tank(config);
    if let Err(e) = client
        .publish(config.discovery_topic_water_tank(), qos, true, disc)
        .await
    {
        tracing::error!(?e, "publish Frozen water tank discovery");
    }
    let disc = discovery_payload_firmware_message(config);
    if let Err(e) = client
        .publish(config.discovery_topic_firmware_message(), qos, true, disc)
        .await
    {
        tracing::error!(?e, "publish Frozen firmware message discovery");
    }
    if let Err(e) = client
        .publish(config.availability_topic(), qos, true, "online")
        .await
    {
        tracing::error!(?e, "publish availability online");
    }
}

async fn setup_session(client: &AsyncClient, config: &BridgeConfig) {
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
        if let Err(e) = client
            .subscribe(config.climate_mode_state_topic(side), qos)
            .await
        {
            tracing::error!(?e, side = ?side, "subscribe climate mode state topic (broker retain)");
        }
        if let Err(e) = client
            .subscribe(config.climate_temperature_state_topic(side), qos)
            .await
        {
            tracing::error!(?e, side = ?side, "subscribe climate temperature state topic (broker retain)");
        }
    }
    if config.i2c_device.is_some() {
        if let Err(e) = client.subscribe(config.light_state_topic(), qos).await {
            tracing::error!(?e, "subscribe light state topic (broker retain)");
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
        for topic in [
            config.vibration_intensity_command_topic(),
            config.vibration_intensity_state_topic(),
            config.vibration_duration_command_topic(),
            config.vibration_duration_state_topic(),
            config.vibration_pattern_command_topic(),
            config.vibration_pattern_state_topic(),
            config.vibration_cancel_preamble_command_topic(),
            config.vibration_cancel_preamble_state_topic(),
        ] {
            if let Err(e) = client.subscribe(topic.as_str(), qos).await {
                tracing::error!(?e, topic = %topic, "subscribe vibration settings topic");
            }
        }
        if config.presence_discovery {
            if let Err(e) = client
                .subscribe(config.calibrate_presence_command_topic(), qos)
                .await
            {
                tracing::error!(?e, "subscribe calibrate_presence button topic");
            }
            for topic in [
                config.presence_cap_threshold_command_topic(),
                config.presence_cap_threshold_state_topic(),
                config.presence_baseline_delta_command_topic(),
                config.presence_baseline_delta_state_topic(),
                config.presence_baseline_zones_state_topic(),
            ] {
                if let Err(e) = client.subscribe(topic.as_str(), qos).await {
                    tracing::error!(?e, topic = %topic, "subscribe presence sensitivity topic");
                }
            }
        }
    }
    if let Err(e) = client.subscribe(HA_STATUS_TOPIC, qos).await {
        tracing::error!(?e, "subscribe Home Assistant birth topic");
    }
    publish_discovery_and_online(client, config).await;
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

    if config.sensor_device.is_some()
        && config.presence_discovery
        && st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst)
    {
        if p.topic == config.presence_cap_threshold_state_topic() {
            if let Some(v) = parse_and_clamp_presence_cap_threshold_mqtt(&p.payload) {
                config.presence_cap_threshold.store(v, Ordering::SeqCst);
            }
            return;
        }
        if p.topic == config.presence_baseline_delta_state_topic() {
            if let Some(v) = parse_and_clamp_presence_baseline_delta_mqtt(&p.payload) {
                config.presence_baseline_delta.store(v, Ordering::SeqCst);
            }
            return;
        }
        if p.topic == config.presence_baseline_zones_state_topic() {
            if let Some(z) = parse_presence_baseline_zones_payload(p.payload.as_ref()) {
                *config.presence_baselines_mtx.lock().await = Some(z);
            }
            return;
        }
    }

    if config.sensor_device.is_some() && config.presence_discovery {
        if p.topic == config.presence_cap_threshold_command_topic() {
            apply_presence_cap_threshold_mqtt(client, config, &p.payload).await;
            return;
        }
        if p.topic == config.presence_baseline_delta_command_topic() {
            apply_presence_baseline_delta_mqtt(client, config, &p.payload).await;
            return;
        }
    }

    if config.sensor_device.is_some() {
        if p.topic == config.vibration_intensity_command_topic() {
            apply_vibration_intensity_mqtt(client, config, &p.payload).await;
            return;
        }
        if p.topic == config.vibration_duration_command_topic() {
            apply_vibration_duration_mqtt(client, config, &p.payload).await;
            return;
        }
        if p.topic == config.vibration_pattern_command_topic() {
            apply_vibration_pattern_mqtt(client, config, &p.payload).await;
            return;
        }
        if p.topic == config.vibration_cancel_preamble_command_topic() {
            apply_vibration_cancel_preamble_mqtt(client, config, &p.payload).await;
            return;
        }
        if p.topic == config.vibration_intensity_state_topic() {
            if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
                apply_vibration_intensity_mqtt(client, config, &p.payload).await;
            }
            return;
        }
        if p.topic == config.vibration_duration_state_topic() {
            if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
                apply_vibration_duration_mqtt(client, config, &p.payload).await;
            }
            return;
        }
        if p.topic == config.vibration_pattern_state_topic() {
            if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
                apply_vibration_pattern_mqtt(client, config, &p.payload).await;
            }
            return;
        }
        if p.topic == config.vibration_cancel_preamble_state_topic() {
            if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
                apply_vibration_cancel_preamble_mqtt(client, config, &p.payload).await;
            }
            return;
        }
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

    if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
        if p.topic == config.climate_mode_state_topic(BedSide::Left) {
            ingest_climate_mode_from_state_payload(&mut *st.climate_left.lock().await, &p.payload);
            return;
        }
        if p.topic == config.climate_mode_state_topic(BedSide::Right) {
            ingest_climate_mode_from_state_payload(&mut *st.climate_right.lock().await, &p.payload);
            return;
        }
        if p.topic == config.climate_temperature_state_topic(BedSide::Left) {
            ingest_climate_temperature_from_state_payload(
                &mut *st.climate_left.lock().await,
                config,
                &p.payload,
            );
            return;
        }
        if p.topic == config.climate_temperature_state_topic(BedSide::Right) {
            ingest_climate_temperature_from_state_payload(
                &mut *st.climate_right.lock().await,
                config,
                &p.payload,
            );
            return;
        }
        if config.i2c_device.is_some() && p.topic == config.light_state_topic() {
            if let Some(cmd) = parse_light_command(&p.payload) {
                let base = LightStateSnapshot::default();
                *st.light_state.lock().await = compute_light_state(&cmd, &base);
            }
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

    for side in [BedSide::Left, BedSide::Right] {
        if p.topic == config.climate_mode_state_topic(side)
            || p.topic == config.climate_temperature_state_topic(side)
        {
            return;
        }
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
    // Like vibration `…/number/…/state`: ignore echoes on `light/led/state`; broker retain is drained only during bootstrap.
    if config.i2c_device.is_some() && p.topic == config.light_state_topic() {
        return;
    }

    if p.topic == HA_STATUS_TOPIC && p.payload.as_ref() == b"online" {
        tracing::debug!("Home Assistant online; republishing discovery");
        publish_discovery_and_online(client, config).await;
    }
}

/// Run the MQTT event loop until a fatal error (or process kill).
pub async fn run(
    config: BridgeConfig,
    prime_frame: Arc<[u8]>,
    frozen_temperature_rx: Option<mpsc::Receiver<FrozenTemperatureUpdate>>,
    frozen_water_tank_rx: mpsc::Receiver<bool>,
    frozen_firmware_message_rx: mpsc::Receiver<String>,
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
        mqtt_ha_state_bootstrap: Arc::new(AtomicBool::new(false)),
        climate_left: Arc::new(Mutex::new(ClimateSideState::default())),
        climate_right: Arc::new(Mutex::new(ClimateSideState::default())),
        presence_calibrate_tx: presence_calibrate_tx.clone(),
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

    let mut water_rx = frozen_water_tank_rx;
    let c = client.clone();
    let cfg = config.clone();
    tokio::spawn(async move {
        let mut last: Option<bool> = None;
        while let Some(present) = water_rx.recv().await {
            if last == Some(present) {
                continue;
            }
            last = Some(present);
            publish_water_tank_state(&c, &cfg, present).await;
        }
    });

    let mut fw_rx = frozen_firmware_message_rx;
    let c = client.clone();
    let cfg = config.clone();
    tokio::spawn(async move {
        while let Some(msg) = fw_rx.recv().await {
            publish_firmware_message_state(&c, &cfg, &msg).await;
        }
    });

    if let Some(mut cap_rx) = capacitance_rx {
        if let Some(mut cal_rx) = presence_calibrate_rx {
            let c = client.clone();
            let cfg = config.clone();
            let cal_wait = Duration::from_secs(cfg.presence_calibrate_secs.max(3));
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
                            publish_presence_calibration_running_state(&c, &cfg, true).await;
                        }
                        z_opt = cap_rx.recv() => {
                            let Some(z) = z_opt else {
                                return;
                            };

                            saw_cap_sample = true;

                            let bl_mtx = *cfg.presence_baselines_mtx.lock().await;
                            inference.sync_baselines_from_shared(&bl_mtx);

                            if cal_until.is_some() {
                                cal_samples.push(z.zones);
                            }
                            if let Some(end) = cal_until {
                                if Instant::now() >= end {
                                    cal_until = None;
                                    match mean_baselines_from_samples(&cal_samples) {
                                        Some(nb) => {
                                            let (tuned_cap, tuned_delta) =
                                                presence_tune_from_calibration_samples(
                                                    &cal_samples,
                                                    &nb,
                                                );
                                            cfg.presence_cap_threshold
                                                .store(tuned_cap, Ordering::SeqCst);
                                            cfg.presence_baseline_delta
                                                .store(tuned_delta, Ordering::SeqCst);

                                            {
                                                let mut g =
                                                    cfg.presence_baselines_mtx.lock().await;
                                                *g = Some(nb);
                                            }
                                            inference.set_baselines(nb);
                                            publish_presence_cap_threshold_state(
                                                &c, &cfg, tuned_cap,
                                            )
                                            .await;
                                            publish_presence_baseline_delta_state(
                                                &c, &cfg, tuned_delta,
                                            )
                                            .await;
                                            publish_presence_baseline_zones_state(
                                                &c, &cfg, &nb,
                                            )
                                            .await;
                                            tracing::info!(
                                                baseline = ?nb,
                                                frames = cal_samples.len(),
                                                presence_cap_threshold = tuned_cap,
                                                presence_baseline_delta = tuned_delta,
                                                debounce_frames = PRESENCE_DEBOUNCE_FRAMES,
                                                pad_cap = PRESENCE_CALIB_CAP_PADDING,
                                                pad_delta = PRESENCE_CALIB_DELTA_PADDING,
                                                "presence calibrated: baselines + MQTT threshold/Δ tuned from empty-bed samples"
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
                                    publish_presence_calibration_running_state(&c, &cfg, false)
                                        .await;
                                }
                            }

                            let abs_thr =
                                cfg.presence_cap_threshold.load(Ordering::Relaxed);
                            let baseline_delta =
                                cfg.presence_baseline_delta.load(Ordering::Relaxed);
                            let (left_on, right_on) =
                                inference_occupancy(&z, &mut inference, abs_thr, baseline_delta);
                            let calibrating = cal_until.is_some();
                            if presence_debug {
                                log_presence_debug(
                                    &z,
                                    &inference,
                                    abs_thr,
                                    baseline_delta,
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
                    // Retained `…/state` replays can arrive on the next `poll()` before the spawned
                    // session task runs — set bootstrap flags here so `handle_publish` ingests them
                    // instead of dropping them at the `light/.../state` echo guard (`bootstrap == false`).
                    handler_state
                        .mqtt_ha_state_bootstrap
                        .store(true, Ordering::SeqCst);
                    handler_state
                        .startup_led_broker_retain_seen
                        .store(false, Ordering::SeqCst);
                    // rumqttc: `AsyncClient` requests are only processed while `eventloop.poll()` runs.
                    // Awaiting `publish`/`subscribe` on the same task that calls `poll()` deadlocks the
                    // event loop — spawn so the broker actually receives discovery and availability.
                    let c = client.clone();
                    let cfg = config.clone();
                    let hs = handler_state.clone();
                    let hw_once = startup_led_hw_once_per_process.clone();
                    tokio::spawn(async move {
                        setup_session(&c, &cfg).await;

                        // Broker retain replay (climate/light/vibration/presence **`state_topic`** + startup_led)
                        // while `mqtt_ha_state_bootstrap` is observed in `handle_publish`.
                        const HA_RETAIN_DRAIN: Duration = Duration::from_millis(850);
                        sleep(HA_RETAIN_DRAIN).await;
                        hs.mqtt_ha_state_bootstrap.store(false, Ordering::SeqCst);

                        for side in [BedSide::Left, BedSide::Right] {
                            let snap = match side {
                                BedSide::Left => hs.climate_left.lock().await.clone(),
                                BedSide::Right => hs.climate_right.lock().await.clone(),
                            };
                            let frame =
                                set_target_temperature_frame(side, snap.enabled, snap.target_centi);
                            if let Err(e) = enqueue_frozen_frame(&cfg, frame).await {
                                tracing::warn!(
                                    ?side,
                                    error = %e,
                                    "climate restore: Frozen enqueue skipped"
                                );
                            }
                            publish_climate_state(&c, &cfg, side, &snap).await;
                        }

                        if cfg.sensor_device.is_some() {
                            publish_vibration_mqtt_states(&c, &cfg).await;
                            if cfg.presence_discovery {
                                publish_presence_bootstrap_finalize(&c, &cfg).await;
                                let no_stored_baselines =
                                    cfg.presence_baselines_mtx.lock().await.is_none();
                                if no_stored_baselines {
                                    tracing::info!(
                                        "presence: no MQTT-retained baseline zones after bootstrap — starting calibration (leave mattress empty)"
                                    );
                                    handle_presence_calibrate_press(
                                        &c,
                                        &cfg,
                                        hs.presence_calibrate_tx.as_ref(),
                                    )
                                    .await;
                                }
                            }
                        }

                        if cfg.i2c_device.is_some() {
                            let startup_on = *hs.startup_led_on.lock().await;
                            if startup_on
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
                            } else {
                                let mqtt_snap = hs.light_state.lock().await.clone();
                                let _ = commit_light_snapshot(&c, &cfg, &hs.light_state, mqtt_snap)
                                    .await;
                            }
                            let snap = hs.light_state.lock().await.clone();
                            publish_light_state(&c, &cfg, &snap).await;
                        }
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
        let disc_any = discovery_payload_presence_any(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_any).unwrap();
        assert_eq!(v["name"].as_str(), Some("Presence Any"));
        assert_eq!(v["device_class"].as_str(), Some("occupancy"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.presence_any_state_topic().as_str()),
        );
    }

    #[test]
    fn presence_calibration_running_discovery_matches_state_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let mut cfg = BridgeConfig::from_cli(&cli);
        cfg.presence_discovery = true;
        cfg.sensor_device = Some(std::path::PathBuf::from("/dev/null"));
        let disc = discovery_payload_presence_calibration_binary_sensor(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["name"].as_str(), Some("Presence Calibration"));
        assert_eq!(v["device_class"].as_str(), Some("running"));
        assert_eq!(v["icon"].as_str(), Some("mdi:leak"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.presence_calibration_state_topic().as_str()),
        );
    }

    #[test]
    fn firmware_message_discovery_matches_state_topic() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_l = discovery_payload_deviceinfo_device_label(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_l).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.deviceinfo_device_label_state_topic().as_str()),
        );
        let disc_i = discovery_payload_deviceinfo_device_id(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_i).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.deviceinfo_device_id_state_topic().as_str()),
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
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
    fn presence_sensitivity_number_discovery_topics() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        for (disc, name) in [
            (
                discovery_payload_presence_cap_threshold_number(&cfg),
                cfg.presence_cap_threshold_state_topic(),
            ),
            (
                discovery_payload_presence_baseline_delta_number(&cfg),
                cfg.presence_baseline_delta_state_topic(),
            ),
        ] {
            let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
            assert_eq!(v["entity_category"].as_str(), Some("config"));
            assert_eq!(v["enabled_by_default"].as_bool(), Some(false));
            assert_eq!(v["state_topic"].as_str(), Some(name.as_str()));
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &discovery_payload_presence_cap_threshold_number(&cfg)
            )
            .unwrap()["command_topic"]
                .as_str(),
            Some(cfg.presence_cap_threshold_command_topic().as_str()),
        );
    }

    #[test]
    fn startup_led_discovery_is_configuration_entity() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_startup_led_switch(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["entity_category"].as_str(), Some("config"));
    }

    #[test]
    fn water_tank_discovery_is_mqtt_binary_sensor_with_plug_device_class() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        assert!(cfg
            .water_tank_state_topic()
            .contains("/binary_sensor/water_tank/"));
        assert!(cfg.discovery_topic_water_tank().contains("/binary_sensor/"));
        let disc = discovery_payload_water_tank(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert!(
            v.get("entity_category").is_none(),
            "omit entity_category so Water Tank stays with primary entities"
        );
        assert_eq!(v["device_class"].as_str(), Some("plug"));
        assert_eq!(v["payload_on"].as_str(), Some("ON"));
        assert_eq!(v["payload_off"].as_str(), Some("OFF"));
    }

    #[test]
    fn vibration_settings_discovery_payloads_use_config_entity_category() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        for (disc, exp_state, exp_name) in [
            (
                discovery_payload_vibration_intensity_number(&cfg),
                cfg.vibration_intensity_state_topic(),
                "Vibration Intensity",
            ),
            (
                discovery_payload_vibration_duration_number(&cfg),
                cfg.vibration_duration_state_topic(),
                "Vibration Duration",
            ),
            (
                discovery_payload_vibration_pattern_select(&cfg),
                cfg.vibration_pattern_state_topic(),
                "Vibration Pattern",
            ),
            (
                discovery_payload_vibration_cancel_preamble_switch(&cfg),
                cfg.vibration_cancel_preamble_state_topic(),
                "Vibration Cancel Preamble",
            ),
        ] {
            let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
            assert_eq!(v["entity_category"].as_str(), Some("config"));
            assert_eq!(v["state_topic"].as_str(), Some(exp_state.as_str()));
            assert_eq!(v["name"].as_str(), Some(exp_name));
        }
        let cancel: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibration_cancel_preamble_switch(&cfg))
                .unwrap();
        assert_eq!(
            cancel["enabled_by_default"].as_bool(),
            Some(false),
            "HA entity disabled by default; runtime still defaults cancel preamble on"
        );
        let v: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibration_pattern_select(&cfg)).unwrap();
        let opts = v["options"].as_array().expect("options array");
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].as_str(), Some("single"));
        assert_eq!(opts[1].as_str(), Some("double"));
    }

    #[test]
    fn vibration_pattern_parse_roundtrip() {
        assert_eq!(
            parse_vibration_pattern_str(b"single"),
            Some(AlarmPattern::Single)
        );
        assert_eq!(
            parse_vibration_pattern_str(b"DOUBLE"),
            Some(AlarmPattern::Double)
        );
        assert_eq!(parse_vibration_pattern_str(b"triple"), None);
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
    fn presence_tune_from_calibration_flat_reads() {
        let samples = [[500_u16; 6], [500; 6]];
        let b = [500; 6];
        // Match `PRESENCE_CALIB_CAP_PADDING` / `PRESENCE_CALIB_DELTA_PADDING`.
        assert_eq!(
            presence_tune_from_calibration_samples(&samples, &b),
            (548, 16)
        );
    }

    #[test]
    fn presence_tune_from_calibration_peak_and_deviation() {
        let mut spike = [400_u16; 6];
        spike[0] = 500;
        let samples = [spike];
        let b = [400; 6];
        assert_eq!(
            presence_tune_from_calibration_samples(&samples, &b),
            (548, 116)
        );
    }

    #[test]
    fn climate_state_topic_payload_ingest() {
        let cli = crate::cli::Cli::parse_from(["narcolepsy", "--pod", "4"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let mut st = ClimateSideState::default();
        assert!(ingest_climate_mode_from_state_payload(
            &mut st,
            CLIMATE_MODE_HEAT_COOL.as_bytes()
        ));
        assert!(st.enabled);
        assert!(ingest_climate_temperature_from_state_payload(
            &mut st, &cfg, b"41.52"
        ));
        assert_eq!(st.target_centi, 4152);
    }

    #[test]
    fn presence_baseline_zones_payload_json_formats() {
        let arr = b"[100,101,102,103,104,105]";
        assert_eq!(
            parse_presence_baseline_zones_payload(arr),
            Some([100, 101, 102, 103, 104, 105])
        );
        let obj = br#"{"zones":[200,201,202,203,204,205]}"#;
        assert_eq!(
            parse_presence_baseline_zones_payload(obj),
            Some([200, 201, 202, 203, 204, 205])
        );
        assert!(parse_presence_baseline_zones_payload(b"").is_none());
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
                inference_occupancy(&z, &mut inference, 9999, 50),
                (false, false)
            );
        }
        assert_eq!(
            inference_occupancy(&z, &mut inference, 9999, 50),
            (true, true)
        );
    }
}
