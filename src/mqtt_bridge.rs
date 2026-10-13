//! MQTT: Home Assistant discovery, prime, MQTT **Restart Lunaris** (re-`exec` lunaris; **diagnostic**), per-side vibration (Sensor) + retained **number/select/switch**
//! vibration tuning, optional capacitance **presence**,
//! Frozen **water tank** + **Frozen Message** (`0x07`), Sensor **Sensor Message** (`0x07`), **Cover Button Left / Right** (**`0`..=`5`** taps from **`dismissing alarm (N taps)`**), plus **Ambient Temperature** / **Ambient Humidity** from `[ambient]` MCU lines,
//! **Cover Button Vibration Feedback** switch (stock-firmware haptic: one short vibration per cover-button tap, `N` taps → `N` pulses on the tapped side),
//! mattress climate, JSON light (I²C),
//! every left/right pair also has a **combined** entity: climate Cover (both sides at once), Vibrate Cover button (both sides), mean Current / Target Cover Temperature, Heart Rate (pooled beat intervals of both sides),
//! MQTT **update** (self-install from GitHub `releases/latest` asset), diagnostic **Internet Access** (HTTP `2xx` reachability of [`Cli::internet_access_url`](crate::cli::Cli::internet_access_url); default the lunaris GitHub project page).
//!
//! **rumqttc:** subscribe/publish must not block the task that runs [`rumqttc::EventLoop::poll`].
//! Outbound work runs in [`tokio::spawn`] so the event loop keeps draining requests (see upstream docs on [`AsyncClient`]).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, LastWill, MqttOptions, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{interval, sleep, Duration, Instant};

use crate::cli::Cli;
use crate::deviceinfo;
use crate::frozen_frame::{get_temperatures_frame, set_target_temperature_frame, BedSide};
use crate::frozen_rx::FrozenTemperatureUpdate;
use crate::hrv::{mean_bpm, HeartRateTracker};
use crate::is31fl3194::{shutdown_led, Is31fl3194};
use crate::sensor_frame::{
    cover_button_feedback_frames, vibration_sequence_frames, vibration_sequence_frames_both,
    AlarmPattern, VibrationBatch,
};
use crate::sensor_rx::{
    ambient_from_sensor_message_line, cover_button_dismiss_tap_count_from_sensor_message,
    CoverButtonTapSide, SensorCapacitanceZones, SensorPiezoSamples, PIEZO_DEBUG_DUMP_LIMIT,
};
use crate::serial_prime;

const HA_STATUS_TOPIC: &str = "homeassistant/status";
const DISCOVERY_OBJECT_ID_DEVICEINFO_LABEL: &str = "lunaris_deviceinfo_device_label";
const DISCOVERY_OBJECT_ID_DEVICEINFO_ID: &str = "lunaris_deviceinfo_device_id";
const DISCOVERY_OBJECT_ID_SYSTEM_UPTIME: &str = "lunaris_system_uptime";
const DISCOVERY_OBJECT_ID_FROZEN_USART_LINK: &str = "lunaris_frozen_usart_link";
const DISCOVERY_OBJECT_ID_SENSOR_USART_FRAMING: &str = "lunaris_sensor_usart_framing";
const DISCOVERY_OBJECT_ID_INTERNET_ACCESS: &str = "lunaris_internet_access";
/// Interval between reachability checks for MQTT **Internet Access** (target URL configurable via [`Cli::internet_access_url`](crate::cli::Cli::internet_access_url)).
const INTERNET_ACCESS_PROBE_INTERVAL_SECS: u64 = 60;
const DISCOVERY_OBJECT_ID_PRIME: &str = "lunaris_prime";
const DISCOVERY_OBJECT_ID_REQUEST_TEMPERATURES: &str = "lunaris_request_temperatures";
const DISCOVERY_OBJECT_ID_REBOOT: &str = "lunaris_reboot";
const DISCOVERY_OBJECT_ID_SHUTDOWN: &str = "lunaris_shutdown";
const DISCOVERY_OBJECT_ID_RESTART_LUNARIS: &str = "lunaris_restart_lunaris";
const DISCOVERY_OBJECT_ID_LED: &str = "lunaris_led";
const DISCOVERY_OBJECT_ID_LED_BEHAVIOR: &str = "lunaris_led_behavior";
const DISCOVERY_OBJECT_ID_CLIMATE_LEFT: &str = "lunaris_climate_left";
const DISCOVERY_OBJECT_ID_CLIMATE_RIGHT: &str = "lunaris_climate_right";
const DISCOVERY_OBJECT_ID_VIBRATE_LEFT: &str = "lunaris_vibrate_left";
const DISCOVERY_OBJECT_ID_VIBRATE_RIGHT: &str = "lunaris_vibrate_right";
const DISCOVERY_OBJECT_ID_TEMP_LEFT: &str = "lunaris_temp_left";
const DISCOVERY_OBJECT_ID_TEMP_RIGHT: &str = "lunaris_temp_right";
const DISCOVERY_OBJECT_ID_HEATSINK_TEMP: &str = "lunaris_heatsink_temp";
const DISCOVERY_OBJECT_ID_VIBRATION_INTENSITY: &str = "lunaris_vibration_intensity";
const DISCOVERY_OBJECT_ID_VIBRATION_DURATION: &str = "lunaris_vibration_duration";
const DISCOVERY_OBJECT_ID_VIBRATION_PATTERN: &str = "lunaris_vibration_pattern";
const DISCOVERY_OBJECT_ID_VIBRATION_CANCEL_PREAMBLE: &str = "lunaris_vibration_cancel_preamble";
const DISCOVERY_OBJECT_ID_TARGET_TEMP_LEFT: &str = "lunaris_target_temp_left";
const DISCOVERY_OBJECT_ID_TARGET_TEMP_RIGHT: &str = "lunaris_target_temp_right";
const DISCOVERY_OBJECT_ID_PRESENCE_LEFT: &str = "lunaris_presence_left";
const DISCOVERY_OBJECT_ID_PRESENCE_RIGHT: &str = "lunaris_presence_right";
const DISCOVERY_OBJECT_ID_PRESENCE_ANY: &str = "lunaris_presence_any";
const DISCOVERY_OBJECT_ID_CALIBRATE_PRESENCE: &str = "lunaris_calibrate_presence";
const DISCOVERY_OBJECT_ID_PRESENCE_CAP_THRESHOLD: &str = "lunaris_presence_cap_threshold";
const DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_DELTA: &str = "lunaris_presence_baseline_delta";
const DISCOVERY_OBJECT_ID_PRESENCE_BASELINE_ZONES: &str = "lunaris_presence_baseline_zones";
const DISCOVERY_OBJECT_ID_PRESENCE_CALIBRATION: &str = "lunaris_presence_calibration";
const DISCOVERY_OBJECT_ID_WATER_TANK: &str = "lunaris_water_tank";
const DISCOVERY_OBJECT_ID_FIRMWARE_MESSAGE: &str = "lunaris_firmware_message";
const DISCOVERY_OBJECT_ID_SENSOR_MESSAGE: &str = "lunaris_sensor_message";
const DISCOVERY_OBJECT_ID_COVER_BUTTON_LEFT: &str = "lunaris_cover_button_left";
const DISCOVERY_OBJECT_ID_COVER_BUTTON_RIGHT: &str = "lunaris_cover_button_right";
const DISCOVERY_OBJECT_ID_COVER_BUTTON_VIBRATION_FEEDBACK: &str =
    "lunaris_cover_button_vibration_feedback";
const DISCOVERY_OBJECT_ID_COVER_BUTTON_FEEDBACK_INTENSITY: &str =
    "lunaris_cover_button_feedback_intensity";
const DISCOVERY_OBJECT_ID_COVER_BUTTON_FEEDBACK_DURATION: &str =
    "lunaris_cover_button_feedback_duration";
const DISCOVERY_OBJECT_ID_AMBIENT_TEMPERATURE: &str = "lunaris_ambient_temperature";
const DISCOVERY_OBJECT_ID_AMBIENT_HUMIDITY: &str = "lunaris_ambient_humidity";
const DISCOVERY_OBJECT_ID_HEART_RATE_LEFT: &str = "lunaris_heart_rate_left";
const DISCOVERY_OBJECT_ID_HEART_RATE_RIGHT: &str = "lunaris_heart_rate_right";
const DISCOVERY_OBJECT_ID_CLIMATE_COMBINED: &str = "lunaris_climate_combined";
const DISCOVERY_OBJECT_ID_VIBRATE_COMBINED: &str = "lunaris_vibrate_combined";
const DISCOVERY_OBJECT_ID_TEMP_COMBINED: &str = "lunaris_temp_combined";
const DISCOVERY_OBJECT_ID_TARGET_TEMP_COMBINED: &str = "lunaris_target_temp_combined";
const DISCOVERY_OBJECT_ID_HEART_RATE_COMBINED: &str = "lunaris_heart_rate_combined";
const DISCOVERY_OBJECT_ID_UPDATE: &str = "lunaris_update";
/// Home Assistant [HVACMode](https://developers.home-assistant.io/docs/core/entity/climate#hvac-modes) for active regulation.
const CLIMATE_MODE_HEAT_COOL: &str = "heat_cool";
const CLIMATE_MODE_OFF: &str = "off";
/// Resting MQTT state for **Cover Button** sensors (**no recent dismissal**).
const COVER_BUTTON_SENSOR_STATE_REST: &str = "0";
/// Minimum interval between HR/HRV state publishes per side (piezo `0x32` frames arrive every ~40–60 ms).
const HEART_RATE_PUBLISH_INTERVAL: Duration = Duration::from_secs(5);
/// Artifact guard: an IBI window of at least this many **identical** IBIs (span below one
/// sample period) is a periodic artifact (e.g. per-frame structure of a misparsed piezo
/// stream), not a real pulse — the reading is rejected and 0 is published instead.
const HEART_RATE_ARTIFACT_MIN_IBIS: usize = 8;
/// Publish `0.0` on the heart-rate state topic only after the side has had no
/// valid reading for this long (2× `HEART_RATE_PUBLISH_INTERVAL`, i.e. several
/// consecutive zero publish intervals) — a single sporadic zero (e.g. a short
/// lock blip) is held back so it does not land in HA and pollute the statistics.
const HEART_RATE_ZERO_PUBLISH_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum LedBehavior {
    #[default]
    Manual,
    Status,
    Startup,
}

impl LedBehavior {
    fn as_mqtt_payload(self) -> &'static str {
        match self {
            Self::Manual => "Manual",
            Self::Status => "Status",
            Self::Startup => "Startup",
        }
    }
}

fn parse_led_behavior(payload: &[u8]) -> Option<LedBehavior> {
    let text = std::str::from_utf8(payload).ok()?.trim();
    if text.eq_ignore_ascii_case("manual") {
        return Some(LedBehavior::Manual);
    }
    if text.eq_ignore_ascii_case("status") {
        return Some(LedBehavior::Status);
    }
    if text.eq_ignore_ascii_case("startup") {
        return Some(LedBehavior::Startup);
    }
    None
}

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
    /// Piezo `SetPiezoFreq` target Hz (`--pod` default: 1000 on Pod 3, 500 on Pod 4/5).
    pub piezo_freq_hz: u32,
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
    /// Latest capacitance occupancy left (shared with HR piezo consumer for presence gate).
    pub presence_occupied_left: Arc<AtomicBool>,
    /// Latest capacitance occupancy right (shared with HR piezo consumer for presence gate).
    pub presence_occupied_right: Arc<AtomicBool>,
    /// Swap piezo interleaved channels before HR (see [`Cli::sensor_piezo_swap_lr`](crate::cli::Cli::sensor_piezo_swap_lr)).
    pub sensor_piezo_swap_lr: bool,
    /// Re-arm the `--sensor-piezo-debug` payload dumps when presence flips ON (the
    /// lying-time frames reveal the channel↔side mapping).
    pub sensor_piezo_debug: bool,
    pub vibration_settings: Arc<Mutex<VibrationSettings>>,
    /// Stock-firmware haptic: one short vibration per cover-button tap on the tapped
    /// side. MQTT switch **Cover Button Vibration Feedback** (retained state, restored
    /// during the HA bootstrap window); defaults to **on** like the original firmware.
    pub cover_button_feedback: Arc<AtomicBool>,
    /// When set, Frozen frames are queued to [`crate::frozen_link`] instead of opening the port per command.
    pub frozen_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// When set, vibration batches go to [`crate::sensor_link`].
    pub sensor_tx: Option<mpsc::Sender<VibrationBatch>>,
    /// Publish MQTT sensors for Frozen inbound temperatures (`0x41` / `0xC1`); set by [`crate::main`] with [`crate::frozen_link`].
    pub frozen_temperature_discovery: bool,
    /// Self-update poll interval in seconds (`0` disables polling and MQTT update entity).
    pub self_update_poll_secs: u64,
    /// URL used by the MQTT **Internet Access** diagnostic sensor for reachability probes (HTTP **2xx** → `ON`).
    pub internet_access_url: String,
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
            piezo_freq_hz: cli.effective_piezo_freq_hz(),
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
            presence_occupied_left: Arc::new(AtomicBool::new(false)),
            presence_occupied_right: Arc::new(AtomicBool::new(false)),
            sensor_piezo_swap_lr: cli.sensor_piezo_swap_lr,
            sensor_piezo_debug: cli.sensor_piezo_debug,
            vibration_settings: Arc::new(Mutex::new(VibrationSettings::default())),
            cover_button_feedback: Arc::new(AtomicBool::new(true)),
            frozen_tx: None,
            sensor_tx: None,
            frozen_temperature_discovery: false,
            self_update_poll_secs: cli.self_update_poll_secs,
            internet_access_url: cli.internet_access_url.clone(),
        }
    }

    fn self_update_enabled(&self) -> bool {
        self.self_update_poll_secs > 0
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

    pub fn reboot_command_topic(&self) -> String {
        format!("{}/button/reboot/set", self.topic_prefix)
    }

    pub fn discovery_topic_reboot(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_REBOOT
        )
    }

    pub fn shutdown_command_topic(&self) -> String {
        format!("{}/button/shutdown/set", self.topic_prefix)
    }

    pub fn discovery_topic_shutdown(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_SHUTDOWN
        )
    }

    pub fn restart_lunaris_command_topic(&self) -> String {
        format!("{}/button/restart_lunaris/set", self.topic_prefix)
    }

    pub fn discovery_topic_restart_lunaris(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_RESTART_LUNARIS
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

    pub fn discovery_topic_led_behavior(&self) -> String {
        format!(
            "{}/select/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_LED_BEHAVIOR
        )
    }

    pub fn led_behavior_command_topic(&self) -> String {
        format!("{}/select/led_behavior/set", self.topic_prefix)
    }

    pub fn led_behavior_state_topic(&self) -> String {
        format!("{}/select/led_behavior/state", self.topic_prefix)
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

    pub fn climate_combined_discovery_topic(&self) -> String {
        format!(
            "{}/climate/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_CLIMATE_COMBINED
        )
    }

    pub fn climate_combined_mode_command_topic(&self) -> String {
        format!("{}/climate/combined/mode/set", self.topic_prefix)
    }

    pub fn climate_combined_mode_state_topic(&self) -> String {
        format!("{}/climate/combined/mode/state", self.topic_prefix)
    }

    pub fn climate_combined_temperature_command_topic(&self) -> String {
        format!("{}/climate/combined/temperature/set", self.topic_prefix)
    }

    pub fn climate_combined_temperature_state_topic(&self) -> String {
        format!("{}/climate/combined/temperature/state", self.topic_prefix)
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

    pub fn vibrate_combined_discovery_topic(&self) -> String {
        format!(
            "{}/button/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_VIBRATE_COMBINED
        )
    }

    pub fn vibrate_combined_command_topic(&self) -> String {
        format!("{}/button/vibrate_combined/set", self.topic_prefix)
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

    /// Inbound Frozen `0x41` / `0xC1` — mean of both sides' current water temperature.
    pub fn frozen_current_temp_combined_state_topic(&self) -> String {
        format!(
            "{}/sensor/cover_temperature_combined/state",
            self.topic_prefix
        )
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

    pub fn discovery_topic_frozen_current_temp_combined(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_TEMP_COMBINED
        )
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

    /// MQTT bridge combined target (°C) — mean of both sides, mirrors `climate_combined_temperature_state_topic`.
    pub fn target_temperature_combined_state_topic(&self) -> String {
        format!(
            "{}/sensor/target_temperature_combined/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_target_temperature(&self, side: BedSide) -> String {
        let id = match side {
            BedSide::Left => DISCOVERY_OBJECT_ID_TARGET_TEMP_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_TARGET_TEMP_RIGHT,
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, id)
    }

    pub fn discovery_topic_target_temperature_combined(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_TARGET_TEMP_COMBINED
        )
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

    pub fn sensor_message_state_topic(&self) -> String {
        format!("{}/sensor/sensor_message/state", self.topic_prefix)
    }

    pub fn discovery_topic_sensor_message(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_SENSOR_MESSAGE
        )
    }

    pub fn cover_button_left_state_topic(&self) -> String {
        format!("{}/sensor/cover_button_left/state", self.topic_prefix)
    }

    pub fn discovery_topic_cover_button_left(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_COVER_BUTTON_LEFT
        )
    }

    pub fn cover_button_right_state_topic(&self) -> String {
        format!("{}/sensor/cover_button_right/state", self.topic_prefix)
    }

    pub fn discovery_topic_cover_button_right(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_COVER_BUTTON_RIGHT
        )
    }

    pub fn cover_button_feedback_command_topic(&self) -> String {
        format!(
            "{}/switch/cover_button_vibration_feedback/set",
            self.topic_prefix
        )
    }

    pub fn cover_button_feedback_state_topic(&self) -> String {
        format!(
            "{}/switch/cover_button_vibration_feedback/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_cover_button_vibration_feedback(&self) -> String {
        format!(
            "{}/switch/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_COVER_BUTTON_VIBRATION_FEEDBACK
        )
    }

    pub fn discovery_topic_cover_button_feedback_intensity(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_COVER_BUTTON_FEEDBACK_INTENSITY
        )
    }

    pub fn discovery_topic_cover_button_feedback_duration(&self) -> String {
        format!(
            "{}/number/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_COVER_BUTTON_FEEDBACK_DURATION
        )
    }

    pub fn ambient_temperature_state_topic(&self) -> String {
        format!("{}/sensor/ambient_temperature/state", self.topic_prefix)
    }

    pub fn ambient_humidity_state_topic(&self) -> String {
        format!("{}/sensor/ambient_humidity/state", self.topic_prefix)
    }

    pub fn discovery_topic_ambient_temperature(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_AMBIENT_TEMPERATURE
        )
    }

    pub fn discovery_topic_ambient_humidity(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_AMBIENT_HUMIDITY
        )
    }

    pub fn heart_rate_left_state_topic(&self) -> String {
        format!("{}/sensor/heart_rate_left/state", self.topic_prefix)
    }

    pub fn heart_rate_right_state_topic(&self) -> String {
        format!("{}/sensor/heart_rate_right/state", self.topic_prefix)
    }

    /// Mean over the sides with a valid reading (both-sides Heart Rate sensor).
    pub fn heart_rate_combined_state_topic(&self) -> String {
        format!("{}/sensor/heart_rate_combined/state", self.topic_prefix)
    }

    pub fn discovery_topic_heart_rate(&self, side: BedSide) -> String {
        let object_id = match side {
            BedSide::Left => DISCOVERY_OBJECT_ID_HEART_RATE_LEFT,
            BedSide::Right => DISCOVERY_OBJECT_ID_HEART_RATE_RIGHT,
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, object_id)
    }

    pub fn discovery_topic_heart_rate_combined(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_HEART_RATE_COMBINED
        )
    }

    /// HRV (RMSSD) discovery config topic — the HRV entities are not exposed to
    /// Home Assistant for now (values not calibrated yet); an empty retained
    /// payload here removes previously discovered ones.
    pub fn discovery_topic_hrv_rmssd(&self, side: BedSide) -> String {
        let object_id = match side {
            BedSide::Left => "lunaris_hrv_rmssd_left",
            BedSide::Right => "lunaris_hrv_rmssd_right",
        };
        format!("{}/sensor/{}/config", self.discovery_prefix, object_id)
    }

    pub fn update_state_topic(&self) -> String {
        format!("{}/update/lunaris/state", self.topic_prefix)
    }

    pub fn update_command_topic(&self) -> String {
        format!("{}/update/lunaris/set", self.topic_prefix)
    }

    pub fn discovery_topic_update(&self) -> String {
        format!(
            "{}/update/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_UPDATE
        )
    }

    pub fn deviceinfo_device_label_state_topic(&self) -> String {
        format!("{}/sensor/deviceinfo_device_label/state", self.topic_prefix)
    }

    pub fn deviceinfo_device_id_state_topic(&self) -> String {
        format!("{}/sensor/deviceinfo_device_id/state", self.topic_prefix)
    }

    pub fn system_uptime_state_topic(&self) -> String {
        format!("{}/sensor/system_uptime/state", self.topic_prefix)
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

    pub fn discovery_topic_system_uptime(&self) -> String {
        format!(
            "{}/sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_SYSTEM_UPTIME
        )
    }

    pub fn frozen_usart_link_state_topic(&self) -> String {
        format!(
            "{}/binary_sensor/frozen_usart_link/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_frozen_usart_link(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_FROZEN_USART_LINK
        )
    }

    pub fn sensor_usart_framing_state_topic(&self) -> String {
        format!(
            "{}/binary_sensor/sensor_usart_framing/state",
            self.topic_prefix
        )
    }

    pub fn discovery_topic_sensor_usart_framing(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_SENSOR_USART_FRAMING
        )
    }

    pub fn internet_access_state_topic(&self) -> String {
        format!("{}/binary_sensor/internet_access/state", self.topic_prefix)
    }

    pub fn discovery_topic_internet_access(&self) -> String {
        format!(
            "{}/binary_sensor/{}/config",
            self.discovery_prefix, DISCOVERY_OBJECT_ID_INTERNET_ACCESS
        )
    }

    pub fn result_topic(&self) -> String {
        format!("{}/result", self.topic_prefix)
    }

    fn device_json(&self) -> serde_json::Value {
        json!({
            "identifiers": [self.device_identifier.clone()],
            "name": self.device_name,
            "manufacturer": "Lunaris",
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
    batch: VibrationBatch,
) -> Result<(), String> {
    if let Some(tx) = &config.sensor_tx {
        tx.send(batch)
            .await
            .map_err(|e| format!("sensor UART task disconnected: {e:?}"))
    } else if let Some(ref path) = config.sensor_device {
        serial_prime::send_frames(path, config.sensor_baud, batch.frames())
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
            "name": "lunaris",
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
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_reboot_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Reboot Pod",
        "command_topic": config.reboot_command_topic(),
        "payload_press": config.payload_press,
        "entity_category": "diagnostic",
        "icon": "mdi:restart",
        "unique_id": format!("{}_reboot", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_shutdown_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Shutdown Pod",
        "command_topic": config.shutdown_command_topic(),
        "payload_press": config.payload_press,
        "entity_category": "diagnostic",
        "icon": "mdi:power",
        "unique_id": format!("{}_shutdown", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_restart_lunaris_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Restart Lunaris",
        "command_topic": config.restart_lunaris_command_topic(),
        "payload_press": config.payload_press,
        "entity_category": "diagnostic",
        "enabled_by_default": true,
        "icon": "mdi:cached",
        "unique_id": format!("{}_restart_lunaris", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

async fn trigger_pod_reboot() -> Result<(), String> {
    Command::new("systemctl")
        .arg("reboot")
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("failed to run systemctl reboot: {e}"))
}

async fn trigger_pod_shutdown() -> Result<(), String> {
    Command::new("systemctl")
        .arg("poweroff")
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("failed to run systemctl poweroff: {e}"))
}

fn discovery_payload_led_behavior_select(config: &BridgeConfig) -> String {
    json!({
        "name": "LED Behavior",
        "icon": "mdi:lightbulb",
        "command_topic": config.led_behavior_command_topic(),
        "state_topic": config.led_behavior_state_topic(),
        "options": ["Manual", "Status", "Startup"],
        "entity_category": "config",
        "unique_id": format!("{}_led_behavior", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// Combined (both-sides) climate: one entity drives both mattress sides at once. Mode is
/// `heat_cool` when either side is enabled, the target is the mean of both sides, and the
/// current temperature reads the combined cover-temperature sensor.
fn discovery_payload_climate_combined(config: &BridgeConfig) -> String {
    json!({
        "name": "Cover",
        "unique_id": format!("{}_climate_combined", config.device_identifier),
        "temperature_unit": "C",
        "min_temp": config.climate_min_temp,
        "max_temp": config.climate_max_temp,
        "temp_step": config.climate_temp_step,
        "precision": 0.1,
        "modes": [CLIMATE_MODE_OFF, CLIMATE_MODE_HEAT_COOL],
        "mode_command_topic": config.climate_combined_mode_command_topic(),
        "mode_state_topic": config.climate_combined_mode_state_topic(),
        "temperature_command_topic": config.climate_combined_temperature_command_topic(),
        "temperature_state_topic": config.climate_combined_temperature_state_topic(),
        "current_temperature_topic": config.frozen_current_temp_combined_state_topic(),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
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
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// Combined vibrate button: one `SetAlarm` sequence for both sides in a single batch.
fn discovery_payload_vibrate_combined_button(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibrate Cover",
        "command_topic": config.vibrate_combined_command_topic(),
        "payload_press": config.payload_press,
        "unique_id": format!("{}_vibrate_combined", config.device_identifier),
        "device": config.device_json(),
        "icon": "mdi:vibrate",
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_intensity_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Intensity",
        "icon": "mdi:vibrate",
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
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_vibration_duration_number(config: &BridgeConfig) -> String {
    json!({
        "name": "Vibration Duration",
        "icon": "mdi:vibrate",
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
            "name": "lunaris",
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
            "name": "lunaris",
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
        "icon": "mdi:vibrate-off",
        "payload_on": "ON",
        "payload_off": "OFF",
        "entity_category": "config",
        "enabled_by_default": false,
        "unique_id": format!("{}_vibration_cancel_preamble", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
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
        "icon": "mdi:water",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "plug",
        "unique_id": format!("{}_water_tank", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_firmware_message(config: &BridgeConfig) -> String {
    json!({
        "name": "Frozen Message",
        "state_topic": config.firmware_message_state_topic(),
        "icon": "mdi:message-text",
        "entity_category": "diagnostic",
        "enabled_by_default": false,
        "unique_id": format!("{}_firmware_message", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_sensor_message(config: &BridgeConfig) -> String {
    json!({
        "name": "Sensor Message",
        "state_topic": config.sensor_message_state_topic(),
        "icon": "mdi:message-text-outline",
        "entity_category": "diagnostic",
        "enabled_by_default": false,
        "unique_id": format!("{}_sensor_message", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_cover_button_left(config: &BridgeConfig) -> String {
    json!({
        "name": "Cover Button Left",
        "state_topic": config.cover_button_left_state_topic(),
        "icon": "mdi:gesture-tap-button",
        "state_class": "measurement",
        "unit_of_measurement": "taps",
        "unique_id": format!("{}_cover_button_left", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_cover_button_right(config: &BridgeConfig) -> String {
    json!({
        "name": "Cover Button Right",
        "state_topic": config.cover_button_right_state_topic(),
        "icon": "mdi:gesture-tap-button",
        "state_class": "measurement",
        "unit_of_measurement": "taps",
        "unique_id": format!("{}_cover_button_right", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// Switch for the stock-firmware haptic: one short vibration per cover-button tap.
fn discovery_payload_cover_button_vibration_feedback_switch(config: &BridgeConfig) -> String {
    json!({
        "name": "Cover Button Vibration Feedback",
        "command_topic": config.cover_button_feedback_command_topic(),
        "state_topic": config.cover_button_feedback_state_topic(),
        "icon": "mdi:vibrate",
        "payload_on": "ON",
        "payload_off": "OFF",
        "entity_category": "config",
        "enabled_by_default": true,
        "unique_id": format!("{}_cover_button_vibration_feedback", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_ambient_temperature(config: &BridgeConfig) -> String {
    json!({
        "name": "Ambient Temperature",
        "state_topic": config.ambient_temperature_state_topic(),
        "unit_of_measurement": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "unique_id": format!("{}_ambient_temperature", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_ambient_humidity(config: &BridgeConfig) -> String {
    json!({
        "name": "Ambient Humidity",
        "state_topic": config.ambient_humidity_state_topic(),
        "unit_of_measurement": "%",
        "device_class": "humidity",
        "state_class": "measurement",
        "unique_id": format!("{}_ambient_humidity", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

/// Heart-rate state topic selector: one mattress side or the combined (both-sides) sensor.
#[derive(Clone, Copy, Debug)]
enum HrTopic {
    Side(BedSide),
    Combined,
}

impl HrTopic {
    fn state_topic(&self, config: &BridgeConfig) -> String {
        match self {
            HrTopic::Side(side) => heart_rate_state_topic(config, *side),
            HrTopic::Combined => config.heart_rate_combined_state_topic(),
        }
    }
}

fn discovery_payload_heart_rate(config: &BridgeConfig, topic: HrTopic) -> String {
    let (name, state_topic, unique_suffix) = match topic {
        HrTopic::Side(BedSide::Left) => (
            "Heart Rate Left",
            config.heart_rate_left_state_topic(),
            "heart_rate_left",
        ),
        HrTopic::Side(BedSide::Right) => (
            "Heart Rate Right",
            config.heart_rate_right_state_topic(),
            "heart_rate_right",
        ),
        HrTopic::Combined => (
            "Heart Rate",
            config.heart_rate_combined_state_topic(),
            "heart_rate_combined",
        ),
    };
    json!({
        "name": name,
        "state_topic": state_topic,
        "unit_of_measurement": "bpm",
        "state_class": "measurement",
        "icon": "mdi:heart-pulse",
        "unique_id": format!("{}_{}", config.device_identifier, unique_suffix),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn heart_rate_state_topic(config: &BridgeConfig, side: BedSide) -> String {
    match side {
        BedSide::Left => config.heart_rate_left_state_topic(),
        BedSide::Right => config.heart_rate_right_state_topic(),
    }
}

/// Combined (both-sides) heart rate (bpm): one rate computed from the **pooled**
/// inter-beat intervals of all accepted sides (`ibis_ms` windows concatenated) —
/// all beat data together, not the mean of the two per-side readings. `None` when
/// no occupied side has a valid reading (the caller then applies the same
/// zero/unknown publish rules as for a single side).
fn combined_heart_rate_bpm(pooled_ibis_ms: &[f32]) -> Option<f32> {
    mean_bpm(pooled_ibis_ms)
}

async fn publish_heart_rate_reading(
    client: &AsyncClient,
    config: &BridgeConfig,
    topic: HrTopic,
    bpm: f32,
) {
    let hr_topic = topic.state_topic(config);
    let qos = QoS::AtLeastOnce;
    // Not retained: a stale bpm after a lunaris restart would be misleading.
    // When there is no valid reading (presence OFF, lock expired, artifact lock),
    // `publish_heart_rate_no_reading` publishes `0.0` so HA shows 0 instead of unknown.
    // The HRV (RMSSD) value is computed and logged but not published — the HRV
    // entities are not exposed to Home Assistant for now (not calibrated yet).
    let retain = false;
    let bpm = format!("{bpm:.1}");
    if let Err(e) = client.publish(hr_topic, qos, retain, bpm.as_bytes()).await {
        tracing::error!(?e, ?topic, "publish heart rate state");
    }
}

/// Per-side heart-rate publish state. A `0.0` is published only once the side
/// has had no valid reading for `HEART_RATE_ZERO_PUBLISH_DELAY` — a single
/// sporadic zero (e.g. a short lock blip) is held back so it does not land in
/// HA and pollute the statistics.
#[derive(Debug, Default)]
struct HrSideState {
    /// A valid reading was announced (is currently shown in HA).
    announced: bool,
    /// The HA state topic holds a payload (a real reading or `0.0`).
    published: bool,
    /// `0.0` is currently shown in HA.
    zero_published: bool,
    /// When the current no-valid-reading streak started.
    zero_since: Option<Instant>,
}

/// Why the side currently has no valid heart-rate reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZeroReason {
    /// Nobody is in bed (presence OFF) — the state is cleared to an empty
    /// payload (HA shows `unknown`), so "not in bed" stays distinguishable
    /// from "in bed, no reading" for downstream processing.
    PresenceOff,
    /// In bed but the beat lock expired.
    LockExpired,
    /// In bed but the IBI window was rejected as a periodic artifact.
    Artifact,
}

impl ZeroReason {
    fn as_str(self) -> &'static str {
        match self {
            ZeroReason::PresenceOff => "presence OFF",
            ZeroReason::LockExpired => "lock expired",
            ZeroReason::Artifact => "artifact lock",
        }
    }

    /// Whether the no-reading state is published as an empty payload (`unknown`
    /// in HA) instead of `0.0` — only when nobody is in bed.
    fn clears_to_unknown(self) -> bool {
        matches!(self, ZeroReason::PresenceOff)
    }
}

/// What to do about a `0.0` heart-rate publish for the current zero streak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZeroPublish {
    /// Nothing published yet — publish the startup seed immediately.
    Seed,
    /// The streak persisted for `HEART_RATE_ZERO_PUBLISH_DELAY` — publish `0.0`.
    Due,
    /// Hold back (sporadic zero or `0.0` already shown).
    Hold,
}

impl HrSideState {
    /// Pure decision for `note_zero` (unit-tested).
    fn zero_publish_decision(&self) -> ZeroPublish {
        if !self.published {
            return ZeroPublish::Seed;
        }
        if self.zero_published {
            return ZeroPublish::Hold;
        }
        if self
            .zero_since
            .is_some_and(|t| t.elapsed() >= HEART_RATE_ZERO_PUBLISH_DELAY)
        {
            ZeroPublish::Due
        } else {
            ZeroPublish::Hold
        }
    }

    /// Note that the side (or the combined sensor) currently has no valid reading.
    /// Publishes the no-reading state once the streak persisted for
    /// `HEART_RATE_ZERO_PUBLISH_DELAY` (several consecutive publish intervals):
    /// an **empty payload** (HA shows `unknown`) when nobody is in bed, `0.0`
    /// when the side is occupied but has no valid reading (lock expired /
    /// artifact). The startup seed (nothing published yet) publishes
    /// immediately.
    async fn note_zero(
        &mut self,
        client: &AsyncClient,
        config: &BridgeConfig,
        topic: HrTopic,
        reason: ZeroReason,
    ) {
        if self.zero_since.is_none() {
            self.zero_since = Some(Instant::now());
        }
        match self.zero_publish_decision() {
            ZeroPublish::Seed => {
                // Startup seed: nothing in HA yet.
                publish_heart_rate_no_reading(client, config, topic, reason).await;
                self.published = true;
                self.zero_published = true;
            }
            ZeroPublish::Due => {
                publish_heart_rate_no_reading(client, config, topic, reason).await;
                self.zero_published = true;
                if reason.clears_to_unknown() {
                    tracing::info!(
                        ?topic,
                        reason = reason.as_str(),
                        "heart rate state cleared to unknown (empty payload)"
                    );
                } else {
                    tracing::info!(
                        ?topic,
                        reason = reason.as_str(),
                        "heart rate / HRV set to 0"
                    );
                }
            }
            ZeroPublish::Hold => {}
        }
    }

    /// Note that a valid reading exists again — resets the zero streak.
    fn note_reading(&mut self) {
        self.zero_since = None;
        self.zero_published = false;
    }
}

/// Publish the no-valid-reading state on the heart-rate topic (see
/// `HrSideState::note_zero`): an **empty payload** when nobody is in bed
/// (presence OFF) — Home Assistant shows the sensor as `unknown` then, which
/// keeps "not in bed" distinguishable from "in bed, no reading" for downstream
/// processing — and `0.0` when the side is occupied but the beat lock expired
/// or the IBI window was rejected as a periodic artifact. The HRV (RMSSD)
/// topic is not published — the HRV entities are not exposed to Home
/// Assistant for now.
async fn publish_heart_rate_no_reading(
    client: &AsyncClient,
    config: &BridgeConfig,
    topic: HrTopic,
    reason: ZeroReason,
) {
    let hr_topic = topic.state_topic(config);
    let qos = QoS::AtLeastOnce;
    let retain = false;
    let payload: &[u8] = if reason.clears_to_unknown() {
        b""
    } else {
        b"0.0"
    };
    if let Err(e) = client.publish(hr_topic, qos, retain, payload).await {
        tracing::error!(?e, ?topic, "reset heart rate state");
    }
}

/// Effective tick rate for Pod 5 `0x35` piezo frames, derived from consecutive frame
/// ms-timestamps: `ticks / Δt`. The MCU streams ~200 ticks/s (11 ticks per ~55 ms
/// frame) regardless of the 500 Hz SetPiezoFreq setting — the hub firmware also
/// defaults to 200 Hz (5 ms/tick) when its freq field reads 0. EMA-smoothed, only
/// updates inside a plausible band; reconnects (timestamp reset) are rejected via
/// the wrapping delta. `None` keeps `ema` unchanged.
fn update_piezo35_rate(
    prev_ms: Option<u32>,
    ts_ms: u32,
    ticks: usize,
    ema: Option<f32>,
) -> Option<f32> {
    let Some(prev) = prev_ms else {
        return ema;
    };
    let dt = ts_ms.wrapping_sub(prev);
    if dt == 0 {
        return ema;
    }
    let r = ticks as f32 * 1000.0 / dt as f32;
    if !(30.0..=1000.0).contains(&r) {
        return ema;
    }
    Some(match ema {
        None => r,
        Some(e) => e * 0.8 + r * 0.2,
    })
}

/// Sample variance of a piezo channel (for opposite-side lock diagnostics).
fn piezo_channel_variance(samples: &[u16]) -> f32 {
    if samples.len() < 2 {
        return 0.0;
    }
    let n = samples.len() as f32;
    let mean = samples.iter().map(|&s| f32::from(s)).sum::<f32>() / n;
    let var = samples
        .iter()
        .map(|&s| {
            let d = f32::from(s) - mean;
            d * d
        })
        .sum::<f32>()
        / n;
    var
}

fn discovery_payload_self_update(config: &BridgeConfig) -> String {
    json!({
        "platform": "update",
        "name": "Firmware",
        "title": "Firmware",
        "state_topic": config.update_state_topic(),
        "command_topic": config.update_command_topic(),
        "payload_install": crate::self_update::PAYLOAD_INSTALL,
        "device_class": "firmware",
        "entity_category": "config",
        "unique_id": format!("{}_lunaris_update", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_system_uptime(config: &BridgeConfig) -> String {
    json!({
        "name": "System Up Since",
        "state_topic": config.system_uptime_state_topic(),
        "icon": "mdi:clock-start",
        "entity_category": "diagnostic",
        "enabled_by_default": true,
        "device_class": "timestamp",
        "unique_id": format!("{}_system_uptime", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_frozen_usart_link(config: &BridgeConfig) -> String {
    json!({
        "name": "Frozen Link",
        "state_topic": config.frozen_usart_link_state_topic(),
        "icon": "mdi:connection",
        "entity_category": "diagnostic",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "connectivity",
        "unique_id": format!("{}_frozen_usart_link", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_sensor_usart_framing(config: &BridgeConfig) -> String {
    json!({
        "name": "Sensor Link",
        "state_topic": config.sensor_usart_framing_state_topic(),
        "icon": "mdi:connection",
        "entity_category": "diagnostic",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "connectivity",
        "unique_id": format!("{}_sensor_usart_framing", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

fn discovery_payload_internet_access(config: &BridgeConfig) -> String {
    json!({
        "name": "Internet Access",
        "state_topic": config.internet_access_state_topic(),
        "icon": "mdi:web",
        "entity_category": "diagnostic",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "connectivity",
        "unique_id": format!("{}_internet_access", config.device_identifier),
        "device": config.device_json(),
        "origin": {
            "name": "lunaris",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
}

async fn read_system_uptime_secs() -> Option<u64> {
    let content = tokio::fs::read_to_string("/proc/uptime").await.ok()?;
    let raw = content.split_whitespace().next()?;
    let secs = raw.parse::<f64>().ok()?;
    if !secs.is_finite() || secs.is_sign_negative() {
        return None;
    }
    Some(secs.floor() as u64)
}

fn format_local_time_iso8601(secs_since_epoch: i64) -> Option<String> {
    let mut local_tm = libc::tm {
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: -1,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    // SAFETY: `local_tm` and `secs_since_epoch` are valid for `localtime_r`.
    let tm_ptr = unsafe { libc::localtime_r(&secs_since_epoch, &mut local_tm) };
    if tm_ptr.is_null() {
        return None;
    }

    let mut buf = [0u8; 64];
    let fmt = c"%Y-%m-%dT%H:%M:%S%z";
    // SAFETY: `buf` is writable and `fmt` is a valid NUL-terminated format string.
    let written = unsafe {
        libc::strftime(
            buf.as_mut_ptr().cast(),
            buf.len(),
            fmt.as_ptr(),
            &local_tm as *const libc::tm,
        )
    };
    if written == 0 {
        return None;
    }

    let raw = std::str::from_utf8(&buf[..written]).ok()?;
    if raw.len() >= 5 {
        let (head, tail) = raw.split_at(raw.len() - 5);
        Some(format!("{}{}:{}", head, &tail[..3], &tail[3..]))
    } else {
        Some(raw.to_string())
    }
}

async fn read_system_up_since_local_iso8601() -> Option<String> {
    let uptime_secs = read_system_uptime_secs().await?;
    let now_epoch_secs = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let boot_epoch_secs = now_epoch_secs.checked_sub(uptime_secs)?;
    format_local_time_iso8601(boot_epoch_secs.try_into().ok()?)
}

async fn publish_system_uptime_state(client: &AsyncClient, config: &BridgeConfig) {
    let qos = QoS::AtLeastOnce;
    let Some(system_up_since) = read_system_up_since_local_iso8601().await else {
        tracing::warn!("read /proc/uptime failed; skipping System Uptime publish");
        return;
    };
    if let Err(e) = client
        .publish(
            config.system_uptime_state_topic(),
            qos,
            true,
            system_up_since,
        )
        .await
    {
        tracing::error!(?e, "publish system uptime state");
    }
}

async fn publish_frozen_usart_link_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    frozen_mcu_connected: &AtomicBool,
) {
    let payload = if frozen_mcu_connected.load(Ordering::Relaxed) {
        "ON"
    } else {
        "OFF"
    };
    if let Err(e) = client
        .publish(
            config.frozen_usart_link_state_topic(),
            QoS::AtLeastOnce,
            false,
            payload,
        )
        .await
    {
        tracing::error!(?e, "publish Frozen USART link state");
    }
}

async fn publish_sensor_usart_framing_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    sensor_rx_framing: Option<&Arc<AtomicBool>>,
) {
    let payload = match sensor_rx_framing {
        Some(a) if a.load(Ordering::Relaxed) => "ON",
        Some(_) => "OFF",
        None => "OFF",
    };
    if let Err(e) = client
        .publish(
            config.sensor_usart_framing_state_topic(),
            QoS::AtLeastOnce,
            false,
            payload,
        )
        .await
    {
        tracing::error!(?e, "publish Sensor USART framing state");
    }
}

async fn publish_internet_access_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    reachable: bool,
) {
    let payload = if reachable { "ON" } else { "OFF" };
    if let Err(e) = client
        .publish(
            config.internet_access_state_topic(),
            QoS::AtLeastOnce,
            false,
            payload,
        )
        .await
    {
        tracing::error!(?e, "publish Internet Access state");
    }
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
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
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
            "name": "lunaris",
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

/// Home Assistant MQTT `binary_sensor`: `payload == "None"` → state **unknown** (`PAYLOAD_NONE` in HA Core).
const PRESENCE_BINARY_SENSOR_STATE_UNKNOWN: &str = "None";

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
            let dev = s[i].abs_diff(baselines[i]);
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
/// The combined value is decided directly on **all six zones** (max over the whole
/// grid), not by OR-ing the two per-side results.
fn occupancy_uncalibrated_max(
    z: &SensorCapacitanceZones,
    abs_threshold: u16,
) -> (bool, bool, bool) {
    let left_max = z.zones[..3].iter().copied().max().unwrap_or(0);
    let right_max = z.zones[3..].iter().copied().max().unwrap_or(0);
    let any_max = z.zones.iter().copied().max().unwrap_or(0);
    (
        left_max >= abs_threshold,
        right_max >= abs_threshold,
        any_max >= abs_threshold,
    )
}

/// Calibrated: opensleep [`PresenseManager::update_presence`](https://github.com/LiamSnow/opensleep/blob/main/src/sensor/presence.rs).
/// The combined value is decided directly on the debounce grid of **all six zones**,
/// not by OR-ing the two per-side results.
fn occupancy_calibrated(
    z: &SensorCapacitanceZones,
    inference: &mut PresenceInferenceState,
    baseline_delta: u16,
) -> (bool, bool, bool) {
    let Some(ref baselines) = inference.baselines else {
        return (false, false, false);
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
    let any = inference
        .debounce
        .iter()
        .any(|&c| c >= PRESENCE_DEBOUNCE_FRAMES);
    (left, right, any)
}

/// Occupancy for both sides **and** the combined sensor in one inference over the
/// whole six-zone grid.
fn inference_occupancy(
    z: &SensorCapacitanceZones,
    inference: &mut PresenceInferenceState,
    fallback_abs_threshold: u16,
    baseline_delta: u16,
) -> (bool, bool, bool) {
    if inference.baselines.is_some() {
        occupancy_calibrated(z, inference, baseline_delta)
    } else {
        occupancy_uncalibrated_max(z, fallback_abs_threshold)
    }
}

/// Per-sample diagnostic for [`BridgeConfig::presence_debug`].
#[allow(clippy::too_many_arguments)]
fn log_presence_debug(
    z: &SensorCapacitanceZones,
    inference: &PresenceInferenceState,
    fallback_abs_threshold: u16,
    baseline_delta: u16,
    left_on: bool,
    right_on: bool,
    any_on: bool,
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
                occupied_any = any_on,
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
                occupied_any = any_on,
                calibrating,
                "presence debug (calibrated: side ON when any zone on that side exceeds baseline+Δ for debounce_need consecutive frames; combined ON when any zone of the whole grid does)"
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
    if let Err(e) = tx.send(()).await {
        tracing::warn!(?e, "presence calibration notify failed (channel closed)");
        publish_json_result(
            client,
            config,
            "calibrate_presence",
            "error",
            &format!("calibration notify failed: {e:?}"),
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
            "name": "lunaris",
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

/// Publish all three occupancy states. `any_on` is the combined value decided
/// directly on all six zones by [`inference_occupancy`] (not `left_on || right_on`).
async fn publish_presence_readings(
    client: &AsyncClient,
    config: &BridgeConfig,
    left_on: bool,
    right_on: bool,
    any_on: bool,
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

async fn publish_presence_readings_unknown(client: &AsyncClient, config: &BridgeConfig) {
    let qos = QoS::AtLeastOnce;
    let payload = PRESENCE_BINARY_SENSOR_STATE_UNKNOWN;
    for topic in [
        config.presence_state_topic(BedSide::Left),
        config.presence_state_topic(BedSide::Right),
        config.presence_any_state_topic(),
    ] {
        if let Err(e) = client.publish(topic, qos, true, payload.as_bytes()).await {
            tracing::error!(?e, "publish presence occupancy unknown state");
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

async fn publish_sensor_message_state(client: &AsyncClient, config: &BridgeConfig, msg: &str) {
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.sensor_message_state_topic(),
            qos,
            false,
            msg.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish Sensor MCU message sensor state");
    }
}

async fn publish_cover_button_sensors_rest(client: &AsyncClient, config: &BridgeConfig) {
    let qos = QoS::AtLeastOnce;
    // Retained so HA (re)connects with a known idle value (`0` + unit **taps** → **0 taps**), not **unknown**.
    let retain = true;
    if let Err(e) = client
        .publish(
            config.cover_button_left_state_topic(),
            qos,
            retain,
            COVER_BUTTON_SENSOR_STATE_REST.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish Cover Button Left resting state");
    }
    if let Err(e) = client
        .publish(
            config.cover_button_right_state_topic(),
            qos,
            retain,
            COVER_BUTTON_SENSOR_STATE_REST.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish Cover Button Right resting state");
    }
}

/// Publishes tap count (**`1`..=`5`**), then **`0`** so repeated runs still produce state changes in HA.
async fn pulse_cover_button_tap_count(
    client: &AsyncClient,
    config: &BridgeConfig,
    side: CoverButtonTapSide,
    taps: u8,
) {
    let topic = match side {
        CoverButtonTapSide::Left => config.cover_button_left_state_topic(),
        CoverButtonTapSide::Right => config.cover_button_right_state_topic(),
    };
    let qos = QoS::AtLeastOnce;
    let count = taps.to_string();
    if let Err(e) = client
        .publish(topic.as_str(), qos, false, count.as_bytes())
        .await
    {
        tracing::error!(?e, ?side, taps, "publish cover button tap count");
        return;
    }
    tracing::debug!(?side, taps, "cover button sensor: published tap count");
    sleep(Duration::from_millis(400)).await;
    if let Err(e) = client
        .publish(
            topic.as_str(),
            qos,
            true,
            COVER_BUTTON_SENSOR_STATE_REST.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, ?side, "publish cover button rest after tap count");
    }
}

/// Stock-firmware haptic: one short vibration per cover-button tap on the tapped side
/// (`1` tap → `1` pulse, `2` taps → `2` pulses, …). Gated by the MQTT switch **Cover
/// Button Vibration Feedback**; needs the Sensor UART for vibration. One native
/// [`VibrationBatch::Feedback`] command runs the stock waveform on the MCU.
async fn handle_cover_button_tap_feedback(
    config: &BridgeConfig,
    side: CoverButtonTapSide,
    taps: u8,
) {
    if !config.cover_button_feedback.load(Ordering::SeqCst) {
        return;
    }
    if config.sensor_device.is_none() {
        return;
    }
    let side = match side {
        CoverButtonTapSide::Left => BedSide::Left,
        CoverButtonTapSide::Right => BedSide::Right,
    };
    let cancel_preamble = config.vibration_settings.lock().await.cancel_preamble;
    let frames = cover_button_feedback_frames(side, taps, cancel_preamble, config.piezo_freq_hz);
    let frame_count = frames.len();
    match enqueue_sensor_vibration(config, VibrationBatch::Feedback(frames)).await {
        Ok(()) => {
            tracing::info!(
                ?side,
                taps,
                frame_count,
                "cover button tap: stock vibration feedback queued (fixed power/timing; MCU-paced)"
            );
        }
        Err(e) => {
            tracing::error!(%e, ?side, taps, "cover button tap: vibration feedback enqueue failed");
        }
    }
}

async fn publish_ambient_from_sensor_message(
    client: &AsyncClient,
    config: &BridgeConfig,
    msg: &str,
) {
    let Some((temp, rh)) = ambient_from_sensor_message_line(msg) else {
        return;
    };
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.ambient_temperature_state_topic(),
            qos,
            true,
            temp.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish ambient temperature state");
    }
    if let Err(e) = client
        .publish(
            config.ambient_humidity_state_topic(),
            qos,
            true,
            rh.as_bytes(),
        )
        .await
    {
        tracing::error!(?e, "publish ambient humidity state");
    }
}

/// Mean of two centidegree readings, formatted like the per-side payloads (`{:.2}` °C).
fn mean_temperature_payload(a_centi: u16, b_centi: u16) -> String {
    format!(
        "{:.2}",
        (f64::from(a_centi) + f64::from(b_centi)) / 2.0 / 100.0
    )
}

async fn publish_frozen_temperature_readings(
    client: &AsyncClient,
    config: &BridgeConfig,
    u: FrozenTemperatureUpdate,
) {
    let qos = QoS::AtLeastOnce;
    let left = format!("{:.2}", u.left_centi as f64 / 100.0);
    let right = format!("{:.2}", u.right_centi as f64 / 100.0);
    let combined = mean_temperature_payload(u.left_centi, u.right_centi);
    let hs = format!("{:.2}", u.heatsink_centi as f64 / 100.0);
    let publishes = [
        (config.frozen_current_temp_state_topic(BedSide::Left), left),
        (
            config.frozen_current_temp_state_topic(BedSide::Right),
            right,
        ),
        (config.frozen_current_temp_combined_state_topic(), combined),
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
    led_behavior: Arc<Mutex<LedBehavior>>,
    /// Set when an inbound `state_topic` message was sent with the MQTT retain flag (broker-stored preference), not a live publish.
    led_behavior_broker_retain_seen: Arc<AtomicBool>,
    /// While **true**, inbound retained **`state_topic`** payloads repopulate UI-facing settings so broker-stored HA choices survive lunaris restarts (climate, MQTT light snapshot, vibration, presence). Ignore after drain — later `…/state` echoes must not overwrite values set via `…/set` (no `unsubscribe`).
    mqtt_ha_state_bootstrap: Arc<AtomicBool>,
    climate_left: Arc<Mutex<ClimateSideState>>,
    climate_right: Arc<Mutex<ClimateSideState>>,
    /// Notify presence task to start baseline sampling (MQTT **Calibrate presence**).
    presence_calibrate_tx: Option<mpsc::Sender<()>>,
    /// Shared with [`crate::frozen_link`] — `true` when CRC‑valid Frozen firmware traffic was decoded.
    frozen_mcu_connected: Arc<AtomicBool>,
    /// Shared with [`crate::sensor_link`] when the Sensor UART is open — `true` after `0x7E` appears on RX.
    sensor_rx_framing: Option<Arc<AtomicBool>>,
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

async fn apply_cover_button_feedback_mqtt(
    client: &AsyncClient,
    config: &BridgeConfig,
    payload: &[u8],
) {
    let Some(on) = parse_mqtt_on_off(payload) else {
        tracing::trace!("ignored cover button vibration feedback MQTT payload");
        return;
    };
    if config.cover_button_feedback.load(Ordering::SeqCst) == on {
        return;
    }
    config.cover_button_feedback.store(on, Ordering::SeqCst);
    publish_cover_button_feedback_state(client, config, on).await;
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
    let frames = vibration_sequence_frames(
        side,
        intensity,
        pattern,
        duration,
        cancel_preamble,
        config.piezo_freq_hz,
    );
    let frame_count = frames.len();
    match enqueue_sensor_vibration(config, VibrationBatch::Side(frames)).await {
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

/// Combined vibrate button: one batch with `SetAlarm` for **both** sides, so they
/// vibrate at the same time (shared piezo priming in a single sequence).
async fn handle_vibrate_press_combined(client: &AsyncClient, config: &BridgeConfig) {
    if config.sensor_device.is_none() {
        return;
    }
    let (intensity, duration, pattern, cancel_preamble) = {
        let g = config.vibration_settings.lock().await;
        (g.intensity, g.duration_sec, g.pattern, g.cancel_preamble)
    };
    let intensity = intensity.clamp(1, 100);
    let duration = duration.clamp(1, 600);
    let frames = vibration_sequence_frames_both(
        intensity,
        pattern,
        duration,
        cancel_preamble,
        config.piezo_freq_hz,
    );
    let frame_count = frames.len();
    match enqueue_sensor_vibration(config, VibrationBatch::Both(frames)).await {
        Ok(()) => {
            tracing::info!(
                intensity,
                duration,
                pattern = ?pattern,
                frame_count,
                "Sensor vibration sequence sent for both sides (opensleep: optional cancels + EnableVibration + piezo + SetAlarm left/right)"
            );
            publish_json_result(
                client,
                config,
                "vibrate_combined",
                "success",
                "vibration sequence (both sides)",
            )
            .await;
        }
        Err(e) => {
            tracing::error!(%e, "Sensor vibration enqueue failed (both sides)");
            publish_json_result(client, config, "vibrate_combined", "error", &e).await;
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
        // Keep brightness so a later ON without brightness restores the previous level (HA toggle).
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
            // ON with no brightness/color: keep previous levels; if brightness is still 0 (e.g. broker
            // retain or dim-to-zero), use full scale so ON is not immediately cleared below.
            if next.brightness == 0 {
                next.brightness = 255;
            }
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

async fn publish_cover_button_feedback_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    on: bool,
) {
    let payload = if on { "ON" } else { "OFF" };
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.cover_button_feedback_state_topic(),
            qos,
            true,
            payload,
        )
        .await
    {
        tracing::error!(?e, "publish cover button vibration feedback state");
    }
}

async fn publish_vibration_mqtt_states(client: &AsyncClient, config: &BridgeConfig) {
    let vs = *config.vibration_settings.lock().await;
    publish_vibration_intensity_state(client, config, vs.intensity).await;
    publish_vibration_duration_state(client, config, vs.duration_sec).await;
    publish_vibration_pattern_state(client, config, vs.pattern).await;
    publish_vibration_cancel_preamble_state(client, config, vs.cancel_preamble).await;
}

async fn publish_led_behavior_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    behavior: LedBehavior,
) {
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(
            config.led_behavior_state_topic(),
            qos,
            true,
            behavior.as_mqtt_payload(),
        )
        .await
    {
        tracing::error!(?e, "publish LED behavior state");
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

async fn apply_led_snapshot_hw_only(
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
            *light_state.lock().await = snap;
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

/// Combined climate mode: active when **either** side is enabled (like Presence Any).
fn combined_climate_mode(left: &ClimateSideState, right: &ClimateSideState) -> &'static str {
    if left.enabled || right.enabled {
        CLIMATE_MODE_HEAT_COOL
    } else {
        CLIMATE_MODE_OFF
    }
}

/// Combined climate target (°C): mean of both sides' targets.
fn combined_target_temp_c(left: &ClimateSideState, right: &ClimateSideState) -> f64 {
    (f64::from(left.target_centi) + f64::from(right.target_centi)) / 2.0 / 100.0
}

/// Publish the combined climate state (mode + target) and the combined Target
/// Cover Temperature sensor from both sides' states.
async fn publish_climate_combined_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    left: &ClimateSideState,
    right: &ClimateSideState,
) {
    let mode = combined_climate_mode(left, right);
    let temp_str = format!("{:.2}", combined_target_temp_c(left, right));
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(config.climate_combined_mode_state_topic(), qos, true, mode)
        .await
    {
        tracing::error!(?e, "publish combined climate mode state");
    }
    if let Err(e) = client
        .publish(
            config.climate_combined_temperature_state_topic(),
            qos,
            true,
            temp_str.as_str(),
        )
        .await
    {
        tracing::error!(?e, "publish combined climate temperature state");
    }
    if let Err(e) = client
        .publish(
            config.target_temperature_combined_state_topic(),
            qos,
            true,
            temp_str.as_str(),
        )
        .await
    {
        tracing::error!(?e, "publish combined target temperature sensor state");
    }
}

/// Republish the combined climate state after any per-side climate change.
async fn publish_climate_combined_after_change(
    client: &AsyncClient,
    config: &BridgeConfig,
    st: &PublishHandlerState,
) {
    let left = st.climate_left.lock().await.clone();
    let right = st.climate_right.lock().await.clone();
    publish_climate_combined_state(client, config, &left, &right).await;
}

/// Combined climate mode command: apply the mode to **both** sides (each keeps its own target).
async fn handle_climate_combined_mode_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    st: &PublishHandlerState,
    payload: &[u8],
) {
    let Ok(mode_str) = std::str::from_utf8(payload) else {
        tracing::warn!("combined climate mode: invalid UTF-8");
        return;
    };
    let enabled = match mode_str.trim() {
        CLIMATE_MODE_OFF => false,
        CLIMATE_MODE_HEAT_COOL => true,
        other => {
            tracing::warn!(%other, "ignored unknown climate mode");
            return;
        }
    };

    let (backup_left, backup_right, frame_left, frame_right, snap_left, snap_right) = {
        let mut l = st.climate_left.lock().await;
        let mut r = st.climate_right.lock().await;
        let backups = (l.clone(), r.clone());
        l.enabled = enabled;
        r.enabled = enabled;
        let frames = (
            set_target_temperature_frame(BedSide::Left, l.enabled, l.target_centi),
            set_target_temperature_frame(BedSide::Right, r.enabled, r.target_centi),
        );
        (
            backups.0,
            backups.1,
            frames.0,
            frames.1,
            l.clone(),
            r.clone(),
        )
    };

    let res_left = enqueue_frozen_frame(config, frame_left).await;
    let res_right = enqueue_frozen_frame(config, frame_right).await;
    if let Err(e) = res_left.as_ref().and(res_right.as_ref()) {
        tracing::error!(%e, "combined climate mode Frozen UART enqueue failed");
        *st.climate_left.lock().await = backup_left;
        *st.climate_right.lock().await = backup_right;
        publish_json_result(client, config, "climate_combined", "error", e).await;
        return;
    }
    tracing::info!(enabled, "combined climate mode (both sides)");
    publish_climate_state(client, config, BedSide::Left, &snap_left).await;
    publish_climate_state(client, config, BedSide::Right, &snap_right).await;
    publish_climate_combined_state(client, config, &snap_left, &snap_right).await;
    publish_json_result(
        client,
        config,
        "climate_combined",
        "success",
        "set target temperature (both sides)",
    )
    .await;
}

/// Combined climate temperature command: set the target on **both** sides; only enabled
/// sides are sent to the MCU (like the per-side handler), disabled sides keep the new
/// target for when they are turned on.
async fn handle_climate_combined_temperature_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    st: &PublishHandlerState,
    payload: &[u8],
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        return;
    };
    let Ok(temp_c) = text.trim().parse::<f64>() else {
        tracing::warn!("ignored non-numeric climate temperature");
        return;
    };
    let clamped = temp_c.clamp(config.climate_min_temp, config.climate_max_temp);
    let centi = (clamped * 100.0).round() as u16;

    let (backup_left, backup_right, snap_left, snap_right, send_left, send_right) = {
        let mut l = st.climate_left.lock().await;
        let mut r = st.climate_right.lock().await;
        let backups = (l.clone(), r.clone());
        l.target_centi = centi;
        r.target_centi = centi;
        (
            backups.0,
            backups.1,
            l.clone(),
            r.clone(),
            l.enabled,
            r.enabled,
        )
    };

    let mut failed: Option<String> = None;
    if send_left {
        if let Err(e) = enqueue_frozen_frame(
            config,
            set_target_temperature_frame(BedSide::Left, true, centi),
        )
        .await
        {
            failed = Some(e);
        }
    }
    if failed.is_none() && send_right {
        if let Err(e) = enqueue_frozen_frame(
            config,
            set_target_temperature_frame(BedSide::Right, true, centi),
        )
        .await
        {
            failed = Some(e);
        }
    }
    if let Some(e) = failed {
        tracing::error!(%e, "combined climate temperature Frozen UART enqueue failed");
        *st.climate_left.lock().await = backup_left;
        *st.climate_right.lock().await = backup_right;
        publish_json_result(client, config, "climate_combined", "error", &e).await;
        return;
    }
    tracing::info!(
        target_centi = centi,
        "combined climate temperature (both sides)"
    );
    publish_climate_state(client, config, BedSide::Left, &snap_left).await;
    publish_climate_state(client, config, BedSide::Right, &snap_right).await;
    publish_climate_combined_state(client, config, &snap_left, &snap_right).await;
    publish_json_result(
        client,
        config,
        "climate_combined",
        "success",
        "set target temperature (both sides)",
    )
    .await;
}

async fn publish_self_update_state(
    client: &AsyncClient,
    config: &BridgeConfig,
    latest: Option<&str>,
    in_progress: bool,
    update_percentage: Option<f64>,
) {
    let body = crate::self_update::update_state_json(
        crate::self_update::installed_version(),
        latest,
        in_progress,
        update_percentage,
    );
    let qos = QoS::AtLeastOnce;
    if let Err(e) = client
        .publish(config.update_state_topic(), qos, true, body)
        .await
    {
        tracing::error!(?e, "publish self-update state");
    }
}

async fn handle_self_update_install(client: &AsyncClient, config: &BridgeConfig) {
    let latest = match tokio::task::spawn_blocking(
        crate::self_update::fetch_latest_version_blocking,
    )
    .await
    {
        Ok(Ok(v)) => Some(v),
        Ok(Err(e)) => {
            tracing::warn!(%e, "self-update: could not resolve latest version before install");
            None
        }
        Err(e) => {
            tracing::error!(?e, "self-update: spawn_blocking join failed (fetch)");
            None
        }
    };
    publish_self_update_state(client, config, latest.as_deref(), true, None).await;

    match tokio::task::spawn_blocking(crate::self_update::perform_install_blocking).await {
        Ok(Err(e)) => {
            tracing::error!(%e, "self-update failed");
            let latest_after = match tokio::task::spawn_blocking(
                crate::self_update::fetch_latest_version_blocking,
            )
            .await
            {
                Ok(Ok(v)) => Some(v),
                Ok(Err(fetch_err)) => {
                    tracing::warn!(%fetch_err, "self-update: post-error fetch latest failed");
                    None
                }
                Err(join_err) => {
                    tracing::error!(
                        ?join_err,
                        "self-update: spawn_blocking join failed (post-error fetch)"
                    );
                    None
                }
            };
            publish_self_update_state(client, config, latest_after.as_deref(), false, None).await;
            publish_json_result(client, config, "self_update", "error", &e.to_string()).await;
        }
        Ok(Ok(())) => {}
        Err(e) => {
            tracing::error!(?e, "self-update: spawn_blocking join failed (install)");
            publish_self_update_state(client, config, None, false, None).await;
            publish_json_result(client, config, "self_update", "error", &e.to_string()).await;
        }
    }
}

async fn self_update_version_poll_loop(
    client: AsyncClient,
    config: BridgeConfig,
    poll_interval: Duration,
) {
    loop {
        let latest =
            match tokio::task::spawn_blocking(crate::self_update::fetch_latest_version_blocking)
                .await
            {
                Ok(Ok(v)) => Some(v),
                Ok(Err(e)) => {
                    tracing::warn!(%e, "self-update: poll fetch failed");
                    None
                }
                Err(e) => {
                    tracing::error!(?e, "self-update: spawn_blocking join failed (poll)");
                    None
                }
            };
        publish_self_update_state(&client, &config, latest.as_deref(), false, None).await;
        sleep(poll_interval).await;
    }
}

async fn internet_access_probe_loop(client: AsyncClient, config: BridgeConfig) {
    loop {
        sleep(Duration::from_secs(INTERNET_ACCESS_PROBE_INTERVAL_SECS)).await;
        let url = config.internet_access_url.clone();
        let reachable = match tokio::task::spawn_blocking(move || {
            crate::self_update::probe_url_reachable_blocking(&url)
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(?e, "Internet Access: spawn_blocking join failed (probe)");
                false
            }
        };
        publish_internet_access_state(&client, &config, reachable).await;
    }
}

async fn remove_obsolete_feedback_tuning_discovery(client: &AsyncClient, config: &BridgeConfig) {
    for topic in [
        config.discovery_topic_cover_button_feedback_intensity(),
        config.discovery_topic_cover_button_feedback_duration(),
    ] {
        if let Err(e) = client.publish(topic, QoS::AtLeastOnce, true, "").await {
            tracing::error!(?e, "remove obsolete cover button feedback tuning discovery");
        }
    }
}

async fn publish_discovery_and_online(
    client: &AsyncClient,
    config: &BridgeConfig,
    frozen_mcu_connected: &AtomicBool,
    sensor_rx_framing: Option<&Arc<AtomicBool>>,
) {
    let qos = QoS::AtLeastOnce;
    // Remove the former feedback tuning entities, even when the Sensor is disabled.
    // Their retained settings are no longer subscribed to or applied.
    remove_obsolete_feedback_tuning_discovery(client, config).await;
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
    let disc_reboot = discovery_payload_reboot_button(config);
    if let Err(e) = client
        .publish(config.discovery_topic_reboot(), qos, true, disc_reboot)
        .await
    {
        tracing::error!(?e, "publish reboot button discovery");
    }
    let disc_shutdown = discovery_payload_shutdown_button(config);
    if let Err(e) = client
        .publish(config.discovery_topic_shutdown(), qos, true, disc_shutdown)
        .await
    {
        tracing::error!(?e, "publish shutdown button discovery");
    }
    let disc_restart_lunaris = discovery_payload_restart_lunaris_button(config);
    if let Err(e) = client
        .publish(
            config.discovery_topic_restart_lunaris(),
            qos,
            true,
            disc_restart_lunaris,
        )
        .await
    {
        tracing::error!(?e, "publish restart lunaris button discovery");
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
        let disc = discovery_payload_system_uptime(config);
        if let Err(e) = client
            .publish(config.discovery_topic_system_uptime(), qos, true, disc)
            .await
        {
            tracing::error!(?e, "publish system uptime discovery");
        }
        publish_system_uptime_state(client, config).await;
        let disc = discovery_payload_frozen_usart_link(config);
        if let Err(e) = client
            .publish(config.discovery_topic_frozen_usart_link(), qos, true, disc)
            .await
        {
            tracing::error!(?e, "publish Frozen USART link discovery");
        }
        let disc = discovery_payload_sensor_usart_framing(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_sensor_usart_framing(),
                qos,
                true,
                disc,
            )
            .await
        {
            tracing::error!(?e, "publish Sensor USART framing discovery");
        }
        let disc = discovery_payload_internet_access(config);
        if let Err(e) = client
            .publish(config.discovery_topic_internet_access(), qos, true, disc)
            .await
        {
            tracing::error!(?e, "publish Internet Access discovery");
        }
        // Do not await GitHub here: ureq connect/read timeouts still delay MQTT `online` and the rest
        // of discovery. The periodic probe loop keeps the sensor updated after startup.
        let c_net0 = client.clone();
        let cfg_net0 = config.clone();
        tokio::spawn(async move {
            let url = cfg_net0.internet_access_url.clone();
            let initial_internet = match tokio::task::spawn_blocking(move || {
                crate::self_update::probe_url_reachable_blocking(&url)
            })
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        ?e,
                        "Internet Access: spawn_blocking join failed (initial probe)"
                    );
                    false
                }
            };
            publish_internet_access_state(&c_net0, &cfg_net0, initial_internet).await;
        });
        publish_frozen_usart_link_state(client, config, frozen_mcu_connected).await;
        publish_sensor_usart_framing_state(client, config, sensor_rx_framing).await;
    }
    if config.i2c_device.is_some() {
        let disc_led = discovery_payload_light(config);
        if let Err(e) = client
            .publish(config.discovery_topic_light(), qos, true, disc_led)
            .await
        {
            tracing::error!(?e, "publish light discovery");
        }
        let disc_sw = discovery_payload_led_behavior_select(config);
        if let Err(e) = client
            .publish(config.discovery_topic_led_behavior(), qos, true, disc_sw)
            .await
        {
            tracing::error!(?e, "publish LED behavior select discovery");
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
            BedSide::Left => ("Target Cover Temperature Left", "target_temp_left"),
            BedSide::Right => ("Target Cover Temperature Right", "target_temp_right"),
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
    {
        let disc_climate_combined = discovery_payload_climate_combined(config);
        if let Err(e) = client
            .publish(
                config.climate_combined_discovery_topic(),
                qos,
                true,
                disc_climate_combined,
            )
            .await
        {
            tracing::error!(?e, "publish combined climate discovery");
        }
        let disc_target_combined = discovery_payload_frozen_temperature(
            config,
            "Target Cover Temperature",
            "target_temp_combined",
            config.target_temperature_combined_state_topic(),
        );
        if let Err(e) = client
            .publish(
                config.discovery_topic_target_temperature_combined(),
                qos,
                true,
                disc_target_combined,
            )
            .await
        {
            tracing::error!(?e, "publish combined target temperature sensor discovery");
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
        let disc_vc = discovery_payload_vibrate_combined_button(config);
        if let Err(e) = client
            .publish(
                config.vibrate_combined_discovery_topic(),
                qos,
                true,
                disc_vc,
            )
            .await
        {
            tracing::error!(?e, "publish combined vibrate discovery");
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
        let disc_fb = discovery_payload_cover_button_vibration_feedback_switch(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_cover_button_vibration_feedback(),
                qos,
                true,
                disc_fb,
            )
            .await
        {
            tracing::error!(?e, "publish cover button vibration feedback discovery");
        }
        let disc_sm = discovery_payload_sensor_message(config);
        if let Err(e) = client
            .publish(config.discovery_topic_sensor_message(), qos, true, disc_sm)
            .await
        {
            tracing::error!(?e, "publish Sensor MCU message discovery");
        }
        let disc_cbl = discovery_payload_cover_button_left(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_cover_button_left(),
                qos,
                true,
                disc_cbl,
            )
            .await
        {
            tracing::error!(?e, "publish Cover Button Left discovery");
        }
        let disc_cbr = discovery_payload_cover_button_right(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_cover_button_right(),
                qos,
                true,
                disc_cbr,
            )
            .await
        {
            tracing::error!(?e, "publish Cover Button Right discovery");
        }
        publish_cover_button_sensors_rest(client, config).await;
        let disc_at = discovery_payload_ambient_temperature(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_ambient_temperature(),
                qos,
                true,
                disc_at,
            )
            .await
        {
            tracing::error!(?e, "publish ambient temperature discovery");
        }
        let disc_ah = discovery_payload_ambient_humidity(config);
        if let Err(e) = client
            .publish(
                config.discovery_topic_ambient_humidity(),
                qos,
                true,
                disc_ah,
            )
            .await
        {
            tracing::error!(?e, "publish ambient humidity discovery");
        }
        for side in [BedSide::Left, BedSide::Right] {
            let disc_hr = discovery_payload_heart_rate(config, HrTopic::Side(side));
            if let Err(e) = client
                .publish(config.discovery_topic_heart_rate(side), qos, true, disc_hr)
                .await
            {
                tracing::error!(?e, ?side, "publish heart rate discovery");
            }
            // The HRV (RMSSD) entities are not exposed to Home Assistant for now
            // (values not calibrated yet) — an empty retained payload removes any
            // previously discovered ones from HA.
            if let Err(e) = client
                .publish(config.discovery_topic_hrv_rmssd(side), qos, true, "")
                .await
            {
                tracing::error!(?e, ?side, "remove HRV RMSSD discovery");
            }
        }
        let disc_hr_combined = discovery_payload_heart_rate(config, HrTopic::Combined);
        if let Err(e) = client
            .publish(
                config.discovery_topic_heart_rate_combined(),
                qos,
                true,
                disc_hr_combined,
            )
            .await
        {
            tracing::error!(?e, "publish combined heart rate discovery");
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
        publish_presence_readings(client, config, false, false, false).await;
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
            (
                BedSide::Left,
                "Current Cover Temperature Left",
                "cover_temp_left",
            ),
            (
                BedSide::Right,
                "Current Cover Temperature Right",
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
        let disc_combined = discovery_payload_frozen_temperature(
            config,
            "Current Cover Temperature",
            "cover_temp_combined",
            config.frozen_current_temp_combined_state_topic(),
        );
        if let Err(e) = client
            .publish(
                config.discovery_topic_frozen_current_temp_combined(),
                qos,
                true,
                disc_combined,
            )
            .await
        {
            tracing::error!(?e, "publish Frozen combined current temperature discovery");
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
    if config.self_update_enabled() {
        let disc_update = discovery_payload_self_update(config);
        if let Err(e) = client
            .publish(config.discovery_topic_update(), qos, true, disc_update)
            .await
        {
            tracing::error!(?e, "publish self-update discovery");
        }
        // Avoid blocking `online` on GitHub: publish installed version only; `self_update_version_poll_loop`
        // (spawned right after this function) fetches `latest_version` on its first iteration.
        publish_self_update_state(client, config, None, false, None).await;
    }
    if let Err(e) = client
        .publish(config.availability_topic(), qos, true, "online")
        .await
    {
        tracing::error!(?e, "publish availability online");
    }
}

async fn setup_session(client: &AsyncClient, config: &BridgeConfig, st: &PublishHandlerState) {
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
    if let Err(e) = client.subscribe(config.reboot_command_topic(), qos).await {
        tracing::error!(?e, "subscribe reboot command topic");
    }
    if let Err(e) = client.subscribe(config.shutdown_command_topic(), qos).await {
        tracing::error!(?e, "subscribe shutdown command topic");
    }
    if let Err(e) = client
        .subscribe(config.restart_lunaris_command_topic(), qos)
        .await
    {
        tracing::error!(?e, "subscribe restart_lunaris command topic");
    }
    if config.self_update_enabled() {
        if let Err(e) = client.subscribe(config.update_command_topic(), qos).await {
            tracing::error!(?e, "subscribe self-update command topic");
        }
    }
    if config.i2c_device.is_some() {
        if let Err(e) = client.subscribe(config.light_command_topic(), qos).await {
            tracing::error!(?e, "subscribe light command topic");
        }
        if let Err(e) = client
            .subscribe(config.led_behavior_command_topic(), qos)
            .await
        {
            tracing::error!(?e, "subscribe LED behavior command topic");
        }
        if let Err(e) = client
            .subscribe(config.led_behavior_state_topic(), qos)
            .await
        {
            tracing::error!(?e, "subscribe LED behavior state topic (retained sync)");
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
    if let Err(e) = client
        .subscribe(config.climate_combined_mode_command_topic(), qos)
        .await
    {
        tracing::error!(?e, "subscribe combined climate mode topic");
    }
    if let Err(e) = client
        .subscribe(config.climate_combined_temperature_command_topic(), qos)
        .await
    {
        tracing::error!(?e, "subscribe combined climate temperature topic");
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
        if let Err(e) = client
            .subscribe(config.vibrate_combined_command_topic(), qos)
            .await
        {
            tracing::error!(?e, "subscribe combined vibrate command topic");
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
            config.cover_button_feedback_command_topic(),
            config.cover_button_feedback_state_topic(),
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
    publish_discovery_and_online(
        client,
        config,
        st.frozen_mcu_connected.as_ref(),
        st.sensor_rx_framing.as_ref(),
    )
    .await;
    if config.self_update_enabled() {
        let c = client.clone();
        let cfg = config.clone();
        let poll_interval = Duration::from_secs(config.self_update_poll_secs);
        tokio::spawn(async move {
            self_update_version_poll_loop(c, cfg, poll_interval).await;
        });
    }
    let c_net = client.clone();
    let cfg_net = config.clone();
    tokio::spawn(async move {
        internet_access_probe_loop(c_net, cfg_net).await;
    });
}

async fn handle_light_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    light_state: &Arc<Mutex<LightStateSnapshot>>,
    led_behavior: &Arc<Mutex<LedBehavior>>,
    payload: &[u8],
) {
    let Some(cmd) = parse_light_command(payload) else {
        tracing::warn!("ignored non-JSON light command");
        return;
    };
    if config.i2c_device.is_none() {
        return;
    }
    if *led_behavior.lock().await == LedBehavior::Status {
        tracing::info!("ignoring LED light command while LED behavior is Status");
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

async fn handle_led_behavior_command(
    client: &AsyncClient,
    config: &BridgeConfig,
    led_behavior: &Arc<Mutex<LedBehavior>>,
    light_state: &Arc<Mutex<LightStateSnapshot>>,
    payload: &[u8],
) {
    let Some(new_behavior) = parse_led_behavior(payload) else {
        tracing::warn!(
            "LED behavior select: ignored payload (expected Manual, Status, or Startup)"
        );
        return;
    };
    *led_behavior.lock().await = new_behavior;
    publish_led_behavior_state(client, config, new_behavior).await;

    if config.i2c_device.is_some() {
        match new_behavior {
            LedBehavior::Status => {
                let green = LightStateSnapshot {
                    on: true,
                    brightness: 255,
                    base_r: 0,
                    base_g: 255,
                    base_b: 0,
                };
                if let Err(e) = commit_light_snapshot(client, config, light_state, green).await {
                    tracing::error!(%e, "LED behavior status: I²C LED write failed");
                    publish_json_result(client, config, "led_behavior", "error", &e).await;
                } else {
                    tracing::info!("LED behavior set to Status (solid green while lunaris runs)");
                }
            }
            LedBehavior::Manual => {
                tracing::info!("LED behavior set to Manual (LED controlled by light entity)");
            }
            LedBehavior::Startup => {
                tracing::info!(
                    "LED behavior set to Startup (green for 5s on next lunaris start, then off)"
                );
            }
        }
    }
}

/// Inbound sync on `state_topic` — updates internal preference from retained broker state.
async fn handle_led_behavior_state_message(
    led_behavior: &Arc<Mutex<LedBehavior>>,
    led_behavior_broker_retain_seen: &Arc<AtomicBool>,
    payload: &[u8],
    mqtt_retain: bool,
) {
    let Some(parsed_behavior) = parse_led_behavior(payload) else {
        return;
    };
    *led_behavior.lock().await = parsed_behavior;
    if mqtt_retain {
        led_behavior_broker_retain_seen.store(true, Ordering::SeqCst);
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

    if p.topic == config.reboot_command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            publish_json_result(client, config, "reboot", "success", "pod reboot requested").await;
            if let Err(e) = trigger_pod_reboot().await {
                tracing::error!(%e, "pod reboot failed");
                publish_json_result(client, config, "reboot", "error", &e).await;
            } else {
                tracing::warn!("pod reboot command executed");
            }
        }
        return;
    }

    if p.topic == config.shutdown_command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            publish_json_result(
                client,
                config,
                "shutdown",
                "success",
                "pod shutdown requested",
            )
            .await;
            if let Err(e) = trigger_pod_shutdown().await {
                tracing::error!(%e, "pod shutdown failed");
                publish_json_result(client, config, "shutdown", "error", &e).await;
            } else {
                tracing::warn!("pod shutdown command executed");
            }
        }
        return;
    }

    if p.topic == config.restart_lunaris_command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            publish_json_result(
                client,
                config,
                "restart_lunaris",
                "success",
                "lunaris restart requested",
            )
            .await;
            let c = client.clone();
            let cfg = config.clone();
            tokio::spawn(async move {
                match tokio::task::spawn_blocking(crate::self_update::restart_current_exe_blocking)
                    .await
                {
                    Ok(Ok(())) => {
                        tracing::warn!("lunaris restart returned without exec (unexpected)");
                    }
                    Ok(Err(e)) => {
                        tracing::error!(%e, "lunaris restart failed");
                        publish_json_result(&c, &cfg, "restart_lunaris", "error", &e).await;
                    }
                    Err(e) => {
                        tracing::error!(?e, "lunaris restart join failed");
                        publish_json_result(
                            &c,
                            &cfg,
                            "restart_lunaris",
                            "error",
                            &format!("task join failed: {e:?}"),
                        )
                        .await;
                    }
                }
            });
        }
        return;
    }

    if config.self_update_enabled() && p.topic == config.update_command_topic() {
        if p.payload.as_ref() == crate::self_update::PAYLOAD_INSTALL.as_bytes() {
            let c = client.clone();
            let cfg = config.clone();
            tokio::spawn(async move {
                handle_self_update_install(&c, &cfg).await;
            });
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
        if p.topic == config.cover_button_feedback_command_topic() {
            apply_cover_button_feedback_mqtt(client, config, &p.payload).await;
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
        if p.topic == config.cover_button_feedback_state_topic() {
            if st.mqtt_ha_state_bootstrap.load(Ordering::SeqCst) {
                apply_cover_button_feedback_mqtt(client, config, &p.payload).await;
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
        if p.topic == config.vibrate_combined_command_topic() && p.payload.as_ref() == expected {
            handle_vibrate_press_combined(client, config).await;
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
        publish_climate_combined_after_change(client, config, st).await;
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
        publish_climate_combined_after_change(client, config, st).await;
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
        publish_climate_combined_after_change(client, config, st).await;
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
        publish_climate_combined_after_change(client, config, st).await;
        return;
    }

    if p.topic == config.climate_combined_mode_command_topic() {
        handle_climate_combined_mode_command(client, config, st, &p.payload).await;
        return;
    }
    if p.topic == config.climate_combined_temperature_command_topic() {
        handle_climate_combined_temperature_command(client, config, st, &p.payload).await;
        return;
    }

    for side in [BedSide::Left, BedSide::Right] {
        if p.topic == config.climate_mode_state_topic(side)
            || p.topic == config.climate_temperature_state_topic(side)
        {
            return;
        }
    }

    if config.i2c_device.is_some() && p.topic == config.led_behavior_command_topic() {
        handle_led_behavior_command(
            client,
            config,
            &st.led_behavior,
            &st.light_state,
            &p.payload,
        )
        .await;
        return;
    }
    if config.i2c_device.is_some() && p.topic == config.led_behavior_state_topic() {
        handle_led_behavior_state_message(
            &st.led_behavior,
            &st.led_behavior_broker_retain_seen,
            &p.payload,
            p.retain,
        )
        .await;
        return;
    }

    if config.i2c_device.is_some() && p.topic == config.light_command_topic() {
        handle_light_command(
            client,
            config,
            &st.light_state,
            &st.led_behavior,
            &p.payload,
        )
        .await;
        return;
    }
    // Like vibration `…/number/…/state`: ignore echoes on `light/led/state`; broker retain is drained only during bootstrap.
    if config.i2c_device.is_some() && p.topic == config.light_state_topic() {
        return;
    }

    if p.topic == HA_STATUS_TOPIC && p.payload.as_ref() == b"online" {
        tracing::debug!("Home Assistant online; republishing discovery");
        publish_discovery_and_online(
            client,
            config,
            st.frozen_mcu_connected.as_ref(),
            st.sensor_rx_framing.as_ref(),
        )
        .await;
    }
}

/// Run the MQTT event loop until a fatal error (or process kill).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: BridgeConfig,
    prime_frame: Arc<[u8]>,
    frozen_temperature_rx: Option<mpsc::Receiver<FrozenTemperatureUpdate>>,
    frozen_water_tank_rx: mpsc::Receiver<bool>,
    frozen_firmware_message_rx: mpsc::Receiver<String>,
    capacitance_rx: Option<mpsc::Receiver<SensorCapacitanceZones>>,
    sensor_message_rx: Option<mpsc::Receiver<String>>,
    piezo_rx: Option<mpsc::Receiver<SensorPiezoSamples>>,
    frozen_mcu_connected: Arc<AtomicBool>,
    sensor_rx_framing: Option<Arc<AtomicBool>>,
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
        led_behavior: Arc::new(Mutex::new(LedBehavior::default())),
        led_behavior_broker_retain_seen: Arc::new(AtomicBool::new(false)),
        mqtt_ha_state_bootstrap: Arc::new(AtomicBool::new(false)),
        climate_left: Arc::new(Mutex::new(ClimateSideState::default())),
        climate_right: Arc::new(Mutex::new(ClimateSideState::default())),
        presence_calibrate_tx: presence_calibrate_tx.clone(),
        frozen_mcu_connected,
        sensor_rx_framing,
    };

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

    if let Some(mut sensor_msg_rx) = sensor_message_rx {
        let c = client.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            while let Some(msg) = sensor_msg_rx.recv().await {
                if let Some((side, taps)) = cover_button_dismiss_tap_count_from_sensor_message(&msg)
                {
                    // Time-critical first: the feedback pulse must go out before the
                    // cover-button sensor pulse (which sleeps 400 ms between publishes)
                    // so the haptic feels immediate, like the stock firmware.
                    handle_cover_button_tap_feedback(&cfg, side, taps).await;
                    pulse_cover_button_tap_count(&c, &cfg, side, taps).await;
                }
                publish_sensor_message_state(&c, &cfg, &msg).await;
                publish_ambient_from_sensor_message(&c, &cfg, &msg).await;
            }
        });
    }

    if let Some(mut piezo_rx) = piezo_rx {
        let c = client.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            // One tracker per mattress side (left/right piezo channel of `0x32` / `0x35`).
            let mut trackers = [
                (BedSide::Left, HeartRateTracker::new()),
                (BedSide::Right, HeartRateTracker::new()),
            ];
            let mut last_publish = [None::<Instant>; 2];
            // Per-side publish state (announced / published / zero streak).
            let mut state = [HrSideState::default(), HrSideState::default()];
            // Combined (both-sides) Heart Rate sensor: same publish rules as per side.
            let mut hr_combined = HrSideState::default();
            let mut last_publish_combined: Option<Instant> = None;
            let mut artifact_warned = [false; 2];
            let mut saw_any_samples = false;
            let mut last_nolock_warn = Instant::now();
            let session_start = Instant::now();
            let mut no_sample_warned = false;
            let mut swap_hint_logged = false;
            let presence_gate = cfg.presence_discovery;
            // Pod 5 `0x35`: effective tick rate derived from consecutive frame
            // ms-timestamps (see `update_piezo35_rate`); `prev_piezo_ts_ms` seeds it.
            let mut prev_piezo_ts_ms: Option<u32> = None;
            let mut tick_rate_hz: Option<f32> = None;
            let mut prev_occ = [false; 2];
            // --sensor-piezo-debug: rolling 10 s per-channel signal window (raw c0..c3).
            // The per-frame variance cannot show a slow ~1 Hz BCG wave; the 10 s min/max
            // span can — it reveals the wave amplitude and the per-side DC shifts.
            let mut dbg_win_start = Instant::now();
            let mut dbg_min = [u16::MAX; 4];
            let mut dbg_max = [0u16; 4];
            let mut dbg_sum = [0f64; 4];
            let mut dbg_n = [0u64; 4];
            let mut rate_logged = false;
            let swap_lr = cfg.sensor_piezo_swap_lr;
            if swap_lr {
                tracing::info!(
                    "heart rate / HRV: --sensor-piezo-swap-lr — feeding first interleaved sample to Right"
                );
            }
            loop {
                tokio::select! {
                    samples = piezo_rx.recv() => {
                        let Some(samples) = samples else {
                            tracing::warn!("Sensor piezo channel closed; HR/HRV tracking stopped");
                            break;
                        };
                        saw_any_samples = true;
                        let (left_ch, right_ch) = if swap_lr {
                            (&samples.right_samples, &samples.left_samples)
                        } else {
                            (&samples.left_samples, &samples.right_samples)
                        };
                        let occ = [
                            cfg.presence_occupied_left.load(Ordering::Relaxed),
                            cfg.presence_occupied_right.load(Ordering::Relaxed),
                        ];
                        // The lying-time frames are the ones that reveal the channel↔side
                        // mapping — re-arm the raw payload dumps when presence flips ON.
                        if cfg.sensor_piezo_debug
                            && ((occ[0] && !prev_occ[0]) || (occ[1] && !prev_occ[1]))
                        {
                            PIEZO_DEBUG_DUMP_LIMIT.store(3, Ordering::Relaxed);
                        }
                        prev_occ = occ;
                        if cfg.sensor_piezo_debug {
                            for (c, st) in samples.channel_stats.iter().enumerate() {
                                if st.min > st.max {
                                    continue; // empty frame for this channel
                                }
                                dbg_min[c] = dbg_min[c].min(st.min);
                                dbg_max[c] = dbg_max[c].max(st.max);
                                // Frames carry a constant tick count, so mean-of-means is fine.
                                dbg_sum[c] += f64::from(st.mean);
                                dbg_n[c] += 1;
                            }
                            if dbg_win_start.elapsed() >= Duration::from_secs(10) {
                                dbg_win_start = Instant::now();
                                let mut any = false;
                                let mut fields: Vec<String> = Vec::new();
                                for c in 0..4 {
                                    if dbg_n[c] == 0 {
                                        continue;
                                    }
                                    any = true;
                                    let mean = dbg_sum[c] / dbg_n[c] as f64;
                                    let span = dbg_max[c].saturating_sub(dbg_min[c]);
                                    fields.push(format!(
                                        "c{} mean={:.1} min={} max={} span={}",
                                        c, mean, dbg_min[c], dbg_max[c], span
                                    ));
                                    dbg_min[c] = u16::MAX;
                                    dbg_max[c] = 0;
                                    dbg_sum[c] = 0.0;
                                    dbg_n[c] = 0;
                                }
                                if any {
                                    let joined = fields.join(" | ");
                                    tracing::warn!(
                                        "Sensor: piezo 10s channel window (--sensor-piezo-debug): {joined}"
                                    );
                                }
                            }
                        }
                        let lv = piezo_channel_variance(left_ch);
                        let rv = piezo_channel_variance(right_ch);
                        // Pod 4: BCG often lands on the opposite wire label vs capacitance Presence.
                        // When only one side is occupied and the other channel is much livelier,
                        // feed that lively channel to the occupied side (so Right HR can lock).
                        const VAR_RATIO: f32 = 3.0;
                        // Corrected parse scale: per-frame idle wobble variance ≈ 0.1–0.2,
                        // the slow BCG wave over one 55 ms frame only reaches single digits
                        // (the old 50.0 floor was tuned against the misaligned parse's
                        // garbage variance and blocked every routing).
                        const VAR_MIN: f32 = 1.0;
                        let left_only = presence_gate && occ[0] && !occ[1];
                        let right_only = presence_gate && occ[1] && !occ[0];
                        let feed_left_from_right = left_only && rv > lv * VAR_RATIO && rv > VAR_MIN;
                        let feed_right_from_left = right_only && lv > rv * VAR_RATIO && lv > VAR_MIN;
                        if !swap_hint_logged && (feed_left_from_right || feed_right_from_left) {
                            swap_hint_logged = true;
                            tracing::info!(
                                presence_left = occ[0],
                                presence_right = occ[1],
                                left_var = lv,
                                right_var = rv,
                                "heart rate / HRV: routing livelier piezo channel to the occupied side (wire L/R vs Presence mismatch)"
                            );
                        }
                        // Pod 5 `0x35` frames carry no usable rate field — derive the
                        // effective tick rate (hub firmware default: 200 Hz) from the ms
                        // timestamp deltas; the 500 Hz SetPiezoFreq target would inflate
                        // bpm by 2.5×. `0x32` keeps the frame rate / SetPiezoFreq target.
                        let freq_hz = if samples.tag == 0x35 {
                            tick_rate_hz = update_piezo35_rate(
                                prev_piezo_ts_ms,
                                samples.sequence,
                                samples.left_samples.len(),
                                tick_rate_hz,
                            );
                            prev_piezo_ts_ms = Some(samples.sequence);
                            if let (Some(r), false) = (tick_rate_hz, rate_logged) {
                                rate_logged = true;
                                tracing::info!(
                                    rate_hz = r.round() as u32,
                                    "heart rate / HRV: effective piezo tick rate derived from frame timestamps"
                                );
                            }
                            tick_rate_hz.map_or(0, |r| r.round() as u32) // 0 = not derived yet
                        } else if samples.freq > 0 {
                            samples.freq
                        } else {
                            cfg.piezo_freq_hz
                        };
                        // Per-side readings accepted in this frame (their IBI windows
                        // are pooled for the combined both-sides Heart Rate sensor).
                        let mut accepted = [false; 2];
                        for (i, (side, tracker)) in trackers.iter_mut().enumerate() {
                            let occupied = !presence_gate || occ[i];
                            if !occupied {
                                tracker.clear_lock();
                                state[i].announced = false;
                                state[i]
                                    .note_zero(&c, &cfg, HrTopic::Side(*side), ZeroReason::PresenceOff)
                                    .await;
                                continue;
                            }
                            let channel = match side {
                                BedSide::Left if feed_left_from_right => right_ch,
                                BedSide::Right if feed_right_from_left => left_ch,
                                BedSide::Left => left_ch,
                                BedSide::Right => right_ch,
                            };
                            if freq_hz == 0 {
                                // 0x35 before the first rate derivation — skip feeding.
                                continue;
                            }
                            tracker.set_freq(freq_hz);
                            tracker.push_samples(channel);
                            let Some(reading) = tracker.reading() else {
                                state[i].announced = false;
                                state[i]
                                    .note_zero(&c, &cfg, HrTopic::Side(*side), ZeroReason::LockExpired)
                                    .await;
                                continue;
                            };
                            // Artifact guard: IBIs are quantized to whole samples, so a
                            // window of many identical IBIs (span below one sample period)
                            // is a periodic artifact — e.g. per-frame structure of a
                            // misparsed piezo stream — not a real pulse. Publish 0.
                            // The post-bandpass moving average widens peaks, so a
                            // perfectly periodic (artifact) signal still shows an IBI
                            // jitter floor of a few ms — allow two sample periods.
                            let sample_period_ms = 1000.0 / tracker.freq_hz().max(1) as f32;
                            let metronome = tracker.ibi_count() >= HEART_RATE_ARTIFACT_MIN_IBIS
                                && tracker
                                    .ibi_span_ms()
                                    .is_none_or(|span| span < 2.0 * sample_period_ms);
                            if metronome {
                                let ibis = tracker.ibi_count();
                                tracker.clear_lock();
                                state[i].announced = false;
                                state[i]
                                    .note_zero(&c, &cfg, HrTopic::Side(*side), ZeroReason::Artifact)
                                    .await;
                                if !artifact_warned[i] {
                                    artifact_warned[i] = true;
                                    tracing::warn!(
                                        ?side,
                                        ibis,
                                        bpm = reading.bpm,
                                        "heart rate / HRV: constant IBI window (RMSSD ≈ 0 over many beats) — periodic artifact, not a real pulse; publishing 0 (check piezo frame layout, --sensor-piezo-debug)"
                                    );
                                }
                                continue;
                            }
                            artifact_warned[i] = false;
                            state[i].note_reading();
                            accepted[i] = true;
                            let due = last_publish[i]
                                .is_none_or(|t| t.elapsed() >= HEART_RATE_PUBLISH_INTERVAL);
                            if !due {
                                continue;
                            }
                            last_publish[i] = Some(Instant::now());
                            if !state[i].announced {
                                state[i].announced = true;
                                tracing::info!(
                                    ?side,
                                    bpm = reading.bpm,
                                    rmssd_ms = reading.rmssd_ms,
                                    freq_hz = tracker.freq_hz(),
                                    samples = tracker.samples_seen(),
                                    "heart rate / HRV tracking started (Sensor piezo stream)"
                                );
                            } else {
                                tracing::debug!(
                                    ?side,
                                    bpm = reading.bpm,
                                    rmssd_ms = reading.rmssd_ms,
                                    ibis = tracker.ibi_count(),
                                    "heart rate / HRV reading"
                                );
                            }
                            publish_heart_rate_reading(&c, &cfg, HrTopic::Side(*side), reading.bpm)
                                .await;
                            state[i].published = true;
                            if cfg.sensor_piezo_debug {
                                // Beat-pattern diagnostics: the IBI list shows whether
                                // each heartbeat is counted once (e.g. [950, 948, ...])
                                // or twice (dicrotic second peak → [600, 1200, ...]);
                                // the bandpassed window (decimated 4× ≈ 50 Hz) shows
                                // the wave shape behind the detected peaks.
                                tracing::warn!(
                                    ?side,
                                    bpm = reading.bpm,
                                    rmssd_ms = reading.rmssd_ms,
                                    ibis_ms = ?tracker.ibis_ms(),
                                    wave = ?tracker.detrended_window(4),
                                    "heart rate / HRV reading (--sensor-piezo-debug)"
                                );
                            }
                        }
                        // Combined (both-sides) Heart Rate sensor: one rate over the
                        // pooled inter-beat intervals of all accepted sides — all beat
                        // data together, not the mean of the two per-side readings.
                        // When nobody is in bed the state is cleared to an empty
                        // payload (HA `unknown`), and `0.0` is published only for a
                        // persistent no-reading streak — same rules as per side.
                        let mut pooled_ibis_ms: Vec<f32> = Vec::new();
                        for (i, &side_accepted) in accepted.iter().enumerate() {
                            if side_accepted {
                                pooled_ibis_ms.extend_from_slice(&trackers[i].1.ibis_ms());
                            }
                        }
                        match combined_heart_rate_bpm(&pooled_ibis_ms) {
                            Some(bpm) => {
                                hr_combined.note_reading();
                                let due = last_publish_combined
                                    .is_none_or(|t| t.elapsed() >= HEART_RATE_PUBLISH_INTERVAL);
                                if due {
                                    last_publish_combined = Some(Instant::now());
                                    if !hr_combined.announced {
                                        hr_combined.announced = true;
                                        tracing::info!(
                                            bpm,
                                            "heart rate / HRV combined tracking started (Sensor piezo stream)"
                                        );
                                    }
                                    publish_heart_rate_reading(&c, &cfg, HrTopic::Combined, bpm)
                                        .await;
                                    hr_combined.published = true;
                                }
                            }
                            None => {
                                let any_occupied = !presence_gate || occ[0] || occ[1];
                                let reason = if any_occupied {
                                    ZeroReason::LockExpired
                                } else {
                                    ZeroReason::PresenceOff
                                };
                                hr_combined
                                    .note_zero(&c, &cfg, HrTopic::Combined, reason)
                                    .await;
                            }
                        }
                        if (!presence_gate || occ[0] || occ[1])
                            && !state[0].announced
                            && !state[1].announced
                            && last_nolock_warn.elapsed() >= Duration::from_secs(60)
                        {
                            last_nolock_warn = Instant::now();
                            tracing::warn!(
                                left_ibis = trackers[0].1.ibi_count(),
                                right_ibis = trackers[1].1.ibi_count(),
                                left_samples = trackers[0].1.samples_seen(),
                                right_samples = trackers[1].1.samples_seen(),
                                presence_left = occ[0],
                                presence_right = occ[1],
                                "heart rate / HRV: piezo samples flowing but no lock yet (need ≥4 IBIs + presence ON when capacitance is available)"
                            );
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        if !saw_any_samples && !no_sample_warned && session_start.elapsed() >= Duration::from_secs(30)
                        {
                            no_sample_warned = true;
                            tracing::warn!(
                                "heart rate / HRV: no Sensor piezo samples (0x32/0x35) after 30s — check piezo setup / Sensor USART"
                            );
                        }
                    }
                }
            }
        });
    }

    let c = client.clone();
    let cfg = config.clone();
    tokio::spawn(async move {
        let mut uptime_interval = interval(Duration::from_secs(60));
        uptime_interval.tick().await;
        loop {
            publish_system_uptime_state(&c, &cfg).await;
            uptime_interval.tick().await;
        }
    });

    let c = client.clone();
    let cfg = config.clone();
    let hs_usart = handler_state.clone();
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(5));
        tick.tick().await;
        loop {
            publish_frozen_usart_link_state(&c, &cfg, hs_usart.frozen_mcu_connected.as_ref()).await;
            publish_sensor_usart_framing_state(&c, &cfg, hs_usart.sensor_rx_framing.as_ref()).await;
            tick.tick().await;
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
                let mut prev_publish = Some((false, false, false));
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
                            cfg.presence_occupied_left.store(false, Ordering::Relaxed);
                            cfg.presence_occupied_right.store(false, Ordering::Relaxed);
                            publish_presence_calibration_running_state(&c, &cfg, true).await;
                            publish_presence_readings_unknown(&c, &cfg).await;
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
                                            publish_presence_bootstrap_finalize(&c, &cfg).await;
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
                                            prev_publish = None;
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
                            let (left_on, right_on, any_on) =
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
                                    any_on,
                                    calibrating,
                                );
                            }
                            if calibrating {
                                continue;
                            }
                            cfg.presence_occupied_left.store(left_on, Ordering::Relaxed);
                            cfg.presence_occupied_right.store(right_on, Ordering::Relaxed);
                            if prev_publish == Some((left_on, right_on, any_on)) {
                                continue;
                            }
                            tracing::trace!(
                                left_on,
                                right_on,
                                any_on,
                                calibrated = inference.baselines.is_some(),
                                zones = ?z.zones,
                                "Sensor presence (capacitance)"
                            );
                            prev_publish = Some((left_on, right_on, any_on));
                            publish_presence_readings(&c, &cfg, left_on, right_on, any_on).await;
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
                                    "presence debug: no capacitance `0x33` occupancy sample in 60s — MCU likely not sending usable `0x33`, or lunaris rejected layout (Sensor logs first bad `0x33` WARNING once)"
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
        heart_rate = config.sensor_device.is_some(),
        "MQTT client created; polling event loop"
    );

    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install SIGINT handler");
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");

    let mut mqtt_connected_once = false;
    let mut mqtt_start_connect_failures: u8 = 0;
    let mut keep_led_state_on_exit = false;

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
                    mqtt_connected_once = true;
                    mqtt_start_connect_failures = 0;
                    tracing::info!("MQTT connected");
                    // Retained `…/state` replays can arrive on the next `poll()` before the spawned
                    // session task runs — set bootstrap flags here so `handle_publish` ingests them
                    // instead of dropping them at the `light/.../state` echo guard (`bootstrap == false`).
                    handler_state
                        .mqtt_ha_state_bootstrap
                        .store(true, Ordering::SeqCst);
                    handler_state
                        .led_behavior_broker_retain_seen
                        .store(false, Ordering::SeqCst);
                    // rumqttc: `AsyncClient` requests are only processed while `eventloop.poll()` runs.
                    // Awaiting `publish`/`subscribe` on the same task that calls `poll()` deadlocks the
                    // event loop — spawn so the broker actually receives discovery and availability.
                    let c = client.clone();
                    let cfg = config.clone();
                    let hs = handler_state.clone();
                    tokio::spawn(async move {
                        setup_session(&c, &cfg, &hs).await;

                        // Broker retain replay (climate/light/vibration/presence **`state_topic`** + led_behavior)
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
                        // The combined climate mirrors the restored per-side states.
                        publish_climate_combined_after_change(&c, &cfg, &hs).await;

                        if cfg.sensor_device.is_some() {
                            publish_vibration_mqtt_states(&c, &cfg).await;
                            let feedback_on = cfg.cover_button_feedback.load(Ordering::SeqCst);
                            publish_cover_button_feedback_state(&c, &cfg, feedback_on).await;
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
                            let led_behavior = *hs.led_behavior.lock().await;
                            match led_behavior {
                                LedBehavior::Manual => {
                                    let off = LightStateSnapshot {
                                        on: false,
                                        brightness: 255,
                                        base_r: 0,
                                        base_g: 255,
                                        base_b: 0,
                                    };
                                    let _ =
                                        commit_light_snapshot(&c, &cfg, &hs.light_state, off).await;
                                }
                                LedBehavior::Status => {
                                    let green = LightStateSnapshot {
                                        on: true,
                                        brightness: 255,
                                        base_r: 0,
                                        base_g: 255,
                                        base_b: 0,
                                    };
                                    if let Err(e) =
                                        commit_light_snapshot(&c, &cfg, &hs.light_state, green)
                                            .await
                                    {
                                        tracing::error!(%e, "LED behavior status: I²C LED write failed");
                                        publish_json_result(&c, &cfg, "led_behavior", "error", &e)
                                            .await;
                                    }
                                }
                                LedBehavior::Startup => {
                                    let green = LightStateSnapshot {
                                        on: true,
                                        brightness: 255,
                                        base_r: 0,
                                        base_g: 255,
                                        base_b: 0,
                                    };
                                    if let Err(e) =
                                        commit_light_snapshot(&c, &cfg, &hs.light_state, green)
                                            .await
                                    {
                                        tracing::error!(%e, "LED behavior startup: I²C LED write failed");
                                        publish_json_result(&c, &cfg, "led_behavior", "error", &e)
                                            .await;
                                    } else {
                                        sleep(Duration::from_secs(5)).await;
                                        let off = LightStateSnapshot {
                                            on: false,
                                            brightness: 255,
                                            base_r: 0,
                                            base_g: 255,
                                            base_b: 0,
                                        };
                                        let _ =
                                            commit_light_snapshot(&c, &cfg, &hs.light_state, off)
                                                .await;
                                    }
                                }
                            }
                            let snap = hs.light_state.lock().await.clone();
                            publish_light_state(&c, &cfg, &snap).await;
                            publish_led_behavior_state(&c, &cfg, led_behavior).await;
                        }
                    });
                } else {
                    tracing::error!(code = ?ack.code, "MQTT connection refused");
                    if !mqtt_connected_once {
                        mqtt_start_connect_failures = mqtt_start_connect_failures.saturating_add(1);
                        tracing::warn!(
                            attempt = mqtt_start_connect_failures,
                            max_attempts = 5,
                            "MQTT connect attempt failed during startup"
                        );
                        if mqtt_start_connect_failures >= 5 {
                            let red = LightStateSnapshot {
                                on: true,
                                brightness: 255,
                                base_r: 255,
                                base_g: 0,
                                base_b: 0,
                            };
                            if let Err(e) =
                                apply_led_snapshot_hw_only(&config, &handler_state.light_state, red)
                                    .await
                            {
                                tracing::error!(%e, "startup MQTT failure: red LED apply failed");
                            } else {
                                keep_led_state_on_exit = true;
                                tracing::error!(
                                    "MQTT startup failed after 5 attempts; LED set red, terminating"
                                );
                            }
                            break;
                        }
                    }
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
                if !mqtt_connected_once {
                    mqtt_start_connect_failures = mqtt_start_connect_failures.saturating_add(1);
                    tracing::warn!(
                        attempt = mqtt_start_connect_failures,
                        max_attempts = 5,
                        "MQTT connect attempt failed during startup"
                    );
                    if mqtt_start_connect_failures >= 5 {
                        let red = LightStateSnapshot {
                            on: true,
                            brightness: 255,
                            base_r: 255,
                            base_g: 0,
                            base_b: 0,
                        };
                        if let Err(err) =
                            apply_led_snapshot_hw_only(&config, &handler_state.light_state, red)
                                .await
                        {
                            tracing::error!(%err, "startup MQTT failure: red LED apply failed");
                        } else {
                            keep_led_state_on_exit = true;
                            tracing::error!(
                                "MQTT startup failed after 5 attempts; LED set red, terminating"
                            );
                        }
                        break;
                    }
                } else if config.i2c_device.is_some()
                    && *handler_state.led_behavior.lock().await == LedBehavior::Status
                {
                    let red = LightStateSnapshot {
                        on: true,
                        brightness: 255,
                        base_r: 255,
                        base_g: 0,
                        base_b: 0,
                    };
                    if let Err(err) =
                        apply_led_snapshot_hw_only(&config, &handler_state.light_state, red).await
                    {
                        tracing::error!(%err, "MQTT disconnect in Status mode: red LED apply failed");
                    } else {
                        tracing::warn!(
                            "MQTT disconnected while running; LED behavior Status -> set LED red"
                        );
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if !keep_led_state_on_exit {
        if let Some(ref path) = config.i2c_device {
            match shutdown_led(path) {
                Ok(()) => tracing::info!("LED turned off on exit"),
                Err(e) => tracing::warn!(error = %e, "LED turn-off on exit failed (I²C)"),
            }
        }
    } else {
        tracing::warn!("keeping LED state on exit (startup MQTT failure)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn heart_rate_zero_publish_waits_for_persistent_streak() {
        // Startup seed: nothing published yet → publish immediately.
        let s = HrSideState::default();
        assert_eq!(s.zero_publish_decision(), ZeroPublish::Seed);
        // After a real reading, a short zero blip is held back…
        let s = HrSideState {
            announced: true,
            published: true,
            zero_published: false,
            zero_since: Some(Instant::now()),
        };
        assert_eq!(s.zero_publish_decision(), ZeroPublish::Hold);
        // …but a streak older than the delay publishes 0.
        let s = HrSideState {
            announced: true,
            published: true,
            zero_published: false,
            zero_since: Some(
                Instant::now()
                    .checked_sub(HEART_RATE_ZERO_PUBLISH_DELAY)
                    .unwrap(),
            ),
        };
        assert_eq!(s.zero_publish_decision(), ZeroPublish::Due);
        // Once 0 is shown, further zero evaluations hold.
        let mut s = HrSideState {
            announced: false,
            published: true,
            zero_published: true,
            zero_since: Some(
                Instant::now()
                    .checked_sub(HEART_RATE_ZERO_PUBLISH_DELAY)
                    .unwrap(),
            ),
        };
        assert_eq!(s.zero_publish_decision(), ZeroPublish::Hold);
        // A new valid reading resets the streak.
        s.note_reading();
        assert_eq!(s.zero_since, None);
        assert!(!s.zero_published);
        // Only "nobody in bed" clears to an empty payload (unknown in HA);
        // lock expired / artifact keep `0.0`.
        assert!(ZeroReason::PresenceOff.clears_to_unknown());
        assert!(!ZeroReason::LockExpired.clears_to_unknown());
        assert!(!ZeroReason::Artifact.clears_to_unknown());
        assert_eq!(ZeroReason::PresenceOff.as_str(), "presence OFF");
        assert_eq!(ZeroReason::LockExpired.as_str(), "lock expired");
        assert_eq!(ZeroReason::Artifact.as_str(), "artifact lock");
    }

    #[test]
    fn piezo35_tick_rate_derived_from_frame_timestamps() {
        // Real Pod 5 cadence: 11 ticks per ~55 ms frame → 200 Hz.
        let r = update_piezo35_rate(None, 1445, 11, None);
        assert_eq!(r, None); // first frame only seeds the timestamp
        let r = update_piezo35_rate(Some(1445), 1500, 11, r).unwrap();
        assert!((r - 200.0).abs() < 2.0, "rate {r} should be ≈ 200 Hz");
        // EMA keeps converging on the same rate.
        let r2 = update_piezo35_rate(Some(1500), 1555, 11, Some(r)).unwrap();
        assert!((r2 - 200.0).abs() < 2.0, "rate {r2} should stay ≈ 200 Hz");
    }

    #[test]
    fn piezo35_tick_rate_rejects_implausible_deltas() {
        let seeded = Some(200.0f32);
        // Reconnect: MCU timestamp reset → wrapping delta huge → rejected.
        assert_eq!(
            update_piezo35_rate(Some(1500), 4_000_000_000, 11, seeded),
            seeded
        );
        // Same-ms frame and zero-tick frames → no update.
        assert_eq!(update_piezo35_rate(Some(1500), 1500, 11, seeded), seeded);
        // Garbage rate (e.g. wrong timestamp unit) stays outside the band; a
        // backwards timestamp jump also wraps to a huge delta and is rejected.
        assert_eq!(update_piezo35_rate(Some(1500), 1300, 11, seeded), seeded);
        assert_eq!(update_piezo35_rate(Some(1500), 1501, 11, seeded), seeded);
    }

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
    fn compute_on_after_off_preserves_brightness() {
        let was_on = LightStateSnapshot {
            on: true,
            brightness: 128,
            base_r: 255,
            base_g: 255,
            base_b: 255,
        };
        let off_cmd = HaLightCommand {
            state: Some("OFF".into()),
            ..Default::default()
        };
        let after_off = compute_light_state(&off_cmd, &was_on);
        assert!(!after_off.on);
        assert_eq!(after_off.brightness, 128);

        let on_cmd = HaLightCommand {
            state: Some("ON".into()),
            ..Default::default()
        };
        let after_on = compute_light_state(&on_cmd, &after_off);
        assert!(after_on.on);
        assert_eq!(after_on.brightness, 128);
    }

    #[test]
    fn compute_on_restores_full_brightness_when_still_zero() {
        let prev = LightStateSnapshot {
            on: false,
            brightness: 0,
            base_r: 255,
            base_g: 255,
            base_b: 255,
        };
        let cmd = HaLightCommand {
            state: Some("ON".into()),
            ..Default::default()
        };
        let next = compute_light_state(&cmd, &prev);
        assert!(next.on);
        assert_eq!(next.brightness, 255);
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        for side in [BedSide::Left, BedSide::Right] {
            let (name, suffix) = match side {
                BedSide::Left => ("Target Cover Temperature Left", "target_temp_left"),
                BedSide::Right => ("Target Cover Temperature Right", "target_temp_right"),
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_firmware_message(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.firmware_message_state_topic().as_str()),
        );
        assert_eq!(v["name"].as_str(), Some("Frozen Message"));
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(
            v["enabled_by_default"].as_bool(),
            Some(false),
            "disabled in HA registry by default — enable under device entities if desired"
        );
    }

    #[test]
    fn sensor_message_discovery_matches_state_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_sensor_message(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["name"].as_str(), Some("Sensor Message"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.sensor_message_state_topic().as_str()),
        );
        assert_eq!(v["enabled_by_default"].as_bool(), Some(false),);
    }

    #[test]
    fn cover_button_discovery_matches_state_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_l = discovery_payload_cover_button_left(&cfg);
        let vl: serde_json::Value = serde_json::from_str(&disc_l).unwrap();
        assert_eq!(vl["name"].as_str(), Some("Cover Button Left"));
        assert_eq!(
            vl["state_topic"].as_str(),
            Some(cfg.cover_button_left_state_topic().as_str()),
        );
        assert_eq!(vl["state_class"].as_str(), Some("measurement"));
        assert_eq!(vl["unit_of_measurement"].as_str(), Some("taps"));
        let disc_r = discovery_payload_cover_button_right(&cfg);
        let vr: serde_json::Value = serde_json::from_str(&disc_r).unwrap();
        assert_eq!(vr["name"].as_str(), Some("Cover Button Right"));
        assert_eq!(
            vr["state_topic"].as_str(),
            Some(cfg.cover_button_right_state_topic().as_str()),
        );
        assert_eq!(vr["state_class"].as_str(), Some("measurement"));
        assert_eq!(vr["unit_of_measurement"].as_str(), Some("taps"));
    }

    #[test]
    fn ambient_discovery_matches_state_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_t = discovery_payload_ambient_temperature(&cfg);
        let vt: serde_json::Value = serde_json::from_str(&disc_t).unwrap();
        assert_eq!(vt["name"].as_str(), Some("Ambient Temperature"));
        assert_eq!(
            vt["state_topic"].as_str(),
            Some(cfg.ambient_temperature_state_topic().as_str()),
        );
        assert_eq!(vt["device_class"].as_str(), Some("temperature"));
        let disc_h = discovery_payload_ambient_humidity(&cfg);
        let vh: serde_json::Value = serde_json::from_str(&disc_h).unwrap();
        assert_eq!(vh["name"].as_str(), Some("Ambient Humidity"));
        assert_eq!(
            vh["state_topic"].as_str(),
            Some(cfg.ambient_humidity_state_topic().as_str()),
        );
        assert_eq!(vh["device_class"].as_str(), Some("humidity"));
    }

    #[test]
    fn self_update_discovery_matches_mqtt_update_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_self_update(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["platform"].as_str(), Some("update"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.update_state_topic().as_str()),
        );
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.update_command_topic().as_str()),
        );
        assert_eq!(
            v["payload_install"].as_str(),
            Some(crate::self_update::PAYLOAD_INSTALL),
        );
        assert_eq!(v["device_class"].as_str(), Some("firmware"));
        assert_eq!(v["name"].as_str(), Some("Firmware"));
        assert_eq!(v["title"].as_str(), Some("Firmware"));
    }

    #[test]
    fn deviceinfo_discovery_matches_state_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
    fn system_uptime_discovery_matches_state_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_system_uptime(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.system_uptime_state_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(v["enabled_by_default"].as_bool(), Some(true));
        assert_eq!(v["device_class"].as_str(), Some("timestamp"));
        assert_eq!(v["name"].as_str(), Some("System Up Since"));
    }

    #[test]
    fn usart_link_diagnostic_discovery_matches_state_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_f = discovery_payload_frozen_usart_link(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_f).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.frozen_usart_link_state_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(v["device_class"].as_str(), Some("connectivity"));
        assert_eq!(v["name"].as_str(), Some("Frozen Link"));
        assert_eq!(v["icon"].as_str(), Some("mdi:connection"));
        let disc_s = discovery_payload_sensor_usart_framing(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_s).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.sensor_usart_framing_state_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(v["device_class"].as_str(), Some("connectivity"));
        assert_eq!(v["name"].as_str(), Some("Sensor Link"));
        assert_eq!(v["icon"].as_str(), Some("mdi:connection"));
        let disc_i = discovery_payload_internet_access(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc_i).unwrap();
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.internet_access_state_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
        assert_eq!(v["device_class"].as_str(), Some("connectivity"));
        assert_eq!(v["name"].as_str(), Some("Internet Access"));
        assert_eq!(v["icon"].as_str(), Some("mdi:web"));
    }

    #[test]
    fn request_get_temperatures_button_discovery_matches_command_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
    fn reboot_button_discovery_matches_command_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_reboot_button(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.reboot_command_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
    }

    #[test]
    fn shutdown_button_discovery_matches_command_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_shutdown_button(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.shutdown_command_topic().as_str()),
        );
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
    }

    #[test]
    fn restart_lunaris_button_discovery_matches_command_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_restart_lunaris_button(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.restart_lunaris_command_topic().as_str()),
        );
        assert_eq!(v["enabled_by_default"].as_bool(), Some(true));
        assert_eq!(v["entity_category"].as_str(), Some("diagnostic"));
    }

    #[test]
    fn calibrate_presence_discovery_has_entity_category_config() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
    fn led_behavior_discovery_is_configuration_entity() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_led_behavior_select(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["entity_category"].as_str(), Some("config"));
        assert_eq!(v["icon"].as_str(), Some("mdi:lightbulb"));
        assert_eq!(v["name"].as_str(), Some("LED Behavior"));
    }

    #[test]
    fn water_tank_discovery_is_mqtt_binary_sensor_with_plug_device_class() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        assert_eq!(v["icon"].as_str(), Some("mdi:water"));
        assert_eq!(v["payload_on"].as_str(), Some("ON"));
        assert_eq!(v["payload_off"].as_str(), Some("OFF"));
    }

    #[test]
    fn vibration_settings_discovery_payloads_use_config_entity_category() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
        assert_eq!(cancel["icon"].as_str(), Some("mdi:vibrate-off"));
        let v: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibration_pattern_select(&cfg)).unwrap();
        let opts = v["options"].as_array().expect("options array");
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].as_str(), Some("single"));
        assert_eq!(opts[1].as_str(), Some("double"));
        let duration: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibration_duration_number(&cfg)).unwrap();
        assert_eq!(duration["icon"].as_str(), Some("mdi:vibrate"));
        let intensity: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibration_intensity_number(&cfg)).unwrap();
        assert_eq!(intensity["icon"].as_str(), Some("mdi:vibrate"));
    }

    #[test]
    fn cover_button_vibration_feedback_switch_discovery() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc = discovery_payload_cover_button_vibration_feedback_switch(&cfg);
        let v: serde_json::Value = serde_json::from_str(&disc).unwrap();
        assert_eq!(v["name"].as_str(), Some("Cover Button Vibration Feedback"));
        assert_eq!(v["entity_category"].as_str(), Some("config"));
        assert_eq!(
            v["enabled_by_default"].as_bool(),
            Some(true),
            "visible by default: the stock firmware always gives this feedback"
        );
        assert_eq!(v["icon"].as_str(), Some("mdi:vibrate"));
        assert_eq!(v["payload_on"].as_str(), Some("ON"));
        assert_eq!(v["payload_off"].as_str(), Some("OFF"));
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.cover_button_feedback_command_topic().as_str())
        );
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.cover_button_feedback_state_topic().as_str())
        );
        assert!(cfg
            .cover_button_feedback_command_topic()
            .contains("/switch/cover_button_vibration_feedback/set"));
        assert!(cfg
            .cover_button_feedback_state_topic()
            .contains("/switch/cover_button_vibration_feedback/state"));
        assert!(cfg
            .discovery_topic_cover_button_vibration_feedback()
            .contains("/switch/lunaris_cover_button_vibration_feedback/config"));
        // Runtime default matches the original firmware (feedback on).
        assert!(cfg.cover_button_feedback.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cover_button_feedback_queues_native_frames_and_respects_switch() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let mut cfg = BridgeConfig::from_cli(&cli);
        cfg.sensor_device = Some(PathBuf::from("/not-opened-in-this-test"));
        let (tx, mut rx) = mpsc::channel(4);
        cfg.sensor_tx = Some(tx);

        handle_cover_button_tap_feedback(&cfg, CoverButtonTapSide::Left, 2).await;
        let batch = rx.try_recv().unwrap();
        assert_eq!(batch.alarm_count(), 1);
        for frame in &batch.frames()[batch.cancel_count() + 4..] {
            assert_eq!(&frame[2..7], &[0x40, 0, 30, 2, 2]);
        }

        // The ordinary vibration settings cannot change stock tap feedback.
        {
            let mut settings = cfg.vibration_settings.lock().await;
            settings.intensity = 100;
            settings.duration_sec = 60;
            settings.pattern = AlarmPattern::Double;
        }
        handle_cover_button_tap_feedback(&cfg, CoverButtonTapSide::Right, 1).await;
        let batch = rx.try_recv().unwrap();
        assert_eq!(&batch.frames().last().unwrap()[2..7], &[0x40, 1, 30, 2, 1]);

        cfg.cover_button_feedback.store(false, Ordering::SeqCst);
        handle_cover_button_tap_feedback(&cfg, CoverButtonTapSide::Left, 2).await;
        assert!(
            rx.try_recv().is_err(),
            "disabled feedback must not enqueue commands"
        );
        cfg.cover_button_feedback.store(true, Ordering::SeqCst);
        cfg.sensor_device = None;
        handle_cover_button_tap_feedback(&cfg, CoverButtonTapSide::Left, 1).await;
        assert!(
            rx.try_recv().is_err(),
            "disabled Sensor must not enqueue commands"
        );
    }

    #[tokio::test]
    async fn obsolete_cover_button_feedback_tuning_discovery_removed() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        assert_eq!(
            cfg.discovery_topic_cover_button_feedback_intensity(),
            "homeassistant/number/lunaris_cover_button_feedback_intensity/config"
        );
        assert_eq!(
            cfg.discovery_topic_cover_button_feedback_duration(),
            "homeassistant/number/lunaris_cover_button_feedback_duration/config"
        );
        let (client, mut eventloop) = AsyncClient::new(
            MqttOptions::new("feedback-migration-test", "localhost", 1883),
            4,
        );
        remove_obsolete_feedback_tuning_discovery(&client, &cfg).await;
        // Inspect queued MQTT requests without connecting to a broker.
        eventloop.clean();
        assert_eq!(eventloop.pending.len(), 2);
        for (request, topic) in eventloop.pending.iter().zip([
            cfg.discovery_topic_cover_button_feedback_intensity(),
            cfg.discovery_topic_cover_button_feedback_duration(),
        ]) {
            let rumqttc::Request::Publish(publish) = request else {
                panic!("expected discovery removal publish");
            };
            assert_eq!(publish.topic, topic);
            assert!(publish.retain);
            assert!(publish.payload.is_empty());
        }
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
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
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
    fn presence_binary_unknown_payload_matches_ha_mqtt_payload_none() {
        assert_eq!(PRESENCE_BINARY_SENSOR_STATE_UNKNOWN, "None");
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
                (false, false, false)
            );
        }
        assert_eq!(
            inference_occupancy(&z, &mut inference, 9999, 50),
            (true, true, true)
        );
    }

    #[test]
    fn combined_climate_discovery_uses_combined_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let v: serde_json::Value =
            serde_json::from_str(&discovery_payload_climate_combined(&cfg)).unwrap();
        assert_eq!(v["name"].as_str(), Some("Cover"));
        assert_eq!(
            v["mode_command_topic"].as_str(),
            Some(cfg.climate_combined_mode_command_topic().as_str()),
        );
        assert_eq!(
            v["mode_state_topic"].as_str(),
            Some(cfg.climate_combined_mode_state_topic().as_str()),
        );
        assert_eq!(
            v["temperature_command_topic"].as_str(),
            Some(cfg.climate_combined_temperature_command_topic().as_str()),
        );
        assert_eq!(
            v["temperature_state_topic"].as_str(),
            Some(cfg.climate_combined_temperature_state_topic().as_str()),
        );
        // The combined climate reads the combined current cover temperature sensor.
        assert_eq!(
            v["current_temperature_topic"].as_str(),
            Some(cfg.frozen_current_temp_combined_state_topic().as_str()),
        );
        assert!(v["unique_id"]
            .as_str()
            .unwrap()
            .ends_with("_climate_combined"));
    }

    #[test]
    fn combined_climate_state_is_any_enabled_and_mean_target() {
        let mut left = ClimateSideState::default();
        let mut right = ClimateSideState::default();
        // Both off → off, mean of defaults (30.00).
        assert_eq!(combined_climate_mode(&left, &right), CLIMATE_MODE_OFF);
        assert_eq!(combined_target_temp_c(&left, &right), 30.0);
        // Either side on → heat_cool.
        left.enabled = true;
        left.target_centi = 2000;
        assert_eq!(combined_climate_mode(&left, &right), CLIMATE_MODE_HEAT_COOL);
        assert_eq!(combined_target_temp_c(&left, &right), 25.0);
        // Both on with different targets → still heat_cool, mean target.
        right.enabled = true;
        right.target_centi = 2200;
        assert_eq!(combined_climate_mode(&left, &right), CLIMATE_MODE_HEAT_COOL);
        assert_eq!(combined_target_temp_c(&left, &right), 21.0);
        // Back off on one side → off only when both are off.
        right.enabled = false;
        assert_eq!(combined_climate_mode(&left, &right), CLIMATE_MODE_HEAT_COOL);
        left.enabled = false;
        assert_eq!(combined_climate_mode(&left, &right), CLIMATE_MODE_OFF);
    }

    #[test]
    fn combined_vibrate_button_discovery_matches_command_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let v: serde_json::Value =
            serde_json::from_str(&discovery_payload_vibrate_combined_button(&cfg)).unwrap();
        assert_eq!(v["name"].as_str(), Some("Vibrate Cover"));
        assert_eq!(
            v["command_topic"].as_str(),
            Some(cfg.vibrate_combined_command_topic().as_str()),
        );
        assert_eq!(v["icon"].as_str(), Some("mdi:vibrate"));
        assert!(v["unique_id"]
            .as_str()
            .unwrap()
            .ends_with("_vibrate_combined"));
    }

    #[test]
    fn combined_temperature_sensor_discovery_matches_state_topics() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let disc_current = discovery_payload_frozen_temperature(
            &cfg,
            "Current Cover Temperature",
            "cover_temp_combined",
            cfg.frozen_current_temp_combined_state_topic(),
        );
        let v: serde_json::Value = serde_json::from_str(&disc_current).unwrap();
        assert_eq!(v["name"].as_str(), Some("Current Cover Temperature"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.frozen_current_temp_combined_state_topic().as_str()),
        );
        assert_eq!(v["device_class"].as_str(), Some("temperature"));
        let disc_target = discovery_payload_frozen_temperature(
            &cfg,
            "Target Cover Temperature",
            "target_temp_combined",
            cfg.target_temperature_combined_state_topic(),
        );
        let v: serde_json::Value = serde_json::from_str(&disc_target).unwrap();
        assert_eq!(v["name"].as_str(), Some("Target Cover Temperature"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.target_temperature_combined_state_topic().as_str()),
        );
        assert_eq!(v["device_class"].as_str(), Some("temperature"));
    }

    #[test]
    fn mean_temperature_payload_formats_average() {
        assert_eq!(mean_temperature_payload(2550, 2675), "26.12");
        assert_eq!(mean_temperature_payload(2000, 2000), "20.00");
        assert_eq!(mean_temperature_payload(2001, 2000), "20.00");
        assert_eq!(mean_temperature_payload(0, 100), "0.50");
    }

    #[test]
    fn combined_heart_rate_discovery_matches_state_topic() {
        let cli =
            crate::cli::Cli::parse_from(["lunaris", "--pod", "4", "--mqtt-host", "localhost"]);
        let cfg = BridgeConfig::from_cli(&cli);
        let v: serde_json::Value =
            serde_json::from_str(&discovery_payload_heart_rate(&cfg, HrTopic::Combined)).unwrap();
        assert_eq!(v["name"].as_str(), Some("Heart Rate"));
        assert_eq!(
            v["state_topic"].as_str(),
            Some(cfg.heart_rate_combined_state_topic().as_str()),
        );
        assert_eq!(v["unit_of_measurement"].as_str(), Some("bpm"));
        assert_eq!(v["state_class"].as_str(), Some("measurement"));
        assert!(v["unique_id"]
            .as_str()
            .unwrap()
            .ends_with("_heart_rate_combined"));
    }

    #[test]
    fn combined_heart_rate_bpm_pools_all_ibis() {
        // No beat intervals → None (caller applies the zero/unknown publish rules).
        assert_eq!(combined_heart_rate_bpm(&[]), None);
        // One side's beats only → that side's rate, computed from all its intervals.
        let left = [857.0f32; 4]; // ≈ 70 bpm
        assert_eq!(combined_heart_rate_bpm(&left), Some(60_000.0 / 857.0));
        // Both sides pooled → one rate over all intervals, weighted by how many
        // beats each side contributed — not the mean of the two per-side readings.
        let right = [968.0f32; 8]; // ≈ 62 bpm
        let mut pooled = left.to_vec();
        pooled.extend_from_slice(&right);
        let expected = 60_000.0 / ((4.0 * 857.0 + 8.0 * 968.0) / 12.0);
        assert_eq!(combined_heart_rate_bpm(&pooled), Some(expected));
        // The pooled rate differs from the plain mean of the two side rates.
        let mean_of_sides = (60_000.0 / 857.0 + 60_000.0 / 968.0) / 2.0;
        assert!((expected - mean_of_sides).abs() > 0.5);
    }

    #[test]
    fn presence_combined_decided_on_all_six_zones() {
        // Uncalibrated: a single zone on either side flips the combined occupancy,
        // decided directly on the whole grid.
        let mut inference = PresenceInferenceState::default();
        let z_left = SensorCapacitanceZones {
            sequence: 1,
            zones: [900, 100, 100, 100, 100, 100],
        };
        assert_eq!(
            inference_occupancy(&z_left, &mut inference, 800, 50),
            (true, false, true)
        );
        let mut inference = PresenceInferenceState::default();
        let z_right = SensorCapacitanceZones {
            sequence: 1,
            zones: [100, 100, 100, 100, 100, 900],
        };
        assert_eq!(
            inference_occupancy(&z_right, &mut inference, 800, 50),
            (false, true, true)
        );
        // Calibrated: any zone across the whole grid (5-frame debounce) flips the
        // combined occupancy even when only one side is occupied.
        let mut inference = PresenceInferenceState::default();
        inference.set_baselines([100; 6]);
        let z = SensorCapacitanceZones {
            sequence: 1,
            zones: [100, 100, 100, 200, 100, 100],
        };
        for _ in 0..4 {
            assert_eq!(
                inference_occupancy(&z, &mut inference, 9999, 50),
                (false, false, false)
            );
        }
        assert_eq!(
            inference_occupancy(&z, &mut inference, 9999, 50),
            (false, true, true)
        );
    }
}
