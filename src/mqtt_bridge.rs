//! MQTT: Home Assistant discovery, prime button, mattress climate (left/right), JSON light (IS31FL3194 via I²C).
//!
//! **rumqttc:** subscribe/publish must not block the task that runs [`rumqttc::EventLoop::poll`].
//! Outbound work runs in [`tokio::spawn`] so the event loop keeps draining requests (see upstream docs on [`AsyncClient`]).

use std::path::PathBuf;
use std::sync::Arc;

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, LastWill, MqttOptions, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::cli::Cli;
use crate::frozen_frame::{set_target_temperature_frame, BedSide};
use crate::is31fl3194::Is31fl3194;
use crate::serial_prime;

const HA_STATUS_TOPIC: &str = "homeassistant/status";
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
    pub discovery_object_id_led: String,
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
            discovery_object_id_led: cli.discovery_object_id_led.clone(),
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
        BedSide::Left => "Cover links",
        BedSide::Right => "Cover rechts",
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
        "device": config.device_json(),
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": config.availability_json(),
    })
    .to_string()
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
            temp_str,
        )
        .await
    {
        tracing::error!(?e, "publish climate temperature state");
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

    match serial_prime::send_frame(&config.serial_device, config.serial_baud, &frame).await {
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
            tracing::error!(?e, ?side, "climate mode serial write failed");
            *state.lock().await = backup;
            publish_json_result(
                client,
                config,
                climate_action_label(side),
                "error",
                &e.to_string(),
            )
            .await;
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
        match serial_prime::send_frame(&config.serial_device, config.serial_baud, &frame).await {
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
                tracing::error!(?e, ?side, "climate temperature serial write failed");
                *state.lock().await = backup;
                publish_json_result(
                    client,
                    config,
                    climate_action_label(side),
                    "error",
                    &e.to_string(),
                )
                .await;
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
    if config.i2c_device.is_some() {
        let disc_led = discovery_payload_light(config);
        if let Err(e) = client
            .publish(config.discovery_topic_light(), qos, true, disc_led)
            .await
        {
            tracing::error!(?e, "publish light discovery");
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
    if config.i2c_device.is_some() {
        if let Err(e) = client.subscribe(config.light_command_topic(), qos).await {
            tracing::error!(?e, "subscribe light command topic");
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
    let Some(i2c_path) = config.i2c_device.clone() else {
        return;
    };

    let prev = light_state.lock().await.clone();
    let snap = compute_light_state(&cmd, &prev);
    let (cr, cg, cb) = snap.chip_rgb();
    let path = i2c_path.clone();
    let on = snap.on && snap.brightness > 0;
    let set_res = tokio::task::spawn_blocking(move || {
        let mut dev = Is31fl3194::open(&path)?;
        dev.set_solid_rgb(on, cr, cg, cb)
    })
    .await;

    match set_res {
        Ok(Ok(())) => {
            *light_state.lock().await = snap.clone();
            publish_light_state(client, config, &snap).await;
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
        Ok(Err(e)) => {
            tracing::error!(?e, "I²C LED write failed");
            publish_json_result(client, config, "led", "error", &e.to_string()).await;
        }
        Err(e) => tracing::error!(?e, "LED task join failed"),
    }
}

async fn handle_publish(
    client: &AsyncClient,
    config: &BridgeConfig,
    prime_frame: &[u8],
    light_state: &Arc<Mutex<LightStateSnapshot>>,
    climate_left: &Arc<Mutex<ClimateSideState>>,
    climate_right: &Arc<Mutex<ClimateSideState>>,
    p: Publish,
) {
    if p.topic == config.command_topic() {
        let expected = config.payload_press.as_bytes();
        if p.payload.as_ref() == expected {
            match serial_prime::send_frame(&config.serial_device, config.serial_baud, prime_frame)
                .await
            {
                Ok(()) => {
                    tracing::info!("prime frame sent to Frozen serial port");
                    publish_json_result(client, config, "prime", "success", "prime frame sent")
                        .await;
                }
                Err(e) => {
                    tracing::error!(?e, "serial write failed");
                    publish_json_result(client, config, "prime", "error", &e.to_string()).await;
                }
            }
        }
        return;
    }

    if p.topic == config.climate_mode_command_topic(BedSide::Left) {
        handle_climate_mode_command(client, config, BedSide::Left, climate_left, &p.payload).await;
        return;
    }
    if p.topic == config.climate_mode_command_topic(BedSide::Right) {
        handle_climate_mode_command(client, config, BedSide::Right, climate_right, &p.payload)
            .await;
        return;
    }
    if p.topic == config.climate_temperature_command_topic(BedSide::Left) {
        handle_climate_temperature_command(client, config, BedSide::Left, climate_left, &p.payload)
            .await;
        return;
    }
    if p.topic == config.climate_temperature_command_topic(BedSide::Right) {
        handle_climate_temperature_command(
            client,
            config,
            BedSide::Right,
            climate_right,
            &p.payload,
        )
        .await;
        return;
    }

    if config.i2c_device.is_some() && p.topic == config.light_command_topic() {
        handle_light_command(client, config, light_state, &p.payload).await;
        return;
    }

    if p.topic == HA_STATUS_TOPIC && p.payload.as_ref() == b"online" {
        tracing::debug!("Home Assistant online; republishing discovery");
        publish_discovery_and_online(client, config).await;
    }
}

/// Run the MQTT event loop until a fatal error (or process kill).
pub async fn run(config: BridgeConfig, prime_frame: Arc<[u8]>) {
    let light_state = Arc::new(Mutex::new(LightStateSnapshot::default()));
    let climate_left = Arc::new(Mutex::new(ClimateSideState::default()));
    let climate_right = Arc::new(Mutex::new(ClimateSideState::default()));
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

    tracing::info!(
        host = %config.mqtt_host,
        port = config.mqtt_port,
        led = config.i2c_device.is_some(),
        "MQTT client created; polling event loop"
    );

    loop {
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
                    let ls = light_state.clone();
                    let cl = climate_left.clone();
                    let cr = climate_right.clone();
                    tokio::spawn(async move {
                        setup_session(&c, &cfg).await;
                        if cfg.i2c_device.is_some() {
                            let snap = ls.lock().await.clone();
                            publish_light_state(&c, &cfg, &snap).await;
                        }
                        let left = cl.lock().await.clone();
                        publish_climate_state(&c, &cfg, BedSide::Left, &left).await;
                        let right = cr.lock().await.clone();
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
                let ls = light_state.clone();
                let cl = climate_left.clone();
                let cr = climate_right.clone();
                tokio::spawn(async move {
                    handle_publish(&c, &cfg, &pf, &ls, &cl, &cr, p).await;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
