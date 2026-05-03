//! MQTT: Home Assistant discovery, prime button, JSON light (IS31FL3194 via I²C).

use std::path::PathBuf;
use std::sync::Arc;

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, LastWill, MqttOptions, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::cli::Cli;
use crate::is31fl3194::Is31fl3194;
use crate::serial_prime;

const HA_STATUS_TOPIC: &str = "homeassistant/status";

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
                    setup_session(&client, &config).await;
                    if config.i2c_device.is_some() {
                        let snap = light_state.lock().await.clone();
                        publish_light_state(&client, &config, &snap).await;
                    }
                } else {
                    tracing::error!(code = ?ack.code, "MQTT connection refused");
                }
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                handle_publish(&client, &config, &prime_frame, &light_state, p).await;
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
