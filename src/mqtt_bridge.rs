//! MQTT client: discovery, availability, button command → prime on serial.

use std::sync::Arc;

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, LastWill, MqttOptions, Publish, QoS};
use serde_json::json;
use tokio::time::{sleep, Duration};

use crate::cli::Cli;
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
    pub device_name: String,
    pub device_identifier: String,
    pub sw_version: String,
    pub payload_press: String,
    pub serial_device: std::path::PathBuf,
    pub serial_baud: u32,
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
            device_name: cli.device_name.clone(),
            device_identifier: cli.device_identifier.clone(),
            sw_version: env!("CARGO_PKG_VERSION").to_string(),
            payload_press: cli.payload_press.clone(),
            serial_device: cli.serial_device.clone(),
            serial_baud: cli.serial_baud,
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

    pub fn result_topic(&self) -> String {
        format!("{}/result", self.topic_prefix)
    }
}

fn discovery_payload(config: &BridgeConfig) -> String {
    json!({
        "name": "Prime",
        "command_topic": config.command_topic(),
        "payload_press": config.payload_press,
        "unique_id": format!("{}_prime_button", config.device_identifier),
        "device": {
            "identifiers": [config.device_identifier.clone()],
            "name": config.device_name,
            "model": "Eight Sleep Pod",
            "sw_version": config.sw_version,
        },
        "origin": {
            "name": "narcolepsy",
            "sw": config.sw_version,
        },
        "availability": [
            {
                "topic": config.availability_topic(),
                "payload_available": "online",
                "payload_not_available": "offline",
            }
        ],
    })
    .to_string()
}

async fn publish_json_result(
    client: &AsyncClient,
    config: &BridgeConfig,
    status: &str,
    message: &str,
) {
    let body = json!({
        "action": "prime",
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

async fn publish_discovery_and_online(client: &AsyncClient, config: &BridgeConfig) {
    let qos = QoS::AtLeastOnce;
    let disc = discovery_payload(config);
    if let Err(e) = client
        .publish(config.discovery_topic(), qos, true, disc)
        .await
    {
        tracing::error!(?e, "publish discovery");
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
    if let Err(e) = client.subscribe(HA_STATUS_TOPIC, qos).await {
        tracing::error!(?e, "subscribe Home Assistant birth topic");
    }
    publish_discovery_and_online(client, config).await;
}

async fn handle_publish(
    client: &AsyncClient,
    config: &BridgeConfig,
    prime_frame: &[u8],
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
                    publish_json_result(client, config, "success", "prime frame sent").await;
                }
                Err(e) => {
                    tracing::error!(?e, "serial write failed");
                    publish_json_result(client, config, "error", &e.to_string()).await;
                }
            }
        }
        return;
    }

    if p.topic == HA_STATUS_TOPIC && p.payload.as_ref() == b"online" {
        tracing::debug!("Home Assistant online; republishing discovery");
        publish_discovery_and_online(client, config).await;
    }
}

/// Run the MQTT event loop until a fatal error (or process kill).
pub async fn run(config: BridgeConfig, prime_frame: Arc<[u8]>) {
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
        "MQTT client created; polling event loop"
    );

    loop {
        let evt = eventloop.poll().await;
        match evt {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                if ack.code == rumqttc::ConnectReturnCode::Success {
                    tracing::info!("MQTT connected");
                    setup_session(&client, &config).await;
                } else {
                    tracing::error!(code = ?ack.code, "MQTT connection refused");
                }
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                handle_publish(&client, &config, &prime_frame, p).await;
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
