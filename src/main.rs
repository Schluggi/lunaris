//! Eight Sleep Pod Frozen bridge: MQTT (Home Assistant discovery: prime, climate left/right, LED) → USART / I²C.
//!
//! SPDX-License-Identifier: GPL-3.0-only

mod cli;
mod deviceinfo;
mod frozen_frame;
mod frozen_link;
mod frozen_rx;
mod is31fl3194;
mod machine_config;
mod mqtt_bridge;
mod self_update;
mod sensor_frame;
mod sensor_link;
mod sensor_rx;
mod serial_prime;
mod wire_buffer;

use std::sync::Arc;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Some(flag) = std::env::args().nth(1) {
        if (flag == "--version" || flag == "-V") && std::env::args().len() == 2 {
            println!("{}", env!("CARGO_PKG_VERSION"));
            return;
        }
    }

    let cli = machine_config::parse_cli_overlay_machine_json();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(cli.log_level.clone()));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting lunaris");

    if let Err(e) =
        serial_prime::check_device_accessible(&cli.serial_device, cli.effective_serial_baud()).await
    {
        tracing::error!(
            device = %cli.serial_device.display(),
            baud = cli.effective_serial_baud(),
            error = %e,
            "cannot open serial device (Frozen USART); refusing to start without it \
             (Pod 4: try --serial-device /dev/ttyS1 per github.com/LiamSnow/opensleep/issues/11; Pod 3: often /dev/ttymxc2)"
        );
        std::process::exit(1);
    }
    tracing::info!(
        device = %cli.serial_device.display(),
        baud = cli.effective_serial_baud(),
        "serial device opened successfully (startup check)"
    );

    let mut config = mqtt_bridge::BridgeConfig::from_cli(&cli);

    let frozen_link = frozen_link::spawn(cli.serial_device.clone(), cli.effective_serial_baud());
    config.frozen_tx = Some(frozen_link.tx);
    config.frozen_temperature_discovery = true;

    match crate::is31fl3194::probe(&cli.i2c_device) {
        Ok(()) => {
            tracing::info!(
                device = %cli.i2c_device.display(),
                "I²C LED bus opened (IS31FL3194 @ 0x53)"
            );
            config.i2c_device = Some(cli.i2c_device.clone());
        }
        Err(e) => {
            tracing::warn!(
                device = %cli.i2c_device.display(),
                error = %e,
                "cannot open I²C for LED; continuing without MQTT light entity"
            );
        }
    }

    let mut presence_cap_rx =
        None::<tokio::sync::mpsc::Receiver<sensor_rx::SensorCapacitanceZones>>;
    let mut sensor_message_rx = None::<tokio::sync::mpsc::Receiver<String>>;

    if let Err(e) =
        serial_prime::check_device_accessible(&cli.sensor_device, cli.effective_sensor_baud()).await
    {
        tracing::warn!(
            device = %cli.sensor_device.display(),
            error = %e,
            "cannot open Sensor serial for vibration; continuing without vibrate buttons"
        );
    } else {
        tracing::info!(
            device = %cli.sensor_device.display(),
            baud = cli.effective_sensor_baud(),
            "Sensor serial opened (vibration / SetAlarm)"
        );
        config.sensor_device = Some(cli.sensor_device.clone());
        config.presence_discovery = true;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        presence_cap_rx = Some(rx);
        let presence_cap_parse_diag = cli.presence_debug.then(sensor_rx::PresenceCapDiag::new_arc);
        let sensor = sensor_link::spawn(
            cli.sensor_device.clone(),
            cli.effective_sensor_baud(),
            true,
            cli.sensor_vibrate_no_ack_wait,
            Some(tx),
            presence_cap_parse_diag,
        );
        config.sensor_tx = Some(sensor.tx.clone());
        sensor_message_rx = Some(sensor.sensor_message_rx);
    }

    let frame = frozen_frame::prime_frame();
    let arc = Arc::from(frame.into_boxed_slice());
    mqtt_bridge::run(
        config,
        arc,
        Some(frozen_link.temperature_rx),
        frozen_link.water_tank_rx,
        frozen_link.firmware_message_rx,
        presence_cap_rx,
        sensor_message_rx,
    )
    .await;
}
