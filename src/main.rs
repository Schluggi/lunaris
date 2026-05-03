//! Eight Sleep Pod Frozen bridge: MQTT (Home Assistant discovery: prime, climate left/right, LED) → USART / I²C.
//!
//! SPDX-License-Identifier: GPL-3.0-only

mod cli;
mod frozen_frame;
mod frozen_link;
mod frozen_rx;
mod is31fl3194;
mod mqtt_bridge;
mod sensor_frame;
mod sensor_link;
mod sensor_rx;
mod serial_prime;
mod wire_buffer;

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = cli::Cli::parse();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(cli.log_level.clone()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    if let Err(e) = serial_prime::check_device_accessible(&cli.serial_device, cli.serial_baud).await
    {
        tracing::error!(
            device = %cli.serial_device.display(),
            baud = cli.serial_baud,
            error = %e,
            "cannot open serial device (Frozen USART); refusing to start without it \
             (Pod 4: try --serial-device /dev/ttyS1 per github.com/LiamSnow/opensleep/issues/11; Pod 3: often /dev/ttymxc2)"
        );
        std::process::exit(1);
    }
    tracing::info!(
        device = %cli.serial_device.display(),
        baud = cli.serial_baud,
        "serial device opened successfully (startup check)"
    );

    let mut config = mqtt_bridge::BridgeConfig::from_cli(&cli);

    let frozen_link = frozen_link::spawn(cli.serial_device.clone(), cli.serial_baud);
    config.frozen_tx = Some(frozen_link.tx);
    config.frozen_temperature_discovery = true;

    if cli.no_led {
        tracing::info!("LED control disabled (--no-led)");
    } else {
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
    }

    let mut sensor_priming_events: Option<tokio::sync::mpsc::Receiver<sensor_link::PrimingEvent>> =
        None;
    let mut presence_cap_rx =
        None::<tokio::sync::mpsc::Receiver<sensor_rx::SensorCapacitanceZones>>;

    if cli.no_vibration {
        tracing::info!("Vibration / Sensor UART disabled (--no-vibration)");
    } else if let Err(e) =
        serial_prime::check_device_accessible(&cli.sensor_device, cli.sensor_baud).await
    {
        tracing::warn!(
            device = %cli.sensor_device.display(),
            error = %e,
            "cannot open Sensor serial for vibration; continuing without vibrate buttons"
        );
    } else {
        tracing::info!(
            device = %cli.sensor_device.display(),
            baud = cli.sensor_baud,
            "Sensor serial opened (vibration / SetAlarm)"
        );
        config.sensor_device = Some(cli.sensor_device.clone());
        let cap_tx = if cli.no_presence_detection {
            None
        } else {
            config.presence_discovery = true;
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            presence_cap_rx = Some(rx);
            Some(tx)
        };
        let sensor = sensor_link::spawn(
            cli.sensor_device.clone(),
            cli.sensor_baud,
            !cli.no_sensor_bootloader_handshake,
            cli.sensor_vibrate_no_ack_wait,
            cap_tx,
        );
        config.sensor_tx = Some(sensor.tx.clone());
        config.sensor_priming_counts = Some(sensor.priming_counts.clone());
        sensor_priming_events = Some(sensor.priming_events_rx);
    }

    let frame = frozen_frame::prime_frame();
    let arc = Arc::from(frame.into_boxed_slice());
    mqtt_bridge::run(
        config,
        arc,
        sensor_priming_events,
        Some(frozen_link.temperature_rx),
        presence_cap_rx,
    )
    .await;
}
