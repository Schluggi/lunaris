//! Eight Sleep Pod Frozen **prime** bridge: MQTT (Home Assistant discovery) → USART.
//!
//! SPDX-License-Identifier: GPL-3.0-only

mod cli;
mod frozen_frame;
mod is31fl3194;
mod mqtt_bridge;
mod serial_prime;

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

    let frame = frozen_frame::prime_frame();
    let arc = Arc::from(frame.into_boxed_slice());
    mqtt_bridge::run(config, arc).await;
}
