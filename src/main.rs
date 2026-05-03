//! Eight Sleep Pod Frozen **prime** bridge: MQTT (Home Assistant discovery) → USART.
//!
//! SPDX-License-Identifier: GPL-3.0-only

mod cli;
mod frozen_frame;
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

    let config = mqtt_bridge::BridgeConfig::from_cli(&cli);
    let frame = frozen_frame::prime_frame();
    let arc = Arc::from(frame.into_boxed_slice());
    mqtt_bridge::run(config, arc).await;
}
