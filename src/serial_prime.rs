//! Blocking/async helpers to send a pre-encoded Frozen frame on a serial port.

use std::path::Path;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_serial::SerialPortBuilderExt;

#[derive(Debug, Error)]
pub enum SerialPrimeError {
    #[error("serial I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("serial port: {0}")]
    Port(#[from] tokio_serial::Error),
}

/// Opens the serial port and closes it. Used at process start so we fail fast if the device is missing or locked.
pub async fn check_device_accessible(device: &Path, baud: u32) -> Result<(), SerialPrimeError> {
    let path = device.to_string_lossy().to_string();
    let _port = tokio_serial::new(path, baud).open_native_async()?;
    Ok(())
}

pub async fn send_frame(device: &Path, baud: u32, frame: &[u8]) -> Result<(), SerialPrimeError> {
    let path = device.to_string_lossy().to_string();
    let mut port = tokio_serial::new(path, baud).open_native_async()?;
    port.write_all(frame).await?;
    port.flush().await?;
    Ok(())
}
