//! Blocking/async helpers to send pre-encoded frames on a serial port.
//!
//! Used when UART queues from [`crate::frozen_link`] / [`crate::sensor_link`] are not wired (e.g. tests), or as the send path in [`crate::mqtt_bridge`] when a queue is unset.

use std::path::Path;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_serial::SerialPortBuilderExt;

/// Mark the serial device `FD_CLOEXEC` so a later `exec` (e.g. self-update) does not inherit a
/// second open handle; the new process can open `/dev/ttyS*` again for the startup check in `main`.
#[cfg(unix)]
pub(crate) fn set_serial_cloexec(port: &tokio_serial::SerialStream) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = port.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn set_serial_cloexec(_port: &tokio_serial::SerialStream) -> std::io::Result<()> {
    Ok(())
}

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
    let port = tokio_serial::new(path, baud).open_native_async()?;
    set_serial_cloexec(&port)?;
    Ok(())
}

/// Checks that a serial device node exists and is readable/writable **without opening it**.
///
/// Unlike [`check_device_accessible`], this performs no open/close cycle — every open of a
/// `ttyS*` can toggle modem-control lines (DTR/RTS) and reset the attached MCU, which is
/// exactly what must not happen for the optional Sensor USART before [`crate::sensor_link`]
/// runs its probe/handshake sequence. `sensor_link` retries open errors in the background.
pub fn check_device_present(device: &Path) -> Result<(), SerialPrimeError> {
    use std::os::unix::fs::FileTypeExt;

    let md = std::fs::metadata(device)?;
    if !md.file_type().is_char_device() {
        return Err(SerialPrimeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a character device", device.display()),
        )));
    }
    let c_path = std::ffi::CString::new(device.as_os_str().as_encoded_bytes())
        .map_err(std::io::Error::from)?;
    if unsafe { libc::access(c_path.as_ptr(), libc::R_OK | libc::W_OK) } != 0 {
        return Err(SerialPrimeError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub async fn send_frame(device: &Path, baud: u32, frame: &[u8]) -> Result<(), SerialPrimeError> {
    let path = device.to_string_lossy().to_string();
    let mut port = tokio_serial::new(path, baud).open_native_async()?;
    set_serial_cloexec(&port)?;
    port.write_all(frame).await?;
    port.flush().await?;
    Ok(())
}

/// Writes several frames on one open handle (Sensor vibration sequence).
pub async fn send_frames(
    device: &Path,
    baud: u32,
    frames: &[Vec<u8>],
) -> Result<(), SerialPrimeError> {
    let path = device.to_string_lossy().to_string();
    let mut port = tokio_serial::new(path, baud).open_native_async()?;
    set_serial_cloexec(&port)?;
    for frame in frames {
        port.write_all(frame).await?;
    }
    port.flush().await?;
    Ok(())
}
