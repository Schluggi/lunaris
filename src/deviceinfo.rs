//! Pod host files mirrored to MQTT state topics of the same path string (`/deviceinfo/…`).
//!
//! SPDX-License-Identifier: GPL-3.0-only

/// Same path as the MQTT state topic [`crate::mqtt_bridge`].
pub const DEVICE_LABEL_PATH: &str = "/deviceinfo/device-label";
/// Same path as the MQTT state topic [`crate::mqtt_bridge`].
pub const DEVICE_ID_PATH: &str = "/deviceinfo/device-id";

fn trim_payload(mut v: Vec<u8>) -> Vec<u8> {
    while matches!(
        v.last().copied(),
        Some(b'\n' | b'\r' | b' ' | b'\t') | Some(0)
    ) {
        v.pop();
    }
    while matches!(v.first().copied(), Some(b'\n' | b'\r' | b' ' | b'\t')) {
        v.remove(0);
    }
    v
}

/// Raw file bytes for MQTT publish (empty if missing or unreadable).
pub fn read_deviceinfo_file(path: &str) -> Vec<u8> {
    match std::fs::read(path) {
        Ok(b) => trim_payload(b),
        Err(e) => {
            tracing::trace!(path, error = %e, "deviceinfo file not read — publishing empty state");
            Vec::new()
        }
    }
}

/// Label and ID payloads read from the host paths [`DEVICE_LABEL_PATH`] / [`DEVICE_ID_PATH`].
pub fn device_label_and_id_payloads() -> (Vec<u8>, Vec<u8>) {
    (
        read_deviceinfo_file(DEVICE_LABEL_PATH),
        read_deviceinfo_file(DEVICE_ID_PATH),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_payload_strips_crlf_and_null() {
        assert_eq!(trim_payload(b"pod\n\r\0".to_vec()), b"pod");
        assert_eq!(trim_payload(b"\n  x\t".to_vec()), b"x");
    }

    #[test]
    fn read_deviceinfo_file_trims() {
        let p = std::env::temp_dir().join("narcolepsy_deviceinfo_test_label");
        std::fs::write(&p, b"my-id\n").unwrap();
        let got = read_deviceinfo_file(p.to_str().unwrap());
        assert_eq!(got, b"my-id");
        let _ = std::fs::remove_file(&p);
    }
}
