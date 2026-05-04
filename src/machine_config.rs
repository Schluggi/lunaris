//! Eight stock layout: USART paths under `/opt/eight/config/machine.json` (`frozenPort` / `sensorPort`).
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::path::PathBuf;

use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches};
use serde::Deserialize;

use crate::cli::Cli;

/// Path used on Pod OS (“Eight Layer”) for hardware port mapping.
pub const MACHINE_JSON_PATH: &str = "/opt/eight/config/machine.json";

#[derive(Debug, Deserialize)]
struct MachineJson {
    #[serde(rename = "frozenPort")]
    frozen_port: Option<String>,
    #[serde(rename = "sensorPort")]
    sensor_port: Option<String>,
}

fn trim_nonempty(s: &str) -> Option<&str> {
    let s = s.trim().trim_matches('\0');
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Merge `frozenPort` / `sensorPort` into `cli` when keys are usable.
/// Ports set on the **command line** (`--serial-device` / `--sensor-device`) are left unchanged.
fn apply_machine_json_ports(matches: &ArgMatches, cli: &mut Cli) {
    let Ok(raw) = std::fs::read_to_string(MACHINE_JSON_PATH) else {
        tracing::trace!(
            path = MACHINE_JSON_PATH,
            "machine.json not read — keeping CLI Frozen/Sensor defaults"
        );
        return;
    };
    let parsed: MachineJson = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                path = MACHINE_JSON_PATH,
                error = %e,
                "machine.json present but invalid — ignoring overrides"
            );
            return;
        }
    };

    let serial_explicit = matches.value_source("serial_device") == Some(ValueSource::CommandLine);
    let sensor_explicit = matches.value_source("sensor_device") == Some(ValueSource::CommandLine);

    if !serial_explicit {
        if let Some(s) = parsed.frozen_port.as_deref().and_then(trim_nonempty) {
            cli.serial_device = PathBuf::from(s);
            tracing::info!(
                path = MACHINE_JSON_PATH,
                frozen_port = %s,
                "Frozen serial path from machine.json"
            );
        }
    }

    if !sensor_explicit {
        if let Some(s) = parsed.sensor_port.as_deref().and_then(trim_nonempty) {
            cli.sensor_device = PathBuf::from(s);
            tracing::info!(
                path = MACHINE_JSON_PATH,
                sensor_port = %s,
                "Sensor serial path from machine.json"
            );
        }
    }
}

/// [`Cli::parse()`] merged with `/opt/eight/config/machine.json` where allowed.
pub fn parse_cli_overlay_machine_json() -> Cli {
    let matches = Cli::command().get_matches();
    let mut cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    apply_machine_json_ports(&matches, &mut cli);
    cli
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_eight_pod_example_keys() {
        let raw = r#"{"sensorPort": "/dev/ttyS2", "frozenPort": "/dev/ttyS1"}"#;
        let m: MachineJson = serde_json::from_str(raw).unwrap();
        assert_eq!(m.frozen_port.as_deref(), Some("/dev/ttyS1"));
        assert_eq!(m.sensor_port.as_deref(), Some("/dev/ttyS2"));
    }
}
