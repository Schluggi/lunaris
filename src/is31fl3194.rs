//! IS31FL3194 3-channel LED driver over Linux I²C (`/dev/i2c-*`).
//! Register sequence derived from [opensleep](https://github.com/LiamSnow/opensleep)
//! `src/led/controller.rs` and `src/led/model.rs` (GPL-3.0). Datasheet: Lumissil IS31FL3194.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::path::Path;

use i2cdev::core::I2CDevice;
use i2cdev::linux::LinuxI2CDevice;
use thiserror::Error;

const I2C_ADDR: u16 = 0x53;

#[derive(Debug, Error)]
pub enum Is31Error {
    #[error("I²C: {0}")]
    I2C(#[from] i2cdev::linux::LinuxI2CError),
}

/// Open the bus and verify the adapter is usable (does not require the chip to respond).
pub fn probe(i2c_dev: &Path) -> Result<(), Is31Error> {
    let _dev = LinuxI2CDevice::new(i2c_dev, I2C_ADDR)?;
    Ok(())
}

/// Turn off all LED outputs (e.g. when narcolepsy exits).
pub fn shutdown_led(i2c_dev: &Path) -> Result<(), Is31Error> {
    let mut dev = Is31fl3194::open(i2c_dev)?;
    dev.set_solid_rgb(false, 0, 0, 0)
}

pub struct Is31fl3194 {
    dev: LinuxI2CDevice,
}

impl Is31fl3194 {
    pub fn open(i2c_dev: &Path) -> Result<Self, Is31Error> {
        let dev = LinuxI2CDevice::new(i2c_dev, I2C_ADDR)?;
        Ok(Self { dev })
    }

    fn write_reg(&mut self, reg: u8, value: u8) -> Result<(), Is31Error> {
        self.dev.write(&[reg, value])?;
        Ok(())
    }

    /// Solid RGB in “current level” mode, `band` = max 30 mA (opensleep default).
    /// `enabled == false` turns outputs off.
    pub fn set_solid_rgb(&mut self, enabled: bool, r: u8, g: u8, b: u8) -> Result<(), Is31Error> {
        // OperatingMode::CurrentLevel
        const REG_OP_CONFIG: u8 = 0x01;
        let out_mode = 0b000u8;
        let led_mode = 0b00u8; // single-channel mode per channel
        self.write_reg(
            REG_OP_CONFIG,
            (out_mode << 4) | (led_mode << 1) | 0b1, // normal operation
        )?;

        // CurrentBand::Three = 0b10
        const REG_CURRENT_BAND: u8 = 0x03;
        let band = 0b10u8;
        self.write_reg(REG_CURRENT_BAND, (band << 4) | (band << 2) | band)?;

        const REG_OUT_CONFIG: u8 = 0x02;
        self.write_reg(
            REG_OUT_CONFIG,
            ((enabled as u8) << 2) | ((enabled as u8) << 1) | (enabled as u8),
        )?;

        if enabled {
            // PCB wiring: BRG order at registers (opensleep `current_level`)
            const REG_B_CURRENT_LEVEL: u8 = 0x10;
            const REG_R_CURRENT_LEVEL: u8 = 0x21;
            const REG_G_CURRENT_LEVEL: u8 = 0x32;
            self.write_reg(REG_R_CURRENT_LEVEL, r)?;
            self.write_reg(REG_G_CURRENT_LEVEL, g)?;
            self.write_reg(REG_B_CURRENT_LEVEL, b)?;
        } else {
            const REG_B_CURRENT_LEVEL: u8 = 0x10;
            const REG_R_CURRENT_LEVEL: u8 = 0x21;
            const REG_G_CURRENT_LEVEL: u8 = 0x32;
            self.write_reg(REG_R_CURRENT_LEVEL, 0)?;
            self.write_reg(REG_G_CURRENT_LEVEL, 0)?;
            self.write_reg(REG_B_CURRENT_LEVEL, 0)?;
        }

        const REG_COLOR_UPDATE: u8 = 0x40;
        const UPDATE_VALUE: u8 = 0xC5;
        self.write_reg(REG_COLOR_UPDATE, UPDATE_VALUE)?;
        Ok(())
    }
}
