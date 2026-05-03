//! Frozen USART framing and CRC, compatible with [opensleep](https://github.com/LiamSnow/opensleep)
//! (`src/common/checksum.rs`, `src/common/codec.rs`, `src/frozen/command.rs`).
//! SPDX-License-Identifier: GPL-3.0-only

const CRC_START: u16 = 0x1D0F;
const CRC_POLY_CCITT: u16 = 0x1021;
const CRC_TABLE: [u16; 256] = make_crc_table();
const START: u8 = 0x7E;

const fn make_crc_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = 0u16;
        let mut c = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if (crc ^ c) & 0x8000 != 0 {
                crc = (crc << 1) ^ CRC_POLY_CCITT;
            } else {
                crc <<= 1;
            }
            c <<= 1;
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// CRC-CCITT as used on the Pod Frozen link (opensleep `checksum::compute`).
pub const fn crc_ccitt(input: &[u8]) -> u16 {
    let mut crc = CRC_START;
    let mut i = 0;
    while i < input.len() {
        let byte = input[i];
        let index = ((crc >> 8) ^ (byte as u16)) & 0x00FF;
        crc = (crc << 8) ^ CRC_TABLE[index as usize];
        i += 1;
    }
    crc
}

/// Wraps a Frozen payload with `0x7E` start, length byte, and big-endian CRC.
pub fn encode_command(payload: &[u8]) -> Vec<u8> {
    assert!(
        payload.len() <= u8::MAX as usize,
        "Frozen payload length must fit in one byte"
    );
    let checksum = crc_ccitt(payload);
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.push(START);
    out.push(payload.len() as u8);
    out.extend_from_slice(payload);
    out.push((checksum >> 8) as u8);
    out.push(checksum as u8);
    out
}

/// Prime command: raw payload byte `0x52` (opensleep `FrozenCommand::Prime`).
pub fn prime_frame() -> Vec<u8> {
    encode_command(&[0x52])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_examples_match_opensleep_tests() {
        assert_eq!(crc_ccitt(&[0x40, 0x00, 0x01, 0x0E, 0x10]), 0xE6A8);
        // Payload for Prime only; CRC appears as BE bytes `B6 2B` in `7E 01 52 B6 2B`.
        assert_eq!(crc_ccitt(&[0x52]), 0xB62B);
    }

    #[test]
    fn prime_frame_matches_opensleep_hex() {
        assert_eq!(prime_frame(), vec![0x7E, 0x01, 0x52, 0xB6, 0x2B]);
    }
}
