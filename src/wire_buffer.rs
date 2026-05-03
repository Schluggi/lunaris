//! Rules for buffering incoming **`0x7E`**-framed streams across partial [`tokio::io::AsyncRead`] chunks.
//!
//! SPDX-License-Identifier: GPL-3.0-only

/// Bytes retained when no sync byte has appeared yet — avoids dropping a frame split across reads.
pub const UNSYNCED_TAIL_KEEP: usize = 512;

/// If there is still no `0x7E`, **do not** `clear()` the whole buffer (that drops partial frames).
/// Instead drop only older noise once the buffer grows large.
#[inline]
pub fn trim_when_no_sync_byte(buffer: &mut Vec<u8>) {
    if buffer.len() > UNSYNCED_TAIL_KEEP {
        let drop = buffer.len() - UNSYNCED_TAIL_KEEP;
        buffer.drain(..drop);
        tracing::trace!(
            kept = buffer.len(),
            "RX trim (no 0x7E sync yet; kept tail for reassembly)"
        );
    }
}
