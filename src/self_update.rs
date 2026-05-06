//! Self-update: GitHub latest release + replace running binary (Unix).
//!
//! HTTPS uses **native TLS without certificate verification** (Eight Sleep Pod often has no CA
//! store). Release integrity is checked with the published **SHA256SUMS** file (see
//! [`.github/workflows/release.yml`](../../.github/workflows/release.yml)). A network MITM that
//! controls TLS can still substitute both files; this trade-off is intentional for the target.
//!
//! SPDX-License-Identifier: GPL-3.0-only

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const PAYLOAD_INSTALL: &str = "INSTALL";

/// Right before `exec`, mark every open `/dev/tty*` fd close-on-exec so the new image can open
/// UARTs again (covers fds where earlier `set_serial_cloexec` did not stick).
#[cfg(target_os = "linux")]
fn set_cloexec_on_open_tty_fds() {
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return;
    };
    for e in entries.flatten() {
        let fname = e.file_name();
        let Some(s) = fname.to_str() else {
            continue;
        };
        let Ok(fd) = s.parse::<libc::c_int>() else {
            continue;
        };
        if fd < 3 {
            continue;
        }
        let ep = e.path();
        let Ok(target) = std::fs::read_link(&ep) else {
            continue;
        };
        if !target.to_string_lossy().starts_with("/dev/tty") {
            continue;
        }
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            continue;
        }
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cloexec_on_open_tty_fds() {}

/// Re-`exec` this binary with the same argv (MQTT **Restart Lunaris**). On success the process is replaced
/// and this function does not return.
#[cfg(unix)]
pub(crate) fn restart_current_exe_blocking() -> Result<(), String> {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let argv_rest: Vec<String> = std::env::args().skip(1).collect();
    set_cloexec_on_open_tty_fds();
    let err = Command::new(&exe).args(&argv_rest).exec();
    Err(err.to_string())
}

#[cfg(not(unix))]
pub(crate) fn restart_current_exe_blocking() -> Result<(), String> {
    Err("lunaris restart is only supported on Unix".into())
}

const GITHUB_API_LATEST: &str = "https://api.github.com/repos/Schluggi/lunaris/releases/latest";
const ASSET_LUNARIS: &str = "lunaris";
const ASSET_SHA256SUMS: &str = "SHA256SUMS";

pub fn installed_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

fn user_agent() -> String {
    format!("lunaris/{}", installed_version())
}

/// `Agent` with TLS server certificate verification **disabled** (no system CA required).
static INSECURE_AGENT: OnceLock<Result<ureq::Agent, String>> = OnceLock::new();

fn insecure_agent() -> Result<&'static ureq::Agent, FetchError> {
    let slot = INSECURE_AGENT.get_or_init(|| {
        let connector = match native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
        {
            Ok(c) => c,
            Err(e) => return Err(format!("native_tls: {e}")),
        };
        Ok(ureq::builder()
            .tls_connector(std::sync::Arc::new(connector))
            .build())
    });
    match slot {
        Ok(a) => Ok(a),
        Err(e) => Err(FetchError::Tls(e.clone())),
    }
}

/// Parse semver from a path like `/…/releases/download/v1.2.3/lunaris` (diagnostics / tests).
#[allow(dead_code)] // unit tests; optional diagnostics — release binary uses GitHub API JSON only
pub fn parse_version_from_release_path(path: &str) -> Option<String> {
    const NEEDLE: &str = "/releases/download/";
    let idx = path.find(NEEDLE)? + NEEDLE.len();
    let rest = path.get(idx..)?;
    let tag = rest.split('/').next()?;
    let v = tag.strip_prefix('v').unwrap_or(tag);
    if v.is_empty() {
        return None;
    }
    if !v
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'))
    {
        return None;
    }
    Some(v.to_string())
}

/// Parse from full URL or path containing `/releases/download/`.
#[allow(dead_code)]
pub fn parse_version_from_release_asset_url(url: &str) -> Option<String> {
    parse_version_from_release_path(url)
}

#[derive(Debug, Clone)]
struct LatestRelease {
    tag_name: String,
    lunaris_url: String,
    sha256sums_url: String,
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("TLS: {0}")]
    Tls(String),
    #[error("HTTP: {0}")]
    Http(String),
    #[error("GitHub API: missing tag_name")]
    MissingTag,
    #[error("GitHub API: missing release asset {0:?}")]
    MissingAsset(String),
}

fn fetch_latest_release(agent: &ureq::Agent) -> Result<LatestRelease, FetchError> {
    let resp = agent
        .get(GITHUB_API_LATEST)
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", &user_agent())
        .call()
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let v: Value = resp
        .into_json()
        .map_err(|e| FetchError::Http(e.to_string()))?;
    let tag_name = v["tag_name"]
        .as_str()
        .ok_or(FetchError::MissingTag)?
        .to_string();
    let assets = v["assets"]
        .as_array()
        .ok_or_else(|| FetchError::Http("GitHub API: assets is not an array".into()))?;
    let lunaris_url = asset_download_url(assets, ASSET_LUNARIS)
        .ok_or_else(|| FetchError::MissingAsset(ASSET_LUNARIS.into()))?;
    let sha256sums_url = asset_download_url(assets, ASSET_SHA256SUMS)
        .ok_or_else(|| FetchError::MissingAsset(ASSET_SHA256SUMS.into()))?;
    Ok(LatestRelease {
        tag_name,
        lunaris_url,
        sha256sums_url,
    })
}

fn asset_download_url(assets: &[Value], name: &str) -> Option<String> {
    for a in assets {
        let n = a.get("name")?.as_str()?;
        if n == name {
            return a.get("browser_download_url")?.as_str().map(String::from);
        }
    }
    None
}

pub fn fetch_latest_version_blocking() -> Result<String, FetchError> {
    let agent = insecure_agent()?;
    let rel = fetch_latest_release(agent)?;
    Ok(rel
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&rel.tag_name)
        .to_string())
}

pub fn update_state_json(
    installed: &str,
    latest: Option<&str>,
    in_progress: bool,
    update_percentage: Option<f64>,
) -> String {
    let mut m = serde_json::Map::new();
    m.insert("installed_version".to_string(), json!(installed));
    if let Some(l) = latest {
        m.insert("latest_version".to_string(), json!(l));
        m.insert(
            "release_url".to_string(),
            json!(format!(
                "https://github.com/Schluggi/lunaris/releases/tag/v{l}"
            )),
        );
    }
    m.insert("title".to_string(), json!("Lunaris"));
    m.insert("in_progress".to_string(), json!(in_progress));
    if let Some(p) = update_percentage {
        m.insert("update_percentage".to_string(), json!(p));
    }
    serde_json::Value::Object(m).to_string()
}

fn elf_e_machine_from_prefix(buf: &[u8]) -> io::Result<u16> {
    if buf.len() < 20 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated ELF header",
        ));
    }
    if &buf[0..4] != b"\x7fELF" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an ELF binary",
        ));
    }
    Ok(u16::from_le_bytes([buf[18], buf[19]]))
}

fn elf_e_machine_from_path(path: &Path) -> io::Result<u16> {
    let mut f = std::fs::File::open(path)?;
    let mut hdr = [0u8; 64];
    f.read_exact(&mut hdr)?;
    elf_e_machine_from_prefix(&hdr)
}

#[derive(Debug, Error)]
pub enum InstallError {
    /// Only returned on non-Unix when install is attempted.
    #[allow(dead_code)]
    #[error("self-update is only supported on Unix")]
    Unsupported,
    #[error("TLS / HTTP: {0}")]
    Http(String),
    #[error("release fetch: {0}")]
    Fetch(#[from] FetchError),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("current executable path unavailable")]
    NoExePath,
    #[error("ELF architecture mismatch (running e_machine={current:#x}, download={new:#x})")]
    ArchMismatch { current: u16, new: u16 },
    #[error("SHA256 mismatch (expected {expected}, actual {actual})")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("SHA256SUMS: no entry for file name {0:?}")]
    MissingChecksumEntry(String),
    #[error("SHA256SUMS: invalid UTF-8")]
    BadChecksumsUtf8,
    #[error("SHA256SUMS: invalid format or hash")]
    BadChecksumsFormat,
    #[error("exec: {0}")]
    Exec(String),
}

fn download_bytes(agent: &ureq::Agent, url: &str) -> Result<Vec<u8>, InstallError> {
    let resp = agent
        .get(url)
        .set("User-Agent", &user_agent())
        .call()
        .map_err(|e| InstallError::Http(e.to_string()))?;
    let mut buf = Vec::new();
    resp.into_reader()
        .read_to_end(&mut buf)
        .map_err(|e| InstallError::Http(e.to_string()))?;
    Ok(buf)
}

/// Parse GNU `sha256sum` output; match lines whose path basename equals `basename`.
pub fn expected_sha256_from_sums_file(
    content: &str,
    basename: &str,
) -> Result<[u8; 32], InstallError> {
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let hex = match parts.next() {
            Some(h) if h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit()) => h,
            _ => continue,
        };
        let path_str = parts.collect::<Vec<_>>().join(" ");
        if path_str.is_empty() {
            continue;
        }
        let path = Path::new(path_str.trim_start_matches('*'));
        if path.file_name().and_then(|s| s.to_str()) != Some(basename) {
            continue;
        }
        return hex_decode_32(hex).ok_or(InstallError::BadChecksumsFormat);
    }
    Err(InstallError::MissingChecksumEntry(basename.into()))
}

fn hex_decode_32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
        let hi = char::from(chunk[0]).to_digit(16)?;
        let lo = char::from(chunk[1]).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

/// Download release binary, verify SHA256 + ELF `e_machine`, chmod, atomic replace, [`exec`].
#[cfg(unix)]
pub fn perform_install_blocking() -> Result<(), InstallError> {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let agent = insecure_agent()?;
    let rel = fetch_latest_release(agent)?;

    let sums_bytes = download_bytes(agent, &rel.sha256sums_url)?;
    let sums_text = std::str::from_utf8(&sums_bytes).map_err(|_| InstallError::BadChecksumsUtf8)?;
    let expected = expected_sha256_from_sums_file(sums_text, ASSET_LUNARIS)?;

    let bin_bytes = download_bytes(agent, &rel.lunaris_url)?;
    let actual = Sha256::digest(&bin_bytes);
    if actual.as_slice() != expected.as_slice() {
        return Err(InstallError::ChecksumMismatch {
            expected: hex_lower(&expected),
            actual: hex_lower(actual.as_slice()),
        });
    }

    let exe = std::env::current_exe().map_err(|_| InstallError::NoExePath)?;
    let dir = exe.parent().ok_or(InstallError::NoExePath)?;
    let temp: PathBuf = dir.join(format!(".lunaris.new.{}", std::process::id()));

    let cur_machine = elf_e_machine_from_path(&exe)?;

    let dl_machine = elf_e_machine_from_prefix(bin_bytes.as_ref()).map_err(|e| {
        InstallError::Io(io::Error::new(
            e.kind(),
            format!("downloaded file is not a valid ELF binary: {e}"),
        ))
    })?;
    if dl_machine != cur_machine {
        return Err(InstallError::ArchMismatch {
            current: cur_machine,
            new: dl_machine,
        });
    }

    std::fs::write(&temp, &bin_bytes)?;

    let mut perms = std::fs::metadata(&temp)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&temp, perms)?;

    let disk_machine = elf_e_machine_from_path(&temp)?;
    if disk_machine != cur_machine {
        let _ = std::fs::remove_file(&temp);
        return Err(InstallError::ArchMismatch {
            current: cur_machine,
            new: disk_machine,
        });
    }

    std::fs::rename(&temp, &exe)?;

    let argv_rest: Vec<String> = std::env::args().skip(1).collect();

    // Ensure no `/dev/tty*` handle is inherited across `exec` (Pod: second open in `main` would fail).
    set_cloexec_on_open_tty_fds();

    let err = Command::new(&exe).args(&argv_rest).exec();
    Err(InstallError::Exec(err.to_string()))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(not(unix))]
pub fn perform_install_blocking() -> Result<(), InstallError> {
    Err(InstallError::Unsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_redirect_style_urls() {
        let u = "https://github.com/Schluggi/lunaris/releases/download/v1.2.4/lunaris";
        assert_eq!(
            parse_version_from_release_asset_url(u).as_deref(),
            Some("1.2.4")
        );
    }

    #[test]
    fn parse_path_only() {
        let p = "/Schluggi/lunaris/releases/download/v0.9.0/lunaris";
        assert_eq!(parse_version_from_release_path(p).as_deref(), Some("0.9.0"));
    }

    #[test]
    fn sha256sums_matches_dist_lunaris_line() {
        let sample = "\
abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789  dist/lunaris\n\
";
        let h = expected_sha256_from_sums_file(sample, "lunaris").unwrap();
        assert_eq!(
            hex_lower(&h),
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn sha256sums_star_prefix_binary_mode() {
        let sample = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb *lunaris\n";
        let h = expected_sha256_from_sums_file(sample, "lunaris").unwrap();
        assert_eq!(
            hex_lower(&h),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn asset_download_url_finds_browser_link() {
        let assets: Vec<Value> = serde_json::from_str(
            r#"[
            {"name":"other","browser_download_url":"https://x/o"},
            {"name":"lunaris","browser_download_url":"https://dl/lunaris"},
            {"name":"SHA256SUMS","browser_download_url":"https://dl/sums"}
        ]"#,
        )
        .unwrap();
        assert_eq!(
            asset_download_url(&assets, "lunaris").as_deref(),
            Some("https://dl/lunaris")
        );
        assert_eq!(
            asset_download_url(&assets, "SHA256SUMS").as_deref(),
            Some("https://dl/sums")
        );
    }
}
