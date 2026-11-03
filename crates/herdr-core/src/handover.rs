//! Foreground service-owner handover. No detached successor and no RPC replay:
//! the process selected by the OS owner is the process that ultimately serves.

use crate::{paths::Paths, platform, server};
use serde_json::{Value, json};
use std::io::{self, BufRead, BufReader, Read, Write};

#[derive(clap::Args)]
pub(crate) struct ServeArgs {
    /// Exact existing core identity; all three handover fields are required together.
    #[arg(long, requires_all = ["handover_pid", "handover_version"])]
    handover_core_id: Option<String>,
    #[arg(long, requires = "handover_core_id", value_parser = clap::value_parser!(u32).range(1..=i32::MAX as i64))]
    handover_pid: Option<u32>,
    #[arg(long, requires = "handover_core_id")]
    handover_version: Option<String>,
    /// Optional kernel birth token: Linux starttime ticks; Darwin start seconds:microseconds.
    #[arg(long, requires = "handover_core_id")]
    handover_birth: Option<String>,
}

pub(crate) fn serve(args: ServeArgs) -> io::Result<()> {
    let Some(expected) = args.handover_core_id else { return server::serve() };
    let pid = args.handover_pid.expect("clap requires the pid");
    let version = args.handover_version.expect("clap requires the version");
    let paths = Paths::from_env().map_err(|e| io::Error::other(e.message))?;
    let identity_path = paths.state.join("core.json");
    let identity_bytes = std::fs::read(&identity_path)?;
    let info = server::handover_identity(&identity_bytes, &expected)?;
    let lock_path = paths.state.join("core.lock");
    let mut lock = platform::handover::open_lock(&lock_path)?;
    let mut socket = Vec::new();
    (&mut lock).take(4096).read_to_end(&mut socket)?;
    if socket != paths.socket.as_os_str().as_encoded_bytes() {
        return Err(io::Error::other("handover lock names a different socket"));
    }
    if platform::try_lock(&lock) {
        return Err(io::Error::other("handover predecessor no longer holds the session lock"));
    }
    let stream = platform::connect(&paths.socket)?;
    let process = platform::handover::Predecessor::pin(&stream, pid, args.handover_birth.as_deref())?;
    let mut reader = BufReader::new(&stream);
    let status = call(&stream, &mut reader, 1, "server.status")?;
    if status["core"]["id"] != expected || status["pid"] != pid || status["version"] != version {
        return Err(io::Error::other("handover predecessor core/PID/version mismatch; restart not sent"));
    }
    platform::handover::same_lock(&lock, &lock_path)?;
    if std::fs::read(&identity_path)? != identity_bytes {
        return Err(io::Error::other("handover saved identity changed; restart not sent"));
    }
    // The socket still names the process pinned above. No reconnect, retry,
    // CLI autostart, or status-based substitution if its response is lost.
    let reply = call(&stream, &mut reader, 2, "server.restart")
        .map_err(|e| io::Error::other(format!("handover restart outcome unknown; no retry: {e}")))?;
    if reply["stopped"] != true {
        return Err(io::Error::other("handover restart not acknowledged; no retry"));
    }
    drop(reader);
    drop(stream);
    process.wait()?;
    // Any competing owner holds instead of being adopted or restarted. This
    // exact file handle stays locked through Core::start and the serving loop.
    if !platform::try_lock(&lock) {
        return Err(io::Error::other("handover adoption held: another process owns the session lock"));
    }
    platform::handover::same_lock(&lock, &lock_path)?;
    if std::fs::read(identity_path)? != identity_bytes {
        return Err(io::Error::other("handover adoption held: saved identity changed"));
    }
    server::serve_locked(paths, lock, info)
}

/// Bounded, correlated replies on the one existing connection. A malformed,
/// missing or mismatched result never becomes permission to start a core.
fn call(
    stream: &platform::Stream,
    reader: &mut BufReader<&platform::Stream>,
    id: u64,
    method: &str,
) -> io::Result<Value> {
    writeln!(&*stream, "{}", json!({"jsonrpc": "2.0", "id": id, "method": method, "params": {}}))?;
    let mut bytes = Vec::new();
    reader.take(65_537).read_until(b'\n', &mut bytes)?;
    if bytes.len() > 65_536 || bytes.last() != Some(&b'\n') {
        return Err(io::Error::other("missing or oversized handover response"));
    }
    let reply: Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if reply["jsonrpc"] != "2.0" || reply["id"] != id || reply.get("error").is_some() {
        return Err(io::Error::other("handover response does not match the request or reports failure"));
    }
    reply.get("result").cloned().ok_or_else(|| io::Error::other("handover response has no result"))
}
