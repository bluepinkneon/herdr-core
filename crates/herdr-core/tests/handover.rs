//! A service owner starts the replacement itself; no detached successor or
//! return to its caller between the old restart and adoption.
#![cfg(unix)]

mod common;

use common::*;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn target(s: &Session, old: &Value) -> Command {
    let mut cmd = s.program(BIN);
    cmd.args([
        "serve",
        "--handover-core-id",
        old["core"]["id"].as_str().unwrap(),
        "--handover-pid",
        &old["pid"].to_string(),
        "--handover-version",
        old["version"].as_str().unwrap(),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::piped());
    cmd
}

// The endpoint can disappear between the old response and the new listener.
// Readiness reads never autostart or retry the mutating restart request.
fn observed_status(s: &Session) -> Option<Value> {
    let mut socket = UnixStream::connect(s.socket()).ok()?;
    socket.set_read_timeout(Some(Duration::from_millis(100))).ok()?;
    writeln!(socket, "{}", json!({"jsonrpc":"2.0", "id":1, "method":"server.status", "params":{}})).ok()?;
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line).ok()?;
    let reply: Value = serde_json::from_str(&line).ok()?;
    reply.get("result").cloned()
}

struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
        }
        self.0.wait().unwrap();
    }
}

#[test]
fn replacement_serves_in_its_owner_pid_and_preserves_programs_without_caller_progress() {
    let mut s = Session::new();
    // Optional exact old release; the normal suite also exercises the same path.
    if let Some(bin) = std::env::var_os("HC_OLD_BIN") {
        s.bin = bin.into();
    }
    let mut old_process = Owned(s.cmd().arg("serve").stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    wait_until("old core socket", || s.connect_ok());
    let old = s.connect().call(1, "server.status", json!({}))["result"].clone();
    let terminal = run(&s, &["sh", "-c", "echo retained; while read line; do echo got-$line; done"]);
    wait_until("original output", || text(&s, &terminal).contains("retained"));
    let program = get(&s, &terminal);
    let document = json!({"unknown": "preserve-this-original", "session": "same"});
    let set = s.connect().call(2, "doc.set", json!({"name": "handover", "value": document, "version": 0}));
    assert!(set.get("error").is_none(), "{set}");

    let replacement = Owned(target(&s, &old).spawn().unwrap());
    // No caller-side restart, readiness worker, old reap, or target start follows.
    // Beyond the stock holder's 3-second grace, the replacement must still own it.
    std::thread::sleep(Duration::from_secs(4));
    let status = s.connect().call(3, "server.status", json!({}))["result"].clone();
    assert_eq!(status["pid"], replacement.0.id(), "target main PID serves; no daemonized child");
    assert_eq!(status["core"]["id"], old["core"]["id"]);
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert!(old_process.0.try_wait().unwrap().unwrap().success(), "old process exited, not merely unlocked");
    assert_eq!(get(&s, &terminal)["pid"], program["pid"], "same live program");
    assert_eq!(get(&s, &terminal)["state"], "live");
    assert!(text(&s, &terminal).contains("retained"));
    assert_eq!(s.connect().call(4, "doc.get", json!({"name": "handover"}))["result"]["value"], document);
    s.json(&["send", &terminal, "after"]);
    wait_until("same program accepts input", || text(&s, &terminal).contains("got-after"));
    s.connect().call(5, "server.stop", json!({}));
    drop(replacement);
}

#[test]
fn mismatched_identity_birth_or_state_refuses_without_restarting_the_old_core() {
    let s = Session::new();
    let old = s.json(&["server", "status"]);
    for (field, value) in
        [("pid", json!(std::process::id())), ("version", json!("wrong")), ("core", json!({"id": "wrong"}))]
    {
        let mut bad = old.clone();
        bad[field] = value;
        let out = target(&s, &bad).output().unwrap();
        assert!(!out.status.success(), "{field} must refuse");
        assert_eq!(s.connect().call(1, "server.status", json!({}))["result"]["pid"], old["pid"]);
    }
    let out = target(&s, &old).args(["--handover-birth", "wrong-birth"]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("birth mismatch"));
    // Correct socket with another state root must not retire it and initialize a new identity.
    let other = Session::new();
    let out = target(&other, &old).env("HC_SOCKET", s.socket()).output().unwrap();
    assert!(!out.status.success());
    assert_eq!(s.connect().call(2, "server.status", json!({}))["result"]["pid"], old["pid"]);
    assert!(!other.root.join("state/herdr-core/test/core.json").exists());
    let out = s.cmd().args(["serve", "--handover-pid", &old["pid"].to_string()]).output().unwrap();
    assert!(!out.status.success(), "partial flags cannot become ordinary serve");
}

#[test]
fn lost_or_mismatched_restart_response_is_held_once_without_serving() {
    for response in [
        None,
        Some(json!({"jsonrpc":"2.0", "id":1, "result":{"stopped":true}})),
        Some(json!({"jsonrpc":"2.0", "id":2, "result":{"stopped":false}})),
    ] {
        let s = Session::new();
        let state = s.root.join("state/herdr-core/test");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::create_dir_all(s.socket().parent().unwrap()).unwrap();
        let identity = br#"{"id":"0123456789abcdef0123456789abcdef","name":"private"}"#;
        std::fs::write(state.join("core.json"), identity).unwrap();
        let lock = std::fs::File::create(state.join("core.lock")).unwrap();
        (&lock).write_all(s.socket().as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
        let listener = UnixListener::bind(s.socket()).unwrap();
        let old =
            json!({"pid":std::process::id(), "core":{"id":"0123456789abcdef0123456789abcdef"}, "version":"private"});
        let mut launched = target(&s, &old).spawn().unwrap();
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut read = BufReader::new(socket.try_clone().unwrap());
        let mut line = String::new();
        read.read_line(&mut line).unwrap();
        let status: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(status["method"], "server.status");
        writeln!(socket, "{}", json!({"jsonrpc":"2.0", "id":status["id"], "result":old})).unwrap();
        line.clear();
        read.read_line(&mut line).unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["method"], "server.restart");
        assert_eq!(request["id"], 2);
        if let Some(response) = response {
            writeln!(socket, "{response}").unwrap();
        }
        drop(read);
        drop(socket);
        let mut error = String::new();
        launched.stderr.take().unwrap().read_to_string(&mut error).unwrap();
        assert!(!launched.wait().unwrap().success(), "{error}");
        assert!(error.contains("no retry"), "{error}");
        listener.set_nonblocking(true).unwrap();
        assert!(
            matches!(listener.accept(), Err(e) if e.kind()==std::io::ErrorKind::WouldBlock),
            "no second connection/replay"
        );
        assert_eq!(std::fs::read(state.join("core.json")).unwrap(), identity);
        assert!(!state.join("terminals.json").exists(), "no adoption/state initialization");
        // The fake responder is gone before the Session's ordinary isolated cleanup.
        drop(listener);
        drop(lock);
        std::fs::remove_file(s.socket()).unwrap();
    }
}

#[test]
fn unlocked_but_live_predecessor_cannot_be_adopted_until_kernel_exit() {
    let s = Session::new();
    let code = r#"
import os,sys,json,socket,fcntl
state=os.environ['XDG_STATE_HOME']+'/herdr-core/test'
path=os.environ['XDG_RUNTIME_DIR']+'/herdr-core/test.sock'
os.makedirs(state); os.makedirs(os.path.dirname(path))
identity={'id':'0123456789abcdef0123456789abcdef','name':'private'}
open(state+'/core.json','w').write(json.dumps(identity))
lock=open(state+'/core.lock','w'); lock.write(path); lock.flush()
fcntl.flock(lock,fcntl.LOCK_EX)
listener=socket.socket(socket.AF_UNIX); listener.bind(path); listener.listen()
status={'core':identity,'pid':os.getpid(),'version':'private'}
print(json.dumps(status),flush=True)
connection,_=listener.accept(); reader=connection.makefile('rb')
request=json.loads(reader.readline())
assert request['method']=='server.status'
connection.sendall((json.dumps({'jsonrpc':'2.0','id':request['id'],'result':status})+'\n').encode())
request=json.loads(reader.readline())
assert request['method']=='server.restart'
connection.sendall((json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{'stopped':True}})+'\n').encode())
lock.close()
print('unlocked-but-alive',flush=True)
assert sys.stdin.readline()=='exit\n'
"#;
    let mut old =
        Owned(s.program("python3").args(["-c", code]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap());
    let mut events = BufReader::new(old.0.stdout.take().unwrap());
    let mut line = String::new();
    events.read_line(&mut line).unwrap();
    let status: Value = serde_json::from_str(&line).unwrap();
    let mut replacement = Owned(target(&s, &status).spawn().unwrap());
    line.clear();
    events.read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "unlocked-but-alive");
    let socket = std::fs::metadata(s.socket()).unwrap();
    let lock = std::fs::File::open(s.root.join("state/herdr-core/test/core.lock")).unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0, "old lock is free");
    // A lock-only launcher would already have failed or replaced the socket.
    std::thread::sleep(Duration::from_millis(100));
    assert!(replacement.0.try_wait().unwrap().is_none());
    use std::os::unix::fs::MetadataExt;
    assert_eq!(std::fs::metadata(s.socket()).unwrap().ino(), socket.ino());
    assert!(old.0.try_wait().unwrap().is_none());
    drop(lock);
    old.0.stdin.take().unwrap().write_all(b"exit\n").unwrap();
    // Wait for its kernel exit without reaping it on behalf of the native launcher.
    wait_until("target serves after old exit", || {
        observed_status(&s).is_some_and(|status| status["pid"] == replacement.0.id())
    });
    assert!(old.0.wait().unwrap().success());
    s.connect().call(4, "server.stop", json!({}));
}
