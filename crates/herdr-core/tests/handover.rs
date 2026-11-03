//! A service owner starts the replacement itself; no detached successor or
//! return to its caller between the old restart and adoption.
#![cfg(unix)]

mod common;

use common::framed::Framed;
use common::*;
use herdr_core::{holder_proto as proto, wire};
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

// The test has already stopped its exact Core and keeper. Close the retained
// private holder directly: ordinary CLI autostart must refuse external custody.
fn close_private_holder(s: &Session, keeper: i32) {
    let records = std::fs::read_dir(s.root.join("state/herdr-core/test/keepers")).unwrap();
    let record: Value = records
        .map(|entry| serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap())
        .find(|record| record["keeper"]["pid"] == keeper)
        .unwrap();
    let mut socket = UnixStream::connect(record["holder"].as_str().unwrap()).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    socket.write_all(&proto::json_frame(proto::HELLO, 0, &proto::Hello { version: proto::VERSION })).unwrap();
    let (_, _, welcome) = proto::read(&mut socket).unwrap().unwrap();
    let welcome: proto::Welcome = proto::parse(&welcome).unwrap();
    for _ in welcome.terminals {
        assert_eq!(proto::read(&mut socket).unwrap().unwrap().0, proto::TAIL);
    }
    socket.write_all(&proto::json_frame(proto::GOODBYE, 0, &proto::Goodbye::Shutdown)).unwrap();
    while let Some((kind, _, _)) = proto::read(&mut socket).unwrap() {
        if kind == proto::BYE {
            return;
        }
    }
    panic!("private holder did not acknowledge shutdown");
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
fn externally_owned_recovery_drains_stock_holder_while_the_owner_is_absent() {
    let mut s = Session::new();
    if let Some(bin) = std::env::var_os("HC_OLD_BIN") {
        s.bin = bin.into();
    }
    let old = s.json(&["server", "status"]);
    let trigger = s.root.join("trigger");
    let completed = s.root.join("drained");
    let effects = s.root.join("effects");
    let code = "import os,sys\nfifo,done,effects=sys.argv[1:]\nos.mkfifo(fifo)\nprint('original',flush=True)\nwith open(fifo) as f: f.read()\nfor n in range(140000): print('retained-output-%06d'%n)\nsys.stdout.flush()\nopen(done,'w').write('complete')\nfor line in sys.stdin:\n with open(effects,'a') as f: f.write(line)\n print('once-'+line.strip(),flush=True)";
    let terminal = run(
        &s,
        &[
            "python3",
            "-u",
            "-c",
            code,
            trigger.to_str().unwrap(),
            completed.to_str().unwrap(),
            effects.to_str().unwrap(),
        ],
    );
    wait_until("original program", || text(&s, &terminal).contains("original"));
    let program = get(&s, &terminal)["pid"].clone();
    let mut replacement = Owned(target(&s, &old).spawn().unwrap());
    wait_until("target owns session", || observed_status(&s).is_some_and(|v| v["pid"] == replacement.0.id()));
    replacement.0.kill().unwrap();
    replacement.0.wait().unwrap();
    // No Source progress or native restart request occurs in this interval.
    std::fs::OpenOptions::new().write(true).open(&trigger).unwrap().write_all(b"go").unwrap();
    std::thread::sleep(Duration::from_secs(4));
    assert!(observed_status(&s).is_none(), "old holder must not auto-launch any core");
    wait_until("holder drains beyond socket capacity without core", || completed.exists());
    let resumed = Owned(
        s.program(BIN)
            .args(["serve", "--external-owner"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    wait_until("same-PID external recovery", || observed_status(&s).is_some_and(|v| v["pid"] == resumed.0.id()));
    assert_eq!(get(&s, &terminal)["pid"], program);
    assert!(text(&s, &terminal).contains("retained-output-139999"), "native holder tail is the recovery source");
    let status = observed_status(&s).unwrap();
    let keeper = &status["holders"][0]["keeper"];
    assert!(keeper["pid"].as_u64().is_some());
    assert!(!keeper["birth"].as_str().unwrap().is_empty());
    s.bin = BIN.into();
    s.json(&["send", &terminal, "fresh"]);
    wait_until("fresh once-only input", || std::fs::read_to_string(&effects).is_ok_and(|v| v == "fresh\n"));
    s.connect().call(6, "server.stop", json!({}));
    drop(resumed);
}

#[test]
fn external_handover_preserves_saved_screen_and_rejects_unowned_keeper_attach() {
    let s = Session::new();
    let first = Owned(
        s.cmd().args(["serve", "--external-owner"]).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap(),
    );
    wait_until("external core", || s.connect_ok());
    let terminal = run(&s, &["sh", "-c", "echo saved-screen; while read line; do echo got-$line; done"]);
    wait_until("saved screen", || text(&s, &terminal).contains("saved-screen"));
    let before = observed_status(&s).unwrap();
    let keeper = &before["holders"][0]["keeper"];
    let mut intruder = UnixStream::connect(keeper["socket"].as_str().unwrap()).unwrap();
    intruder.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    intruder.write_all(b"A").unwrap();
    let mut byte = [0];
    assert_eq!(intruder.read(&mut byte).unwrap(), 0, "no descriptor cannot steal the holder");
    let competing = s.cmd().args(["serve", "--external-owner"]).output().unwrap();
    assert!(!competing.status.success(), "live owner must refuse without releasing its lock");
    assert_eq!(observed_status(&s).unwrap()["pid"], first.0.id());
    let second = Owned(target(&s, &before).spawn().unwrap());
    wait_until("planned external handover", || observed_status(&s).is_some_and(|v| v["pid"] == second.0.id()));
    let after = observed_status(&s).unwrap();
    assert_eq!(after["holders"][0]["keeper"], *keeper, "same keeper, not a new history owner");
    assert!(text(&s, &terminal).contains("saved-screen"));
    s.json(&["send", &terminal, "new"]);
    wait_until("one new input after planned handover", || text(&s, &terminal).contains("got-new"));
    s.connect().call(8, "server.stop", json!({}));
    drop(second);
}

#[test]
fn lost_keeper_fences_native_admission_without_finishing_or_killing_its_program() {
    let mut s = Session::new();
    if let Some(bin) = std::env::var_os("HC_OLD_BIN") {
        s.bin = bin.into();
    }
    let old = s.json(&["server", "status"]);
    let terminal = run(&s, &["sh", "-c", "echo preserved; while read line; do echo got-$line; done"]);
    wait_until("original program", || text(&s, &terminal).contains("preserved"));
    let program = get(&s, &terminal)["pid"].as_i64().unwrap() as i32;
    let mut replacement = Owned(target(&s, &old).spawn().unwrap());
    wait_until("target core", || observed_status(&s).is_some_and(|v| v["pid"] == replacement.0.id()));
    let status = observed_status(&s).unwrap();
    let keeper = status["holders"][0]["keeper"]["pid"].as_i64().unwrap() as i32;
    let state = s.root.join("state/herdr-core/test/terminals.json");
    let original = std::fs::read(&state).unwrap();
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGKILL) }, 0);
    wait_until("native reports keeper loss", || observed_status(&s).is_some_and(|v| v["keepers_ready"] == false));
    std::thread::sleep(Duration::from_secs(4));
    assert!(replacement.0.try_wait().unwrap().is_none(), "native keeps its lock; old stock cannot replace it");
    assert_eq!(observed_status(&s).unwrap()["pid"], replacement.0.id());
    assert!(
        s.connect().call(9, "terminal.send", json!({"terminal":terminal,"text":"must-not-run"})).get("error").is_some()
    );
    assert_eq!(unsafe { libc::kill(program, 0) }, 0, "no invented holder-lost program cleanup");
    assert_eq!(std::fs::read(state).unwrap(), original, "no fabricated finished record");
    let held = s.program(BIN).args(["serve", "--external-owner"]).output().unwrap();
    assert!(!held.status.success(), "missing recorded keeper is HOLD, never an automatic replacement");
    replacement.0.kill().unwrap();
    replacement.0.wait().unwrap();
    close_private_holder(&s, keeper);
}

#[test]
fn admitted_partial_and_queued_input_refuse_after_keeper_loss_even_on_another_live_link() {
    let mut s = Session::new();
    if let Some(bin) = std::env::var_os("HC_OLD_BIN") {
        s.bin = bin.into();
    }
    let old = s.json(&["server", "status"]);
    let templates = s.root.join("config/herdr-core/templates");
    std::fs::create_dir_all(&templates).unwrap();
    let launch = |name: &str| {
        let file = s.root.join(name);
        let command = format!(
            "python3 -u -c 'import os,tty; tty.setraw(0); print(\"ready\",flush=True); f=open(\"{}\",\"ab\",buffering=0); exec(\"while True: f.write(os.read(0,4096))\")'",
            file.display()
        );
        std::fs::write(
            templates.join(format!("{name}.md")),
            format!("---\npaste_gap_ms: 2000\ncommand: {command}\n---\n"),
        )
        .unwrap();
        let result = s.json(&["run", "--bg", name]);
        let terminal = result["address"].as_str().unwrap().to_string();
        wait_until("raw program ready", || text(&s, &terminal).contains("ready"));
        (terminal, file)
    };
    let (old_term, old_input) = launch("old-input");
    let mut replacement = Owned(target(&s, &old).spawn().unwrap());
    wait_until("target core", || observed_status(&s).is_some_and(|v| v["pid"] == replacement.0.id()));
    let lost = observed_status(&s).unwrap()["holders"][0]["keeper"]["pid"].as_i64().unwrap() as i32;
    let (new_term, new_input) = launch("new-input");
    let status = observed_status(&s).unwrap();
    let keepers: Vec<i32> =
        status["holders"].as_array().unwrap().iter().map(|h| h["keeper"]["pid"].as_i64().unwrap() as i32).collect();
    let mut conn = Framed::open(&s);
    for (id, method, params) in [
        (101, "terminal.send", json!({"terminal":old_term,"text":"paste"})),
        (102, "terminal.send", json!({"terminal":new_term,"text":"other"})),
    ] {
        conn.send(
            0,
            wire::DATA,
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string().as_bytes(),
        );
    }
    wait_until("both admitted pastes before Enter", || {
        std::fs::read(&old_input).unwrap_or_default() == b"paste"
            && std::fs::read(&new_input).unwrap_or_default() == b"other"
    });
    conn.send(
        0,
        wire::DATA,
        json!({"jsonrpc":"2.0","id":103,"method":"terminal.keys","params":{"terminal":old_term,"keys":["x"]}})
            .to_string()
            .as_bytes(),
    );
    assert!(conn.call("server.status", json!({})).get("error").is_none());
    assert_eq!(unsafe { libc::kill(lost, libc::SIGKILL) }, 0);
    wait_until("custody lost", || observed_status(&s).is_some_and(|v| v["keepers_ready"] == false));
    let mut replies = Vec::new();
    while replies.len() < 3 {
        let (channel, kind, bytes) =
            conn.stash.pop_front().or_else(|| conn.read(Duration::from_secs(8))).expect("input outcome");
        if channel == 0 && kind == wire::DATA {
            replies.push(serde_json::from_slice::<Value>(&bytes).unwrap());
        }
    }
    let old_bytes = std::fs::read(&old_input).unwrap();
    let new_bytes = std::fs::read(&new_input).unwrap();
    replacement.0.kill().unwrap();
    replacement.0.wait().unwrap();
    for keeper in keepers {
        if keeper != lost {
            assert_eq!(unsafe { libc::kill(keeper, libc::SIGKILL) }, 0);
        }
        close_private_holder(&s, keeper);
    }
    for reply in replies {
        assert!(reply.get("error").is_some(), "admitted input must not claim success after custody loss: {reply}");
        let hint = reply["error"]["data"]["hint"].as_str().unwrap();
        assert!(hint.contains(if reply["id"] == 103 { "nothing was typed" } else { "unknown" }), "{reply}");
    }
    assert_eq!(old_bytes, b"paste", "no Enter or queued key after loss");
    assert_eq!(new_bytes, b"other", "global custody fence covers a still-live link");
}

#[test]
fn legacy_holder_drains_output_while_native_input_is_under_backpressure() {
    let mut s = Session::new();
    if let Some(bin) = std::env::var_os("HC_OLD_BIN") {
        s.bin = bin.into();
    }
    let old = s.json(&["server", "status"]);
    let trigger = s.root.join("pressure-trigger");
    let done = s.root.join("pressure-done");
    let code = r#"
import os,sys,tty,threading,json
fifo,done=sys.argv[1:]
tty.setraw(0); os.mkfifo(fifo); print('ready-pressure',flush=True)
with open(fifo) as f: f.read()
def output():
 for _ in range(1024):
  data=b'output-pressure '*1024+b'\r\n'
  while data: data=data[os.write(1,data):]
writer=threading.Thread(target=output); writer.start()
total=0; valid=True
while total < 4*1024*1024:
 data=os.read(0,65536); total+=len(data); valid=valid and data==b'x'*len(data)
writer.join(); open(done,'w').write(json.dumps({'bytes':total,'valid':valid}))
while True: os.read(0,4096)
"#;
    let terminal = run(&s, &["python3", "-u", "-c", code, trigger.to_str().unwrap(), done.to_str().unwrap()]);
    wait_until("pressure program ready", || text(&s, &terminal).contains("ready-pressure"));
    let mut replacement = Owned(target(&s, &old).spawn().unwrap());
    wait_until("target core", || observed_status(&s).is_some_and(|v| v["pid"] == replacement.0.id()));
    let before = observed_status(&s).unwrap();
    let keeper = before["holders"][0]["keeper"]["pid"].as_i64().unwrap() as i32;
    let mut viewer = common::framed::Viewer::attach(&s, &terminal, 44, 46, json!({}));
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGSTOP) }, 0);
    for _ in 0..128 {
        viewer.typed(&vec![b'x'; 32768]);
    }
    assert!(viewer.conn.call("server.status", json!({})).get("error").is_none());
    assert_eq!(unsafe { libc::kill(replacement.0.id() as i32, libc::SIGSTOP) }, 0);
    std::fs::write(&trigger, b"go").unwrap();
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGCONT) }, 0);
    // Hold the output consumer long enough to fill both private socket paths.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(unsafe { libc::kill(replacement.0.id() as i32, libc::SIGCONT) }, 0);
    let started = std::time::Instant::now();
    while !done.exists() && started.elapsed() < Duration::from_secs(15) {
        std::thread::sleep(Duration::from_millis(20));
    }
    let receipt = std::fs::read(&done).ok().map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap());
    let after = observed_status(&s);
    drop(viewer);
    replacement.0.kill().unwrap();
    replacement.0.wait().unwrap();
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGKILL) }, 0);
    close_private_holder(&s, keeper);
    assert_eq!(
        receipt,
        Some(json!({"bytes":4*1024*1024,"valid":true})),
        "concurrent holder output and input must both settle"
    );
    assert_eq!(after.unwrap()["keepers_ready"], true, "pressure must not fabricate custody loss");
}

#[test]
fn future_start_waits_for_exact_keeper_settlement_and_preserves_external_mode() {
    let s = Session::new();
    let initial = s.json(&["server", "start", "--external-owner"]);
    assert_eq!(initial["external_owner"], true);
    let terminal = run(&s, &["sh", "-c", "echo awake; while read line; do echo got-$line; done"]);
    wait_until("original output", || text(&s, &terminal).contains("awake"));
    let status = observed_status(&s).unwrap();
    let keeper = status["holders"][0]["keeper"]["pid"].as_i64().unwrap() as i32;
    let core = status["pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGSTOP) }, 0);
    assert_eq!(unsafe { libc::kill(core, libc::SIGKILL) }, 0);
    let mut recovery = s
        .cmd()
        .args(["server", "start", "--external-owner", "--json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        recovery.try_wait().unwrap().is_none(),
        "exact held keeper lock must settle, not become a permanent refusal"
    );
    assert_eq!(unsafe { libc::kill(keeper, libc::SIGCONT) }, 0);
    let output = recovery.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let resumed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(resumed["external_owner"], true);
    assert_eq!(resumed["keepers_ready"], true);
    assert_eq!(resumed["holders"][0]["keeper"], status["holders"][0]["keeper"]);
    s.json(&["send", &terminal, "fresh"]);
    wait_until("same program after exact settlement", || text(&s, &terminal).contains("got-fresh"));
    s.connect().call(9, "server.stop", json!({}));
}

#[test]
fn delayed_holder_bye_settles_after_the_old_core_has_exited() {
    let s = Session::new();
    let mut first = Owned(
        s.cmd().args(["serve", "--external-owner"]).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap(),
    );
    wait_until("external core", || s.connect_ok());
    let terminal = run(&s, &["sh", "-c", "echo saved-before-stop; while read line; do echo got-$line; done"]);
    wait_until("saved screen", || text(&s, &terminal).contains("saved-before-stop"));
    let before = observed_status(&s).unwrap();
    let holder = before["holders"][0]["pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(holder, libc::SIGSTOP) }, 0);
    let reply = s.connect().call(1, "server.restart", json!({}));
    assert_eq!(reply["result"]["stopped"], true);
    // Core's existing bounded goodbye ends before this stopped holder answers.
    assert!(first.0.wait().unwrap().success());
    let lock = std::fs::File::open(s.root.join("state/herdr-core/test/core.lock")).unwrap();
    assert_ne!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
    assert_eq!(unsafe { libc::kill(holder, libc::SIGCONT) }, 0);
    let output = s.cmd().args(["server", "start", "--external-owner", "--json"]).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let after: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(after["holders"][0]["keeper"], before["holders"][0]["keeper"]);
    assert!(text(&s, &terminal).contains("saved-before-stop"));
    s.json(&["send", &terminal, "fresh"]);
    wait_until("same program after late BYE", || text(&s, &terminal).contains("got-fresh"));
    s.connect().call(2, "server.stop", json!({}));
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
