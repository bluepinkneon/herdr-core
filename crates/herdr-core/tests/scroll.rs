#![cfg(unix)]

mod common;

use common::framed::{Framed, Viewer};
use common::*;
use herdr_core::wire;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

fn scroll(v: &mut Viewer, terminal: &str, lines: i32) -> Value {
    let (cols, rows) = v.screen.size();
    v.conn.call(
        "terminal.scroll",
        json!({"channel": v.channel, "terminal": terminal, "lines": lines,
            "column": 3, "row": 4, "cols": cols, "rows": rows}),
    )
}

#[test]
fn native_mouse_scroll_uses_the_attachment_without_delaying_keyboard_quiet() {
    let mut s = Session::new();
    s.env.push(("HC_QUIET_MS".into(), "1600".into()));
    let t = run(&s, &["sh", "-c", &format!("stty raw -echo; printf '\\033[?1000h\\033[?1006h'; {BYTES}")]);
    let mut v = Viewer::attach(&s, &t, 44, 46, json!({"scroll_input": true}));
    v.until("mouse mode", |v| v.screen.dec_mode(1006));
    let reply = scroll(&mut v, &t, -1);
    assert_eq!(reply["result"], json!({"route": "mouse", "input": "forwarded"}), "{reply}");
    let start = Instant::now();
    s.json(&["keys", &t, "x"]);
    assert!(start.elapsed() < Duration::from_millis(1100), "scroll advanced keyboard quiet");
    v.shows("78");
    v.typed(b"z");
    v.shows("7a");
    let start = Instant::now();
    s.json(&["keys", &t, "y"]);
    assert!(start.elapsed() >= Duration::from_millis(1300), "keyboard quiet was bypassed");
    v.shows("79");
}

#[test]
fn one_viewer_holds_native_history_while_other_viewers_and_output_stay_live() {
    let s = Session::new();
    let program = script(
        &s,
        "w go; i=0; while [ $i -lt 100 ]; do printf 'row-%03d\\r\\n' $i; i=$((i+1)); done; w more; printf 'NEW-LIVE\\r\\n'; w stop",
    );
    let t = run(&s, &["sh", "-c", &program]);
    let mut held = Viewer::attach(&s, &t, 44, 10, json!({}));
    let mut live = Viewer::attach(&s, &t, 44, 10, json!({"observe": true}));
    touch(&s, "go");
    held.shows("row-099");
    live.shows("row-099");
    let result = scroll(&mut held, &t, -20);
    assert_eq!(result["result"], json!({"route":"viewport", "input":"none", "at_bottom":false}), "{result}");
    held.until("native older rows", |v| v.screen.screen_rows()[0].starts_with("row-071"));
    let frozen = held.screen.screen_rows();
    touch(&s, "more");
    live.shows("NEW-LIVE");
    held.drain(Duration::from_millis(80));
    assert_eq!(held.screen.screen_rows(), frozen, "new output moved the held viewer");
    assert!(live.screen.screen_rows().iter().any(|r| r.contains("NEW-LIVE")));
    let result = scroll(&mut held, &t, 1000);
    assert_eq!(result["result"]["at_bottom"], true);
    held.shows("NEW-LIVE");
    assert_eq!(held.screen.screen_rows(), live.screen.screen_rows());
}

#[test]
fn passive_permissions_exact_channel_and_geometry_are_checked_before_input() {
    let s = Session::new();
    let t = run(&s, &["sh", "-c", &format!("stty raw -echo; printf '\\033[?1000h\\033[?1006h'; {BYTES}")]);
    let mut leader = Viewer::attach(&s, &t, 44, 10, json!({}));
    leader.until("mouse", |v| v.screen.dec_mode(1006));
    assert_eq!(scroll(&mut leader, &t, -1)["error"]["data"]["kind"], "input_not_allowed");
    let mut observer = Viewer::attach(&s, &t, 44, 10, json!({"observe":true, "scroll_input":true}));
    assert_eq!(scroll(&mut observer, &t, -1)["result"]["route"], "mouse");
    observer.typed(b"BAD");
    let p = json!({"channel":observer.channel,"terminal":t,"lines":-1,"column":0,"row":0,"cols":44,"rows":10});
    assert_eq!(s.connect().call(5, "terminal.scroll", p.clone())["error"]["data"]["kind"], "needs_framed");
    assert_eq!(Framed::open(&s).call("terminal.scroll", p.clone())["error"]["data"]["kind"], "stale_channel");
    for (key, value, kind) in [
        ("terminal", json!("remote/t1"), "stale_channel"),
        ("channel", json!(observer.channel + 100), "stale_channel"),
        ("cols", json!(43), "geometry_mismatch"),
        ("column", json!(44), "invalid_params"),
        ("lines", json!(1001), "invalid_params"),
        ("unknown", json!(true), "invalid_params"),
    ] {
        let mut bad = p.clone();
        bad[key] = value;
        let reply = observer.conn.call("terminal.scroll", bad);
        assert_eq!(reply["error"]["data"]["kind"], kind, "{key}: {reply}");
    }
    observer.conn.send(observer.channel, wire::RESIZE, &[100, 0, 50, 0]);
    let _ = observer.conn.call("server.status", json!({}));
    assert_eq!(get(&s, &t)["cols"], 44);
    assert_eq!(get(&s, &t)["rows"], 10);
    assert_eq!(observer.conn.call("terminal.scroll", p.clone())["error"]["data"]["kind"], "geometry_mismatch");
    s.json(&["keys", &t, "x"]);
    leader.shows("78");
    let hex: String = leader.text().split_whitespace().collect();
    assert!(hex.contains("1b5b3c36343b343b354d"), "pointer cell/direction: {hex}");
    assert!(!hex.contains("424144"), "passive DATA reached the program: {hex}");
    observer.conn.send(observer.channel, wire::CLOSE, b"{}");
    assert_eq!(observer.conn.call("terminal.scroll", p)["error"]["data"]["kind"], "stale_channel");
}

#[test]
fn native_alternate_scroll_uses_current_key_modes_and_mouse_takes_precedence() {
    let s = Session::new();
    let program = script(
        &s,
        "stty raw -echo; printf '\\033[?1049h\\033[?1h\\033[?1007hREADY'; w mouse; printf '\\033[?1000h\\033[?1006hMOUSE'; w off; printf '\\033[?1000l\\033[?1006l\\033[?1007lOFF'; w stop",
    );
    let t = run(&s, &["sh", "-c", &program]);
    let mut v = Viewer::attach(&s, &t, 44, 10, json!({"scroll_input":true}));
    v.shows("READY");
    assert_eq!(scroll(&mut v, &t, -2)["result"]["route"], "alternate");
    touch(&s, "mouse");
    v.shows("MOUSE");
    assert_eq!(scroll(&mut v, &t, 1)["result"]["route"], "mouse");
    touch(&s, "off");
    v.shows("OFF");
    assert_eq!(scroll(&mut v, &t, -1)["result"], json!({"route":"none","input":"none"}));
}

#[test]
fn stalled_history_viewer_keeps_its_anchor_and_never_blocks_a_live_viewer() {
    let s = Session::new();
    let program = script(
        &s,
        "w go; i=0; while [ $i -lt 100 ]; do printf 'row-%03d\\r\\n' $i; i=$((i+1)); done; w flood; head -c 8000000 /dev/zero | tr '\\0' x; printf '\\r\\nDONE\\r\\n'; w stop",
    );
    let t = run(&s, &["sh", "-c", &program]);
    let mut held = Viewer::attach(&s, &t, 44, 10, json!({}));
    let mut live = Viewer::attach(&s, &t, 44, 10, json!({"observe":true}));
    touch(&s, "go");
    held.shows("row-099");
    assert_eq!(scroll(&mut held, &t, -20)["result"]["at_bottom"], false);
    held.until("held", |v| v.screen.screen_rows()[0].starts_with("row-071"));
    held.credit = false;
    touch(&s, "flood");
    live.shows("DONE");
    assert!(held.conn.call("server.status", json!({}))["result"]["pid"].is_number());
    held.credit = true;
    held.conn.send(held.channel, wire::CREDIT, &(64u32 << 20).to_le_bytes());
    // This exceeds the native 5 MiB history cap: the discarded anchor returns live.
    held.shows("DONE");
    assert_eq!(held.screen.screen_rows(), live.screen.screen_rows());
}

#[test]
fn scrolling_during_a_real_send_joins_the_held_input_in_order() {
    let s = Session::new();
    let t = run(&s, &["sh", "-c", &format!("stty raw -echo; printf '\\033[?1000h\\033[?1006h'; {BYTES}")]);
    let mut v = Viewer::attach(&s, &t, 44, 46, json!({"scroll_input":true}));
    v.until("mouse", |v| v.screen.dec_mode(1006));
    let mut send = s
        .cmd()
        .args(["--json", "send", &t, "AUTO", "--force"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    v.shows("4f"); // paste arrived, but Enter is still separated by PASTE_GAP
    let reply = scroll(&mut v, &t, -1);
    assert_eq!(reply["result"]["input"], "held", "{reply}");
    v.typed(b"z");
    assert!(send.wait().unwrap().success());
    v.shows("7a");
    let hex: String = v.text().split_whitespace().collect();
    assert!(hex.contains("4155544f0d1b5b3c36343b343b354d7a"), "atomic order: {hex}");
}
