//! Viewers: raw attach streams on framed connections. First paint, live
//! bytes (minus answered queries), input, and who sets the size.

use crate::holder_proto as proto;
use crate::relay::Relay;
use crate::rpc::RpcError;
use crate::runtime::{Core, State};
use crate::wire::Conn;
use ghostty_vt::Rgb;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

/// What a viewer's own terminal looks like, reported by the client.
#[derive(Serialize, Deserialize, JsonSchema, Clone, Default, Debug, PartialEq)]
pub struct Display {
    /// Default foreground as `#rrggbb`.
    pub fg: Option<String>,
    /// Default background as `#rrggbb`.
    pub bg: Option<String>,
    /// Cursor color as `#rrggbb`.
    pub cursor: Option<String>,
    /// Cell size in pixels: `[width, height]`.
    pub cell: Option<[u32; 2]>,
}

pub struct Viewer {
    pub conn: Arc<Conn>,
    pub channel: u32,
    pub observe: bool,
    scroll_input: bool,
    viewport: Option<ghostty_vt::Viewport>,
    size: (u16, u16),
    /// None: not active since it came back after a dropped line.
    active_at: Option<Instant>,
    display: Display,
}

fn rgb(hex: &Option<String>) -> Option<Rgb> {
    let hex = hex.as_deref()?.strip_prefix('#')?;
    let n = u32::from_str_radix(hex, 16).ok().filter(|_| hex.len() == 6)?;
    Some([(n >> 16) as u8, (n >> 8) as u8, n as u8])
}

pub struct AttachRequest {
    pub observe: bool,
    pub scroll_input: bool,
    pub history: usize,
    pub size: (u16, u16),
    pub display: Display,
    pub resumed: bool,
}

impl Core {
    /// Adds a viewer: queues the first paint, then every live byte follows in order.
    pub fn attach(&self, conn: &Arc<Conn>, id: u32, req: AttachRequest) -> Result<u32, RpcError> {
        let mut st = self.lock();
        if let Some(t) = st.reg.terminals.get(&id) {
            t.check_program()?;
        }
        if !st.screens.contains_key(&id) {
            return Err(RpcError::new(
                "finished",
                format!("{} has finished", crate::terminals::address(id)),
                format!("read what it left: hc read {}", crate::terminals::address(id)),
            ));
        }
        let channel = conn.new_channel();
        st.viewers.entry(id).or_default().push(Viewer {
            conn: conn.clone(),
            channel,
            observe: req.observe,
            scroll_input: req.scroll_input,
            viewport: None,
            size: req.size,
            active_at: (!req.resumed).then(Instant::now),
            display: req.display,
        });
        self.apply_size(&mut st, id);
        let paint = first_paint(&st, id, req.history);
        conn.with(|out| {
            out.open(channel);
            out.data(channel, &paint);
        });
        Ok(channel)
    }

    /// Program output: through the relay to the screen and every viewer,
    /// then whatever status or title it reported.
    pub fn output(&self, st: &mut State, id: u32, bytes: &[u8]) {
        st.last_output.insert(id, Instant::now());
        *st.output_seq.entry(id).or_default() += 1;
        if let Some(t) = st.reg.terminals.get_mut(&id) {
            t.printed += bytes.iter().filter(|b| **b == b'\n').count() as u64;
        }
        // While a read scrolls the app, its frames are ours alone: viewers get one repaint after.
        let scrolling = st.stitching.contains(&id);
        let instance = st.reg.terminals.get(&id).and_then(|t| t.holder.clone());
        let State { screens, relays, holders, output_watch, .. } = st;
        let holder = holders.iter().find(|l| Some(&l.instance) == instance.as_ref());
        let Some(screen) = screens.get_mut(&id) else { return };
        let relay = relays.entry(id).or_default();
        let out = relay.feed(screen, bytes);
        let win32_input = relay.win32_input.take();
        if let Some(watchers) = output_watch.get_mut(&id).filter(|_| !scrolling) {
            watchers.retain(|w| w.send(out.clone()).is_ok());
        }
        if let Some(link) = holder {
            link.input(id, &Relay::take_responses(screen));
        }
        let reports = screen.take_status_reports();
        let title = screen.take_title_changed().then(|| screen.title());
        let full_screen = screen.alt_screen();
        // Before any repaint below, so it carries the mode this output just set.
        if let Some(on) = win32_input
            && let Some(t) = st.reg.terminals.get_mut(&id).filter(|t| t.win32_input != on)
        {
            t.win32_input = on;
            self.save(st);
        }
        if full_screen && let Some(t) = st.reg.terminals.get_mut(&id) {
            t.full_screen = true;
        }
        if !reports.is_empty() {
            self.report_status(st, id, &reports);
        }
        if let Some(title) = title {
            st.reg.terminals.get_mut(&id).expect("live terminal").title = title.clone();
            self.save_soon();
            self.emit(st, "title", id, serde_json::json!({"title": title}));
        }
        let State { screens, viewers, reg, .. } = st;
        let screen = screens.get_mut(&id).expect("output screen");
        for v in viewers.get_mut(&id).into_iter().flatten().filter(|_| !scrolling) {
            let paint = v.viewport.is_some().then(|| paint_viewer(screen, &reg.terminals[&id], v));
            let bytes = paint.as_deref().unwrap_or(&out);
            let mut queued = false;
            v.conn.with(|ob| {
                queued = ob.data(v.channel, bytes);
            });
            if !queued {
                let paint = paint.unwrap_or_else(|| paint_viewer(screen, &reg.terminals[&id], v));
                v.conn.with(|ob| {
                    ob.data(v.channel, &paint);
                });
            }
        }
    }

    /// Applies 7501 reports and tells subscribers what changed.
    pub fn report_status(&self, st: &mut State, id: u32, reports: &[ghostty_vt::StatusReport]) {
        let word = |st: &State| st.reg.terminals[&id].status.as_ref().map(|s| s.state.clone());
        let mut word_changed = false;
        for r in reports {
            let was = word(st);
            let t = st.reg.terminals.get_mut(&id).expect("live terminal");
            let change = crate::status::apply(t, r);
            self.status_changed(st, id, change);
            word_changed |= word(st) != was;
        }
        // Losing the holder too (a reboot) leaves no tail to replay: a new word, and
        // with it a turn or news, must already be on disk. Messages and progress can wait.
        if word_changed { self.save(st) } else { self.save_soon() }
    }

    pub fn status_changed(&self, st: &mut State, id: u32, change: crate::status::Change) {
        let t = &st.reg.terminals[&id];
        let (status, records, turn, reported) = (t.status.clone(), t.records.clone(), t.turn, t.reported);
        if change.status {
            self.emit(
                st,
                "status",
                id,
                serde_json::json!({"status": status, "records": records, "reported": reported}),
            );
        }
        if change.turn_ended {
            let state = status.map(|s| s.state);
            self.emit(st, "turn", id, serde_json::json!({"turn": turn, "state": state}));
        }
        if (change.turn_ended || change.stuck) && st.reg.terminals[&id].is_live() {
            self.news(st, id);
        }
    }

    fn viewer<'a>(st: &'a mut State, conn: &Arc<Conn>, channel: u32) -> Option<(u32, &'a mut Viewer)> {
        st.viewers.iter_mut().find_map(|(id, vs)| {
            vs.iter_mut().find(|v| Arc::ptr_eq(&v.conn, conn) && v.channel == channel).map(|v| (*id, v))
        })
    }

    /// Typing, resizing or focusing makes a viewer the one whose size and colors count.
    fn touch(&self, st: &mut State, conn: &Arc<Conn>, channel: u32, f: impl FnOnce(&mut Viewer)) -> Option<u32> {
        let (id, v) = Self::viewer(st, conn, channel)?;
        f(v);
        if v.observe {
            return None;
        }
        v.active_at = Some(Instant::now());
        self.apply_size(st, id);
        Some(id)
    }

    pub fn viewer_input(&self, conn: &Arc<Conn>, channel: u32, bytes: &[u8]) {
        let mut st = self.lock();
        if let Some(id) = self.touch(&mut st, conn, channel, |_| {}) {
            let now = st.gates.entry(id).or_default().human(bytes);
            if let Some(link) = st.link(id) {
                link.input(id, &now);
            }
        }
    }

    /// Route one native wheel request under the same lock as output, modes and input.
    pub fn viewer_scroll(
        &self,
        conn: &Arc<Conn>,
        p: crate::api::ScrollParams,
    ) -> Result<crate::api::ScrollResult, RpcError> {
        use crate::api::{ScrollInput, ScrollResult, ScrollRoute};
        if !(-1000..=1000).contains(&p.lines) || p.cols == 0 || p.rows == 0 || p.column >= p.cols || p.row >= p.rows {
            return Err(RpcError::invalid_params("scroll lines or pointer geometry out of bounds"));
        }
        let mut st = self.lock();
        let (id, v) = Self::viewer(&mut st, conn, p.channel)
            .filter(|(id, _)| p.terminal == crate::terminals::address(*id))
            .ok_or_else(|| RpcError::new("stale_channel", "no matching local attachment on this connection", ""))?;
        let (size, allowed) = (v.size, v.scroll_input);
        if st.stopping || self.restart.load(std::sync::atomic::Ordering::SeqCst) || st.link(id).is_none() {
            return Err(RpcError::new("unavailable", "terminal holder unavailable or core retiring", ""));
        }
        st.reg.terminals[&id].check_program()?;
        let screen = st.screens.get(&id).ok_or_else(|| RpcError::new("finished", "terminal has finished", ""))?;
        if size != (p.cols, p.rows) || screen.size() != size {
            return Err(RpcError::new(
                "geometry_mismatch",
                "attachment and native screen must match requested geometry",
                "",
            ));
        }
        let none = ScrollResult { route: ScrollRoute::None, input: ScrollInput::None, at_bottom: None };
        if p.lines == 0 {
            return Ok(none);
        }
        let mut bytes = screen.wheel_at(p.lines < 0, p.column, p.row);
        let route = if !bytes.is_empty() {
            ScrollRoute::Mouse
        } else if screen.alt_screen() && screen.dec_mode(1007) {
            bytes = crate::keys::encode(screen, if p.lines < 0 { "up" } else { "down" })?;
            ScrollRoute::Alternate
        } else if screen.alt_screen() {
            return Ok(none);
        } else {
            let State { screens, viewers, reg, .. } = &mut *st;
            let screen = screens.get_mut(&id).expect("screen checked under lock");
            let v = viewers
                .get_mut(&id)
                .expect("viewer checked")
                .iter_mut()
                .find(|v| Arc::ptr_eq(&v.conn, conn) && v.channel == p.channel)
                .expect("viewer checked");
            let bottom = screen
                .scroll_viewport(&mut v.viewport, p.lines)
                .map_err(|why| RpcError::new("unavailable", why, ""))?;
            let paint = paint_viewer(screen, &reg.terminals[&id], v);
            v.conn.with(|out| {
                if !out.data(v.channel, &paint) {
                    out.data(v.channel, &paint);
                }
            });
            return Ok(ScrollResult {
                route: ScrollRoute::Viewport,
                input: ScrollInput::None,
                at_bottom: Some(bottom),
            });
        };
        if !allowed {
            return Err(RpcError::new("input_not_allowed", "attachment does not permit native scroll input", ""));
        }
        let bytes = bytes.repeat(p.lines.unsigned_abs() as usize);
        let input = match st.gates.entry(id).or_default().scroll(&bytes) {
            None => ScrollInput::Held,
            Some(bytes) => {
                if !st.link(id).expect("holder checked under lock").try_input(id, &bytes) {
                    return Err(RpcError::new("unavailable", "holder input queue closed", ""));
                }
                ScrollInput::Forwarded
            }
        };
        Ok(ScrollResult { route, input, at_bottom: None })
    }

    pub fn viewer_resize(&self, conn: &Arc<Conn>, channel: u32, cols: u16, rows: u16) {
        let mut st = self.lock();
        self.touch(&mut st, conn, channel, |v| v.size = (cols, rows));
    }

    pub fn viewer_facts(&self, conn: &Arc<Conn>, channel: u32, display: Display) {
        let mut st = self.lock();
        self.touch(&mut st, conn, channel, |v| v.display = display);
    }

    /// Focus changes count as activity, and reach programs that asked for them.
    pub fn viewer_focus(&self, conn: &Arc<Conn>, channel: u32, focused: bool) {
        let mut st = self.lock();
        let id = if focused {
            self.touch(&mut st, conn, channel, |_| {})
        } else {
            Self::viewer(&mut st, conn, channel).filter(|(_, v)| !v.observe).map(|(id, _)| id)
        };
        if let Some(id) = id
            && st.screens.get(&id).is_some_and(|s| s.dec_mode(1004))
            && let Some(link) = st.link(id)
        {
            link.input(id, if focused { b"\x1b[I" } else { b"\x1b[O" });
        }
    }

    /// A viewer left (`channel` = None: the whole connection went away).
    pub fn detach(&self, conn: &Arc<Conn>, channel: Option<u32>) {
        let mut st = self.lock();
        self.unsubscribe(&mut st, conn, channel);
        let mut touched = Vec::new();
        let State { screens, viewers, .. } = &mut *st;
        for (id, vs) in viewers.iter_mut() {
            let before = vs.len();
            vs.retain_mut(|v| {
                let remove = Arc::ptr_eq(&v.conn, conn) && channel.is_none_or(|c| c == v.channel);
                if remove && let Some(screen) = screens.get_mut(id) {
                    screen.release_viewport(&mut v.viewport);
                }
                !remove
            });
            if vs.len() != before {
                touched.push(*id);
            }
        }
        st.viewers.retain(|_, vs| !vs.is_empty());
        for id in touched {
            self.apply_size(&mut st, id);
        }
    }

    /// The most recently active typing viewer sets size and display facts;
    /// with none, the terminal runs headless.
    fn apply_size(&self, st: &mut State, id: u32) {
        let leader = st.viewers.get(&id).and_then(|vs| vs.iter().filter(|v| !v.observe).max_by_key(|v| v.active_at));
        let (cols, rows) = leader.map_or_else(|| self.headless(), |v| v.size);
        let display = leader.map(|v| v.display.clone());
        let Some(screen) = st.screens.get_mut(&id) else { return };
        if let Some(d) = display {
            screen.set_display(rgb(&d.fg), rgb(&d.bg), rgb(&d.cursor), d.cell.map(|[w, h]| (w, h)));
        }
        let rec = st.reg.terminals.get_mut(&id).expect("live terminal");
        if (rec.cols, rec.rows) == (cols, rows) || cols == 0 || rows == 0 {
            return;
        }
        screen.resize(cols, rows);
        rec.cols = cols;
        rec.rows = rows;
        if let Some(link) = st.link(id) {
            link.send(&proto::json_frame(proto::RESIZE, id, &proto::Size { cols, rows }));
        }
        self.save_soon();
        self.emit(st, "size", id, serde_json::json!({"cols": cols, "rows": rows}));
    }
}

/// The screen, then the latest 7501 report of every record and whether the
/// program wants win32 key events (both pass through to viewers, so a new one
/// must see them too; a viewer repainted after falling behind gets the mode
/// either way).
pub(crate) fn first_paint(st: &State, id: u32, history: usize) -> Vec<u8> {
    let mut paint = st.screens[&id].paint(history);
    paint_status(&mut paint, &st.reg.terminals[&id]);
    paint
}

fn paint_status(paint: &mut Vec<u8>, t: &crate::terminals::Terminal) {
    paint.extend(if t.win32_input { b"\x1b[?9001h" } else { b"\x1b[?9001l" });
    let root = t.status.iter().map(|s| ("", s));
    for (rid, status) in root.chain(t.records.iter().map(|(k, s)| (k.as_str(), s))) {
        paint.extend(status.encode(rid).as_bytes());
    }
}

fn paint_viewer(screen: &mut ghostty_vt::Terminal, t: &crate::terminals::Terminal, v: &mut Viewer) -> Vec<u8> {
    let mut paint = screen.paint_viewport(&mut v.viewport);
    paint_status(&mut paint, t);
    paint
}

pub(crate) fn repaint_viewers(st: &mut State, id: u32) {
    let State { screens, viewers, reg, .. } = st;
    let Some(screen) = screens.get_mut(&id) else { return };
    for v in viewers.get_mut(&id).into_iter().flatten() {
        let paint = paint_viewer(screen, &reg.terminals[&id], v);
        v.conn.with(|out| {
            if !out.data(v.channel, &paint) {
                out.data(v.channel, &paint);
            }
        });
    }
}
