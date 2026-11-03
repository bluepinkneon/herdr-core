//! Per-viewer positions in the one native history, never copies of its text.

use super::{Terminal, ffi};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Opaque position owned by one terminal. Release it when its viewer detaches.
pub struct Viewport(u64);

pub(super) struct Anchor(*mut c_void);

impl Drop for Anchor {
    fn drop(&mut self) {
        unsafe { ffi::ghostty_tracked_grid_ref_free(self.0) }
    }
}

impl Terminal {
    fn viewport_top(&self, view: &Viewport) -> Option<u32> {
        let anchor = self.viewports.get(&view.0)?;
        let mut point = ffi::Coord { x: 0, y: 0 };
        (unsafe { ffi::ghostty_tracked_grid_ref_point(anchor.0, ffi::POINT_SCREEN, &mut point) } == ffi::SUCCESS)
            .then_some(point.y)
    }

    /// Move by signed rows (negative = older), clamped to native history. True is live/bottom.
    pub fn scroll_viewport(&mut self, view: &mut Option<Viewport>, lines: i32) -> Result<bool, &'static str> {
        let bottom = self.history_rows();
        let current = view.as_ref().and_then(|v| self.viewport_top(v)).unwrap_or(bottom).min(bottom);
        let top = (i64::from(current) + i64::from(lines)).clamp(0, i64::from(bottom)) as u32;
        if self.alt_screen() || top == bottom {
            self.release_viewport(view);
            return Ok(true);
        }
        let point =
            ffi::Point { tag: ffi::POINT_SCREEN, value: ffi::PointValue { coord: ffi::Coord { x: 0, y: top } } };
        if let Some(anchor) = view.as_ref().and_then(|v| self.viewports.get(&v.0)) {
            if unsafe { ffi::ghostty_tracked_grid_ref_set(anchor.0, self.raw, point) } != ffi::SUCCESS {
                return Err("cannot move native history anchor");
            }
        } else {
            let mut raw = ptr::null_mut();
            if unsafe { ffi::ghostty_terminal_grid_ref_track(self.raw, point, &mut raw) } != ffi::SUCCESS {
                return Err("cannot allocate native history anchor");
            }
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            self.viewports.insert(id, Anchor(raw));
            *view = Some(Viewport(id));
        }
        Ok(false)
    }

    /// Render the tracked rows through the same VT formatter. A discarded anchor returns live.
    pub fn paint_viewport(&mut self, view: &mut Option<Viewport>) -> Vec<u8> {
        let top = (!self.alt_screen()).then(|| view.as_ref().and_then(|v| self.viewport_top(v))).flatten();
        match top.filter(|top| *top < self.history_rows()) {
            Some(top) => self.paint_at(0, Some(top)),
            None => {
                self.release_viewport(view);
                self.paint(0)
            }
        }
    }

    /// Free the native anchor at viewer retirement or when returning to live output.
    pub fn release_viewport(&mut self, view: &mut Option<Viewport>) {
        if let Some(v) = view.take() {
            self.viewports.remove(&v.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(t: &mut Terminal, v: &mut Option<Viewport>) -> Terminal {
        let (cols, rows) = t.size();
        let mut rendered = Terminal::new(cols, rows, 0);
        rendered.write(&t.paint_viewport(v));
        rendered
    }

    #[test]
    fn tracked_rows_survive_output_reflow_and_release_without_changing_other_views() {
        let mut t = Terminal::new(20, 4, 1 << 20);
        for n in 0..50 {
            t.write(format!("\x1b[31mrow-{n:03}-界\x1b[0m\r\n").as_bytes());
        }
        let mut view = None;
        assert!(!t.scroll_viewport(&mut view, -12).unwrap());
        let old = render(&mut t, &mut view).screen_rows();
        assert!(old[0].contains("row-035-界"), "{old:?}");
        for _ in 0..20 {
            t.write(b"more\r\n");
        }
        assert_eq!(render(&mut t, &mut view).screen_rows(), old);
        t.resize(10, 4);
        let rows = render(&mut t, &mut view).screen_rows();
        assert!(rows.join("").contains("row-035-界"), "{rows:?}");
        assert_eq!(t.viewports.len(), 1);
        assert!(t.scroll_viewport(&mut view, 1000).unwrap());
        assert!(view.is_none() && t.viewports.is_empty());
        assert!(render(&mut t, &mut view).screen_rows().join("").contains("more"));
    }

    #[test]
    fn buffer_switch_reset_and_pruning_retire_discarded_native_anchors() {
        let mut t = Terminal::new(20, 4, 4096);
        for n in 0..50 {
            t.write(format!("row-{n}\r\n").as_bytes());
        }
        let mut view = None;
        t.scroll_viewport(&mut view, -10).unwrap();
        t.write(b"\x1b[?1049hALT");
        assert!(render(&mut t, &mut view).alt_screen());
        assert!(view.is_none() && t.viewports.is_empty());
        t.write(b"\x1b[?1049l");
        t.scroll_viewport(&mut view, -10).unwrap();
        t.write(b"\x1bcRESET");
        assert!(render(&mut t, &mut view).screen_rows()[0].contains("RESET"));
        assert!(view.is_none() && t.viewports.is_empty());
        // Isolate pruning, whose byte limit is rounded to native page granularity.
        let mut t = Terminal::new(20, 4, 4096);
        for _ in 0..50 {
            t.write(b"old\r\n");
        }
        t.scroll_viewport(&mut view, -10).unwrap();
        for _ in 0..20000 {
            t.write(b"new\r\n");
        }
        let rows = render(&mut t, &mut view).screen_rows();
        assert!(
            rows.join("").contains("new"),
            "{rows:?}, history {}, top {:?}",
            t.history_rows(),
            view.as_ref().and_then(|v| t.viewport_top(v))
        );
        assert!(view.is_none() && t.viewports.is_empty());
        t.scroll_viewport(&mut view, -5).unwrap();
        t.release_viewport(&mut view);
        assert!(view.is_none() && t.viewports.is_empty());
    }
}
