//! Native process-exit observation for foreground service-owner handover.
//! No polling by PID, signals, detached processes, or reaping another owner's child.

#[cfg(unix)]
mod native;
#[cfg(unix)]
pub use native::*;

#[cfg(windows)]
mod unsupported {
    use crate::platform::Stream;
    use std::{fs::File, io, path::Path};
    fn error() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "foreground handover is supported on Linux and Darwin only")
    }
    pub struct Predecessor;
    impl Predecessor {
        pub fn pin(_: &Stream, _: u32, _: Option<&str>) -> io::Result<Self> {
            Err(error())
        }
        pub fn wait(self) -> io::Result<()> {
            Err(error())
        }
    }
    pub fn open_lock(_: &Path) -> io::Result<File> {
        Err(error())
    }
    pub fn same_lock(_: &File, _: &Path) -> io::Result<()> {
        Err(error())
    }
}
#[cfg(windows)]
pub use unsupported::*;
