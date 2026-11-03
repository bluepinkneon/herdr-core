use crate::platform::Stream;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::time::{Duration, Instant};

pub fn open_lock(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().read(true).write(true).custom_flags(libc::O_NOFOLLOW).open(path)?;
    same_lock(&file, path)?;
    Ok(file)
}

pub fn same_lock(file: &File, path: &Path) -> io::Result<()> {
    let opened = file.metadata()?;
    let current = path.symlink_metadata()?;
    if !current.is_file() || (opened.dev(), opened.ino()) != (current.dev(), current.ino()) {
        return Err(io::Error::other("handover lock file was replaced"));
    }
    Ok(())
}

/// Only contention proves an existing lock owner. Unsupported locking or
/// another syscall error must refuse before the restart request is sent.
pub fn try_lock(file: &File) -> io::Result<bool> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock { Ok(false) } else { Err(error) }
}

pub struct Predecessor {
    event: File,
}

impl Predecessor {
    /// Pin before sending either request. A status reply on this same socket
    /// must then confirm the expected native identity before restart is sent.
    pub fn pin(stream: &Stream, pid: u32, expected_birth: Option<&str>) -> io::Result<Self> {
        if peer_pid(stream)? != pid {
            return Err(io::Error::other("handover socket peer PID mismatch; restart not sent"));
        }
        let before = birth(pid)?;
        if expected_birth.is_some_and(|expected| before != expected) {
            return Err(io::Error::other("handover process birth mismatch; restart not sent"));
        }
        let event = watch(pid)?;
        if birth(pid)? != before {
            return Err(io::Error::other("handover process changed while pinning; restart not sent"));
        }
        stream.set_read_timeout(Some(Duration::from_secs(15)))?;
        stream.set_write_timeout(Some(Duration::from_secs(15)))?;
        Ok(Self { event })
    }

    /// Kernel-observed exit includes an unreaped zombie: the old service owner
    /// need not run/reap anything before this process can become the new core.
    pub fn wait(self) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "handover old process did not exit; adoption held",
                ));
            }
            match exited(&self.event, left) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                result => return result,
            }
        }
    }
}

fn owned_fd(fd: i32) -> io::Result<File> {
    if fd < 0 { Err(io::Error::last_os_error()) } else { Ok(unsafe { File::from_raw_fd(fd) }) }
}

#[cfg(target_os = "linux")]
pub fn peer_pid(stream: &Stream) -> io::Result<u32> {
    let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(stream.as_raw_fd(), libc::SOL_SOCKET, libc::SO_PEERCRED, (&raw mut peer).cast(), &mut size)
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if size as usize != std::mem::size_of_val(&peer) || peer.pid <= 0 || peer.uid != unsafe { libc::geteuid() } {
        return Err(io::Error::other("handover socket peer identity unavailable"));
    }
    Ok(peer.pid as u32)
}

#[cfg(target_os = "macos")]
pub fn peer_pid(stream: &Stream) -> io::Result<u32> {
    let mut pid: libc::pid_t = 0;
    let mut size = std::mem::size_of_val(&pid) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(stream.as_raw_fd(), libc::SOL_LOCAL, libc::LOCAL_PEERPID, (&raw mut pid).cast(), &mut size)
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if size as usize != std::mem::size_of_val(&pid) || pid <= 0 {
        return Err(io::Error::other("handover socket peer PID unavailable"));
    }
    Ok(pid as u32)
}

#[cfg(target_os = "linux")]
pub fn birth(pid: u32) -> io::Result<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, fields) = stat.rsplit_once(')').ok_or_else(|| io::Error::other("unreadable process stat"))?;
    let fields: Vec<_> = fields.split_whitespace().collect();
    if matches!(fields.first(), Some(&"Z" | &"X")) {
        return Err(io::Error::other("handover predecessor already exited"));
    }
    fields
        .get(19)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v != 0)
        .map(|v| v.to_string())
        .ok_or_else(|| io::Error::other("unreadable process starttime"))
}

#[cfg(target_os = "macos")]
pub fn birth(pid: u32) -> io::Result<String> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    if unsafe { libc::proc_pidinfo(pid as i32, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) } != size {
        return Err(io::Error::last_os_error());
    }
    if info.pbi_pid != pid || info.pbi_status == 5 || info.pbi_start_tvsec == 0 {
        return Err(io::Error::other("handover predecessor is not a live process"));
    }
    Ok(format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec))
}

#[cfg(target_os = "linux")]
fn watch(pid: u32) -> io::Result<File> {
    owned_fd(unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) } as i32)
}

pub fn same_process_alive(pid: u32, expected_birth: &str) -> io::Result<bool> {
    let event = match watch(pid) {
        Ok(event) => event,
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(false),
        Err(error) => return Err(error),
    };
    match exited(&event, Duration::ZERO) {
        Ok(()) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::TimedOut => Ok(birth(pid)? == expected_birth),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn watch(pid: u32) -> io::Result<File> {
    let queue = owned_fd(unsafe { libc::kqueue() })?;
    if unsafe { libc::fcntl(queue.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let change = libc::kevent {
        ident: pid as libc::uintptr_t,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    if unsafe { libc::kevent(queue.as_raw_fd(), &change, 1, std::ptr::null_mut(), 0, std::ptr::null()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(queue)
}

#[cfg(target_os = "linux")]
fn exited(event: &File, timeout: Duration) -> io::Result<()> {
    let mut fd = libc::pollfd { fd: event.as_raw_fd(), events: libc::POLLIN, revents: 0 };
    let n = unsafe { libc::poll(&mut fd, 1, timeout.as_millis().min(i32::MAX as u128) as i32) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "handover old process did not exit; adoption held"));
    }
    if fd.revents & (libc::POLLIN | libc::POLLHUP) == 0 {
        return Err(io::Error::other("handover process-exit observation failed"));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn exited(event: &File, timeout: Duration) -> io::Result<()> {
    let mut exit: libc::kevent = unsafe { std::mem::zeroed() };
    let timeout = libc::timespec { tv_sec: timeout.as_secs() as _, tv_nsec: timeout.subsec_nanos() as _ };
    let n = unsafe { libc::kevent(event.as_raw_fd(), std::ptr::null(), 0, &mut exit, 1, &timeout) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "handover old process did not exit; adoption held"));
    }
    if exit.flags & libc::EV_ERROR != 0 || exit.filter != libc::EVFILT_PROC || exit.fflags & libc::NOTE_EXIT == 0 {
        return Err(io::Error::other("handover process-exit observation failed"));
    }
    Ok(())
}
