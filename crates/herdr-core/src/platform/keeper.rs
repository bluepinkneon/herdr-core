//! Only this Unix IPC boundary handles descriptors and kernel peer identities.

#[cfg(unix)]
mod native {
    use crate::platform::{Stream, handover};
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd};

    pub use handover::{birth, peer_pid, same_process_alive};

    pub fn write_timeout(stream: &Stream, timeout: Option<std::time::Duration>) -> io::Result<()> {
        stream.set_write_timeout(timeout)
    }

    pub fn send(stream: &Stream, kind: u8, lock: Option<&File>) -> io::Result<()> {
        let mut byte = kind;
        let mut iov = libc::iovec { iov_base: (&raw mut byte).cast(), iov_len: 1 };
        let mut control = [0usize; 8];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &raw mut iov;
        msg.msg_iovlen = 1;
        if let Some(lock) = lock {
            msg.msg_control = control.as_mut_ptr().cast();
            msg.msg_controllen = unsafe { libc::CMSG_SPACE(std::mem::size_of::<i32>() as _) } as _;
            unsafe {
                let header = libc::CMSG_FIRSTHDR(&msg);
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as _) as _;
                libc::CMSG_DATA(header).cast::<i32>().write(lock.as_raw_fd());
            }
        }
        if unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) } != 1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn receive(stream: &Stream) -> io::Result<(u8, Option<File>)> {
        let mut byte = 0u8;
        let mut iov = libc::iovec { iov_base: (&raw mut byte).cast(), iov_len: 1 };
        let mut control = [0usize; 8];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &raw mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = std::mem::size_of_val(&control) as _;
        if unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) } != 1 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "keeper admission closed"));
        }
        let mut files = Vec::new();
        unsafe {
            let mut header = libc::CMSG_FIRSTHDR(&msg);
            while !header.is_null() {
                if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                    let bytes = (*header).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                    for index in 0..bytes / std::mem::size_of::<i32>() {
                        let fd = libc::CMSG_DATA(header).cast::<i32>().add(index).read();
                        let file = File::from_raw_fd(fd);
                        if libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                            return Err(io::Error::last_os_error());
                        }
                        files.push(file);
                    }
                }
                header = libc::CMSG_NXTHDR(&msg, header);
            }
        }
        if msg.msg_flags & libc::MSG_CTRUNC != 0 || files.len() > 1 {
            return Err(io::Error::other("invalid keeper lock descriptor"));
        }
        Ok((byte, files.pop()))
    }
}
#[cfg(unix)]
pub use native::*;

#[cfg(windows)]
mod unsupported {
    use crate::platform::Stream;
    use std::{fs::File, io};
    fn error() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "external owner requires Unix descriptor custody")
    }
    pub fn write_timeout(_: &Stream, _: Option<std::time::Duration>) -> io::Result<()> {
        Err(error())
    }
    pub fn send(_: &Stream, _: u8, _: Option<&File>) -> io::Result<()> {
        Err(error())
    }
    pub fn receive(_: &Stream) -> io::Result<(u8, Option<File>)> {
        Err(error())
    }
    pub fn birth(_: u32) -> io::Result<String> {
        Err(error())
    }
    pub fn peer_pid(_: &Stream) -> io::Result<u32> {
        Err(error())
    }
    pub fn same_process_alive(_: u32, _: &str) -> io::Result<bool> {
        Err(error())
    }
}
#[cfg(windows)]
pub use unsupported::*;
