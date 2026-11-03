//! Externally owned cores keep legacy holders connected across daemon loss.
//! The holder's tail is the only replay source. An idle keeper only drains;
//! HELLO requires the real session-lock descriptor from its native owner.

use crate::{
    holder_proto as proto,
    paths::{Paths, write_atomic},
    platform::{self, Stream},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufRead, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

const LIMIT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct KeeperInfo {
    pub pid: u32,
    pub birth: String,
    pub instance: String,
    pub socket: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    holder: PathBuf,
    keeper: KeeperInfo,
    state: String,
    error: Option<String>,
}

fn record_path(paths: &Paths, holder: &Path) -> io::Result<PathBuf> {
    Ok(paths
        .state
        .join("keepers")
        .join(holder.file_name().ok_or_else(|| io::Error::other("holder has no name"))?)
        .with_extension("json"))
}

fn read_record(path: &Path) -> io::Result<Record> {
    proto::parse(&std::fs::read(path)?)
}

fn checked_stream(record: &Record) -> io::Result<Stream> {
    if record.state != "active" {
        return Err(io::Error::other(format!("keeper custody {}: {:?}", record.state, record.error)));
    }
    let stream = platform::connect(&record.keeper.socket)?;
    if platform::keeper::peer_pid(&stream)? != record.keeper.pid
        || platform::keeper::birth(record.keeper.pid)? != record.keeper.birth
    {
        return Err(io::Error::other("keeper process identity changed; adoption held"));
    }
    stream.set_read_timeout(Some(LIMIT))?;
    platform::keeper::write_timeout(&stream, Some(LIMIT))?;
    Ok(stream)
}

/// Await exact old keeper lock settlement, not a timed retry of a daemon start.
/// A healthy owner, unreadable record, or missing keeper refuses recovery.
pub fn settle(paths: &Paths) -> io::Result<()> {
    let entries = match std::fs::read_dir(paths.state.join("keepers")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "json") {
            return Err(io::Error::other("unfinished keeper record; adoption held"));
        }
        let record = read_record(&path)?;
        if record.state == "retired" {
            continue;
        }
        let mut stream = checked_stream(&record)?;
        platform::keeper::send(&stream, b'Q', None)?;
        let observed: Record = match proto::read(&mut stream)? {
            Some((proto::WELCOME, _, bytes)) => proto::parse(&bytes)?,
            _ => return Err(io::Error::other("keeper settlement unavailable; adoption held")),
        };
        if observed.keeper != record.keeper || observed.state != "active" {
            return Err(io::Error::other("keeper settlement changed; adoption held"));
        }
    }
    Ok(())
}

pub fn retired(paths: &Paths, info: &KeeperInfo) -> io::Result<bool> {
    for entry in std::fs::read_dir(paths.state.join("keepers"))? {
        let record = read_record(&entry?.path())?;
        if record.keeper == *info {
            return Ok(record.state == "retired");
        }
    }
    Err(io::Error::other("keeper record is missing"))
}

pub fn connect(paths: &Paths, holder: &Path, lock: &File) -> io::Result<Stream> {
    let at = record_path(paths, holder)?;
    let record = match read_record(&at) {
        Ok(record) => record,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let log = std::fs::OpenOptions::new().create(true).append(true).open(paths.state.join("keeper.log"))?;
            // Direct child: the external OS owner keeps this exact process group.
            // No systemd-run, daemon replacement, Source worker or retry.
            let mut child = Command::new(std::env::current_exe()?)
                .args(["keep-holder", "--socket"])
                .arg(holder)
                .env_remove(crate::sweep::VAR)
                .env(crate::sweep::RUNTIME, "1")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(log)
                .spawn()?;
            let output = child.stdout.take().expect("piped");
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut line = String::new();
                let result = std::io::BufReader::new(output)
                    .take(16_385)
                    .read_line(&mut line)
                    .and_then(|_| proto::parse::<Record>(line.as_bytes()));
                let _ = send.send(result);
                let _ = child.wait();
            });
            receive
                .recv_timeout(LIMIT)
                .map_err(|_| io::Error::other("keeper start unsettled; no replacement issued"))??
        }
        Err(error) => return Err(error),
    };
    if record.holder != holder {
        return Err(io::Error::other("keeper holder binding changed"));
    }
    let stream = checked_stream(&record)?;
    platform::keeper::send(&stream, b'A', Some(lock))?;
    // The ordinary holder parser follows the descriptor-bearing admission.
    stream.set_read_timeout(None)?;
    platform::keeper::write_timeout(&stream, None)?;
    Ok(stream)
}

struct Owner {
    lease: u64,
    ready: bool,
    handover: bool,
    pid: u32,
    birth: String,
    _lock: File,
    stream: Stream,
}
struct State {
    owner: Option<Owner>,
    downstream: Option<Stream>,
    epoch: u64,
    lease: u64,
    previous: String,
    terms: HashSet<u32>,
    draining: bool,
    goodbye: Option<proto::Goodbye>,
    record: Record,
}
struct Keeper {
    paths: Paths,
    state: Mutex<State>,
    settled: Condvar,
}

pub fn run(holder: PathBuf) -> io::Result<()> {
    let paths = Paths::from_env().map_err(|e| io::Error::other(e.message))?;
    let socket = PathBuf::from(holder.to_string_lossy().replacen(".holder", ".keeper", 1));
    if socket == holder {
        return Err(io::Error::other("unrecognized holder socket"));
    }
    let dir = paths.state.join("keepers");
    platform::private_dir(&dir)?;
    let at = record_path(&paths, &holder)?;
    if at.try_exists()? {
        return Err(io::Error::other("keeper record already exists; no replacement"));
    }
    let listener = platform::listen(&socket)?;
    let record = Record {
        holder,
        keeper: KeeperInfo {
            pid: std::process::id(),
            birth: platform::keeper::birth(std::process::id())?,
            instance: platform::random_hex(),
            socket,
        },
        state: "active".into(),
        error: None,
    };
    write_atomic(&at, &serde_json::to_vec(&record)?)?;
    println!("{}", serde_json::to_string(&record)?);
    io::stdout().flush()?;
    let keeper = Arc::new(Keeper {
        paths,
        state: Mutex::new(State {
            owner: None,
            downstream: None,
            epoch: 0,
            lease: 0,
            previous: "none".into(),
            terms: HashSet::new(),
            draining: false,
            goodbye: None,
            record,
        }),
        settled: Condvar::new(),
    });
    for stream in listener.incoming() {
        let stream = stream?;
        let keeper = keeper.clone();
        std::thread::spawn(move || {
            if let Err(error) = keeper.serve(stream) {
                eprintln!("keeper: {error}");
            }
        });
    }
    Ok(())
}

impl Keeper {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn save(&self, st: &State) -> io::Result<()> {
        write_atomic(&record_path(&self.paths, &st.record.holder)?, &serde_json::to_vec(&st.record)?)
    }

    fn detach(&self, lease: u64, previous: &str) {
        let mut st = self.lock();
        if st.owner.as_ref().is_none_or(|owner| owner.lease != lease) {
            return;
        }
        if st.goodbye.is_some() && previous == "crash" {
            // A dispatched goodbye owns this lock until its observed BYE and
            // rebind settle, even if the daemon dies first. No new HELLO can
            // overtake an old goodbye that may still clear the holder's tail.
            if let Some(owner) = &mut st.owner {
                owner.ready = false;
                let _ = owner.stream.shutdown(std::net::Shutdown::Both);
            }
            return;
        }
        if let Some(owner) = st.owner.take() {
            let _ = owner.stream.shutdown(std::net::Shutdown::Both);
            st.previous = if owner.handover { "handover" } else { previous }.into();
        }
        self.settled.notify_all();
    }

    fn fail(&self, error: &dyn std::fmt::Display) {
        {
            let mut st = self.lock();
            st.record.state = "failed".into();
            st.record.error = Some(error.to_string());
            if let Err(error) = self.save(&st) {
                eprintln!("keeper cannot record failure: {error}");
            }
        }
        let lease = self.lock().lease;
        self.detach(lease, "crash");
    }

    fn serve(self: &Arc<Self>, mut stream: Stream) -> io::Result<()> {
        stream.set_read_timeout(Some(LIMIT))?;
        platform::keeper::write_timeout(&stream, Some(LIMIT))?;
        let (kind, descriptor) = platform::keeper::receive(&stream)?;
        if kind == b'Q' && descriptor.is_none() {
            let mut st = self.lock();
            if let Some(owner) = &st.owner {
                if platform::keeper::same_process_alive(owner.pid, &owner.birth)? {
                    return Err(io::Error::other("keeper still has a live native owner"));
                }
                let (next, cap) = self
                    .settled
                    .wait_timeout_while(st, LIMIT, |st| st.owner.is_some())
                    .map_err(|error| io::Error::other(error.to_string()))?;
                st = next;
                if cap.timed_out() {
                    return Err(io::Error::other("keeper native lock settlement pending"));
                }
            }
            stream.write_all(&proto::json_frame(proto::WELCOME, 0, &st.record))?;
            return Ok(());
        }
        let lock = descriptor
            .filter(|_| kind == b'A')
            .ok_or_else(|| io::Error::other("keeper requires the native session lock"))?;
        platform::handover::same_lock(&lock, &self.paths.state.join("core.lock"))?;
        let probe = platform::handover::open_lock(&self.paths.state.join("core.lock"))?;
        if platform::handover::try_lock(&probe)? {
            return Err(io::Error::other("keeper received an unowned session lock"));
        }
        if !platform::handover::try_lock(&lock)? {
            return Err(io::Error::other("keeper descriptor does not own the session lock"));
        }
        let pid = platform::keeper::peer_pid(&stream)?;
        let birth = platform::keeper::birth(pid)?;
        match proto::read(&mut stream)? {
            Some((proto::HELLO, _, bytes)) if proto::parse::<proto::Hello>(&bytes)?.version == proto::VERSION => {}
            _ => return Err(io::Error::other("keeper expected native HELLO")),
        }
        let lease = {
            let mut st = self.lock();
            if st.owner.is_some() || st.record.state != "active" {
                return Err(io::Error::other("keeper not available for adoption"));
            }
            st.lease += 1;
            let lease = st.lease;
            st.owner = Some(Owner {
                lease,
                ready: false,
                handover: false,
                pid,
                birth,
                _lock: lock,
                stream: stream.try_clone()?,
            });
            lease
        };
        let result = self.attach(&mut stream).and_then(|_| {
            stream.set_read_timeout(None)?;
            while let Some((kind, id, payload)) = proto::read(&mut stream)? {
                let mut st = self.lock();
                if st.owner.as_ref().is_none_or(|owner| owner.lease != lease) {
                    break;
                }
                if kind == proto::HELLO {
                    return Err(io::Error::other("second HELLO is not admitted"));
                }
                if kind == proto::DRAIN {
                    st.draining = true;
                }
                if kind == proto::GOODBYE {
                    st.goodbye = Some(proto::parse(&payload)?);
                }
                st.downstream
                    .as_mut()
                    .ok_or_else(|| io::Error::other("holder connection absent"))?
                    .write_all(&proto::frame(kind, id, &payload))?;
                if kind == proto::RELEASE {
                    st.terms.remove(&id);
                }
                // No further command may cross a requested handover/stop.
                if kind == proto::GOODBYE {
                    drop(st);
                    let mut byte = [0];
                    let _ = std::io::Read::read(&mut stream, &mut byte);
                    return Ok(());
                }
            }
            Ok(())
        });
        if let Err(error) = &result
            && self.lock().lease == lease
        {
            self.fail(error);
        }
        // A BYE path already settled its observed handover; don't overwrite it.
        self.detach(lease, "crash");
        result
    }

    /// Only called while this keeper holds the admitted core's exact lock FD.
    /// The old downstream stays open until the holder atomically accepts HELLO.
    fn downstream(&self) -> io::Result<(Stream, proto::Welcome, Vec<Vec<u8>>)> {
        let holder = {
            let st = self.lock();
            if st.owner.is_none() {
                return Err(io::Error::other("idle keeper cannot HELLO"));
            }
            st.record.holder.clone()
        };
        let mut stream = platform::connect(&holder)?;
        stream.set_read_timeout(Some(LIMIT))?;
        platform::keeper::write_timeout(&stream, Some(LIMIT))?;
        stream.write_all(&proto::json_frame(proto::HELLO, 0, &proto::Hello { version: proto::VERSION }))?;
        let welcome: proto::Welcome = match proto::read(&mut stream)? {
            Some((proto::WELCOME, _, bytes)) => proto::parse(&bytes)?,
            _ => return Err(io::Error::other("holder welcome absent")),
        };
        let mut tails = Vec::new();
        for _ in &welcome.terminals {
            match proto::read(&mut stream)? {
                Some((proto::TAIL, id, bytes)) => tails.push(proto::frame(proto::TAIL, id, &bytes)),
                _ => return Err(io::Error::other("holder tail absent")),
            }
        }
        stream.set_read_timeout(None)?;
        Ok((stream, welcome, tails))
    }

    fn attach(self: &Arc<Self>, upstream: &mut Stream) -> io::Result<()> {
        // Retire the old pump epoch BEFORE HELLO closes its old downstream.
        let epoch = {
            let mut st = self.lock();
            st.epoch += 1;
            st.epoch
        };
        let (stream, mut welcome, tails) = self.downstream()?;
        {
            let mut st = self.lock();
            if st.previous != "none" {
                welcome.previous = st.previous.clone();
            }
            welcome.keeper = Some(st.record.keeper.clone());
            st.terms = welcome.terminals.iter().map(|term| term.id).collect();
            st.downstream = Some(stream.try_clone()?);
            st.goodbye = None;
        }
        let wrote = upstream.write_all(&proto::json_frame(proto::WELCOME, 0, &welcome)).and_then(|_| {
            for tail in tails {
                upstream.write_all(&tail)?;
            }
            Ok(())
        });
        if wrote.is_ok()
            && let Some(owner) = &mut self.lock().owner
        {
            owner.ready = true;
        }
        let keeper = self.clone();
        std::thread::spawn(move || keeper.pump(stream, epoch));
        wrote
    }

    fn pump(self: Arc<Self>, mut stream: Stream, mut epoch: u64) {
        let result = (|| -> io::Result<()> {
            while let Some((kind, id, payload)) = proto::read(&mut stream)? {
                let upstream = {
                    let mut st = self.lock();
                    if st.epoch != epoch {
                        return Ok(());
                    }
                    if kind == proto::SPAWNED {
                        st.terms.insert(id);
                    }
                    st.owner
                        .as_ref()
                        .filter(|owner| owner.ready)
                        .map(|owner| owner.stream.try_clone().map(|stream| (owner.lease, stream)))
                        .transpose()?
                };
                if kind == proto::BYE {
                    let handover = {
                        let mut st = self.lock();
                        if st.epoch != epoch {
                            return Ok(());
                        }
                        if let Some(owner) = &mut st.owner {
                            owner.ready = false;
                        }
                        st.goodbye == Some(proto::Goodbye::Handover)
                    };
                    if handover {
                        // Observe real tail-clear/BYE, then keep the holder connected
                        // before releasing the native owner. No Source timing race.
                        {
                            let mut st = self.lock();
                            st.epoch += 1;
                            epoch = st.epoch;
                        }
                        let (next, _, _) = self.downstream()?;
                        let (next_epoch, lease) = {
                            let mut st = self.lock();
                            st.downstream = Some(next.try_clone()?);
                            let owner = st.owner.as_mut().ok_or_else(|| io::Error::other("handover owner absent"))?;
                            owner.handover = true;
                            let lease = owner.lease;
                            st.goodbye = None;
                            (st.epoch, lease)
                        };
                        let keeper = self.clone();
                        std::thread::spawn(move || keeper.pump(next, next_epoch));
                        if let Some((_, mut upstream)) = upstream {
                            let _ = upstream.write_all(&proto::frame(proto::BYE, 0, &[]));
                        }
                        self.detach(lease, "handover");
                        return Ok(());
                    }
                    if self.lock().goodbye == Some(proto::Goodbye::Shutdown) {
                        if let Some((_, mut upstream)) = upstream {
                            let _ = upstream.write_all(&proto::frame(proto::BYE, 0, &[]));
                        }
                        self.retire()?;
                    }
                    return Err(io::Error::other("unrequested holder BYE"));
                }
                if let Some((lease, mut upstream)) = upstream
                    && upstream.write_all(&proto::frame(kind, id, &payload)).is_err()
                {
                    self.detach(lease, "crash");
                }
                // With no upstream every frame is drained and discarded. The
                // holder itself retains the bounded tail for the next HELLO.
            }
            let st = self.lock();
            if st.epoch != epoch {
                return Ok(());
            }
            if st.draining && st.terms.is_empty() {
                drop(st);
                self.retire()?;
            }
            Err(io::Error::other("holder stream lost; effects and programs are not declared ended"))
        })();
        if let Err(error) = result
            && self.lock().epoch == epoch
        {
            self.fail(&error);
        }
    }

    fn retire(&self) -> io::Result<()> {
        let mut st = self.lock();
        st.record.state = "retired".into();
        self.save(&st)?;
        platform::remove_endpoint(&st.record.keeper.socket);
        std::process::exit(0)
    }
}
