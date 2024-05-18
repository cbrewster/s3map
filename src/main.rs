use core::slice;
use std::{
    collections::HashSet,
    ffi::c_void,
    io::{self, ErrorKind},
    ops::{Deref, DerefMut},
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use nix::{
    sys::mman::{mmap, munmap, MapFlags, ProtFlags},
    unistd::{sysconf, SysconfVar},
};
use tokio::{
    io::{unix::AsyncFd, AsyncReadExt},
    runtime, select,
    sync::mpsc,
    task::{spawn_local, JoinError, LocalSet},
};
use userfaultfd::{Event, FeatureFlags, ReadWrite, RegisterMode, Uffd, UffdBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let mut s3map = S3Map::new(client, String::from("s3map-demo"), 16, 1)?;

    let slice = s3map.as_mut_slice();
    // Initially set things to all As
    // for c in slice.iter_mut() {
    //     *c = 'A' as u8;
    // }

    // Print out a slice of the contents of memory
    println!("{:?}", &slice[0..16]);
    // Perform some writes in the memory
    for c in slice.iter_mut() {
        *c += 1;
    }
    // Wait a few seconds to demonstrate background persistence of the memory.
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("{:?}", &slice[0..16]);

    // Perform some writes in the memory
    for c in slice.iter_mut() {
        *c += 1;
    }
    println!("{:?}", &slice[0..16]);
    // Writes should be persisted when S3Map is dropped.

    Ok(())
}

/// A wrapper around a [Uffd] to support async reading of events.
struct AsyncUffd {
    inner: AsyncFd<Uffd>,
}

impl AsyncUffd {
    /// Creates a new async [Uffd] wrapper. This must be created in the tokio runtime it is
    /// intended to be used in.
    fn new(uffd: Uffd) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(uffd)?,
        })
    }

    /// Reads one userfaultfd event.
    async fn read(&mut self) -> io::Result<Event> {
        loop {
            let mut guard = self.inner.readable().await?;

            match guard.try_io(|inner| match inner.get_ref().read_event() {
                Ok(Some(event)) => Ok(event),
                Ok(None) => Err(ErrorKind::WouldBlock.into()),
                Err(e) => Err(io::Error::new(ErrorKind::Other.into(), e)),
            }) {
                Ok(result) => return result,
                Err(_would_bock) => continue,
            }
        }
    }
}

impl TryFrom<Uffd> for AsyncUffd {
    type Error = io::Error;

    fn try_from(uffd: Uffd) -> io::Result<Self> {
        AsyncUffd::new(uffd)
    }
}

impl Deref for AsyncUffd {
    type Target = Uffd;

    fn deref(&self) -> &Self::Target {
        self.inner.get_ref()
    }
}

impl DerefMut for AsyncUffd {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.get_mut()
    }
}

/// A memmapped region that is backed by objected in S3. Writes are periodically
/// persisted back to S3 and are flushed when the S3Map is dropped.
pub struct S3Map {
    addr: *mut c_void,
    len: usize,
    join_handle: Option<JoinHandle<Result<Result<()>, JoinError>>>,
    tx: Option<mpsc::Sender<()>>,
}

impl S3Map {
    /// Creates a new S3Map backed by the specified S3 bucket.
    pub fn new(
        client: aws_sdk_s3::Client,
        bucket: String,
        page_count: usize,
        pages_per_object: usize,
    ) -> Result<Self> {
        let page_size = sysconf(SysconfVar::PAGE_SIZE)?.context("getting page size")? as usize;

        ensure!(
            (pages_per_object & (pages_per_object - 1)) == 0,
            "pages per object must be a power of 2"
        );
        ensure!(
            page_count % pages_per_object == 0,
            "page count must be a multiple of pages per object"
        );
        ensure!(
            page_count >= pages_per_object,
            "page count must be >= pages per object"
        );

        let uffd = UffdBuilder::new()
            .close_on_exec(true)
            .non_blocking(true)
            .require_features(FeatureFlags::PAGEFAULT_FLAG_WP)
            .create()
            .context("creating userfaultfd")?;

        let len = page_count * page_size;
        let addr = unsafe {
            mmap(
                None,
                len.try_into()?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
                -1,
                0,
            )
            .context("mmap")?
        };

        uffd.register_with_mode(
            addr,
            len,
            RegisterMode::WRITE_PROTECT | RegisterMode::MISSING,
        )
        .context("registering mmap range with uffd")?;

        uffd.write_protect(addr, len).unwrap();

        let (tx, rx) = mpsc::channel::<()>(1);

        // The page fault handler _must_ run in a different thread than the one where memory is
        // being accessed. Otherwise things can deadlock because the page fault cannot be handled
        // because the thread that is blocked on the read.
        let start = addr as usize;
        let join_handle = thread::spawn(move || {
            // We use a tokio async runtime to make it easier to work with epoll and async I/O.
            let rt = runtime::Runtime::new().unwrap();
            LocalSet::new().block_on(&rt, async move {
                spawn_local(Self::handler(
                    client,
                    bucket,
                    uffd,
                    rx,
                    start,
                    page_size,
                    pages_per_object,
                ))
                .await
            })
        });

        Ok(Self {
            addr,
            len,
            join_handle: Some(join_handle),
            tx: Some(tx),
        })
    }

    /// Returns a mutable slice containing the entire mmapped region.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.addr as *mut u8, self.len) }
    }

    /// Returns a slice containing the entire mmapped region.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.addr as *mut u8, self.len) }
    }

    /// Returns the length in bytes of the mmapped region.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Handle page faults and continually persist data back to S3.
    async fn handler(
        mut client: aws_sdk_s3::Client,
        bucket: String,
        uffd: Uffd,
        mut drop_rx: mpsc::Receiver<()>,
        start: usize,
        page_size: usize,
        pages_per_object: usize,
    ) -> Result<()> {
        let mut uffd = AsyncUffd::new(uffd)?;

        let mut mapped = HashSet::new();
        let mut dirty = HashSet::new();
        let object_len = page_size * pages_per_object;
        let mut buf = vec![0u8; object_len];

        let mut flush_ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            select! {
                event = uffd.read() => match &event? {
                    Event::Pagefault { addr, rw, .. } => {
                        let offset = (*addr as usize) - start;
                        let offset = offset & !(object_len - 1);

                        let dst = (start + offset) as *mut c_void;

                        // We can only map the page once, so if we'll skip this if we've already mapped the
                        // page.
                        if mapped.insert(offset) {
                            let resp = client
                                .get_object()
                                .bucket(&bucket)
                                .key(format!("{:016X}", offset))
                                .send()
                                .await;
                            match resp {
                                Ok(resp) => {
                                    resp.body.into_async_read().read_exact(&mut buf).await?;
                                },
                                Err(aws_sdk_s3::error::SdkError::ServiceError(e)) if e.err().is_no_such_key() => {
                                    for byte in &mut buf {
                                        *byte = 0;
                                    }
                                },
                                Err(e) => return Err(e.into()),
                            }
                            unsafe {
                                uffd.copy(buf.as_ptr() as *const c_void, dst, object_len, true)?;
                            }
                            uffd.write_protect(dst, object_len)?;
                            continue;
                        }

                        // If this is a write, we'll add it to the dirty list and allow the thread
                        // to write. We'll persist the changes in the background.
                        if *rw == ReadWrite::Write {
                            dirty.insert(offset);
                            uffd.remove_write_protection(dst, object_len, true)?;
                            continue;
                        }

                        // At a minimum we need to wake the thread so it can continue.
                        uffd.wake(*addr, page_size)?;
                    }
                    _ => {},
                },
                // This is closed when the S3Map is dropped, so we'll persist one more time and
                // then exit the loop.
                _ = drop_rx.recv() => {
                    S3Map::flush_dirty(&uffd, &mut client, &bucket, &mut dirty, start, object_len).await?;
                    return Ok(())
                },
                // At an interval we'll flush dirty ranges back to S3.
                _ = flush_ticker.tick() => {
                    S3Map::flush_dirty(&uffd, &mut client, &bucket, &mut dirty, start, object_len).await?;
                    continue
                },
            }
        }
    }

    /// Flushes all dirty page ranges to S3 and sets up new write protect ranges.
    async fn flush_dirty(
        uffd: &AsyncUffd,
        client: &mut aws_sdk_s3::Client,
        bucket: &str,
        dirty: &mut HashSet<usize>,
        start: usize,
        pages_per_object: usize,
    ) -> Result<()> {
        for offset in dirty.drain() {
            let addr = (start + offset) as *mut c_void;
            // By write protecting the memory, we ensure it cannot be written to while we are
            // persisting it. If it is written after persisting, we will get an event for it.
            uffd.write_protect(addr, pages_per_object)?;
            let slice = unsafe { slice::from_raw_parts(addr as *const u8, pages_per_object) };
            client
                .put_object()
                .bucket(bucket)
                .key(format!("{:016X}", offset))
                .body(ByteStream::from(slice.to_vec()))
                .send()
                .await?;
        }
        Ok(())
    }
}

impl Drop for S3Map {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .expect("failed to join thread")
                .expect("failed to join async task")
                .expect("failed to handle");
        }
        unsafe {
            munmap(self.addr, self.len).expect("munmap failed");
        }
    }
}
