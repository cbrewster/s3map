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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let mut s3map = S3Map::new(client, String::from("s3map-demo"), 16, 1)?;

    let slice = s3map.as_mut_slice();
    // Initially set things to all As
    // for c in slice.iter_mut() {
    //     *c = 'A' as u8;
    // }
    println!("{:?}", &slice[0..16]);
    for c in slice.iter_mut() {
        *c += 1;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("{:?}", &slice[0..16]);
    for c in slice.iter_mut() {
        *c += 1;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("{:?}", &slice[0..16]);

    Ok(())
}

struct AsyncUffd {
    inner: AsyncFd<Uffd>,
}

impl AsyncUffd {
    fn new(uffd: Uffd) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(uffd)?,
        })
    }

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

pub struct S3Map {
    addr: *mut c_void,
    len: usize,
    join_handle: Option<JoinHandle<Result<Result<()>, JoinError>>>,
    tx: Option<mpsc::Sender<()>>,
}

impl S3Map {
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

        let start = addr as usize;
        let join_handle = thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
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

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.addr as *mut u8, self.len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.addr as *mut u8, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

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

                        // We can only map once, so if we've already mapped, we'll just wake the
                        // thread.
                        // TODO: Handle write protecting ranges and queuing up writes back to the file.
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
                            continue
                        }

                        if *rw == ReadWrite::Write {
                            dirty.insert(offset);
                            uffd.remove_write_protection(dst, object_len, true)?;
                            continue;
                        }

                        uffd.wake(*addr, page_size)?;
                    }
                    _ => todo!(),
                },
                _ = drop_rx.recv() => {
                    S3Map::flush_dirty(&uffd, &mut client, &bucket, &mut dirty, start, object_len).await?;
                    return Ok(())
                },
                _ = flush_ticker.tick() => {
                    S3Map::flush_dirty(&uffd, &mut client, &bucket, &mut dirty, start, object_len).await?;
                    continue
                },
            }
        }
    }

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
                .expect("failed to join")
                .expect("handler error")
                .expect("idk lol");
        }
        unsafe {
            munmap(self.addr, self.len).expect("munmap failed");
        }
    }
}
