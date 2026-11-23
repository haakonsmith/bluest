//! A CoreBluetooth L2CAP channel, read and written through the native socket under its streams.
//!
//! A channel's `NSInputStream` and `NSOutputStream` are toll-free bridged to CFStreams backed by one
//! Unix socket, which `kCFStreamPropertySocketNativeHandle` gives. The streams are never opened.
//! Scheduled on the main run loop, they never deliver their events in a program whose main thread
//! doesn't run one, such as a tokio program's, so nothing is ever read. A thread each reads and
//! writes a copy of the socket with blocking calls instead, through the same pipes as the other
//! platforms. The reader's thread waits while its pipe is full, which leaves the data in the
//! socket, so the channel's credits hold the sender up.

use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::fd::{BorrowedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, pin};

use futures_lite::io::{AsyncRead, AsyncWrite, BlockOn};
use objc2::rc::Retained;
use objc2_core_bluetooth::CBL2CAPChannel;
use objc2_core_foundation::{CFStreamPropertyKey, kCFStreamPropertySocketNativeHandle};
use objc2_foundation::{NSData, NSString};
use tracing::debug;

use super::dispatch::Dispatched;
use crate::Result;
use crate::error::{Error, ErrorKind};
use crate::l2cap_channel::{PIPE_CAPACITY, derive_async_read, derive_async_write};

fn unavailable() -> Error {
    Error::new(
        ErrorKind::ConnectionFailed,
        None,
        "the L2CAP channel has no native socket",
    )
}

fn socket_error(e: std::io::Error) -> Error {
    Error::new(ErrorKind::ConnectionFailed, Some(Box::new(e)), "")
}

/// A copy of the channel's socket. CoreBluetooth keeps its own, so this one can be closed
fn socket(channel: &CBL2CAPChannel) -> Result<UnixStream> {
    // SAFETY: an extern static, which CoreFoundation defines
    let key = unsafe { kCFStreamPropertySocketNativeHandle }.ok_or_else(unavailable)?;
    // SAFETY: CFString is toll-free bridged to NSString, so the key is both, and objc2 has no safe
    // conversion between them
    let key: &NSString = unsafe { &*(key as *const CFStreamPropertyKey).cast::<NSString>() };
    // SAFETY: a property read, which objc2 marks unsafe only as its binding isn't audited
    let input = unsafe { channel.inputStream() }.ok_or_else(unavailable)?;

    // A CFNativeSocketHandle in a CFData
    let handle = input.propertyForKey(key).ok_or_else(unavailable)?;
    let handle: Retained<NSData> = handle.downcast().map_err(|_| unavailable())?;
    let fd = RawFd::from_ne_bytes(handle.to_vec().try_into().map_err(|_| unavailable())?);

    // SAFETY: the channel's socket, which CoreBluetooth keeps open while the channel lives, and the
    // caller holds the channel
    let fd = unsafe { BorrowedFd::borrow_raw(fd) }
        .try_clone_to_owned()
        .map_err(socket_error)?;
    let socket = UnixStream::from(fd);
    // The file status flags are the socket's, shared with CoreBluetooth's copy, which nothing uses
    // as its streams are never opened
    socket.set_nonblocking(false).map_err(socket_error)?;

    Ok(socket)
}

/// The socket, sent out of the dispatch queue's closure
struct SendSocket(UnixStream);

/// Keeps the channel open while its reader or writer lives, and shuts the socket when both are
/// gone, which ends the threads blocked on it
pub(super) struct L2capCloser {
    _channel: Dispatched<CBL2CAPChannel>,
    socket: UnixStream,
}

impl L2capCloser {
    fn close(&self) {
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

impl Drop for L2capCloser {
    fn drop(&mut self) {
        self.close()
    }
}

pub struct L2capChannel {
    pub(super) reader: L2capChannelReader,
    pub(super) writer: L2capChannelWriter,
}

impl L2capChannel {
    /// Opens `channel` for reading and writing through its socket
    pub(super) fn open(channel: Dispatched<CBL2CAPChannel>) -> Result<Self> {
        let socket = channel.dispatch(|channel| socket(channel).map(SendSocket))?.0;
        let mut input = socket.try_clone().map_err(socket_error)?;
        let mut output = socket.try_clone().map_err(socket_error)?;
        let closer = Arc::new(L2capCloser {
            _channel: channel,
            socket,
        });

        let (read_rx, read_tx) = piper::pipe(PIPE_CAPACITY);
        std::thread::spawn(move || {
            let mut read_tx = BlockOn::new(read_tx);
            let mut buf = [0u8; 4096];
            loop {
                match input.read(&mut buf) {
                    Ok(0) => {
                        debug!("L2CAP channel ended");
                        return;
                    }
                    // Blocks while the pipe is full, which leaves the rest in the socket. Fails
                    // once the reader is dropped
                    Ok(read) => {
                        if read_tx.write_all(&buf[..read]).is_err() {
                            return;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        debug!("L2CAP channel read failed: {e}");
                        return;
                    }
                }
            }
        });

        let (write_rx, write_tx) = piper::pipe(PIPE_CAPACITY);
        std::thread::spawn(move || {
            // Ends once the writer is dropped or closed, and what it wrote has gone
            if let Err(e) = std::io::copy(&mut BlockOn::new(write_rx), &mut output) {
                debug!("L2CAP channel write failed: {e}");
            }
        });

        Ok(Self {
            reader: L2capChannelReader {
                stream: read_rx,
                _closer: closer.clone(),
            },
            writer: L2capChannelWriter {
                stream: write_tx,
                closer,
            },
        })
    }

    pub fn split(self) -> (L2capChannelReader, L2capChannelWriter) {
        (self.reader, self.writer)
    }
}

derive_async_read!(L2capChannel, reader);
derive_async_write!(L2capChannel, writer);

/// The reader side of an L2CAP channel.
pub struct L2capChannelReader {
    stream: piper::Reader,
    _closer: Arc<L2capCloser>,
}

derive_async_read!(L2capChannelReader, stream);

impl fmt::Debug for L2capChannelReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("L2capChannelReader")
    }
}

/// The writer side of an L2CAP channel.
pub struct L2capChannelWriter {
    stream: piper::Writer,
    closer: Arc<L2capCloser>,
}

impl AsyncWrite for L2capChannelWriter {
    fn poll_write(mut self: pin::Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let stream = pin::pin!(&mut self.stream);
        stream.poll_write(cx, buf)
    }

    fn poll_flush(mut self: pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let stream = pin::pin!(&mut self.stream);
        stream.poll_flush(cx)
    }

    fn poll_close(mut self: pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.closer.close();
        let stream = pin::pin!(&mut self.stream);
        stream.poll_close(cx)
    }
}

impl fmt::Debug for L2capChannelWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("L2capChannelWriter")
    }
}
