//! Pins for the owned, bounded `run` collector seam (M2-K6 round 3; R9,
//! honest failure): a torn pipe and a dead pump task are TYPED errors,
//! never defaulted empty success; past the cap the collector reports
//! overflow and its cut closes the read end under the writer.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use jinnd_api::ErrorCode;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

use super::collector::{Collector, RUN_OUTPUT_CAP};
use super::stream::Stream;

/// A pipe that tears on the first read.
struct Torn;

impl AsyncRead for Torn {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("pipe torn")))
    }
}

#[tokio::test]
async fn a_torn_pipe_is_a_typed_error_never_empty_success() {
    let mut collector = Collector::start(Torn);
    assert!(collector.drain().await.is_ok(), "the stream ends");
    let error = collector.finish().await.expect_err("honest failure");
    assert_eq!(error.code, ErrorCode::PluginFailed);
    assert!(error.message.contains("pipe torn"), "{}", error.message);
}

#[tokio::test]
async fn a_dead_pump_task_is_a_typed_error_never_empty_success() {
    let stream = Stream::new();
    stream.close();
    let pump = tokio::spawn(std::future::pending());
    pump.abort();
    let collector = Collector::from_parts(stream, pump);
    let error = collector.finish().await.expect_err("honest failure");
    assert_eq!(error.code, ErrorCode::PluginFailed);
    assert!(error.message.contains("collector task"), "{}", error.message);
}

/// Past the cap the collector answers overflow with at most the cap plus
/// one chunk in memory; its cut drops the read end, so the writer's next
/// write fails — the EPIPE a descendant gets from a real pipe.
#[tokio::test]
async fn output_past_the_cap_is_overflow_and_the_cut_fails_the_writer() {
    let (reader, mut writer) = tokio::io::duplex(64 * 1024);
    let writer = tokio::spawn(async move {
        let chunk = vec![b'x'; 8192];
        loop {
            if let Err(error) = writer.write_all(&chunk).await {
                return error.kind();
            }
        }
    });
    let mut collector = Collector::start(reader);
    assert!(collector.drain().await.is_err(), "overflow past the cap");
    assert!(collector.buffered() <= RUN_OUTPUT_CAP + 8192);
    collector.cut().await;
    let kind = writer.await.unwrap_or_else(|error| panic!("writer: {error}"));
    assert_eq!(kind, io::ErrorKind::BrokenPipe);
}

/// The plain path: everything the pipe carried, then a clean finish.
#[tokio::test]
async fn a_finished_pipe_answers_every_byte() {
    let (reader, mut writer) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let _ = writer.write_all(b"hello").await;
    });
    let mut collector = Collector::start(reader);
    assert!(collector.drain().await.is_ok());
    assert_eq!(collector.finish().await, Ok(b"hello".to_vec()));
}
