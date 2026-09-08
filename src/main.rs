//! weeb-3-rs-hls: a standalone Swarm client that connects to peers and
//! watches an HLS stream. Native port of weeb-3, viewer scope only.
//!
//! The node is `!Send` (the browser original is single-threaded and uses
//! `Rc`/`RefCell` throughout), so it runs on a current-thread runtime with a
//! `LocalSet` rather than on the multi-thread scheduler.
//!
//! ```text
//! weeb-3-rs-hls [testnet]                          # peer only
//! weeb-3-rs-hls watch <owner> <topic> [--live]     # watch a stream
//! weeb-3-rs-hls get <reference>                    # retrieve one reference
//! weeb-3-rs-hls feed <owner> <topic>               # resolve one feed update
//! ```
//!
//! Without `--live`, `watch` reads one playlist snapshot and pulls it back to
//! back. With `--live` it joins at the live edge, follows the feed forward and
//! paces playback against a wall clock, so a segment that has not been published
//! yet actually stalls the playhead.

use std::time::{Duration, Instant};

/// The system allocator keeps freed pages dirty: a watch run churns ~26k 4 KB
/// chunk buffers, and `vmmap` showed megabytes sitting in MALLOC regions marked
/// empty. mimalloc returns them, which is most of the difference between the
/// live set and RSS.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
use weeb_3::viewer::{
    LiveStream, SWARM_MAINNET, SWARM_TESTNET, StreamPlaylist, Viewer, run_on_local_set,
};

/// Peers to wait for before asking the network for anything.
const WATCH_MINIMUM_PEERS: u64 = 25;
const WATCH_PEER_TIMEOUT_MS: u64 = 60_000;
/// Segments to pull in the watch run. Enough to prove a real playback runway.
const WATCH_SEGMENTS: usize = 8;
/// Hard cap on one segment's fetch, as a multiple of its own duration.
const SEGMENT_FETCH_ALLOWANCE_FACTOR: f64 = 4.0;

/// What the process does once it has peered.
enum Mode {
    /// Peer only, reporting connection counts.
    Peer,
    /// Resolve a stream feed and pull its segments.
    Watch(String, String),
    /// Join a stream at its live edge and follow the feed forward.
    WatchLive(String, String),
    /// Retrieve one reference and report what came back.
    Get(String),
    /// Resolve one feed update and report its payload.
    Feed(String, String),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = match args.first().map(String::as_str) {
        Some("watch") => {
            let (Some(owner), Some(topic)) = (args.get(1), args.get(2)) else {
                eprintln!(
                    "usage: weeb-3-rs-hls watch <owner> <topic> [--live] [--segments <n>] [testnet]"
                );
                std::process::exit(2);
            };
            if args.iter().any(|arg| arg == "--live") {
                Mode::WatchLive(owner.clone(), topic.clone())
            } else {
                Mode::Watch(owner.clone(), topic.clone())
            }
        }
        Some("get") => {
            let Some(reference) = args.get(1) else {
                eprintln!("usage: weeb-3-rs-hls get <reference> [--out <file>] [testnet]");
                std::process::exit(2);
            };
            Mode::Get(reference.clone())
        }
        Some("feed") => {
            let (Some(owner), Some(topic)) = (args.get(1), args.get(2)) else {
                eprintln!("usage: weeb-3-rs-hls feed <owner> <topic> [--out <file>] [testnet]");
                std::process::exit(2);
            };
            Mode::Feed(owner.clone(), topic.clone())
        }
        _ => Mode::Peer,
    };
    // `--segments <n>` overrides how much of the playlist to watch.
    let segments = args
        .iter()
        .position(|arg| arg == "--segments")
        .and_then(|at| args.get(at + 1))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(WATCH_SEGMENTS);
    // `--idle <n>` keeps the process alive after watching, so a settled heap can
    // be inspected with `heap`/`vmmap`.
    let idle = args
        .iter()
        .position(|arg| arg == "--idle")
        .and_then(|at| args.get(at + 1))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    // `--out <dir>` writes each segment body to disk so it can be played or
    // probed with an ordinary tool.
    let out_dir = args
        .iter()
        .position(|arg| arg == "--out")
        .and_then(|at| args.get(at + 1))
        .map(std::path::PathBuf::from);
    let network_id = if args.iter().any(|arg| arg == "testnet") {
        SWARM_TESTNET
    } else {
        SWARM_MAINNET
    };

    run_on_local_set(async move {
        let viewer = Viewer::new();
        viewer.start(network_id).await?;
        tracing::info!(network_id, "node started");

        viewer.connect_bootnodes(network_id).await;
        tracing::info!("dialing bootnodes");

        match mode {
            Mode::Watch(owner, topic) => {
                watch_stream(&viewer, &owner, &topic, segments, idle, out_dir.as_deref()).await
            }
            Mode::WatchLive(owner, topic) => {
                watch_live_stream(&viewer, &owner, &topic, segments, idle, out_dir.as_deref()).await
            }
            Mode::Get(reference) => get_reference(&viewer, &reference, out_dir.as_deref()).await,
            Mode::Feed(owner, topic) => show_feed(&viewer, &owner, &topic, out_dir.as_deref()).await,
            Mode::Peer => {
                for tick in 1..=24u32 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let peers = viewer.connections().await;
                    tracing::info!(peers, elapsed_s = tick * 5, "peers");
                    drain(&viewer);
                }
                Ok(())
            }
        }
    })??;

    Ok(())
}

async fn watch_stream(
    viewer: &Viewer,
    owner: &str,
    topic: &str,
    segments: usize,
    idle: u64,
    out_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    peer_up(viewer).await?;

    let started = Instant::now();
    let playlist = viewer.playlist(owner, topic).await?;
    drain(viewer);
    tracing::info!(
        resolve_ms = started.elapsed().as_millis() as u64,
        feed_index = playlist.feed_index,
        sequence = playlist.sequence,
        segments = playlist.segments.len(),
        duration_s = playlist.duration(),
        target_duration = playlist.target_duration,
        finalized = playlist.finalized,
        "playlist resolved"
    );

    if let Some(dir) = out_dir {
        std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    }
    play(viewer, &playlist, segments, out_dir).await?;

    if idle > 0 {
        tracing::info!(idle_s = idle, "idling, pid {}", std::process::id());
        tokio::time::sleep(Duration::from_secs(idle)).await;
        tracing::info!("after idle {}", viewer.cache_report());
    }
    Ok(())
}

/// Pull the head of the playlist segment by segment, timing each retrieval
/// against the duration it is supposed to cover. A real player stalls when a
/// segment lands later than the buffer it still has; that margin is the only
/// thing a viewer actually experiences, so it is what we record.
async fn play(
    viewer: &Viewer,
    playlist: &StreamPlaylist,
    want: usize,
    out_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    let mut buffered = 0.0_f64;
    let mut total = 0_usize;
    let mut stalls = 0_u32;
    let mut total_segments = 0_usize;

    for segment in playlist.segments.iter().take(want) {
        let started = Instant::now();
        let bytes = viewer.fetch_segment(segment).await?;
        drain(viewer);
        let elapsed = started.elapsed().as_secs_f64();

        // First segment has no buffer to spend, so it can never be "late".
        let stalled = total > 0 && elapsed > buffered;
        if stalled {
            stalls += 1;
        }
        buffered = (buffered - elapsed).max(0.0) + segment.duration;
        total += bytes.len();
        total_segments += 1;

        if let Some(dir) = out_dir {
            let path = dir.join(format!("{:06}.seg", segment.sequence));
            std::fs::write(&path, &bytes).map_err(|error| error.to_string())?;
        }

        tracing::info!(
            sequence = segment.sequence,
            reference = %segment.reference,
            bytes = bytes.len(),
            fetch_ms = (elapsed * 1000.0) as u64,
            segment_s = segment.duration,
            buffered_s = format!("{buffered:.2}"),
            mpeg_ts = looks_like_mpeg_ts(&bytes),
            stalled,
            peers = viewer.connections().await,
            "segment"
        );

        // Attribute RSS growth to a named cache rather than to "memory".
        if total_segments.is_multiple_of(25) {
            tracing::info!(segments = total_segments, "{}", viewer.cache_report());
        }
    }

    tracing::info!(
        segments = want.min(playlist.segments.len()),
        bytes = total,
        stalls,
        peers = viewer.connections().await,
        "watched"
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Join a stream at its live edge and follow it.
async fn watch_live_stream(
    viewer: &Viewer,
    owner: &str,
    topic: &str,
    segments: usize,
    idle: u64,
    out_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    peer_up(viewer).await?;

    let started = Instant::now();
    let live = viewer.watch_live(owner, topic).await?;
    drain(viewer);
    let playlist = live.playlist();
    tracing::info!(
        join_ms = started.elapsed().as_millis() as u64,
        feed_index = live.joined_at(),
        edge_sequence = live.live_sequence(),
        start_sequence = live.start_sequence(),
        runway_s = format!("{:.2}", live.runway_seconds()),
        window = playlist.segments.len(),
        target_duration = playlist.target_duration,
        "joined live edge"
    );

    if let Some(dir) = out_dir {
        std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    }
    play_live(viewer, &live, segments, out_dir).await?;

    if idle > 0 {
        tracing::info!(idle_s = idle, "idling, pid {}", std::process::id());
        tokio::time::sleep(Duration::from_secs(idle)).await;
        tracing::info!("after idle {}", viewer.cache_report());
    }
    Ok(())
}

/// Play a live stream against a wall clock.
///
/// The difference from [`play`] is the clock. `play` fetches back to back, so it
/// can measure throughput but can never observe a stall: there is no notion of
/// the playhead running dry, only of a segment arriving later than the buffer it
/// had. Here the playhead advances in real time, so a segment the publisher has
/// not written yet actually blocks it, and the time it blocks for is a stall.
///
/// The playhead is `wall_elapsed - stalled_total`: playback runs at 1x except
/// while stalled. A segment is late when it lands after the playhead has already
/// consumed everything acquired before it.
async fn play_live(
    viewer: &Viewer,
    live: &LiveStream,
    want: usize,
    out_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    // Keep no more buffer than the runway we joined with; a real player throttles
    // once its buffer target is met rather than racing the publisher.
    let target_buffer = live.runway_seconds().max(1.0);
    let clock = Instant::now();

    let mut sequence = live.start_sequence();
    let mut media = 0.0_f64;
    let mut stalled_total = 0.0_f64;
    let mut stalls = 0_u32;
    let mut played = 0_usize;
    let mut total = 0_usize;

    while played < want {
        let Some(segment) = live.segment(sequence).await else {
            let playlist = live.playlist();
            if sequence < playlist.sequence {
                // The window slid past us while we were behind. A real player
                // seeks to the edge rather than waiting for segments that are
                // gone, and the skip is the thing worth recording.
                tracing::warn!(
                    from = sequence,
                    to = playlist.sequence,
                    skipped = live.skipped(),
                    "fell off the live window; seeking to the edge"
                );
                sequence = playlist.sequence;
                continue;
            }
            tracing::info!(sequence, "publisher stopped short of this sequence");
            break;
        };

        if segment.gap {
            tracing::warn!(sequence, "gap segment; stepping over it");
            sequence += 1;
            continue;
        }

        // Spend at most the buffer we actually hold on one segment. A body that
        // will never retrieve otherwise costs the whole buffer and then some —
        // measured at 16.6 s on one bad segment, which cascaded into falling off
        // the live window. Waiting inside the buffer is free; waiting past it is
        // a stall, and no single segment is worth one.
        let buffer_now = (media - (clock.elapsed().as_secs_f64() - stalled_total)).max(0.0);
        let allowance = buffer_now
            .max(segment.duration)
            .min(SEGMENT_FETCH_ALLOWANCE_FACTOR * segment.duration);

        let fetch_started = Instant::now();
        let fetch = viewer.fetch_segment(&segment);
        let bytes = match async_std::future::timeout(Duration::from_secs_f64(allowance), fetch).await
        {
            Ok(Ok(bytes)) => bytes,
            outcome => {
                // Two failures on the same segment turn it into a gap upstream;
                // here one is enough to stop blocking the playhead on it.
                live.mark_gap(&segment);
                let reason = match outcome {
                    Ok(Err(error)) => error,
                    _ => format!("no body within {allowance:.1}s"),
                };
                tracing::warn!(sequence, "{reason}; marked as a gap");
                sequence += 1;
                continue;
            }
        };
        drain(viewer);
        let fetch_ms = fetch_started.elapsed().as_millis() as u64;

        // Wall time by which the playhead had consumed everything acquired so
        // far. Landing after it means the playhead was dry in between.
        let due = media + stalled_total;
        let now = clock.elapsed().as_secs_f64();
        let stalled = now > due && played > 0;
        if stalled {
            stalls += 1;
            stalled_total += now - due;
        }

        media += segment.duration;
        total += bytes.len();
        played += 1;

        if let Some(dir) = out_dir {
            let path = dir.join(format!("{:06}.seg", segment.sequence));
            std::fs::write(&path, &bytes).map_err(|error| error.to_string())?;
        }

        let buffered = media - (clock.elapsed().as_secs_f64() - stalled_total);
        tracing::info!(
            sequence = segment.sequence,
            reference = %segment.reference,
            bytes = bytes.len(),
            fetch_ms,
            segment_s = segment.duration,
            buffered_s = format!("{buffered:.2}"),
            feed_index = live.feed_index(),
            mpeg_ts = looks_like_mpeg_ts(&bytes),
            stalled,
            peers = viewer.connections().await,
            "segment"
        );

        if played.is_multiple_of(25) {
            tracing::info!(segments = played, "{}", viewer.cache_report());
        }
        sequence += 1;

        // Hold the buffer at its target instead of running ahead of the clock.
        let ahead = media - (clock.elapsed().as_secs_f64() - stalled_total);
        if ahead > target_buffer {
            async_std::task::sleep(Duration::from_secs_f64(ahead - target_buffer)).await;
        }
    }

    tracing::info!(
        segments = played,
        bytes = total,
        media_s = format!("{media:.1}"),
        wall_s = format!("{:.1}", clock.elapsed().as_secs_f64()),
        stalls,
        stalled_s = format!("{stalled_total:.2}"),
        skipped = live.skipped(),
        finalized = live.finalized(),
        peers = viewer.connections().await,
        "watched live"
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Retrieve one reference from peers and report what came back.
///
/// The gateway is never consulted, so a success here proves the content is
/// reachable in the network rather than merely accepted by an upload endpoint.
async fn get_reference(
    viewer: &Viewer,
    reference: &str,
    out: Option<&std::path::Path>,
) -> Result<(), String> {
    peer_up(viewer).await?;

    let started = Instant::now();
    let bytes = viewer.retrieve_payload(reference).await?;
    drain(viewer);
    let fetch_ms = started.elapsed().as_millis() as u64;
    if let Some(path) = out {
        std::fs::write(path, &bytes).map_err(|error| error.to_string())?;
    }

    tracing::info!(
        reference = %reference,
        bytes = bytes.len(),
        fetch_ms,
        mpeg_ts = looks_like_mpeg_ts(&bytes),
        head = %hex::encode(&bytes[..bytes.len().min(8)]),
        peers = viewer.connections().await,
        "retrieved"
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Resolve one feed update from peers and report its payload.
async fn show_feed(
    viewer: &Viewer,
    owner: &str,
    topic: &str,
    out: Option<&std::path::Path>,
) -> Result<(), String> {
    let peers = peer_up(viewer).await?;

    let started = Instant::now();
    let (index, bytes) = viewer.resolve_feed(owner, topic).await?;
    drain(viewer);
    let resolve_ms = started.elapsed().as_millis() as u64;
    if let Some(path) = out {
        std::fs::write(path, &bytes).map_err(|error| error.to_string())?;
    }

    tracing::info!(
        owner = %owner,
        topic = %topic,
        feed_index = index,
        bytes = bytes.len(),
        resolve_ms,
        peers,
        "feed resolved"
    );
    // A stream feed's payload is the playlist itself, so it is worth seeing.
    if let Ok(text) = std::str::from_utf8(&bytes) {
        println!("{text}");
    }
    Ok(())
}

/// Wait for enough peers to ask the network for anything.
async fn peer_up(viewer: &Viewer) -> Result<u64, String> {
    let peers = viewer
        .wait_for_connections(WATCH_MINIMUM_PEERS, WATCH_PEER_TIMEOUT_MS)
        .await;
    tracing::info!(peers, "peered");
    if peers == 0 {
        return Err("no peers; cannot retrieve anything".to_string());
    }
    Ok(peers)
}

/// MPEG-TS packets are 188 bytes each and start with the 0x47 sync byte.
fn looks_like_mpeg_ts(bytes: &[u8]) -> bool {
    bytes.len() >= 188 && bytes[0] == 0x47 && bytes[188] == 0x47
}

fn drain(viewer: &Viewer) {
    for line in viewer.drain_logs() {
        tracing::debug!("{line}");
    }
}
