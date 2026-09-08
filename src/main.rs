//! weeb-3-rs-hls: a standalone Swarm client that connects to peers and
//! watches an HLS stream. Native port of weeb-3, viewer scope only.
//!
//! The node is `!Send` (the browser original is single-threaded and uses
//! `Rc`/`RefCell` throughout), so it runs on a current-thread runtime with a
//! `LocalSet` rather than on the multi-thread scheduler.
//!
//! ```text
//! weeb-3-rs-hls [testnet]                     # peer only
//! weeb-3-rs-hls watch <owner> <topic> [testnet]
//! ```

use std::time::{Duration, Instant};

/// The system allocator keeps freed pages dirty: a watch run churns ~26k 4 KB
/// chunk buffers, and `vmmap` showed megabytes sitting in MALLOC regions marked
/// empty. mimalloc returns them, which is most of the difference between the
/// live set and RSS.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
use weeb_3::viewer::{SWARM_MAINNET, SWARM_TESTNET, StreamPlaylist, Viewer, run_on_local_set};

/// Peers to wait for before asking the network for anything.
const WATCH_MINIMUM_PEERS: u64 = 25;
const WATCH_PEER_TIMEOUT_MS: u64 = 60_000;
/// Segments to pull in the watch run. Enough to prove a real playback runway.
const WATCH_SEGMENTS: usize = 8;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let watch = match args.first().map(String::as_str) {
        Some("watch") => {
            let (Some(owner), Some(topic)) = (args.get(1), args.get(2)) else {
                eprintln!("usage: weeb-3-rs-hls watch <owner> <topic> [testnet]");
                std::process::exit(2);
            };
            Some((owner.clone(), topic.clone()))
        }
        _ => None,
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

        match watch {
            Some((owner, topic)) => watch_stream(&viewer, &owner, &topic, segments, idle, out_dir.as_deref()).await,
            None => {
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
    let peers = viewer
        .wait_for_connections(WATCH_MINIMUM_PEERS, WATCH_PEER_TIMEOUT_MS)
        .await;
    tracing::info!(peers, "peered");
    if peers == 0 {
        return Err("no peers; cannot retrieve anything".to_string());
    }

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

/// MPEG-TS packets are 188 bytes each and start with the 0x47 sync byte.
fn looks_like_mpeg_ts(bytes: &[u8]) -> bool {
    bytes.len() >= 188 && bytes[0] == 0x47 && bytes[188] == 0x47
}

fn drain(viewer: &Viewer) {
    for line in viewer.drain_logs() {
        tracing::debug!("{line}");
    }
}
