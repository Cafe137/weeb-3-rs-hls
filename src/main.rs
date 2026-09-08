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
//!
//! `--metrics json` adds a machine-readable record of the run on **stdout**, one
//! JSON object per line, while the human `tracing` output stays on **stderr**.
//! That split exists so a fleet supervisor never parses text: every measurement
//! in `CLAUDE.md` and `LIVE-PLAN.md` was extracted with `sed` and `awk`, which
//! is fine for one viewer and untenable for hundreds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// The system allocator keeps freed pages dirty: a watch run churns ~26k 4 KB
/// chunk buffers, and `vmmap` showed megabytes sitting in MALLOC regions marked
/// empty. mimalloc returns them, which is most of the difference between the
/// live set and RSS.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
use weeb_3::viewer::{
    LiveStream, SWARM_MAINNET, SWARM_TESTNET, StreamPlaylist, StreamSegment, Viewer, peer_limit,
    run_on_local_set, set_peer_limit,
};

/// Peers to wait for before asking the network for anything.
const WATCH_MINIMUM_PEERS: u64 = 25;
const WATCH_PEER_TIMEOUT_MS: u64 = 60_000;
/// Segments to pull in the watch run. Enough to prove a real playback runway.
const WATCH_SEGMENTS: usize = 8;
/// How often the peering curve is reported while watching.
const PEER_REPORT_INTERVAL_MS: u64 = 5_000;
/// Ticks of the peer-only mode, when no `--duration` is given.
const PEER_MODE_DEFAULT_SECONDS: u64 = 120;

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

impl Mode {
    fn name(&self) -> &'static str {
        match self {
            Mode::Peer => "peer",
            Mode::Watch(..) => "vod",
            Mode::WatchLive(..) => "live",
            Mode::Get(_) => "get",
            Mode::Feed(..) => "feed",
        }
    }

    fn stream(&self) -> (Option<&str>, Option<&str>) {
        match self {
            Mode::Watch(owner, topic) | Mode::WatchLive(owner, topic) | Mode::Feed(owner, topic) => {
                (Some(owner), Some(topic))
            }
            _ => (None, None),
        }
    }
}

/// Machine-readable run output: one JSON object per line on stdout.
///
/// Rust's stdout is a `LineWriter`, so each line is flushed as it is written and
/// a supervisor sees events as they happen rather than at exit.
struct Metrics {
    enabled: bool,
    started: Instant,
}

impl Metrics {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            started: Instant::now(),
        }
    }

    /// `t` is milliseconds since this struct was built, which is process start.
    ///
    /// Null-valued fields are dropped rather than emitted: a Rust `Option`
    /// serialises to null, but the contract says a field with no value is
    /// *absent*, and a consumer validating "string or missing" rejects null.
    fn emit(&self, event: &str, mut fields: Value) {
        if !self.enabled {
            return;
        }
        if let Some(object) = fields.as_object_mut() {
            object.retain(|_, value| !value.is_null());
            object.insert("ev".to_string(), json!(event));
            object.insert(
                "t".to_string(),
                json!(self.started.elapsed().as_millis() as u64),
            );
        }
        println!("{fields}");
    }
}

/// A cooperative stop, so a supervisor's SIGTERM still yields a summary.
///
/// Without this a viewer killed by a fleet runner dies mid-run and its
/// measurements are lost, which is every viewer in a fixed-duration cohort.
struct Shutdown {
    requested: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
}

impl Shutdown {
    fn install() -> Self {
        let requested = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(tokio::sync::Notify::new());
        let flag = requested.clone();
        let notify = wake.clone();

        tokio::task::spawn_local(async move {
            let mut terminate = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ) {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!("cannot listen for SIGTERM: {error}");
                    return;
                }
            };
            let mut interrupt = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::interrupt(),
            ) {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!("cannot listen for SIGINT: {error}");
                    return;
                }
            };
            loop {
                tokio::select! {
                    _ = terminate.recv() => {}
                    _ = interrupt.recv() => {}
                }
                // A second signal means the caller has stopped asking politely.
                if flag.swap(true, Ordering::AcqRel) {
                    std::process::exit(130);
                }
                tracing::info!("stop requested; finishing and reporting");
                // `notify_one` stores a permit, so a signal that arrives between
                // a flag check and the next await is not lost.
                notify.notify_one();
            }
        });

        Self { requested, wake }
    }

    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        self.wake.notified().await;
    }
}

/// Everything the run modes need that is not the viewer itself.
struct Run {
    metrics: Metrics,
    shutdown: Shutdown,
    segments: usize,
    duration: Option<Duration>,
    idle: u64,
    out_dir: Option<std::path::PathBuf>,
    /// When the peering curve was last reported. A `Cell` because the run is
    /// shared by reference on a single thread and nothing here is `Send`.
    last_peer_report: std::cell::Cell<Instant>,
}

impl Run {
    /// True once the run has been asked to stop, or has run long enough.
    fn should_stop(&self, elapsed: Duration) -> bool {
        self.shutdown.requested() || self.duration.is_some_and(|limit| elapsed >= limit)
    }

    /// Report the peering curve, at most every [`PEER_REPORT_INTERVAL_MS`].
    ///
    /// Peers keep climbing toward the limit long after the 25 needed to start,
    /// and dial failures rising while peers stay flat is how a fleet tells an
    /// exhausted ephemeral port range from a slow network. Neither is visible
    /// from the segment cadence alone, and neither can be reported from a
    /// background task: `Viewer` is `!Send` and borrowed for the whole run.
    fn report_peers(&self, peers: u64, dial_failures: u64) {
        if self.last_peer_report.get().elapsed() < Duration::from_millis(PEER_REPORT_INTERVAL_MS) {
            return;
        }
        self.last_peer_report.set(Instant::now());
        self.metrics
            .emit("peers", json!({ "peers": peers, "dial_failures": dial_failures }));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Human output on stderr, so `--metrics json` can own stdout.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
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
                    "usage: weeb-3-rs-hls watch <owner> <topic> [--live] [--segments <n>] \
                     [--duration <s>] [--peers <n>] [--metrics json] [testnet]"
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
    let flag = |name: &str| -> Option<&String> {
        args.iter()
            .position(|arg| arg == name)
            .and_then(|at| args.get(at + 1))
    };
    // `--duration <s>` bounds the run in wall time. A fleet holds every viewer to
    // the same duration, which a segment count cannot express because segments
    // arrive at whatever rate the network manages.
    let duration = flag("--duration")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|seconds| *seconds > 0.0)
        .map(Duration::from_secs_f64);
    // `--segments <n>` overrides how much of the playlist to watch.
    //
    // The default only applies when nothing else bounds the run: `WATCH_SEGMENTS`
    // is a "prove it works" default for a human at a terminal, and silently
    // capping a `--duration 300` run at 8 segments would turn every fleet
    // measurement into a 17-second one.
    let segments = flag("--segments")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(if duration.is_some() {
            usize::MAX
        } else {
            WATCH_SEGMENTS
        });
    // `--idle <n>` keeps the process alive after watching, so a settled heap can
    // be inspected with `heap`/`vmmap`.
    let idle = flag("--idle")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    // `--out <dir>` writes each segment body to disk so it can be played or
    // probed with an ordinary tool.
    let out_dir = flag("--out").map(std::path::PathBuf::from);
    // `--peers <n>` is the main lever on how many viewers fit on a machine.
    if let Some(limit) = flag("--peers").and_then(|value| value.parse::<u64>().ok()) {
        set_peer_limit(limit);
    }
    let metrics_json = flag("--metrics").is_some_and(|value| value == "json");
    let network_id = if args.iter().any(|arg| arg == "testnet") {
        SWARM_TESTNET
    } else {
        SWARM_MAINNET
    };

    let outcome = run_on_local_set(async move {
        let run = Run {
            metrics: Metrics::new(metrics_json),
            shutdown: Shutdown::install(),
            segments,
            duration,
            idle,
            out_dir,
            last_peer_report: std::cell::Cell::new(Instant::now()),
        };

        let viewer = Viewer::new();
        viewer.start(network_id).await?;
        tracing::info!(network_id, "node started");

        let (owner, topic) = mode.stream();
        run.metrics.emit(
            "start",
            json!({
                "pid": std::process::id(),
                "network_id": network_id,
                "mode": mode.name(),
                "owner": owner,
                "topic": topic,
                "peer_limit": peer_limit(),
                "version": env!("CARGO_PKG_VERSION"),
            }),
        );

        viewer.connect_bootnodes(network_id).await;
        tracing::info!("dialing bootnodes");

        let result = match &mode {
            Mode::Watch(owner, topic) => watch_stream(&viewer, &run, owner, topic).await,
            Mode::WatchLive(owner, topic) => watch_live_stream(&viewer, &run, owner, topic).await,
            Mode::Get(reference) => get_reference(&viewer, &run, reference).await,
            Mode::Feed(owner, topic) => show_feed(&viewer, &run, owner, topic).await,
            Mode::Peer => peer_only(&viewer, &run).await,
        };
        if let Err(error) = &result {
            run.metrics
                .emit("error", json!({ "stage": mode.name(), "message": error }));
        }
        result
    })?;

    outcome.map_err(|error| error.into())
}

/// Peer only, reporting the connection curve.
async fn peer_only(viewer: &Viewer, run: &Run) -> Result<(), String> {
    let clock = Instant::now();
    let limit = run
        .duration
        .unwrap_or_else(|| Duration::from_secs(PEER_MODE_DEFAULT_SECONDS));
    while clock.elapsed() < limit && !run.shutdown.requested() {
        // Bounded by whatever is left, so `--duration 16` does not run for 20.
        let step = Duration::from_secs(5).min(limit.saturating_sub(clock.elapsed()));
        tokio::select! {
            _ = tokio::time::sleep(step) => {}
            _ = run.shutdown.wait() => {}
        }
        let peers = viewer.connections().await;
        let dial_failures = viewer.dial_failures();
        tracing::info!(
            peers,
            dial_failures,
            elapsed_s = clock.elapsed().as_secs(),
            "peers"
        );
        run.metrics
            .emit("peers", json!({ "peers": peers, "dial_failures": dial_failures }));
        drain(viewer);
    }
    run.metrics.emit(
        "summary",
        json!({
            "segments": 0,
            "bytes": 0,
            "wall_s": clock.elapsed().as_secs_f64(),
            "peers": viewer.connections().await,
        }),
    );
    Ok(())
}

async fn watch_stream(
    viewer: &Viewer,
    run: &Run,
    owner: &str,
    topic: &str,
) -> Result<(), String> {
    peer_up(viewer, run).await?;

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
    // A VOD run has no live edge to join, but the moment the playlist resolves
    // is the same milestone: it is when the viewer can start asking for bodies.
    run.metrics.emit(
        "joined",
        json!({
            "join_ms": started.elapsed().as_millis() as u64,
            "feed_index": playlist.feed_index,
            "start_sequence": playlist.sequence,
            "runway_s": playlist.duration(),
            "window": playlist.segments.len(),
        }),
    );

    if let Some(dir) = &run.out_dir {
        std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    }
    play(viewer, run, &playlist).await?;
    idle_after(viewer, run).await;
    Ok(())
}

/// Pull the head of the playlist segment by segment, timing each retrieval
/// against the duration it is supposed to cover. A real player stalls when a
/// segment lands later than the buffer it still has; that margin is the only
/// thing a viewer actually experiences, so it is what we record.
async fn play(viewer: &Viewer, run: &Run, playlist: &StreamPlaylist) -> Result<(), String> {
    let clock = Instant::now();
    let mut buffered = 0.0_f64;
    let mut total = 0_usize;
    let mut stalls = 0_u32;
    let mut stalled_total = 0.0_f64;
    let mut media = 0.0_f64;
    let mut played = 0_usize;
    let mut fetches: Vec<f64> = Vec::new();

    for segment in playlist.segments.iter().take(run.segments) {
        if run.should_stop(clock.elapsed()) {
            break;
        }
        let started = Instant::now();
        let (bytes, attempts) = viewer.fetch_segment_reporting(segment).await?;
        drain(viewer);
        let elapsed = started.elapsed().as_secs_f64();

        // First segment has no buffer to spend, so it can never be "late".
        let stalled = played > 0 && elapsed > buffered;
        let stall_s = if stalled { elapsed - buffered } else { 0.0 };
        if stalled {
            stalls += 1;
            stalled_total += stall_s;
        }
        buffered = (buffered - elapsed).max(0.0) + segment.duration;
        total += bytes.len();
        media += segment.duration;
        played += 1;
        fetches.push(elapsed * 1000.0);

        write_segment(run, segment, &bytes)?;

        let peers = viewer.connections().await;
        run.report_peers(peers, viewer.dial_failures());
        tracing::info!(
            sequence = segment.sequence,
            reference = %segment.reference,
            bytes = bytes.len(),
            fetch_ms = (elapsed * 1000.0) as u64,
            segment_s = segment.duration,
            buffered_s = format!("{buffered:.2}"),
            mpeg_ts = looks_like_mpeg_ts(&bytes),
            stalled,
            peers,
            "segment"
        );
        run.metrics.emit(
            "segment",
            json!({
                "sequence": segment.sequence,
                "bytes": bytes.len(),
                "fetch_ms": elapsed * 1000.0,
                "segment_s": segment.duration,
                "buffered_s": buffered,
                "stalled": stalled,
                "stall_s": stall_s,
                "attempts": attempts,
                "peers": peers,
            }),
        );

        // Attribute RSS growth to a named cache rather than to "memory".
        if played.is_multiple_of(25) {
            tracing::info!(segments = played, "{}", viewer.cache_report());
        }
    }

    let peers = viewer.connections().await;
    tracing::info!(
        segments = played,
        bytes = total,
        stalls,
        peers,
        "watched"
    );
    run.metrics.emit(
        "summary",
        summary_fields(
            played, total, media, clock, stalls, stalled_total, 0, 0, 0, &fetches, peers,
            playlist.finalized, Some(playlist.feed_index), None,
        ),
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Join a stream at its live edge and follow it.
async fn watch_live_stream(
    viewer: &Viewer,
    run: &Run,
    owner: &str,
    topic: &str,
) -> Result<(), String> {
    peer_up(viewer, run).await?;

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
    run.metrics.emit(
        "joined",
        json!({
            "join_ms": started.elapsed().as_millis() as u64,
            "feed_index": live.joined_at(),
            "edge_sequence": live.live_sequence(),
            "start_sequence": live.start_sequence(),
            "runway_s": live.runway_seconds(),
            "window": playlist.segments.len(),
        }),
    );

    if let Some(dir) = &run.out_dir {
        std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    }
    play_live(viewer, run, &live).await?;
    idle_after(viewer, run).await;
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
async fn play_live(viewer: &Viewer, run: &Run, live: &LiveStream) -> Result<(), String> {
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
    let mut gaps = 0_u64;
    let mut body_failures = 0_u64;
    let mut fetches: Vec<f64> = Vec::new();

    while played < run.segments {
        if run.should_stop(clock.elapsed()) {
            tracing::info!(sequence, "stopping at the caller's request");
            break;
        }

        // `segment` waits for a sequence the publisher has not written yet, so
        // the stop signal has to race it or a stalled viewer would ignore
        // SIGTERM until the publisher moved on.
        let found = tokio::select! {
            segment = live.segment(sequence) => segment,
            _ = run.shutdown.wait() => break,
        };
        let Some(segment) = found else {
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
                run.metrics.emit(
                    "skip",
                    json!({
                        "from": sequence,
                        "to": playlist.sequence,
                        "reason": "fell off the live window",
                    }),
                );
                sequence = playlist.sequence;
                continue;
            }
            tracing::info!(sequence, "publisher stopped short of this sequence");
            break;
        };

        if segment.gap {
            tracing::warn!(sequence, "gap segment; stepping over it");
            gaps += 1;
            run.metrics
                .emit("gap", json!({ "sequence": sequence, "source": "publisher" }));
            sequence += 1;
            continue;
        }

        // No deadline on the body. `fetch_segment` already retries six times as
        // upstream's `foreground_hls_body` does, and upstream then gives the
        // segment a second strike before writing it off. Capping the wall time
        // here was tried and was actively harmful — see IMPROVEMENTS.md.
        let fetch_started = Instant::now();
        let (bytes, attempts) = match viewer.fetch_segment_reporting(&segment).await {
            Ok(body) => body,
            Err(error) => {
                body_failures += 1;
                let second_strike = live.record_body_failure(&segment);
                run.metrics.emit(
                    "body_failure",
                    json!({
                        "sequence": sequence,
                        "strike": if second_strike { 2 } else { 1 },
                        "attempts": 6,
                    }),
                );
                if second_strike {
                    tracing::warn!(sequence, "{error}; second strike, treating as a gap");
                    gaps += 1;
                    run.metrics
                        .emit("gap", json!({ "sequence": sequence, "source": "local" }));
                    sequence += 1;
                } else {
                    tracing::warn!(sequence, "{error}; first strike, asking again");
                }
                continue;
            }
        };
        drain(viewer);
        let fetch_ms = fetch_started.elapsed().as_millis() as u64;
        fetches.push(fetch_started.elapsed().as_secs_f64() * 1000.0);

        // Wall time by which the playhead had consumed everything acquired so
        // far. Landing after it means the playhead was dry in between.
        let due = media + stalled_total;
        let now = clock.elapsed().as_secs_f64();
        let stalled = now > due && played > 0;
        let stall_s = if stalled { now - due } else { 0.0 };
        if stalled {
            stalls += 1;
            stalled_total += stall_s;
        }

        media += segment.duration;
        total += bytes.len();
        played += 1;

        write_segment(run, &segment, &bytes)?;

        let buffered = media - (clock.elapsed().as_secs_f64() - stalled_total);
        let peers = viewer.connections().await;
        run.report_peers(peers, viewer.dial_failures());
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
            peers,
            "segment"
        );
        run.metrics.emit(
            "segment",
            json!({
                "sequence": segment.sequence,
                "bytes": bytes.len(),
                "fetch_ms": fetch_ms,
                "segment_s": segment.duration,
                "buffered_s": buffered,
                "stalled": stalled,
                "stall_s": stall_s,
                "feed_index": live.feed_index(),
                "attempts": attempts,
                "peers": peers,
            }),
        );

        if played.is_multiple_of(25) {
            tracing::info!(segments = played, "{}", viewer.cache_report());
        }
        sequence += 1;

        // Hold the buffer at its target instead of running ahead of the clock.
        let ahead = media - (clock.elapsed().as_secs_f64() - stalled_total);
        if ahead > target_buffer {
            let pause = Duration::from_secs_f64(ahead - target_buffer);
            tokio::select! {
                _ = async_std::task::sleep(pause) => {}
                _ = run.shutdown.wait() => {}
            }
        }
    }

    let peers = viewer.connections().await;
    tracing::info!(
        segments = played,
        bytes = total,
        media_s = format!("{media:.1}"),
        wall_s = format!("{:.1}", clock.elapsed().as_secs_f64()),
        stalls,
        stalled_s = format!("{stalled_total:.2}"),
        skipped = live.skipped(),
        finalized = live.finalized(),
        peers,
        "watched live"
    );
    run.metrics.emit(
        "summary",
        summary_fields(
            played,
            total,
            media,
            clock,
            stalls,
            stalled_total,
            live.skipped(),
            gaps,
            body_failures,
            &fetches,
            peers,
            live.finalized(),
            Some(live.feed_index()),
            None,
        ),
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Retrieve one reference from peers and report what came back.
///
/// The gateway is never consulted, so a success here proves the content is
/// reachable in the network rather than merely accepted by an upload endpoint.
async fn get_reference(viewer: &Viewer, run: &Run, reference: &str) -> Result<(), String> {
    peer_up(viewer, run).await?;

    let started = Instant::now();
    let bytes = viewer.retrieve_payload(reference).await?;
    drain(viewer);
    let fetch_ms = started.elapsed().as_millis() as u64;
    if let Some(path) = &run.out_dir {
        std::fs::write(path, &bytes).map_err(|error| error.to_string())?;
    }

    let peers = viewer.connections().await;
    tracing::info!(
        reference = %reference,
        bytes = bytes.len(),
        fetch_ms,
        mpeg_ts = looks_like_mpeg_ts(&bytes),
        head = %hex::encode(&bytes[..bytes.len().min(8)]),
        peers,
        "retrieved"
    );
    run.metrics.emit(
        "summary",
        json!({
            "segments": 1,
            "bytes": bytes.len(),
            "wall_s": started.elapsed().as_secs_f64(),
            "peers": peers,
        }),
    );
    tracing::info!("final {}", viewer.cache_report());
    Ok(())
}

/// Resolve one feed update from peers and report its payload.
async fn show_feed(viewer: &Viewer, run: &Run, owner: &str, topic: &str) -> Result<(), String> {
    let peers = peer_up(viewer, run).await?;

    let started = Instant::now();
    let (index, bytes) = viewer.resolve_feed(owner, topic).await?;
    drain(viewer);
    let resolve_ms = started.elapsed().as_millis() as u64;
    if let Some(path) = &run.out_dir {
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
    run.metrics.emit(
        "summary",
        json!({
            "segments": 0,
            "bytes": bytes.len(),
            "wall_s": started.elapsed().as_secs_f64(),
            "feed_index": index,
            "peers": peers,
        }),
    );
    // A stream feed's payload is the playlist itself, so it is worth seeing.
    // On stderr, because `--metrics json` owns stdout.
    if let Ok(text) = std::str::from_utf8(&bytes) {
        eprintln!("{text}");
    }
    Ok(())
}

/// Wait for enough peers to ask the network for anything.
async fn peer_up(viewer: &Viewer, run: &Run) -> Result<u64, String> {
    let peers = viewer
        .wait_for_connections(WATCH_MINIMUM_PEERS, WATCH_PEER_TIMEOUT_MS)
        .await;
    let dial_failures = viewer.dial_failures();
    tracing::info!(peers, dial_failures, "peered");
    run.metrics
        .emit("peers", json!({ "peers": peers, "dial_failures": dial_failures }));
    if peers == 0 {
        return Err("no peers; cannot retrieve anything".to_string());
    }
    Ok(peers)
}

async fn idle_after(viewer: &Viewer, run: &Run) {
    if run.idle == 0 {
        return;
    }
    tracing::info!(idle_s = run.idle, "idling, pid {}", std::process::id());
    tokio::time::sleep(Duration::from_secs(run.idle)).await;
    tracing::info!("after idle {}", viewer.cache_report());
}

fn write_segment(run: &Run, segment: &StreamSegment, bytes: &[u8]) -> Result<(), String> {
    let Some(dir) = &run.out_dir else {
        return Ok(());
    };
    let path = dir.join(format!("{:06}.seg", segment.sequence));
    std::fs::write(&path, bytes).map_err(|error| error.to_string())
}

/// The authoritative per-viewer record. A fleet aggregates these, so anything
/// missing here has to be re-derived from the event stream by every consumer.
#[allow(clippy::too_many_arguments)]
fn summary_fields(
    segments: usize,
    bytes: usize,
    media: f64,
    clock: Instant,
    stalls: u32,
    stalled_total: f64,
    skipped: u64,
    gaps: u64,
    body_failures: u64,
    fetches: &[f64],
    peers: u64,
    finalized: bool,
    feed_index: Option<u64>,
    join_ms: Option<u64>,
) -> Value {
    let mut sorted = fetches.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let at = |quantile: f64| -> Option<f64> {
        if sorted.is_empty() {
            return None;
        }
        let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
        sorted.get(index).copied()
    };
    json!({
        "segments": segments,
        "bytes": bytes,
        "media_s": media,
        "wall_s": clock.elapsed().as_secs_f64(),
        "stalls": stalls,
        "stalled_s": stalled_total,
        "skipped": skipped,
        "gaps": gaps,
        "body_failures": body_failures,
        "fetch_ms_p50": at(0.5),
        "fetch_ms_p90": at(0.9),
        "fetch_ms_p99": at(0.99),
        "peers": peers,
        "finalized": finalized,
        "feed_index": feed_index,
        "join_ms": join_ms,
    })
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
