//! Public API for the standalone viewer binary.
//!
//! The crate internals are `pub(crate)`; this is the thin public surface the
//! `weeb-3-rs-hls` binary drives. Scope is deliberately narrow: connect to
//! peers, then read a feed and retrieve content. No uploading, no wallet, no
//! chain access, no persistence.
//!
//! The node is `!Send` by construction (the browser original is single-threaded
//! and uses `Rc`/`RefCell` throughout), so it must be driven inside a
//! `tokio::task::LocalSet`. See `run_on_local_set` for the intended entry point.

use crate::bzz_stream::{decode_feed_payload_root, retrieve_feed_payload};
use crate::network_profile::{initial_bootnodes, profile_for_swarm_network_id};
use crate::retrieval::{retrieval_cache_stats, retrieve_data_payload, seek_latest_feed_update_indexed};
use crate::stream_follow::LiveFeed;
use crate::stream_hls::{HlsPlaylist, HlsSegment, MAX_STREAM_FEED_PAYLOAD_BYTES};
use crate::{Weeb3, normalize_feed_topic, strip_hex_prefix};
use async_std::sync::Arc;
use std::time::Duration;

/// Attempts on a segment body, and the linear backoff between them.
///
/// Upstream's `foreground_hls_body`: six tries, 75 ms x attempt apart, with no
/// wall-clock deadline. A segment at the live edge is often not retrievable on
/// first ask, and these retries are part of the load a real viewer generates —
/// so the count matters to what a fleet measures, not just to whether playback
/// succeeds.
const SEGMENT_BODY_ATTEMPTS: usize = 6;
const SEGMENT_BODY_RETRY_DELAY_MS: u64 = 75;

pub const SWARM_MAINNET: u64 = 1;
pub const SWARM_TESTNET: u64 = 10;

/// One entry of a stream playlist: a Swarm reference plus its playback duration.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamSegment {
    /// Media sequence number of this segment within the stream.
    pub sequence: u64,
    /// Swarm reference (64 hex chars) of the segment body.
    pub reference: String,
    /// Segment duration in seconds.
    pub duration: f64,
    /// A `#EXT-X-GAP` placeholder: no body is retrievable for it.
    pub gap: bool,
    /// `#EXT-X-DISCONTINUITY-SEQUENCE` this segment belongs to.
    ///
    /// Bodies either side of a change here do not concatenate into a decodable
    /// stream: the encoder restarted, so timestamps and possibly codec
    /// parameters reset. Anything joining segment bodies must break here.
    pub discontinuity_sequence: u64,
}

/// A stream playlist resolved from a Swarm feed.
///
/// The publisher writes an HLS manifest as the feed payload; segment URIs are
/// gateway URLs whose last path component is the Swarm reference of the
/// segment body. Only the reference is kept — a native viewer retrieves the
/// body from peers, never from the gateway.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamPlaylist {
    /// Feed index this playlist was read from. The publisher appends a new
    /// update per segment, so this doubles as the stream's version.
    pub feed_index: u64,
    /// `#EXT-X-MEDIA-SEQUENCE`: sequence number of the first segment.
    pub sequence: u64,
    /// `#EXT-X-TARGETDURATION` in seconds.
    pub target_duration: u64,
    /// `#EXT-X-ENDLIST` present: the stream is complete (VOD).
    pub finalized: bool,
    /// `#EXT-X-DISCONTINUITY-SEQUENCE` of the first segment listed.
    pub discontinuity_sequence: u64,
    pub segments: Vec<StreamSegment>,
}

impl StreamPlaylist {
    /// Total playback duration of every listed segment, in seconds.
    pub fn duration(&self) -> f64 {
        self.segments.iter().map(|segment| segment.duration).sum()
    }
}

pub struct Viewer {
    inner: Arc<Weeb3>,
}

impl Viewer {
    pub fn new() -> Self {
        Self { inner: Arc::new(Weeb3::new()) }
    }

    /// Select the network and start the swarm event loop.
    pub async fn start(&self, network_id: u64) -> Result<(), String> {
        if profile_for_swarm_network_id(network_id).is_none() {
            return Err(format!("unsupported swarm network id {network_id}"));
        }
        if !self.inner.set_network_id(network_id.to_string()).await {
            return Err("failed to set network id".to_string());
        }
        let inner = self.inner.clone();
        tokio::task::spawn_local(async move { inner.run().await });
        Ok(())
    }

    /// Dial the built-in bootnodes for the given network.
    pub async fn connect_bootnodes(&self, network_id: u64) {
        let nodes = initial_bootnodes_for(network_id);
        self.inner
            .connect_bootnodes_for_current_network(nodes, network_id)
            .await;
    }

    /// Dial an explicit bootnode list (multiaddr strings).
    pub async fn connect_to(&self, addresses: Vec<String>, network_id: u64) {
        let nodes = addresses.into_iter().map(|a| (a, true)).collect();
        self.inner
            .connect_bootnodes_for_current_network(nodes, network_id)
            .await;
    }

    pub async fn connections(&self) -> u64 {
        self.inner.get_connections().await
    }

    pub async fn wait_for_connections(&self, minimum: u64, timeout_ms: u64) -> u64 {
        self.inner.wait_for_connections(minimum, timeout_ms).await
    }

    /// Outbound dials that failed since the node started.
    ///
    /// Rising while peers stay flat is the ephemeral-port ceiling, not a slow
    /// network: a fleet cannot tell those apart from throughput alone.
    pub fn dial_failures(&self) -> u64 {
        self.inner.get_dial_failures()
    }

    /// Live occupancy of every in-process cache that can retain retrieved
    /// content. Diagnostic: use it to attribute RSS growth to a specific cache
    /// rather than to the allocator.
    /// Live occupancy and hit counters for the decoded-chunk cache, which is
    /// the only cache on the viewer's path that retains content.
    pub fn cache_report(&self) -> String {
        let chunk = retrieval_cache_stats();
        let lookups = chunk.hits + chunk.misses;
        let hit_rate = if lookups == 0 {
            0.0
        } else {
            chunk.hits as f64 * 100.0 / lookups as f64
        };
        format!(
            "chunk_cache={} entries/{:.1} MB order={} inflight={} \
             hits={} misses={} ({hit_rate:.1}% of {lookups}) served={:.1} MB \
             inserts={} evictions={}",
            chunk.entries,
            chunk.bytes as f64 / 1e6,
            chunk.order,
            chunk.flights,
            chunk.hits,
            chunk.misses,
            chunk.hit_bytes as f64 / 1e6,
            chunk.inserts,
            chunk.evictions,
        )
    }

    pub fn drain_logs(&self) -> Vec<String> {
        self.inner.get_current_logs()
    }

    /// Retrieve content bytes by Swarm address, prefixed with the 8-byte
    /// little-endian span the bytes tree records. See [`Viewer::retrieve_payload`]
    /// for the body on its own.
    pub async fn retrieve_bytes(&self, address: String) -> Vec<u8> {
        self.inner.retrieve_bytes(address).await
    }

    /// Retrieve content bytes by Swarm address, without the span prefix.
    ///
    /// This is the actual file body — what a player or a file on disk wants.
    pub async fn retrieve_payload(&self, address: &str) -> Result<Vec<u8>, String> {
        let reference =
            hex::decode(strip_hex_prefix(address.trim())).map_err(|_| "invalid reference")?;
        let bytes = retrieve_data_payload(&reference, &self.inner.chunk_port.0).await;
        if bytes.is_empty() {
            return Err(format!("{address} could not be retrieved"));
        }
        Ok(bytes)
    }

    /// Resolve the latest update of a Swarm feed and return its raw payload
    /// together with the feed index the payload was found at.
    ///
    /// `owner` is a 20-byte address (with or without `0x`). `topic` is either
    /// 32 bytes of hex or an arbitrary string, which is hashed the same way
    /// bee-js `Topic.fromString` does it — so the stream identifiers published
    /// by a streaming front end can be passed through verbatim.
    ///
    /// This reads the update payload directly. It deliberately does not go
    /// through the bzz manifest resolver: a stream feed's payload is the
    /// playlist itself, not a manifest pointing at a collection.
    pub async fn resolve_feed(&self, owner: &str, topic: &str) -> Result<(u64, Vec<u8>), String> {
        let owner = normalize_feed_owner(owner)?;
        let topic = normalize_feed_topic(topic);
        let (index, update) =
            seek_latest_feed_update_indexed(owner, topic, &self.inner.chunk_port.0)
                .await
                .ok_or_else(|| "no feed update found".to_string())?;
        let root = decode_feed_payload_root(update)
            .ok_or_else(|| format!("feed update {index} is not a readable payload"))?;
        let bytes = retrieve_feed_payload(
            &root,
            MAX_STREAM_FEED_PAYLOAD_BYTES,
            &self.inner.chunk_port.0,
        )
        .await
        .ok_or_else(|| format!("feed payload at index {index} could not be retrieved"))?;
        Ok((index, bytes))
    }

    /// Resolve a stream's feed and parse its payload as an HLS playlist.
    ///
    /// One snapshot, taken once. For a stream still being published, see
    /// [`Viewer::watch_live`].
    pub async fn playlist(&self, owner: &str, topic: &str) -> Result<StreamPlaylist, String> {
        let (feed_index, bytes) = self.resolve_feed(owner, topic).await?;
        let playlist = HlsPlaylist::parse(&bytes)
            .ok_or_else(|| "feed payload is not a usable HLS playlist".to_string())?;
        Ok(stream_playlist(feed_index, &playlist))
    }

    /// Join a live stream at its edge and start following the feed forward.
    ///
    /// Anchors [`LiveStream::start_sequence`] far enough behind the newest
    /// segment to give playback a startup buffer, then leaves a task polling the
    /// feed so the playlist grows as the publisher writes it.
    ///
    /// Waits for the edge to carry a runway rather than failing on a stream that
    /// has only just started. Fails after 30 seconds, which in practice means the
    /// publisher's live window is under 8 seconds and cannot be joined live at
    /// all, however healthy the stream is.
    pub async fn watch_live(&self, owner: &str, topic: &str) -> Result<LiveStream, String> {
        let owner = normalize_feed_owner(owner)?;
        let topic = normalize_feed_topic(topic);
        let join = LiveFeed::join(self.inner.clone(), owner, topic).await?;
        Ok(LiveStream {
            feed: join.feed,
            start_sequence: join.start_sequence,
            joined_at: join.feed_index,
            runway_seconds: join.plan.runway_end - join.plan.play_position,
        })
    }

    /// Retrieve one segment body from peers, retrying as upstream does.
    ///
    /// See [`SEGMENT_BODY_ATTEMPTS`]. Deliberately has no overall deadline:
    /// bounding it changes how many retrieval attempts a viewer makes, which is
    /// exactly the quantity a load test exists to measure.
    pub async fn fetch_segment(&self, segment: &StreamSegment) -> Result<Vec<u8>, String> {
        self.fetch_segment_reporting(segment)
            .await
            .map(|(bytes, _)| bytes)
    }

    /// As [`Viewer::fetch_segment`], reporting how many attempts it took.
    ///
    /// The attempt count is part of the load a viewer places on the network, so
    /// a rig that only records whether a body arrived is under-reporting: the
    /// ~2-5% of segments that need a second or third ask are requests the
    /// network actually served.
    pub async fn fetch_segment_reporting(
        &self,
        segment: &StreamSegment,
    ) -> Result<(Vec<u8>, usize), String> {
        if segment.gap {
            return Err(format!("segment {} is a gap", segment.sequence));
        }
        let mut last = String::new();
        for attempt in 0..SEGMENT_BODY_ATTEMPTS {
            match self.retrieve_payload(&segment.reference).await {
                Ok(bytes) => return Ok((bytes, attempt + 1)),
                Err(error) => last = error,
            }
            if attempt + 1 < SEGMENT_BODY_ATTEMPTS {
                async_std::task::sleep(Duration::from_millis(
                    SEGMENT_BODY_RETRY_DELAY_MS * (attempt + 1) as u64,
                ))
                .await;
            }
        }
        Err(format!(
            "segment {} after {SEGMENT_BODY_ATTEMPTS} attempts: {last}",
            segment.sequence
        ))
    }
}

/// Set the peer connection limit before starting a node.
///
/// Defaults to 200, the browser client's footprint. Exposed because it is the
/// main lever on how many viewers fit on a machine, and a load rig has to be
/// able to sweep it — not because lowering it is a good idea by itself.
pub fn set_peer_limit(limit: u64) {
    crate::accounting::set_connection_buildup_limit(limit);
}

/// The peer connection limit this process will build toward.
pub fn peer_limit() -> u64 {
    crate::accounting::connection_buildup_limit()
}

/// A live stream being followed forward.
///
/// The feed is polled by a background task on the same `LocalSet`, so the
/// playlist grows underneath this handle. Ask for segments by sequence number
/// and pace the asking yourself: [`LiveStream::segment`] waits for a segment the
/// publisher has not written yet, which is what makes a real stall observable.
pub struct LiveStream {
    feed: LiveFeed,
    start_sequence: u64,
    joined_at: u64,
    runway_seconds: f64,
}

impl LiveStream {
    /// Media sequence to start playback from.
    pub fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    /// Feed index the edge was joined at.
    pub fn joined_at(&self) -> u64 {
        self.joined_at
    }

    /// Buffer available at the join, in seconds.
    pub fn runway_seconds(&self) -> f64 {
        self.runway_seconds
    }

    /// Highest feed index applied so far.
    pub fn feed_index(&self) -> u64 {
        self.feed.feed_index()
    }

    /// `#EXT-X-ENDLIST` has arrived: the publisher stopped.
    pub fn finalized(&self) -> bool {
        self.feed.finalized()
    }

    /// Segments the live window slid past before this viewer reached them.
    pub fn skipped(&self) -> u64 {
        self.feed.skipped()
    }

    /// The playlist as currently known.
    pub fn playlist(&self) -> StreamPlaylist {
        stream_playlist(self.feed.feed_index(), &self.feed.snapshot())
    }

    /// Media sequence of the newest playable segment.
    pub fn live_sequence(&self) -> Option<u64> {
        self.feed.live_sequence()
    }

    /// The segment at `sequence`, waiting for the publisher to write it.
    ///
    /// `None` means it will never arrive: either the stream finalized short of
    /// it, or the live window slid past it. Callers should then resume from
    /// [`LiveStream::live_sequence`].
    pub async fn segment(&self, sequence: u64) -> Option<StreamSegment> {
        let segment = self.feed.segment_at(sequence).await?;
        Some(stream_segment(sequence, &segment))
    }

    /// Record a failed segment body, returning `true` once it should be treated
    /// as a gap.
    ///
    /// Upstream gives a failing body two strikes (`HlsTailFailure`) before
    /// tagging it, and each strike is already six attempts inside
    /// [`Viewer::fetch_segment`]. A caller that gets `false` should ask for the
    /// same sequence again.
    pub fn record_body_failure(&self, segment: &StreamSegment) -> bool {
        self.feed
            .record_body_failure(segment.sequence, &segment.reference)
    }
}

fn stream_segment(sequence: u64, segment: &HlsSegment) -> StreamSegment {
    StreamSegment {
        sequence,
        reference: segment.reference.clone(),
        duration: segment.duration,
        gap: segment.gap,
        discontinuity_sequence: segment.discontinuity_sequence,
    }
}

fn stream_playlist(feed_index: u64, playlist: &HlsPlaylist) -> StreamPlaylist {
    StreamPlaylist {
        feed_index,
        sequence: playlist.sequence,
        target_duration: playlist.target_duration,
        finalized: playlist.finalized,
        discontinuity_sequence: playlist.discontinuity_sequence,
        segments: playlist
            .segments
            .iter()
            .enumerate()
            .map(|(offset, segment)| {
                stream_segment(playlist.sequence.saturating_add(offset as u64), segment)
            })
            .collect(),
    }
}

impl Default for Viewer {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_feed_owner(owner: &str) -> Result<String, String> {
    let owner = strip_hex_prefix(owner.trim()).to_ascii_lowercase();
    if owner.len() != 40 || !owner.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("invalid feed owner {owner:?}"));
    }
    Ok(owner)
}

fn initial_bootnodes_for(network_id: u64) -> Vec<(String, bool)> {
    profile_for_swarm_network_id(network_id)
        .map(|profile| {
            initial_bootnodes(profile)
                .into_iter()
                .map(|address| (address.to_string(), true))
                .collect()
        })
        .unwrap_or_default()
}

/// Drive a future on a current-thread runtime with a `LocalSet`.
///
/// The node is `!Send`, so it cannot live on the multi-thread scheduler. One
/// `LocalSet` per thread, many nodes per `LocalSet`.
pub fn run_on_local_set<F: std::future::Future<Output = T> + 'static, T: 'static>(
    future: F,
) -> std::io::Result<T> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    Ok(local.block_on(&runtime, future))
}
