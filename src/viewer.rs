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
use crate::stream::fetch_cache_stats;
use crate::stream_hls::runtime::body_cache_stats;
use crate::stream_hls::{HlsPlaylist, MAX_STREAM_FEED_PAYLOAD_BYTES};
use crate::{Weeb3, normalize_feed_topic, strip_hex_prefix};
use async_std::sync::Arc;

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

    /// Live occupancy of every in-process cache that can retain retrieved
    /// content. Diagnostic: use it to attribute RSS growth to a specific cache
    /// rather than to the allocator.
    pub fn cache_report(&self) -> String {
        let chunk = retrieval_cache_stats();
        let (metadata, ranges, range_bytes) = fetch_cache_stats();
        let (bodies, body_bytes) = body_cache_stats();
        let lookups = chunk.hits + chunk.misses;
        let hit_rate = if lookups == 0 {
            0.0
        } else {
            chunk.hits as f64 * 100.0 / lookups as f64
        };
        format!(
            "chunk_cache={} entries/{:.1} MB order={} inflight={} \
             hits={} misses={} ({hit_rate:.1}% of {lookups}) served={:.1} MB \
             inserts={} evictions={} \
             fetch_cache={metadata} meta/{ranges} ranges/{:.1} MB \
             body_cache={bodies} bodies/{:.1} MB",
            chunk.entries,
            chunk.bytes as f64 / 1e6,
            chunk.order,
            chunk.flights,
            chunk.hits,
            chunk.misses,
            chunk.hit_bytes as f64 / 1e6,
            chunk.inserts,
            chunk.evictions,
            range_bytes as f64 / 1e6,
            body_bytes as f64 / 1e6,
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
        let root = decode_feed_payload_root(index, update)
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
    pub async fn playlist(&self, owner: &str, topic: &str) -> Result<StreamPlaylist, String> {
        let (feed_index, bytes) = self.resolve_feed(owner, topic).await?;
        let playlist = HlsPlaylist::parse(&bytes)
            .ok_or_else(|| "feed payload is not a usable HLS playlist".to_string())?;
        Ok(StreamPlaylist {
            feed_index,
            sequence: playlist.sequence,
            target_duration: playlist.target_duration,
            finalized: playlist.finalized,
            segments: playlist
                .segments
                .iter()
                .enumerate()
                .map(|(offset, segment)| StreamSegment {
                    sequence: playlist.sequence.saturating_add(offset as u64),
                    reference: segment.reference.clone(),
                    duration: segment.duration,
                    gap: segment.gap,
                })
                .collect(),
        })
    }

    /// Retrieve one segment body from peers.
    pub async fn fetch_segment(&self, segment: &StreamSegment) -> Result<Vec<u8>, String> {
        if segment.gap {
            return Err(format!("segment {} is a gap", segment.sequence));
        }
        self.retrieve_payload(&segment.reference)
            .await
            .map_err(|error| format!("segment {}: {error}", segment.sequence))
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
