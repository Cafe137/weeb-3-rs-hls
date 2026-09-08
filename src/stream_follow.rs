//! Following a live stream feed forward.
//!
//! Ported from the browser weeb-3 driver (`stream_hls/runtime.rs` at `8a9fd93`),
//! which ran as a ServiceWorker session loop. The algorithm is unchanged:
//!
//! 1.  Join at the live edge — resolve the feed frontier, anchor on the last
//!     non-gap segment, and require [`HLS_LIVE_STARTUP_BUFFER_SECONDS`] of
//!     contiguous runway behind it.
//! 2.  Follow forward by probing `head + 1 ..= head + FEED_FOLLOW_AHEAD`,
//!     tolerating exactly one missing index (publishers write a manifest per
//!     segment, and one can land out of order).
//! 3.  On no progress, sleep [`FEED_POLL_INTERVAL`], and every
//!     [`FEED_FRONTIER_REFRESH_INTERVAL`] re-resolve the frontier — that is what
//!     recovers a follower whose window has slid away, and what notices
//!     `#EXT-X-ENDLIST`.
//!
//! Two pieces of the original are deliberately absent, both browser plumbing
//! rather than algorithm:
//!
//! -   **Session generations.** `feed_is_current`/`view_generation`/`end_feed`
//!     existed to cancel a session when the user navigated away mid-stream. This
//!     binary watches one stream and exits, so the session is simply owned by its
//!     caller and dropped.
//! -   **History reconstruction.** When a candidate will not merge, upstream
//!     rebuilt the archive from strided snapshots (`hls_history`, `reconstruct`)
//!     so it could serve a seamless byte range to hls.js. A viewer that has
//!     fallen off the back of a live window does what a real player does instead:
//!     re-joins at the new edge and reports the discontinuity. `reconstruct` is
//!     still in `stream_hls` if the archive is ever wanted.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use async_std::sync::Arc;
use event_listener::Event;

use crate::Weeb3;
use crate::bzz_stream::{
    FeedPayloadRoot, decode_feed_payload_root, retrieve_feed_payload, retrieve_feed_payload_tail,
};
use crate::feed::FeedProbe;
use crate::retrieval::{probe_feed_update_status, seek_latest_feed_update_indexed};
use crate::stream_conventions::HlsStart;
use crate::stream_hls::{
    HLS_LIVE_STARTUP_BUFFER_SECONDS, HlsPlaylist, HlsSegment, HlsStartupPlan, HlsTailFailure,
    MAX_STREAM_FEED_PAYLOAD_BYTES,
};

/// Feed indices probed ahead of the applied head in one pass.
const FEED_FOLLOW_AHEAD: u64 = 4;
/// Pause after a pass that applied nothing.
const FEED_POLL_INTERVAL: Duration = Duration::from_millis(400);
/// Idle time after which the frontier is re-resolved rather than stepped to.
const FEED_FRONTIER_REFRESH_INTERVAL: Duration = Duration::from_secs(15);
/// Whole-join budget: discovery plus waiting for a startup runway.
///
/// Upstream has no such bound — `discover_raw_for_view` retries forever and the
/// browser cancelled the session on navigation. A headless binary needs one, or
/// a mistyped topic hangs the process, so this stands in for that cancellation
/// the same way dropping `view_generation` does.
const LIVE_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between discovery attempts while the publisher has written nothing yet.
const INITIAL_DISCOVERY_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Tail slice fetched before falling back to the whole payload.
const FEED_TAIL_PROBE_BYTES: usize = 4 * 1024;
/// Payload size past which a tail probe is tried first.
const FEED_INLINE_PAYLOAD_BYTES: usize = 4 * 1024;

/// One feed update, resolved to bytes.
struct RawFeedPayload {
    index: u64,
    bytes: Vec<u8>,
}

/// Outcome of asking the network for one feed index.
enum FeedPayloadProbe {
    /// The payload was small enough to fetch inline.
    Found(RawFeedPayload),
    /// The payload is large; its root is known but the body was not fetched.
    Deferred(u64, FeedPayloadRoot),
    /// Authenticated absence: this index has not been written.
    Missing,
    /// Could not tell. Retry.
    Transient,
}

/// Playlist state shared between the follower task and whoever is playing.
struct LiveState {
    playlist: HlsPlaylist,
    /// Highest feed index whose payload has been applied.
    index: u64,
    /// Segments dropped because the window slid past this viewer.
    skipped: u64,
    /// Segments this viewer could not retrieve, as `(sequence, reference)`.
    ///
    /// Held beside the playlist rather than flagged inside it, which is what
    /// upstream does with its `presentation_gaps` set, and for a sharp reason:
    /// `merge_extension` compares overlapping segments for identity, and
    /// `same_payload` includes the gap flag. Flagging a gap in the canonical
    /// playlist therefore makes *every* later manifest fail to merge for as long
    /// as that segment stays inside the publisher's window — measured at ~20 s of
    /// a wedged follower followed by a forced re-anchor that dumped 8 segments.
    /// The canonical playlist must keep saying exactly what the publisher said.
    gaps: HashSet<(u64, String)>,
    /// Strike counter gating gap tagging, as upstream does it.
    tail_failure: HlsTailFailure,
}

/// A live stream being followed.
///
/// Cheap to clone: the state is shared, so the follower task and the player see
/// the same playlist.
#[derive(Clone)]
pub(crate) struct LiveFeed {
    client: Arc<Weeb3>,
    owner: String,
    topic: String,
    state: Rc<RefCell<LiveState>>,
    changed: Rc<Event>,
}

/// Where playback starts, once the edge has been joined.
pub(crate) struct LiveJoin {
    pub(crate) feed: LiveFeed,
    /// Media sequence of the first segment to play.
    pub(crate) start_sequence: u64,
    pub(crate) feed_index: u64,
    pub(crate) plan: HlsStartupPlan,
}

impl LiveFeed {
    /// Resolve the feed frontier and anchor playback at the live edge.
    ///
    /// The runway is measured *backwards* from the newest segment, so a viewer
    /// joins with buffer already in hand rather than starting empty at the edge.
    pub(crate) async fn join(
        client: Arc<Weeb3>,
        owner: String,
        topic: String,
    ) -> Result<LiveJoin, String> {
        let started = Instant::now();
        let (index, playlist) = discover_edge(&client, &owner, &topic, started, LIVE_JOIN_TIMEOUT)
            .await
            .ok_or_else(|| {
                format!(
                    "no readable feed update after {}s: the stream may not have published yet",
                    LIVE_JOIN_TIMEOUT.as_secs()
                )
            })?;

        let feed = Self {
            client,
            owner,
            topic,
            state: Rc::new(RefCell::new(LiveState {
                playlist,
                index,
                skipped: 0,
                gaps: HashSet::new(),
                tail_failure: HlsTailFailure::default(),
            })),
            changed: Rc::new(Event::new()),
        };
        // Follow first, then wait for a runway: a stream only seconds old has not
        // published enough segments to join yet, and the follower is what makes
        // the playlist grow while we wait. Erroring out instead would make
        // joining a stream a race against its own start-up.
        feed.follow();
        feed.await_runway(started, LIVE_JOIN_TIMEOUT).await
    }

    /// Wait until the live edge carries a startup runway, then anchor there.
    async fn await_runway(self, started: Instant, timeout: Duration) -> Result<LiveJoin, String> {
        loop {
            // Listen before looking, so an update between the two is not missed.
            let listener = self.changed.listen();
            let outcome = {
                let state = self.state.borrow();
                match state.playlist.startup_plan(HlsStart::Live) {
                    Some(plan) => {
                        // `play_position` is a duration offset; turn it back into
                        // the sequence number the runway starts on.
                        let start_sequence = state.playlist.sequence.saturating_add(
                            sequence_at_offset(&state.playlist, plan.play_position),
                        );
                        Some(Ok((state.index, plan, start_sequence)))
                    }
                    None if state.playlist.finalized => Some(Err(format!(
                        "the stream finalized carrying only {:.1}s of playlist, and {HLS_LIVE_STARTUP_BUFFER_SECONDS:.1}s is needed to join live",
                        state.playlist.duration()
                    ))),
                    None => None,
                }
            };
            match outcome {
                Some(Ok((feed_index, plan, start_sequence))) => {
                    return Ok(LiveJoin {
                        feed: self,
                        start_sequence,
                        feed_index,
                        plan,
                    });
                }
                Some(Err(error)) => return Err(error),
                None => {}
            }

            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                break;
            };
            if async_std::future::timeout(remaining, listener).await.is_err() {
                break;
            }
        }
        let playlist = self.snapshot();
        Err(format!(
            "no live startup runway after {}s: the edge carries {:.1}s of playlist and {HLS_LIVE_STARTUP_BUFFER_SECONDS:.1}s is needed,              so the publisher's live window is probably too short",
            timeout.as_secs(),
            playlist.duration()
        ))
    }

    /// Spawn the follower. It runs until the stream finalizes.
    fn follow(&self) {
        let feed = self.clone();
        tokio::task::spawn_local(async move { feed.run().await });
    }

    /// A snapshot of the playlist as currently known, with this viewer's own
    /// gaps applied. This is the presentation view; the canonical playlist that
    /// merges run against is never touched.
    pub(crate) fn snapshot(&self) -> HlsPlaylist {
        let state = self.state.borrow();
        let mut playlist = state.playlist.clone();
        for (sequence, reference) in &state.gaps {
            playlist.mark_gap(*sequence, reference);
        }
        playlist
    }

    pub(crate) fn feed_index(&self) -> u64 {
        self.state.borrow().index
    }

    pub(crate) fn skipped(&self) -> u64 {
        self.state.borrow().skipped
    }

    pub(crate) fn finalized(&self) -> bool {
        self.state.borrow().playlist.finalized
    }

    /// The segment at `sequence`, waiting for the follower to publish it.
    ///
    /// Returns `None` when the stream has finalized without ever reaching that
    /// sequence, or when the window has slid past it — the caller then asks for
    /// [`LiveFeed::live_sequence`] instead and takes the skip.
    pub(crate) async fn segment_at(&self, sequence: u64) -> Option<HlsSegment> {
        loop {
            let listener = self.changed.listen();
            {
                let state = self.state.borrow();
                let playlist = &state.playlist;
                if sequence < playlist.sequence {
                    return None;
                }
                if let Ok(offset) = usize::try_from(sequence - playlist.sequence)
                    && let Some(segment) = playlist.segments.get(offset)
                {
                    let mut segment = segment.clone();
                    // Present our own failures as gaps without recording them in
                    // the playlist the publisher owns.
                    if state.gaps.contains(&(sequence, segment.reference.clone())) {
                        segment.gap = true;
                    }
                    return Some(segment);
                }
                if playlist.finalized {
                    return None;
                }
            }
            listener.await;
        }
    }

    /// Media sequence of the newest playable segment.
    pub(crate) fn live_sequence(&self) -> Option<u64> {
        let state = self.state.borrow();
        let playlist = &state.playlist;
        let (position, _) = playlist.segments.iter().enumerate().rfind(|(offset, segment)| {
            !segment.gap
                && playlist
                    .sequence
                    .checked_add(*offset as u64)
                    .is_some_and(|sequence| {
                        !state.gaps.contains(&(sequence, segment.reference.clone()))
                    })
        })?;
        playlist.sequence.checked_add(position as u64)
    }

    /// Record a failed segment body. `true` once it should be treated as a gap.
    ///
    /// Upstream requires two strikes against the same
    /// `(feed index, sequence, reference)` before tagging a presentation gap, so
    /// a body that is merely slow to propagate gets asked for twice more before
    /// playback writes it off.
    pub(crate) fn record_body_failure(&self, sequence: u64, reference: &str) -> bool {
        let mut state = self.state.borrow_mut();
        let index = state.index;
        if !state.tail_failure.record(index, sequence, reference) {
            return false;
        }
        state.tail_failure.clear();
        state.gaps.insert((sequence, reference.to_string()));
        // Anything the window has already shed can never be asked for again.
        let floor = state.playlist.sequence;
        state.gaps.retain(|(sequence, _)| *sequence >= floor);
        drop(state);
        self.changed.notify(usize::MAX);
        true
    }

    /// The follower loop.
    async fn run(&self) {
        let mut last_frontier_check = Instant::now();
        loop {
            if self.finalized() {
                return;
            }
            let head = self.feed_index();

            let mut progressed = false;
            let mut skipped_missing_index = false;
            for offset in 1..=FEED_FOLLOW_AHEAD {
                let Some(index) = head.checked_add(offset) else {
                    return;
                };
                let applied = match self.probe(index).await {
                    FeedPayloadProbe::Found(payload) => HlsPlaylist::parse(&payload.bytes)
                        .and_then(|playlist| self.apply(payload.index, playlist)),
                    FeedPayloadProbe::Deferred(index, root) => {
                        self.apply_deferred(index, &root).await
                    }
                    FeedPayloadProbe::Missing | FeedPayloadProbe::Transient => {
                        // One hole is tolerable; two in a row means the head is
                        // genuinely where we left it.
                        if skipped_missing_index {
                            break;
                        }
                        skipped_missing_index = true;
                        continue;
                    }
                };
                let Some(appended) = applied else {
                    break;
                };
                progressed = true;
                if appended != 0 {
                    self.client.interface_log(format!(
                        "HLS feed advanced to {index}; appended {appended} segment(s)"
                    ));
                }
            }

            if progressed {
                last_frontier_check = Instant::now();
                continue;
            }

            async_std::task::sleep(FEED_POLL_INTERVAL).await;

            if last_frontier_check.elapsed() >= FEED_FRONTIER_REFRESH_INTERVAL {
                last_frontier_check = Instant::now();
                if self.recover_frontier().await {
                    continue;
                }
            }
        }
    }

    /// Ask the network for one feed index.
    async fn probe(&self, index: u64) -> FeedPayloadProbe {
        // Unbounded, as upstream's follower is: the retrieval layer's own
        // admission budget decides when absence is authenticated. Do not wrap
        // this in a deadline — a probe cut short changes how many retrieval
        // attempts a viewer makes, which is part of what the fleet measures.
        let update = match probe_feed_update_status(
            &self.owner,
            &self.topic,
            index,
            &self.client.chunk_port.0,
        )
        .await
        {
            FeedProbe::Found(update) => update,
            FeedProbe::Missing => return FeedPayloadProbe::Missing,
            FeedProbe::Transient => return FeedPayloadProbe::Transient,
        };
        let Some(root) = decode_feed_payload_root(update) else {
            return FeedPayloadProbe::Transient;
        };
        if root.span() > FEED_INLINE_PAYLOAD_BYTES as u64 {
            return FeedPayloadProbe::Deferred(index, root);
        }
        match retrieve_feed_payload(&root, FEED_INLINE_PAYLOAD_BYTES, &self.client.chunk_port.0)
            .await
        {
            Some(bytes) => FeedPayloadProbe::Found(RawFeedPayload { index, bytes }),
            None => FeedPayloadProbe::Transient,
        }
    }

    /// A payload too large to fetch inline: try its tail first.
    ///
    /// A live playlist only grows at the back, so the tail usually carries every
    /// new `#EXTINF` line and `merge_tail` can append them without walking the
    /// whole tree. Falls back to the full payload when it cannot.
    async fn apply_deferred(&self, index: u64, root: &FeedPayloadRoot) -> Option<usize> {
        if let Some(tail) =
            retrieve_feed_payload_tail(root, FEED_TAIL_PROBE_BYTES, &self.client.chunk_port.0).await
            && let Some(appended) = self.apply_with(index, |playlist| playlist.merge_tail(&tail))
        {
            return Some(appended);
        }
        let bytes = retrieve_feed_payload(
            root,
            MAX_STREAM_FEED_PAYLOAD_BYTES,
            &self.client.chunk_port.0,
        )
        .await?;
        self.apply(index, HlsPlaylist::parse(&bytes)?)
    }

    /// Merge a candidate playlist onto the one we hold.
    fn apply(&self, index: u64, candidate: HlsPlaylist) -> Option<usize> {
        self.apply_with(index, move |playlist| playlist.merge_playlist(candidate))
    }

    fn apply_with(
        &self,
        index: u64,
        merge: impl FnOnce(&mut HlsPlaylist) -> Option<usize>,
    ) -> Option<usize> {
        let mut state = self.state.borrow_mut();
        let appended = merge(&mut state.playlist)?;
        state.index = state.index.max(index);
        drop(state);
        self.changed.notify(usize::MAX);
        Some(appended)
    }

    /// Re-resolve the frontier when stepping forward has stopped working.
    ///
    /// Three outcomes matter: the frontier is where we are and the playlist has
    /// finalized (`#EXT-X-ENDLIST` arrived); the frontier is ahead and joins, so
    /// we merge; or the frontier is ahead and does *not* join, meaning the window
    /// slid past us and we re-anchor there, counting what we lost.
    async fn recover_frontier(&self) -> bool {
        let head = self.feed_index();
        let Some((index, update)) = seek_latest_feed_update_indexed(
            self.owner.clone(),
            self.topic.clone(),
            &self.client.chunk_port.0,
        )
        .await
        else {
            return false;
        };
        if index < head {
            return false;
        }
        let Some(root) = decode_feed_payload_root(update) else {
            return false;
        };
        let Some(bytes) =
            retrieve_feed_payload(&root, MAX_STREAM_FEED_PAYLOAD_BYTES, &self.client.chunk_port.0)
                .await
        else {
            return false;
        };
        let Some(candidate) = HlsPlaylist::parse(&bytes) else {
            return false;
        };

        if index == head {
            // Same index, but its payload may have gained the ENDLIST.
            if !candidate.finalized || self.finalized() {
                return false;
            }
            self.client
                .interface_log(format!("HLS feed finalized at {index}"));
            let mut state = self.state.borrow_mut();
            state.playlist.finalized = true;
            drop(state);
            self.changed.notify(usize::MAX);
            return true;
        }

        if let Some(appended) = self.apply(index, candidate.clone()) {
            if appended != 0 {
                self.client
                    .interface_log(format!("HLS frontier recovered at {index}"));
            }
            return true;
        }

        // The window slid past us. Take the skip rather than stall: this is what
        // a real player does when it falls off the live edge.
        let mut state = self.state.borrow_mut();
        let previous_end = state
            .playlist
            .sequence
            .saturating_add(state.playlist.segments.len() as u64);
        state.skipped = state
            .skipped
            .saturating_add(candidate.sequence.saturating_sub(previous_end));
        state.playlist = candidate;
        state.index = index;
        let skipped = state.skipped;
        drop(state);
        self.changed.notify(usize::MAX);
        self.client.interface_log(format!(
            "HLS re-anchored at {index}; {skipped} segment(s) skipped"
        ));
        true
    }
}

/// Resolve the newest feed update and parse it, retrying while there is nothing
/// to read yet.
///
/// Mirrors upstream's `discover_raw_for_view`: a stream whose publisher has not
/// written its first manifest is not an error, it is a stream that has not
/// started. Failing on the first empty seek made joining a stream a race against
/// its own first upload.
async fn discover_edge(
    client: &Arc<Weeb3>,
    owner: &str,
    topic: &str,
    started: Instant,
    timeout: Duration,
) -> Option<(u64, HlsPlaylist)> {
    loop {
        if let Some((index, update)) = seek_latest_feed_update_indexed(
            owner.to_string(),
            topic.to_string(),
            &client.chunk_port.0,
        )
        .await
            && let Some(root) = decode_feed_payload_root(update)
            && let Some(bytes) =
                retrieve_feed_payload(&root, MAX_STREAM_FEED_PAYLOAD_BYTES, &client.chunk_port.0)
                    .await
            && let Some(playlist) = HlsPlaylist::parse(&bytes)
        {
            return Some((index, playlist));
        }
        if started.elapsed() >= timeout {
            return None;
        }
        async_std::task::sleep(INITIAL_DISCOVERY_RETRY_DELAY).await;
    }
}

/// Sequence offset of the segment a duration offset falls on.
fn sequence_at_offset(playlist: &HlsPlaylist, offset: f64) -> u64 {
    let mut elapsed = 0.0;
    for (position, segment) in playlist.segments.iter().enumerate() {
        if elapsed + segment.duration > offset + f64::EPSILON {
            return position as u64;
        }
        elapsed += segment.duration;
    }
    playlist.segments.len().saturating_sub(1) as u64
}
