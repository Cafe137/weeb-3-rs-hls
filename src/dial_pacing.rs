//! How fast a viewer is allowed to open connections.
//!
//! Peering is nearly free once established — 0.005 vCPU for 200 peers — but
//! *acquiring* the peers is not. Every dial that gets as far as the server's
//! Certificate message costs three P-384 ECDSA verifications, because Swarm
//! peers are published as `/tls/ws` with an AutoTLS chain (leaf → Let's Encrypt
//! YE1 → Root YE → ISRG Root X2) whose every link is signed with
//! `ecdsa-with-SHA384`, and `ring` has no assembly path for P-384: measured on
//! an x86 hyperthread, 0.878 ms per verification, so ~2.6 ms per handshake.
//!
//! A viewer opens ~1100 connections to settle on 200 peers — most peers refuse
//! a light client — and unthrottled it opens them all inside six seconds. That
//! is 5.8 core-seconds of work compressed into a **single core**, because the
//! node is `!Send` and everything above runs on one `LocalSet` thread. One
//! viewer starting therefore stalls every viewer already running on the same
//! box, which is the fleet's problem: the load generator's CPU, not Swarm's
//! capacity, then decides what the run measures.
//!
//! So dials are paced. The work is unchanged — the same peers are dialed the
//! same number of times, because that is the load a real viewer puts on Swarm —
//! it is only spread, which turns a 1.0 vCPU spike into a plateau the machine
//! and the fleet's admission control can both absorb.
//!
//! Process-wide, like the peer limit, because one process is one viewer.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Dials per second, process-wide. 0 means unthrottled, the pre-pacing burst.
///
/// 50/s costs ~0.26 vCPU while joining against 1.03 unthrottled, and still
/// reaches 200 peers inside the 60 s a viewer waits for them, because the join
/// is bounded by round trips to a thousand peers rather than by how fast the
/// dials were issued.
pub(crate) const DEFAULT_DIAL_RATE_PER_SECOND: u64 = 50;

/// Dials allowed to go out back to back before the rate binds.
///
/// The first handful of dials are the bootnodes, and nothing can be learned
/// from gossip until one of them answers, so there is no reason to make the
/// very first connection wait for a token.
const DIAL_BURST: f64 = 8.0;

static DIAL_RATE_PER_SECOND: AtomicU64 = AtomicU64::new(DEFAULT_DIAL_RATE_PER_SECOND);

pub(crate) fn dial_rate_per_second() -> u64 {
    DIAL_RATE_PER_SECOND.load(Ordering::Relaxed)
}

/// Override the dial rate. Must be called before the node starts dialing.
pub(crate) fn set_dial_rate_per_second(rate: u64) {
    DIAL_RATE_PER_SECOND.store(rate, Ordering::Relaxed);
}

struct DialBucket {
    tokens: f64,
    refilled: Instant,
}

impl DialBucket {
    fn new(now: Instant) -> Self {
        Self {
            tokens: DIAL_BURST,
            refilled: now,
        }
    }

    /// Spend a token, or say how long until there is one to spend.
    fn take(&mut self, now: Instant, rate: f64) -> Option<Duration> {
        self.tokens = (self.tokens
            + now.saturating_duration_since(self.refilled).as_secs_f64() * rate)
            .min(DIAL_BURST);
        self.refilled = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            None
        } else {
            Some(Duration::from_secs_f64((1.0 - self.tokens) / rate))
        }
    }
}

thread_local! {
    /// One bucket per thread, which is one bucket per node runtime: the node is
    /// `!Send` and its dials are all issued from its own `LocalSet` thread, so a
    /// `RefCell` is enough and nothing is contended on the hot path.
    static DIAL_BUCKET: RefCell<Option<DialBucket>> = const { RefCell::new(None) };
}

/// Wait until this viewer is allowed to open another connection.
///
/// Returns immediately when pacing is off, and never holds the `RefCell` borrow
/// across the sleep.
pub(crate) async fn await_dial_permit() {
    let rate = dial_rate_per_second();
    if rate == 0 {
        return;
    }
    let rate = rate as f64;
    loop {
        let wait = DIAL_BUCKET.with(|bucket| {
            let now = Instant::now();
            bucket
                .borrow_mut()
                .get_or_insert_with(|| DialBucket::new(now))
                .take(now, rate)
        });
        match wait {
            None => return,
            Some(wait) => async_std::task::sleep(wait).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_burst_goes_out_before_the_rate_binds() {
        let start = Instant::now();
        let mut bucket = DialBucket::new(start);
        for spent in 0..DIAL_BURST as usize {
            assert_eq!(
                bucket.take(start, 50.0),
                None,
                "dial {spent} is inside the burst and should not wait"
            );
        }
        assert_eq!(bucket.take(start, 50.0), Some(Duration::from_millis(20)));
    }

    #[test]
    fn tokens_refill_at_the_configured_rate() {
        let start = Instant::now();
        let mut bucket = DialBucket::new(start);
        for _ in 0..DIAL_BURST as usize {
            bucket.take(start, 50.0);
        }
        // A second of refill at 50/s is capped by the burst, not multiplied by it.
        assert_eq!(bucket.take(start + Duration::from_secs(1), 50.0), None);
        assert_eq!(bucket.take(start + Duration::from_secs(1), 50.0), None);
    }

    #[test]
    fn a_steady_rate_paces_one_dial_per_interval() {
        let start = Instant::now();
        let mut bucket = DialBucket::new(start);
        for _ in 0..DIAL_BURST as usize {
            bucket.take(start, 20.0);
        }
        // At 20/s a dial becomes available every 50 ms and not before.
        assert_eq!(
            bucket.take(start + Duration::from_millis(25), 20.0),
            Some(Duration::from_millis(25))
        );
        assert_eq!(bucket.take(start + Duration::from_millis(50), 20.0), None);
    }
}
