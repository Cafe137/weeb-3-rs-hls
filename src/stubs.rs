//! No-op stands-in for capabilities the viewer deliberately does not have.
//!
//! Viewer scope is "connect to peers and watch an HLS stream". There is no
//! wallet, no chequebook, no stamp and no ENS resolution, so the code paths
//! that would consult them are kept intact and answered with "not available"
//! rather than being torn out of the protocol loops.

use alloy_primitives::U256;

/// No swap: the price oracle is never consulted.
pub(crate) async fn get_price_from_oracle() -> Option<(U256, U256)> {
    None
}

/// No ENS: viewers address content by hex reference or by feed owner + topic.
pub(crate) async fn resolve_ens_reference(_reference: String, _suffix: &str) -> Vec<u8> {
    Vec::new()
}

/// No stamp: viewers never upload, so there is no feed owner of our own.
pub(crate) async fn secure_ensure_feed_owner() -> Option<Vec<u8>> {
    None
}

pub(crate) mod secure_vault {
    /// No chequebook, so cheques are never active.
    pub(crate) async fn worker_cheques_active() -> bool {
        false
    }
}
