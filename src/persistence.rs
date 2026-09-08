//! In-memory replacement for the browser IndexedDB store.
//!
//! The viewer keeps no durable state: a load-test client wants a cold cache on
//! every run, and with swap disabled there is no chequebook to persist. These
//! stubs exist only to satisfy the accounting code paths that survive from the
//! browser build.

use async_std::sync::Mutex;
use std::collections::HashMap;
use std::sync::LazyLock;

static PAYOUTS: LazyLock<Mutex<HashMap<Vec<u8>, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn payout_key(chequebook: &[u8], beneficiary: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(chequebook.len() + beneficiary.len());
    key.extend_from_slice(chequebook);
    key.extend_from_slice(beneficiary);
    key
}

/// No chequebook exists in viewer scope.
pub(crate) async fn get_chequebook_address() -> Vec<u8> {
    Vec::new()
}

/// No signing key: the overlay identity is generated per process.
pub(crate) async fn get_chequebook_signer_key() -> Vec<u8> {
    Vec::new()
}

pub(crate) async fn get_chequebook_last_issued_cheque_payout(
    chequebook: &[u8],
    beneficiary: &[u8],
) -> Vec<u8> {
    PAYOUTS
        .lock()
        .await
        .get(&payout_key(chequebook, beneficiary))
        .cloned()
        .unwrap_or_default()
}

pub(crate) async fn set_chequebook_last_issued_cheque_payout(
    chequebook: &[u8],
    beneficiary: &[u8],
    value: &[u8],
) -> bool {
    PAYOUTS
        .lock()
        .await
        .insert(payout_key(chequebook, beneficiary), value.to_vec());
    true
}
