//! In-memory replacement for the browser IndexedDB store.
//!
//! The viewer keeps no durable state: a load-test client wants a cold cache on
//! every run, and with swap disabled there is no chequebook to persist. These
//! stubs exist only to satisfy the accounting code paths that survive from the
//! browser build.







