










pub const MAX_MANIFEST_PAYLOAD_BYTES: usize = 17 * 1024 * 1024;

pub fn manifest_payload_size_allowed(size: u64) -> bool {
    size <= MAX_MANIFEST_PAYLOAD_BYTES as u64
}













