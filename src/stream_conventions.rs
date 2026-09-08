

/// Where playback anchors. Only `Beginning` is reachable until the live path
/// in `stream_hls` is wired through `viewer.rs`.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HlsStart {
    Beginning,
    Live,
}

























