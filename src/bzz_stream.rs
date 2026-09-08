use crate::{
    ChunkRetrieveSender,
    erasure_coding::{CHUNK_SIZE, decode_span, encoded_reference_payload_len},
    manifest::manifest_payload_size_allowed,
    retrieval::{
        DecodedJoinChunk, retrieve_data_range_from_root,
    },
};

















fn embedded_join_root(data: &[u8], encrypted: bool) -> Option<DecodedJoinChunk> {
    let (level, span) = decode_span(data)?;
    let payload_len = if span <= CHUNK_SIZE as u64 {
        usize::try_from(span).ok()?
    } else {
        encoded_reference_payload_len(span, level, encrypted)?
    };
    let end = 8usize.checked_add(payload_len)?;
    if data.len() != end {
        return None;
    }

    Some(DecodedJoinChunk {
        level,
        span,
        payload: bytes::Bytes::copy_from_slice(&data[8..end]),
    })
}



#[derive(Clone, Debug)]
pub(crate) struct FeedPayloadRoot {
    root: DecodedJoinChunk,
    encrypted: bool,
}

pub(crate) fn decode_feed_payload_root(update: Vec<u8>) -> Option<FeedPayloadRoot> {
    [false, true].into_iter().find_map(|encrypted| {
        let root = embedded_join_root(&update, encrypted)?;
        manifest_payload_size_allowed(root.span).then_some(FeedPayloadRoot { root, encrypted })
    })
}

pub(crate) async fn retrieve_feed_payload(
    payload: &FeedPayloadRoot,
    maximum_payload_bytes: usize,
    chunk_retrieve_chan: &ChunkRetrieveSender,
) -> Option<Vec<u8>> {
    let maximum_span = u64::try_from(maximum_payload_bytes).ok()?;
    let span = payload.root.span;
    if span > maximum_span {
        return None;
    }
    if span == 0 {
        return Some(Vec::new());
    }
    let bytes = retrieve_data_range_from_root(
        payload.root.clone(),
        0,
        span.checked_sub(1)?,
        payload.encrypted,
        chunk_retrieve_chan,
    )
    .await?;
    (u64::try_from(bytes.len()).ok()? == span).then_some(bytes)
}



















