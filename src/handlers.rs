use prost::Message;

use crate::mpsc;

use crate::{PrivateKeySigner, StreamControl};
use libp2p::{
    PeerId, Stream, StreamProtocol,
    futures::{AsyncReadExt, AsyncWriteExt},
    swarm::ConnectionId,
};

use alloy_primitives::{Address, U256};

use crate::conventions::*;
use crate::weeb_3::etiquette_1;
use crate::weeb_3::etiquette_2;
use crate::weeb_3::etiquette_4;
use crate::weeb_3::etiquette_5;
use crate::weeb_3::etiquette_6;


use crate::HANDSHAKE_PROTOCOL;
use crate::PSEUDOSETTLE_PROTOCOL;
use crate::RETRIEVAL_PROTOCOL;
use crate::SWAP_PROTOCOL;
use crate::{OutboundProtocolSession, PeerDialInstruction, TransportConnectionSession};

const CONTROL_PROTOCOL_MAX_FRAME_BYTES: u64 = 64 * 1024;
const HIVE_PROTOCOL_MAX_FRAME_BYTES: u64 = 128 * 1024;
const EMPTY_HEADERS_FRAME: &[u8] = &[0];

fn significant_big_endian(bytes: &[u8]) -> &[u8] {
    &bytes[bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len())..]
}

fn trimmed_big_endian(bytes: &[u8]) -> Vec<u8> {
    significant_big_endian(bytes).to_vec()
}

fn decode_big_endian_u64(bytes: &[u8]) -> Option<u64> {
    let bytes = significant_big_endian(bytes);
    if bytes.len() > 8 {
        return None;
    }
    let mut value = [0_u8; 8];
    value[8 - bytes.len()..].copy_from_slice(bytes);
    Some(u64::from_be_bytes(value))
}


async fn read_control_protocol_frame(stream: &mut Stream) -> Option<Vec<u8>> {
    read_control_protocol_frame_bounded(stream, CONTROL_PROTOCOL_MAX_FRAME_BYTES).await
}

async fn read_control_protocol_frame_bounded(stream: &mut Stream, maximum: u64) -> Option<Vec<u8>> {
    let mut frame_len = 0_u64;
    for shift in (0_u32..64).step_by(7) {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.ok()?;
        let value = u64::from(byte[0] & 0x7f);
        if value > (u64::MAX >> shift) {
            return None;
        }
        frame_len |= value << shift;
        if frame_len > maximum {
            return None;
        }
        if byte[0] & 0x80 == 0 {
            let mut frame = vec![0_u8; usize::try_from(frame_len).ok()?];
            stream.read_exact(&mut frame).await.ok()?;
            return Some(frame);
        }
    }
    None
}


async fn handshake_exchange(
    peer: PeerId,
    local_peer: PeerId,
    connection_attempt_id: usize,
    connection_id: ConnectionId,
    network_id: u64,
    mut stream: Stream,
    observed_underlay: &libp2p::core::Multiaddr,
    signer: &PrivateKeySigner,
    connected_peers: &mpsc::Sender<PeerFile>,
) -> Option<()> {
    let syn = etiquette_1::Syn {
        observed_underlay: observed_underlay.to_vec(),
    };

    let syn_frame = syn.encode_length_delimited_to_vec();
    stream.write_all(&syn_frame).await.ok()?;
    stream.flush().await.ok()?;

    let handshake_frame = read_control_protocol_frame(&mut stream).await?;
    let syn_ack = etiquette_1::SynAck::decode(handshake_frame.as_slice()).ok()?;
    let syn = syn_ack.syn?;
    let observed_underlays = crate::addresses::deserialize_underlays(&syn.observed_underlay);
    if observed_underlays.is_empty()
        || observed_underlays
            .iter()
            .any(|underlay| try_from_multiaddr(underlay).as_ref() != Some(&local_peer))
    {
        return None;
    }
    let underlay = syn.observed_underlay;

    let ack = syn_ack.ack?;
    if ack.network_id != network_id {
        return None;
    }
    let peer_address = ack.address?;
    if peer_address.overlay.len() != 32 {
        return None;
    }

    let beneficiary = parse_address(
        &peer_address.underlay,
        &peer_address.overlay,
        &peer_address.signature,
        &peer_address.nonce,
        peer_address.timestamp,
        network_id,
        &peer_address.chequebook_address,
    );
    if beneficiary == Address::ZERO {
        return None;
    }
    let peer_overlay = peer_address.overlay;

    let nonce: [u8; 32] = [0; 32];
    let timestamp = (crate::runtime_conventions::Date::now() / 1000.0).floor() as i64;
    let chequebook_address = EMPTY_CHEQUEBOOK_ADDRESS.to_vec();
    let mut overlay_input = [0_u8; 60];
    overlay_input[..20].copy_from_slice(signer.address().as_slice());
    overlay_input[20..28].copy_from_slice(&network_id.to_le_bytes());
    overlay_input[28..].copy_from_slice(&nonce);
    let overlay = keccak256(overlay_input);
    let sign_data = generate_sign_data(
        &underlay,
        overlay.as_slice(),
        network_id,
        &nonce,
        timestamp,
        &chequebook_address,
    );
    let signature = signer.sign_message(&sign_data).ok()?;

    let ack = etiquette_1::Ack {
        address: Some(etiquette_1::BzzAddress {
            overlay: overlay.to_vec(),
            underlay,
            signature: signature.to_vec(),
            nonce: nonce.to_vec(),
            timestamp,
            chequebook_address,
        }),
        network_id,
        full_node: false,
        welcome_message: "... Ara Ara ...".to_string(),
    };

    let ack_frame = ack.encode_length_delimited_to_vec();
    stream.write_all(&ack_frame).await.ok()?;
    stream.flush().await.ok()?;

    let _ = stream.close().await;

    connected_peers
        .try_send(PeerFile {
            peer_id: peer,
            overlay: peer_overlay,
            beneficiary,
            connection_attempt_id,
            connection_id,
        })
        .ok()
}

pub async fn pricing_handler(
    peer: PeerId,
    mut stream: Stream,
    session: TransportConnectionSession,
    pricing_updates: &mpsc::Sender<(PeerId, u64, TransportConnectionSession)>,
) {
    if read_control_protocol_frame(&mut stream).await.is_none()
        || stream.write_all(EMPTY_HEADERS_FRAME).await.is_err()
    {
        return;
    }
    let _ = stream.flush().await;
    let _ = stream.close().await;

    let Some(announce_frame) = read_control_protocol_frame(&mut stream).await else {
        return;
    };
    let Ok(announcement) = etiquette_4::AnnouncePaymentThreshold::decode(announce_frame.as_slice())
    else {
        return;
    };

    let Some(payment_threshold) = decode_big_endian_u64(&announcement.payment_threshold) else {
        return;
    };

    if !session.is_current() {
        return;
    }
    let _ = pricing_updates.try_send((peer, payment_threshold, session));
}

pub async fn gossip_handler(
    mut stream: Stream,
    peer_dials: &mpsc::Sender<PeerDialInstruction>,
    generation: u64,
) {
    if read_control_protocol_frame(&mut stream).await.is_none()
        || stream.write_all(EMPTY_HEADERS_FRAME).await.is_err()
    {
        return;
    }
    let _ = stream.flush().await;
    let _ = stream.close().await;

    let Some(peers_frame) =
        read_control_protocol_frame_bounded(&mut stream, HIVE_PROTOCOL_MAX_FRAME_BYTES).await
    else {
        return;
    };

    let Ok(peers) = etiquette_2::Peers::decode(peers_frame.as_slice()) else {
        return;
    };

    for peer in peers.peers {
        if peer_dials
            .send(PeerDialInstruction {
                underlay: peer.underlay,
                generation,
                retry: false,
                bootnode: false,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefreshmentOutcome {
    NotDispatched,
    Acknowledged(u64),
    AmbiguousAfterPayment,
}

async fn refreshment_exchange(amount: u64, mut stream: Stream) -> RefreshmentOutcome {
    if stream.write_all(EMPTY_HEADERS_FRAME).await.is_err() {
        return RefreshmentOutcome::NotDispatched;
    }
    if stream.flush().await.is_err() {
        return RefreshmentOutcome::NotDispatched;
    }

    if read_control_protocol_frame(&mut stream).await.is_none() {
        return RefreshmentOutcome::NotDispatched;
    }

    let payment = etiquette_5::Payment {
        amount: trimmed_big_endian(&amount.to_be_bytes()),
    };

    let payment_frame = payment.encode_length_delimited_to_vec();
    if stream.write_all(&payment_frame).await.is_err() {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    }
    if stream.flush().await.is_err() || stream.close().await.is_err() {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    }

    let Some(ack_frame) = read_control_protocol_frame(&mut stream).await else {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    };
    let Ok(ack) = etiquette_5::PaymentAck::decode(ack_frame.as_slice()) else {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    };

    let Some(acknowledged_amount) = decode_big_endian_u64(&ack.amount) else {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    };

    if acknowledged_amount > amount {
        return RefreshmentOutcome::AmbiguousAfterPayment;
    }
    RefreshmentOutcome::Acknowledged(acknowledged_amount)
}

/// Viewer scope holds no chequebook, so no cheque is ever issued. The stream is
/// closed politely and the exchange reports failure.
async fn cheque_exchange(
    _amount: u64,
    mut stream: Stream,
    _beneficiary: Address,
    _price: U256,
    _deduction: U256,
) -> Option<()> {
    let _ = stream.close().await;
    None
}

async fn retrieval_exchange(chunk_address: Vec<u8>, mut stream: Stream) -> Option<Vec<u8>> {
    if stream.write_all(EMPTY_HEADERS_FRAME).await.is_err() {
        return None;
    }
    let _ = stream.flush().await;

    read_control_protocol_frame(&mut stream).await?;

    let request = etiquette_6::Request {
        addr: chunk_address,
    };

    let request_frame = request.encode_length_delimited_to_vec();
    if stream.write_all(&request_frame).await.is_err() {
        return None;
    }
    let _ = stream.flush().await;
    let _ = stream.close().await;

    let delivery = read_control_protocol_frame(&mut stream).await?;
    etiquette_6::Delivery::decode(delivery.as_slice())
        .ok()
        .map(|message| message.data)
}

pub async fn connection_handler(
    peer: PeerId,
    local_peer: PeerId,
    connection_attempt_id: usize,
    connection_id: ConnectionId,
    physical_connections: crate::PhysicalConnectionMap,
    network_id: u64,
    mut control: StreamControl,
    observed_underlay: &libp2p::core::Multiaddr,
    signer: &PrivateKeySigner,
    connected_peers: &mpsc::Sender<PeerFile>,
) -> bool {
    let Ok(stream) = control.open_stream(peer, HANDSHAKE_PROTOCOL).await else {
        return false;
    };
    let Some(session) =
        TransportConnectionSession::capture(peer, connection_id, physical_connections)
    else {
        drop(stream);
        return false;
    };

    handshake_exchange(
        peer,
        local_peer,
        connection_attempt_id,
        session.connection_id(),
        network_id,
        stream,
        observed_underlay,
        signer,
        connected_peers,
    )
    .await
    .is_some()
}

async fn open_current_outbound_stream(
    peer: PeerId,
    mut control: StreamControl,
    protocol: StreamProtocol,
    session: &OutboundProtocolSession,
) -> Option<Stream> {
    if !session.is_current() {
        return None;
    }
    let Ok(stream) = control.open_stream(peer, protocol).await else {
        return None;
    };
    if !session.is_current() {
        drop(stream);
        return None;
    }
    Some(stream)
}

pub async fn refresh_handler(
    peer: PeerId,
    amount: u64,
    control: StreamControl,
    session: OutboundProtocolSession,
) -> RefreshmentOutcome {
    let Some(stream) =
        open_current_outbound_stream(peer, control, PSEUDOSETTLE_PROTOCOL, &session).await
    else {
        return RefreshmentOutcome::NotDispatched;
    };

    refreshment_exchange(amount, stream).await
}

pub async fn issue_handler(
    peer: PeerId,
    amount: u64,
    control: StreamControl,
    session: OutboundProtocolSession,
    beneficiary: Address,
    price: U256,
    deduction: U256,
) -> bool {
    let Some(stream) = open_current_outbound_stream(peer, control, SWAP_PROTOCOL, &session).await
    else {
        return false;
    };

    cheque_exchange(amount, stream, beneficiary, price, deduction)
        .await
        .is_some()
}

pub async fn retrieve_handler(
    peer: PeerId,
    chunk_address: Vec<u8>,
    control: StreamControl,
    session: OutboundProtocolSession,
) -> Option<Vec<u8>> {
    let stream = open_current_outbound_stream(peer, control, RETRIEVAL_PROTOCOL, &session).await?;
    retrieval_exchange(chunk_address, stream).await
}


