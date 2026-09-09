use crate::*;

pub(crate) fn interface_log_to(log_port: &mpsc::Sender<String>, log_start_ms: f64, log0: String) {
    if log_port.is_full() {
        return;
    }
    let elapsed_ms = (Date::now() - log_start_ms).max(0.0).round() as u64;
    let log = format!("[+{}ms] {}", elapsed_ms, log0);
    let _ = log_port.try_send(log);
}

pub(crate) type AsyncPort<T> = (mpsc::Sender<T>, mpsc::Receiver<T>);
pub(crate) type BootnodeChange = (String, bool, u64);

#[derive(Clone)]
pub(crate) struct ChunkRetrieveSender {
    runtime_scope: usize,
    sender: mpsc::Sender<ChunkRetrieveRequest>,
}

impl ChunkRetrieveSender {
    pub(crate) fn runtime_scope(&self) -> usize {
        self.runtime_scope
    }

    pub(crate) fn try_send(
        &self,
        request: ChunkRetrieveRequest,
    ) -> Result<(), mpsc::TrySendError<ChunkRetrieveRequest>> {
        self.sender.try_send(request)
    }
}

pub(crate) type ChunkRetrieveReceiver = mpsc::Receiver<ChunkRetrieveRequest>;

static NEXT_CHUNK_RETRIEVE_RUNTIME_SCOPE: AtomicUsize = AtomicUsize::new(1);

pub(crate) fn chunk_retrieve_channel() -> (ChunkRetrieveSender, ChunkRetrieveReceiver) {
    let (sender, receiver) = mpsc::unbounded::<ChunkRetrieveRequest>();
    let runtime_scope = NEXT_CHUNK_RETRIEVE_RUNTIME_SCOPE.fetch_add(1, Ordering::Relaxed);
    (
        ChunkRetrieveSender {
            runtime_scope,
            sender,
        },
        receiver,
    )
}

pub(crate) struct ChunkRetrieveRequest {
    pub address: Vec<u8>,
    /// What the requester asked for. Both senders know for certain: the
    /// bytes-tree walk asks for content-addressed chunks, the feed path for
    /// single-owner ones. Threading it means a reply is hashed once at most.
    pub expect: crate::conventions::ChunkShape,
    pub chan: mpsc::Sender<Vec<u8>>,
    pub cancel: Option<RetrieveCancelToken>,
    pub admission: Option<retrieval_conventions::RetrieveAdmission>,
    pub hedge_demand: Option<retrieval_conventions::SharedRetrieveHedgeDemand>,
}



/// Native replacement for `js_sys::Date`: milliseconds since the Unix epoch.
pub(crate) struct Date;

impl Date {
    pub(crate) fn now() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64
    }
}
