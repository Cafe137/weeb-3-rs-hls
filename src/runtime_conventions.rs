use crate::*;

impl Weeb3 {
    pub(crate) fn runtime_is_started(&self) -> bool {
        self.runtime_started.load(Ordering::Acquire)
    }

    pub(crate) async fn start_progress(
        &self,
        kind: impl Into<String>,
        subject: impl Into<String>,
        phase: impl Into<String>,
        percent: Option<u8>,
        detail: impl Into<String>,
    ) -> String {
        self.progress
            .lock()
            .await
            .start(kind, subject, phase, percent, detail)
    }

    pub(crate) async fn update_progress(
        &self,
        id: &str,
        phase: impl Into<String>,
        percent: Option<u8>,
        detail: impl Into<String>,
    ) {
        self.progress
            .lock()
            .await
            .update(id, phase, percent, detail);
    }

    pub(crate) async fn finish_progress(
        &self,
        id: &str,
        phase: impl Into<String>,
        detail: impl Into<String>,
        ok: bool,
    ) {
        self.progress.lock().await.finish(id, phase, detail, ok);
    }

    pub(crate) async fn get_progress_snapshot(
        &self,
        seen_revision: u64,
    ) -> Option<(u64, Vec<ProgressRow>)> {
        self.progress
            .lock()
            .await
            .snapshot_if_changed(seen_revision)
    }
}

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
    pub chan: mpsc::Sender<Vec<u8>>,
    pub cancel: Option<RetrieveCancelToken>,
    pub admission: Option<retrieval_conventions::RetrieveAdmission>,
    pub hedge_demand: Option<retrieval_conventions::SharedRetrieveHedgeDemand>,
}

pub(crate) fn chunk_retrieve_request(
    address: Vec<u8>,
    chan: mpsc::Sender<Vec<u8>>,
) -> ChunkRetrieveRequest {
    ChunkRetrieveRequest {
        address,
        chan,
        cancel: None,
        admission: None,
        hedge_demand: None,
    }
}

pub(crate) struct BzzRangeRequest {
    pub(crate) metadata: BzzMetadata,
    pub(crate) start: u64,
    pub(crate) end_inclusive: u64,
    pub(crate) cancel: Option<RetrieveCancelToken>,
    pub(crate) chan: mpsc::Sender<Option<(Vec<u8>, BzzMetadata)>>,
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
