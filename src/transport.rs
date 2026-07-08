use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::message::{
    self, CasReq, CasRes, ClientResponse, InterNodeReq, KvEvent, KvRequest, Outgoing, ReadReq,
    ReadRes,
};

#[derive(Debug, Error)]
pub enum TransportErr {
    #[error("got response, which not awaited")]
    NotAwaited,
    #[error("timeout receive {0}")]
    TimeoutRx(&'static str),
    #[error("sender dropped: {0}")]
    SenderDropped(&'static str),
    #[error("failed to send to {0} channel")]
    ChanSend(&'static str),
}

#[derive(Clone)]
pub struct PendingTable {
    cas: Arc<tokio::sync::Mutex<HashMap<usize, oneshot::Sender<message::CasRes>>>>,
    kv_read: Arc<tokio::sync::Mutex<HashMap<usize, oneshot::Sender<message::ReadRes>>>>,
}

impl PendingTable {
    pub fn new() -> Self {
        PendingTable {
            cas: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            kv_read: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
    async fn add_cas(&self, msg_id: usize) -> oneshot::Receiver<message::CasRes> {
        let (tx, rx) = oneshot::channel();
        self.cas.lock().await.insert(msg_id, tx);
        rx
    }

    async fn reg_cas(&self, msg_id: usize, res: CasRes) -> Result<(), TransportErr> {
        let mut cast = self.cas.lock().await;

        if let Entry::Occupied(entry) = cast.entry(msg_id) {
            let tx = entry.remove();
            tx.send(res);
        } else {
            return Err(TransportErr::NotAwaited);
        }

        Ok(())
    }

    async fn add_read(&self, msg_id: usize) -> oneshot::Receiver<message::ReadRes> {
        let (tx, rx) = oneshot::channel();
        self.kv_read.lock().await.insert(msg_id, tx);
        rx
    }

    async fn reg_read(&self, msg_id: usize, res: ReadRes) -> Result<(), TransportErr> {
        let mut cast = self.kv_read.lock().await;

        if let Entry::Occupied(entry) = cast.entry(msg_id) {
            let tx = entry.remove();
            tx.send(res);
        } else {
            return Err(TransportErr::NotAwaited);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Transport {
    msg_id: Arc<AtomicU64>,
    write_tx: mpsc::Sender<Outgoing>,
    timeout: tokio::time::Duration,
    pending: PendingTable,
}

impl Transport {
    pub fn new(write_tx: mpsc::Sender<Outgoing>, timeout: tokio::time::Duration) -> Self {
        Self {
            msg_id: Arc::new(AtomicU64::new(1)),
            write_tx,
            timeout,
            pending: PendingTable::new(),
        }
    }
    pub async fn rpc_cas(&self, req: &CasReq) -> Result<CasRes, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let req = Outgoing::KvRequest {
            req: KvRequest::Cas(req.clone()),
            msg_id,
        };

        self.write_tx
            .send(req)
            .await
            .map_err(|_| TransportErr::ChanSend("write_tx: cas"))?;

        let rx = self.pending.add_cas(msg_id).await;

        Ok((tokio::time::timeout(self.timeout, rx)
            .await
            .map_err(|_| TransportErr::TimeoutRx("cas"))?)
        .map_err(|_| TransportErr::SenderDropped("cas_tx"))?)
    }

    pub async fn rpc_read(&self, req: &ReadReq) -> Result<usize, TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        let req = Outgoing::KvRequest {
            req: KvRequest::Read(req.clone()),
            msg_id,
        };

        self.write_tx
            .send(req)
            .await
            .map_err(|_| TransportErr::ChanSend("write_tx: read"))?;

        let rx = self.pending.add_read(msg_id).await;

        Ok((tokio::time::timeout(self.timeout, rx)
            .await
            .map_err(|_| TransportErr::TimeoutRx("read"))?)
        .map_err(|_| TransportErr::SenderDropped("read_tx"))?
        .value)
    }

    pub async fn send_client_response(
        &self,
        res: ClientResponse,
        in_reply_to: usize,
        dest: String,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        self.write_tx
            .send(Outgoing::ClientResponse {
                res,
                msg_id,
                in_reply_to,
                dest,
            })
            .await
            .map_err(|_| TransportErr::ChanSend("write_tx"))
    }

    pub async fn send_internode_req(
        &self,
        dest: &String,
        req: InterNodeReq,
    ) -> Result<(), TransportErr> {
        let msg_id = self.msg_id.fetch_add(1, atomic::Ordering::Relaxed) as usize;
        self.write_tx
            .send(Outgoing::InterNodeReq {
                req,
                msg_id,
                dest: dest.to_string(),
            })
            .await
            .map_err(|_| TransportErr::ChanSend("write_tx"))
    }

    pub async fn register_kv_event(&self, event: KvEvent) -> Result<(), TransportErr> {
        let msg_id = event.get_in_reply_to();
        match event {
            KvEvent::CasRes(res) => self.pending.reg_cas(msg_id, res).await,
            KvEvent::ReadRes(res) => self.pending.reg_read(msg_id, res).await,
        }
    }
}
