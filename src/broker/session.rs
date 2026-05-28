use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use crate::protocol::{Packet, PacketId, PublishPacket, QoS};

#[derive(Debug, Clone)]
#[expect(dead_code, reason = "Used for QoS 1 retransmission on timeout")]
struct PendingQoS1 {
    packet: PublishPacket,
}

#[derive(Debug, Clone)]
#[expect(dead_code, reason = "Used for QoS 2 retransmission on timeout")]
struct PendingQoS2 {
    packet: PublishPacket,
    state: QoS2State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QoS2State {
    WaitingPubrec,
    WaitingPubcomp,
}

#[derive(Debug)]
pub(crate) struct Session {
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    sender: Mutex<mpsc::UnboundedSender<Packet>>,
    next_packet_id: AtomicU16,
    pending_qos1: Mutex<HashMap<PacketId, PendingQoS1>>,
    pending_qos2: Mutex<HashMap<PacketId, PendingQoS2>>,
    #[expect(dead_code, reason = "Queue for offline client message buffering")]
    pending_outgoing: Mutex<VecDeque<PublishPacket>>,
}

impl Session {
    pub fn new(
        client_id: String,
        clean_session: bool,
        keep_alive: u16,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<Packet>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let session = Arc::new(Self {
            client_id,
            clean_session,
            keep_alive,
            sender: Mutex::new(tx),
            next_packet_id: AtomicU16::new(1),
            pending_qos1: Mutex::new(HashMap::new()),
            pending_qos2: Mutex::new(HashMap::new()),
            pending_outgoing: Mutex::new(VecDeque::new()),
        });
        (session, rx)
    }

    pub fn new_packet_id(&self) -> PacketId {
        let id = self
            .next_packet_id
            .fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_packet_id.store(1, Ordering::Relaxed);
            PacketId(1)
        } else {
            PacketId(id)
        }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<Packet> {
        self.sender
            .try_lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| {
                let (tx, _) = mpsc::unbounded_channel();
                tx
            })
    }

    pub async fn enqueue_publish(&self, mut packet: PublishPacket) {
        if packet.qos != QoS::AtMostOnce {
            let packet_id = self.new_packet_id();
            packet.packet_id = Some(packet_id);

            let sender = self.sender.lock().await;
            if sender.send(Packet::Publish(packet.clone())).is_err() {
                return;
            }
            drop(sender);

            match packet.qos {
                QoS::AtLeastOnce => {
                    self.pending_qos1
                        .lock()
                        .await
                        .insert(packet_id, PendingQoS1 { packet });
                }
                QoS::ExactlyOnce => {
                    self.pending_qos2.lock().await.insert(
                        packet_id,
                        PendingQoS2 {
                            packet,
                            state: QoS2State::WaitingPubrec,
                        },
                    );
                }
                QoS::AtMostOnce => unreachable!(),
            }
        } else {
            let sender = self.sender.lock().await;
            let _ = sender.send(Packet::Publish(packet));
        }
    }

    pub async fn handle_puback(&self, packet_id: PacketId) {
        self.pending_qos1.lock().await.remove(&packet_id);
    }

    pub async fn handle_pubrec(&self, packet_id: PacketId) {
        let mut qos2 = self.pending_qos2.lock().await;
        if let Some(entry) = qos2.get_mut(&packet_id) {
            entry.state = QoS2State::WaitingPubcomp;
        }
    }

    pub async fn handle_pubcomp(&self, packet_id: PacketId) {
        self.pending_qos2.lock().await.remove(&packet_id);
    }
}
