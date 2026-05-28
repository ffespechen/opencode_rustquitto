use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::broker::session::Session;
use crate::broker::subscriptions::{Subscriber, TopicTree};
use crate::error::Result;
use crate::protocol::{
    ConnackReturnCode, Packet, PacketId, PublishPacket, QoS,
    SubscribeReturnCode, SubackPacket,
};

pub mod session;
pub mod subscriptions;

#[derive(Debug, Default)]
pub struct Broker {
    sessions: DashMap<String, Arc<Session>>,
    subscriptions: RwLock<TopicTree>,
}

impl Broker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_session(&self, session: Arc<Session>) {
        self.sessions
            .insert(session.client_id.clone(), session);
    }

    pub fn remove_session(&self, client_id: &str) {
        self.sessions.remove(client_id);
    }

    pub fn get_session(&self, client_id: &str) -> Option<Arc<Session>> {
        self.sessions.get(client_id).map(|s| Arc::clone(s.value()))
    }

    pub async fn handle_connect(
        &self,
        session: &Arc<Session>,
    ) -> (ConnackReturnCode, bool) {
        if self.sessions.contains_key(&session.client_id) {
            info!(
                "Client '{}' already connected, replacing session",
                session.client_id
            );
        }

        let return_code = ConnackReturnCode::Accepted;
        (return_code, !session.clean_session)
    }

    pub async fn handle_publish(&self, packet: &PublishPacket) -> Result<()> {
        let subscribers = {
            let tree = self.subscriptions.read().await;
            tree.matches(&packet.topic_name)
        };

        debug!(
            "PUBLISH to '{}': {} subscribers matched",
            packet.topic_name,
            subscribers.len()
        );

        for subscriber in &subscribers {
            let effective_qos = std::cmp::min(packet.qos, subscriber.qos);
            let mut publish = packet.clone();
            publish.qos = effective_qos;

            if effective_qos == QoS::AtMostOnce {
                let _ = subscriber.sender.send(Packet::Publish(publish));
            } else if let Some(session) = self.get_session(&subscriber.client_id) {
                session.enqueue_publish(publish).await;
            }
        }

        Ok(())
    }

    pub async fn handle_subscribe(
        &self,
        session: &Arc<Session>,
        packet: &crate::protocol::SubscribePacket,
    ) -> SubackPacket {
        let mut return_codes = Vec::with_capacity(packet.subscriptions.len());

        let sender = session.sender();
        let client_id = session.client_id.clone();

        for (filter, _requested_qos) in &packet.subscriptions {
            let topic = filter;

            if is_valid_topic_filter(topic) {
                let granted_qos = QoS::AtLeastOnce;
                return_codes.push(match granted_qos {
                    QoS::AtMostOnce => SubscribeReturnCode::SuccessQoS0,
                    QoS::AtLeastOnce => SubscribeReturnCode::SuccessQoS1,
                    QoS::ExactlyOnce => SubscribeReturnCode::SuccessQoS2,
                });

                let subscriber = Subscriber {
                    client_id: client_id.clone(),
                    sender: sender.clone(),
                    qos: granted_qos,
                };

                let mut tree = self.subscriptions.write().await;
                tree.add(topic, subscriber);
            } else {
                return_codes.push(SubscribeReturnCode::Failure);
            }
        }

        SubackPacket {
            packet_id: packet.packet_id,
            return_codes,
        }
    }

    pub async fn handle_unsubscribe(
        &self,
        session: &Arc<Session>,
        packet: &crate::protocol::UnsubscribePacket,
    ) {
        let mut tree = self.subscriptions.write().await;
        for filter in &packet.topic_filters {
            tree.remove(filter, &session.client_id);
        }
    }

    pub async fn handle_disconnect(&self, session: &Arc<Session>) {
        info!("Client '{}' disconnected", session.client_id);
        self.remove_session(&session.client_id);
        let mut tree = self.subscriptions.write().await;
        tree.remove_all_for_client(&session.client_id);
    }

    pub async fn handle_puback(&self, session: &Arc<Session>, packet_id: PacketId) {
        session.handle_puback(packet_id).await;
    }

    pub async fn handle_pubrec(&self, session: &Arc<Session>, packet_id: PacketId) {
        session.handle_pubrec(packet_id).await;

        let reply_sender = session.sender();
        let _ = reply_sender.send(Packet::Pubrel(packet_id));
    }

    pub async fn handle_pubrel(&self, session: &Arc<Session>, packet_id: PacketId) {
        let reply_sender = session.sender();
        let _ = reply_sender.send(Packet::Pubcomp(packet_id));
    }

    pub async fn handle_pubcomp(&self, session: &Arc<Session>, packet_id: PacketId) {
        session.handle_pubcomp(packet_id).await;
    }
}

fn is_valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.len() > 65535 {
        return false;
    }

    if let Some(hash_pos) = filter.find('#') {
        if hash_pos > 0 && !filter[..hash_pos].ends_with('/') {
            return false;
        }
        if hash_pos + 1 != filter.len() {
            return false;
        }
    }

    for part in filter.split('/') {
        if part.contains('+') && part.len() > 1 {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_topic_filter_simple() {
        assert!(is_valid_topic_filter("sensor/temperature"));
    }

    #[test]
    fn valid_topic_filter_single_wildcard() {
        assert!(is_valid_topic_filter("sensor/+/temperature"));
    }

    #[test]
    fn valid_topic_filter_multi_wildcard() {
        assert!(is_valid_topic_filter("sensor/#"));
    }

    #[test]
    fn invalid_topic_filter_empty() {
        assert!(!is_valid_topic_filter(""));
    }

    #[test]
    fn invalid_topic_filter_hash_not_at_end() {
        assert!(!is_valid_topic_filter("sensor/#/extra"));
    }
}
