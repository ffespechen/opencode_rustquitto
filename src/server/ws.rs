use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::broker::session::Session;
use crate::broker::Broker;
use crate::config::Config;
use crate::error::{BrokerError, Result};
use crate::protocol::{
    decode, ConnackPacket, Packet, QoS,
};
use crate::protocol::encode;

#[derive(Clone)]
struct AppState {
    broker: Arc<Broker>,
}

pub async fn start(config: &Config, broker: Arc<Broker>) -> Result<()> {
    let state = AppState { broker };
    let app = Router::new()
        .route("/mqtt", get(ws_handler))
        .route("/", get(ws_handler))
        .with_state(state);

    let addr = format!("{}:{}", config.bind_addr, config.ws_port);
    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
        BrokerError::Config(format!("Failed to bind WS on {addr}: {e}"))
    })?;

    info!("MQTT WebSocket server listening on {addr}");

    axum::serve(listener, app).await.map_err(|e| {
        BrokerError::Config(format!("WebSocket server error: {e}"))
    })?;

    Ok(())
}

async fn ws_handler(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state.broker))
}

async fn handle_ws(socket: WebSocket, broker: Arc<Broker>) {
    let (mut sender, mut receiver) = socket.split();

    let mut session: Option<Arc<Session>> = None;
    let mut out_rx: Option<mpsc::UnboundedReceiver<Packet>> = None;

    loop {
        tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        let mut slice: &[u8] = &data;
                        match decode(&mut slice) {
                            Ok(packet) => {
                                let current_sess = session.clone();
                                let current_rx = out_rx.take();
                                match handle_ws_packet(
                                    &broker,
                                    current_sess,
                                    current_rx,
                                    &packet,
                                ).await {
                                    Ok((sess, rx)) => {
                                        session = sess;
                                        out_rx = rx;
                                    }
                                    Err(e) => {
                                        error!("WS packet error: {e}");
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("WS decode error: {e}");
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("WebSocket connection closed");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if sender.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("WebSocket error: {e}");
                        break;
                    }
                    _ => {}
                }
            }
            Some(packet) = recv_from_ws_session(out_rx.as_mut()) => {
                if let Ok(data) = encode::encode(&packet) {
                    if sender.send(Message::Binary(data.to_vec())).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    if let Some(sess) = session {
        broker.handle_disconnect(&sess).await;
    }
}

async fn recv_from_ws_session(
    rx: Option<&mut mpsc::UnboundedReceiver<Packet>>,
) -> Option<Packet> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn handle_ws_packet(
    broker: &Arc<Broker>,
    session: Option<Arc<Session>>,
    out_rx: Option<mpsc::UnboundedReceiver<Packet>>,
    packet: &Packet,
) -> Result<(Option<Arc<Session>>, Option<mpsc::UnboundedReceiver<Packet>>)> {
    match packet {
        Packet::Connect(connect) => {
            debug!("WS CONNECT: client_id='{}'", connect.client_id);

            let (new_session, rx) = Session::new(
                connect.client_id.clone(),
                connect.clean_session,
                connect.keep_alive,
            );

            let (return_code, session_present) =
                broker.handle_connect(&new_session).await;

            broker.register_session(Arc::clone(&new_session));
            info!("WS client '{}' connected", connect.client_id);

            let sender = new_session.sender();
            let _ = sender.send(Packet::Connack(ConnackPacket {
                session_present,
                return_code,
            }));

            Ok((Some(new_session), Some(rx)))
        }
        Packet::Connack(_) => Err(BrokerError::Protocol("Server received CONNACK".into())),

        Packet::Publish(publish) => {
            debug!(
                "WS PUBLISH to '{}' (QoS {:?})",
                publish.topic_name, publish.qos
            );

            match publish.qos {
                QoS::AtMostOnce => {
                    broker.handle_publish(publish).await?;
                }
                QoS::AtLeastOnce => {
                    broker.handle_publish(publish).await?;

                    if let (Some(sess), Some(pid)) = (session.as_ref(), publish.packet_id) {
                        let sender = sess.sender();
                        let _ = sender.send(Packet::Puback(pid));
                    }
                }
                QoS::ExactlyOnce => {
                    broker.handle_publish(publish).await?;

                    if let (Some(sess), Some(pid)) = (session.as_ref(), publish.packet_id) {
                        let sender = sess.sender();
                        let _ = sender.send(Packet::Pubrec(pid));
                    }
                }
            }

            Ok((session, out_rx))
        }

        Packet::Puback(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_puback(sess, *pid).await;
            }
            Ok((session, out_rx))
        }

        Packet::Pubrec(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubrec(sess, *pid).await;
            }
            Ok((session, out_rx))
        }

        Packet::Pubrel(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubrel(sess, *pid).await;
            }
            Ok((session, out_rx))
        }

        Packet::Pubcomp(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubcomp(sess, *pid).await;
            }
            Ok((session, out_rx))
        }

        Packet::Subscribe(sub) => {
            debug!("WS SUBSCRIBE");

            if let Some(sess) = session.as_ref() {
                let suback = broker.handle_subscribe(sess, sub).await;
                let sender = sess.sender();
                let _ = sender.send(Packet::Suback(suback));
            }

            Ok((session, out_rx))
        }

        Packet::Suback(_) => Err(BrokerError::Protocol("Server received SUBACK".into())),

        Packet::Unsubscribe(unsub) => {
            debug!("WS UNSUBSCRIBE");

            if let Some(sess) = session.as_ref() {
                broker.handle_unsubscribe(sess, unsub).await;
                let sender = sess.sender();
                let _ = sender.send(Packet::Unsuback(unsub.packet_id));
            }

            Ok((session, out_rx))
        }

        Packet::Unsuback(_) => Err(BrokerError::Protocol("Server received UNSUBACK".into())),

        Packet::Pingreq => {
            debug!("WS PINGREQ");

            if let Some(sess) = session.as_ref() {
                let sender = sess.sender();
                let _ = sender.send(Packet::Pingresp);
            }

            Ok((session, out_rx))
        }

        Packet::Pingresp => Err(BrokerError::Protocol("Server received PINGRESP".into())),

        Packet::Disconnect => {
            debug!("WS DISCONNECT");

            if let Some(sess) = session.as_ref() {
                broker.handle_disconnect(sess).await;
            }

            Ok((None, None))
        }
    }
}
