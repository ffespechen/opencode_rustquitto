use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::broker::session::Session;
use crate::broker::Broker;
use crate::config::Config;
use crate::error::{BrokerError, Result};
use crate::protocol::{
    decode_fixed_header, ConnackPacket, Packet, QoS,
};
use crate::protocol::encode;

pub async fn start(config: &Config, broker: Arc<Broker>) -> Result<()> {
    let addr = format!("{}:{}", config.bind_addr, config.tcp_port);
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        BrokerError::Config(format!("Failed to bind TCP on {addr}: {e}"))
    })?;

    info!("MQTT TCP server listening on {addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let broker = Arc::clone(&broker);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, broker, peer_addr.to_string()).await {
                warn!("Connection {peer_addr} error: {e}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    broker: Arc<Broker>,
    peer: String,
) -> Result<()> {
    debug!("New TCP connection from {peer}");

    let (reader, mut writer) = stream.split();
    let mut reader = tokio::io::BufReader::new(reader);

    let mut session: Option<Arc<Session>> = None;
    let mut out_rx: Option<mpsc::UnboundedReceiver<Packet>> = None;
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        tokio::select! {
            read_result = read_async(&mut reader, &mut buf) => {
                match read_result {
                    Ok(0) => {
                        debug!("Connection {peer} closed");
                        break;
                    }
                    Ok(_) => {
                        while let Some(packet_result) = extract_packet(&mut buf) {
                            match packet_result {
                                Ok(packet) => {
                                    let (sess, rx, keep_alive) = handle_packet(
                                        &broker,
                                        session,
                                        out_rx,
                                        &packet,
                                        &peer,
                                    ).await?;

                                    session = sess;
                                    out_rx = rx;

                                    let _ = keep_alive;
                                }
                                Err(e) => {
                                    error!("Packet decode error: {e}");
                                    return Err(e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Read error from {peer}: {e}");
                        return Err(e.into());
                    }
                }
            }
            Some(packet) = recv_from_session(out_rx.as_mut()) => {
                let data = encode::encode(&packet)?;
                writer.write_all(&data).await?;
            }
        }
    }

    if let Some(sess) = session.as_ref() {
        broker.handle_disconnect(sess).await;
    }

    Ok(())
}

async fn recv_from_session(rx: Option<&mut mpsc::UnboundedReceiver<Packet>>) -> Option<Packet> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn read_async<R: AsyncReadExt + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    use tokio::io::AsyncReadExt;

    let mut temp = [0u8; 4096];
    let n = reader.read(&mut temp).await?;
    if n > 0 {
        buf.extend_from_slice(&temp[..n]);
    }
    Ok(n)
}

fn extract_packet(buf: &mut BytesMut) -> Option<std::result::Result<Packet, BrokerError>> {
    if buf.len() < 2 {
        return None;
    }

    let mut peek: &[u8] = &buf[..];
    let header = match decode_fixed_header(&mut peek) {
        Ok(h) => h,
        Err(e) => return Some(Err(e)),
    };

    let consumed = buf.len() - peek.len();
    let total_len = consumed + header.remaining_length;

    if buf.len() < total_len {
        return None;
    }

    let packet_data = buf.split_to(total_len);
    let mut slice: &[u8] = &packet_data;
    Some(crate::protocol::decode(&mut slice))
}

async fn handle_packet(
    broker: &Arc<Broker>,
    session: Option<Arc<Session>>,
    out_rx: Option<mpsc::UnboundedReceiver<Packet>>,
    packet: &Packet,
    peer: &str,
) -> Result<(Option<Arc<Session>>, Option<mpsc::UnboundedReceiver<Packet>>, u16)> {
    match packet {
        Packet::Connect(connect) => {
            debug!("CONNECT from {peer}: client_id='{}'", connect.client_id);

            let (new_session, rx) = Session::new(
                connect.client_id.clone(),
                connect.clean_session,
                connect.keep_alive,
            );

            let (return_code, session_present) =
                broker.handle_connect(&new_session).await;

            broker.register_session(Arc::clone(&new_session));
            info!("Client '{}' connected", connect.client_id);

            let sender = new_session.sender();
            let _ = sender.send(Packet::Connack(ConnackPacket {
                session_present,
                return_code,
            }));

            Ok((
                Some(new_session),
                Some(rx),
                connect.keep_alive,
            ))
        }
        Packet::Connack(_) => Err(BrokerError::Protocol("Server received CONNACK".into())),

        Packet::Publish(publish) => {
            debug!(
                "PUBLISH from {peer} to '{}' (QoS {:?})",
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

            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Puback(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_puback(sess, *pid).await;
            }
            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Pubrec(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubrec(sess, *pid).await;
            }
            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Pubrel(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubrel(sess, *pid).await;
            }
            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Pubcomp(pid) => {
            if let Some(sess) = session.as_ref() {
                broker.handle_pubcomp(sess, *pid).await;
            }
            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Subscribe(sub) => {
            debug!("SUBSCRIBE from {peer}");

            if let Some(sess) = session.as_ref() {
                let suback = broker.handle_subscribe(sess, sub).await;
                let sender = sess.sender();
                let _ = sender.send(Packet::Suback(suback));
            }

            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Suback(_) => Err(BrokerError::Protocol("Server received SUBACK".into())),

        Packet::Unsubscribe(unsub) => {
            debug!("UNSUBSCRIBE from {peer}");

            if let Some(sess) = session.as_ref() {
                broker.handle_unsubscribe(sess, unsub).await;
                let sender = sess.sender();
                let _ = sender.send(Packet::Unsuback(unsub.packet_id));
            }

            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Unsuback(_) => Err(BrokerError::Protocol("Server received UNSUBACK".into())),

        Packet::Pingreq => {
            debug!("PINGREQ from {peer}");

            if let Some(sess) = session.as_ref() {
                let sender = sess.sender();
                let _ = sender.send(Packet::Pingresp);
            }

            let keep_alive = session.as_ref().map(|s| s.keep_alive).unwrap_or(0);
            Ok((session, out_rx, keep_alive))
        }

        Packet::Pingresp => Err(BrokerError::Protocol("Server received PINGRESP".into())),

        Packet::Disconnect => {
            debug!("DISCONNECT from {peer}");

            if let Some(sess) = session.as_ref() {
                broker.handle_disconnect(sess).await;
            }

            Ok((None, None, 0))
        }
    }
}
