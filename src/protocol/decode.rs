use bytes::{Buf, BytesMut};

use crate::error::{BrokerError, Result};
use crate::protocol::types::{
    ConnackReturnCode, ConnectPacket, FixedHeader, Packet, PacketId, PacketType, PublishPacket,
    QoS, SubscribePacket, UnsubscribePacket,
};

fn decode_remaining_length(buf: &mut &[u8]) -> Result<(usize, usize)> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut bytes_consumed = 0usize;

    loop {
        if BytesMut::new().remaining() >= buf.len() && buf.is_empty() {
            return Err(BrokerError::Decode("Unexpected end of remaining length".into()));
        }
        let byte = inner_read_u8(buf)?;
        value += multiplier * (byte as usize & 0x7F);
        multiplier *= 128;
        bytes_consumed += 1;

        if byte & 0x80 == 0 {
            break;
        }
        if bytes_consumed >= 4 {
            return Err(BrokerError::Decode("Malformed remaining length".into()));
        }
        if multiplier > 128 * 128 * 128 {
            return Err(BrokerError::Decode("Remaining length too large".into()));
        }
    }
    Ok((value, bytes_consumed))
}

fn inner_read_u8(src: &mut &[u8]) -> Result<u8> {
    if src.is_empty() {
        return Err(BrokerError::Decode("Buffer exhausted".into()));
    }
    let byte = src[0];
    *src = &src[1..];
    Ok(byte)
}

fn inner_read_u16(src: &mut &[u8]) -> Result<u16> {
    if src.len() < 2 {
        return Err(BrokerError::Decode("Buffer too short for u16".into()));
    }
    let value = u16::from_be_bytes([src[0], src[1]]);
    *src = &src[2..];
    Ok(value)
}

fn inner_read_exact<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if src.len() < len {
        return Err(BrokerError::Decode(format!(
            "Buffer too short: need {len}, have {}",
            src.len()
        )));
    }
    let data = &src[..len];
    *src = &src[len..];
    Ok(data)
}

fn read_utf8_string(src: &mut &[u8]) -> Result<String> {
    let len = inner_read_u16(src)? as usize;
    let bytes = inner_read_exact(src, len)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| BrokerError::Decode(format!("Invalid UTF-8 in string: {e}")))
}

fn read_payload(src: &mut &[u8], len: usize) -> Result<Vec<u8>> {
    let bytes = inner_read_exact(src, len)?;
    Ok(bytes.to_vec())
}

pub fn decode_fixed_header(buf: &mut &[u8]) -> Result<FixedHeader> {
    let first_byte = inner_read_u8(buf)?;
    let packet_type_byte = first_byte >> 4;
    let flags = first_byte & 0x0F;

    let packet_type = PacketType::try_from_u8(packet_type_byte).ok_or_else(|| {
        BrokerError::Decode(format!("Unknown packet type: {packet_type_byte}"))
    })?;

    let (remaining_length, _consumed) = decode_remaining_length(buf)?;

    Ok(FixedHeader {
        packet_type,
        flags,
        remaining_length,
    })
}

fn read_packet_id(src: &mut &[u8]) -> Result<PacketId> {
    let raw = inner_read_u16(src)?;
    Ok(PacketId(raw))
}

pub fn decode_connect(src: &mut &[u8]) -> Result<ConnectPacket> {
    let protocol_name_len = inner_read_u16(src)? as usize;
    let protocol_name_bytes = inner_read_exact(src, protocol_name_len)?;
    let protocol_name = String::from_utf8(protocol_name_bytes.to_vec()).map_err(|e| {
        BrokerError::Decode(format!("Invalid UTF-8 in protocol name: {e}"))
    })?;

    let protocol_level = inner_read_u8(src)?;

    let flags_byte = inner_read_u8(src)?;
    let username_flag = (flags_byte & 0x80) != 0;
    let password_flag = (flags_byte & 0x40) != 0;
    let will_retain = (flags_byte & 0x20) != 0;
    let will_qos_bits = (flags_byte >> 3) & 0x03;
    let will_qos = QoS::try_from_u8(will_qos_bits);
    let will_flag = (flags_byte & 0x04) != 0;
    let clean_session = (flags_byte & 0x02) != 0;

    if flags_byte & 0x01 != 0 {
        return Err(BrokerError::Decode("Reserved bit set in connect flags".into()));
    }

    if will_flag && will_qos.is_none() {
        return Err(BrokerError::Decode("Will QoS invalid".into()));
    }

    let keep_alive = inner_read_u16(src)?;
    let client_id = read_utf8_string(src)?;

    let mut will_topic = None;
    let mut will_message = None;

    if will_flag {
        will_topic = Some(read_utf8_string(src)?);
        let msg_len = inner_read_u16(src)? as usize;
        let msg_bytes = inner_read_exact(src, msg_len)?;
        will_message = Some(msg_bytes.to_vec());
    }

    let mut username = None;
    if username_flag {
        username = Some(read_utf8_string(src)?);
    }

    let mut password = None;
    if password_flag {
        let pwd_len = inner_read_u16(src)? as usize;
        let pwd_bytes = inner_read_exact(src, pwd_len)?;
        password = Some(pwd_bytes.to_vec());
    }

    Ok(ConnectPacket {
        protocol_name,
        protocol_level,
        clean_session,
        will_flag,
        will_qos,
        will_retain,
        username_flag,
        password_flag,
        keep_alive,
        client_id,
        will_topic,
        will_message,
        username,
        password,
    })
}

pub fn decode_publish(header: &FixedHeader, src: &mut &[u8]) -> Result<PublishPacket> {
    let dup = (header.flags & 0x08) != 0;
    let qos_bits = (header.flags >> 1) & 0x03;
    let qos = QoS::try_from_u8(qos_bits).ok_or_else(|| {
        BrokerError::Decode(format!("Invalid QoS value: {qos_bits}"))
    })?;
    let retain = (header.flags & 0x01) != 0;

    let topic_name = read_utf8_string(src)?;

    let packet_id = if qos != QoS::AtMostOnce {
        Some(read_packet_id(src)?)
    } else {
        None
    };

    let payload = read_payload(src, src.len())?;

    Ok(PublishPacket {
        dup,
        qos,
        retain,
        topic_name,
        packet_id,
        payload,
    })
}

pub fn decode_puback(src: &mut &[u8]) -> Result<PacketId> {
    read_packet_id(src)
}

pub fn decode_pubrec(src: &mut &[u8]) -> Result<PacketId> {
    read_packet_id(src)
}

pub fn decode_pubrel(src: &mut &[u8]) -> Result<PacketId> {
    read_packet_id(src)
}

pub fn decode_pubcomp(src: &mut &[u8]) -> Result<PacketId> {
    read_packet_id(src)
}

pub fn decode_subscribe(src: &mut &[u8]) -> Result<SubscribePacket> {
    let packet_id = read_packet_id(src)?;
    let mut subscriptions = Vec::new();

    while !src.is_empty() {
        let topic_filter = read_utf8_string(src)?;
        let qos_byte = inner_read_u8(src)?;
        let qos = QoS::try_from_u8(qos_byte & 0x03).ok_or_else(|| {
            BrokerError::Decode(format!("Invalid subscribe QoS: {qos_byte}"))
        })?;
        subscriptions.push((topic_filter, qos));
    }

    if subscriptions.is_empty() {
        return Err(BrokerError::Decode("Subscribe packet with no subscriptions".into()));
    }

    Ok(SubscribePacket {
        packet_id,
        subscriptions,
    })
}

pub fn decode_unsubscribe(src: &mut &[u8]) -> Result<UnsubscribePacket> {
    let packet_id = read_packet_id(src)?;
    let mut topic_filters = Vec::new();

    while !src.is_empty() {
        let topic_filter = read_utf8_string(src)?;
        topic_filters.push(topic_filter);
    }

    if topic_filters.is_empty() {
        return Err(BrokerError::Decode("Unsubscribe packet with no topics".into()));
    }

    Ok(UnsubscribePacket {
        packet_id,
        topic_filters,
    })
}

pub fn decode(src: &mut &[u8]) -> Result<Packet> {
    let header = decode_fixed_header(src)?;

    if src.len() < header.remaining_length {
        return Err(BrokerError::Decode(format!(
            "Incomplete packet: need {} bytes, have {}",
            header.remaining_length,
            src.len()
        )));
    }

    match header.packet_type {
        PacketType::Connect => {
            let packet = decode_connect(src)?;
            Ok(Packet::Connect(packet))
        }
        PacketType::Connack => {
            let _session_present = inner_read_u8(src)?;
            let _return_code = ConnackReturnCode::Accepted;
            Err(BrokerError::Decode("Server received CONNACK".into()))
        }
        PacketType::Publish => {
            let packet = decode_publish(&header, src)?;
            Ok(Packet::Publish(packet))
        }
        PacketType::Puback => {
            let packet_id = decode_puback(src)?;
            Ok(Packet::Puback(packet_id))
        }
        PacketType::Pubrec => {
            let packet_id = decode_pubrec(src)?;
            Ok(Packet::Pubrec(packet_id))
        }
        PacketType::Pubrel => {
            let packet_id = decode_pubrel(src)?;
            Ok(Packet::Pubrel(packet_id))
        }
        PacketType::Pubcomp => {
            let packet_id = decode_pubcomp(src)?;
            Ok(Packet::Pubcomp(packet_id))
        }
        PacketType::Subscribe => {
            let packet = decode_subscribe(src)?;
            Ok(Packet::Subscribe(packet))
        }
        PacketType::Suback => Err(BrokerError::Decode("Server received SUBACK".into())),
        PacketType::Unsubscribe => {
            let packet = decode_unsubscribe(src)?;
            Ok(Packet::Unsubscribe(packet))
        }
        PacketType::Unsuback => Err(BrokerError::Decode("Server received UNSUBACK".into())),
        PacketType::Pingreq => Ok(Packet::Pingreq),
        PacketType::Pingresp => Err(BrokerError::Decode("Server received PINGRESP".into())),
        PacketType::Disconnect => Ok(Packet::Disconnect),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "Tests are expected to panic on failure")]
mod tests {
    use super::*;

    #[test]
    fn decode_remaining_length_zero() {
        let mut data: &[u8] = &[0x00];
        let (val, _) = decode_remaining_length(&mut data).unwrap();
        assert_eq!(val, 0);
    }

    #[test]
    fn decode_remaining_length_127() {
        let mut data: &[u8] = &[0x7F];
        let (val, _) = decode_remaining_length(&mut data).unwrap();
        assert_eq!(val, 127);
    }

    #[test]
    fn decode_remaining_length_128() {
        let mut data: &[u8] = &[0x80, 0x01];
        let (val, _) = decode_remaining_length(&mut data).unwrap();
        assert_eq!(val, 128);
    }

    #[test]
    fn decode_fixed_header_pingreq() {
        let mut data: &[u8] = &[0xC0, 0x00];
        let header = decode_fixed_header(&mut data).unwrap();
        assert_eq!(header.packet_type, PacketType::Pingreq);
        assert_eq!(header.remaining_length, 0);
    }

    #[test]
    fn decode_fixed_header_connect() {
        let mut data: &[u8] = &[0x10, 0x00];
        let header = decode_fixed_header(&mut data).unwrap();
        assert_eq!(header.packet_type, PacketType::Connect);
    }

    #[test]
    fn decode_pingreq() {
        let mut data: &[u8] = &[0xC0, 0x00];
        let packet = decode(&mut data).unwrap();
        assert!(matches!(packet, Packet::Pingreq));
    }

    #[test]
    fn decode_connect_minimal() {
        let data = vec![
            0x10, 0x0E, // Fixed header: CONNECT, remaining length 14
            0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol name "MQTT"
            0x04,       // Protocol level 4
            0x02,       // Connect flags: clean session
            0x00, 0x3C, // Keep alive 60s
            0x00, 0x03, b'r', b's', b't', // Client ID "rst"
        ];
        let mut slice: &[u8] = &data[..];
        let packet = decode(&mut slice).unwrap();

        match packet {
            Packet::Connect(c) => {
                assert_eq!(c.protocol_name, "MQTT");
                assert_eq!(c.protocol_level, 4);
                assert!(c.clean_session);
                assert!(!c.will_flag);
                assert_eq!(c.keep_alive, 60);
                assert_eq!(c.client_id, "rst");
            }
            _ => panic!("Expected CONNECT packet"),
        }
    }

    #[test]
    fn decode_publish_qos0() {
        let data = vec![
            0x30, 0x0B, // Fixed header: PUBLISH QoS0, remaining 11
            0x00, 0x04, b't', b'e', b's', b't', // Topic "test"
            b'h', b'e', b'l', b'l', b'o', // Payload "hello"
        ];
        let mut slice: &[u8] = &data[..];
        let packet = decode(&mut slice).unwrap();

        match packet {
            Packet::Publish(p) => {
                assert_eq!(p.qos, QoS::AtMostOnce);
                assert_eq!(p.topic_name, "test");
                assert_eq!(p.packet_id, None);
                assert_eq!(p.payload, b"hello");
            }
            _ => panic!("Expected PUBLISH packet"),
        }
    }
}
