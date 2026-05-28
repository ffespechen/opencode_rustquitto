use bytes::{BufMut, BytesMut};

use crate::error::Result;
use crate::protocol::types::{ConnackPacket, Packet, PacketId, PublishPacket, QoS, SubackPacket};

fn encode_remaining_length(mut length: usize) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4);
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if length == 0 {
            break;
        }
    }
    encoded
}

fn write_utf8_string(buf: &mut BytesMut, value: &str) {
    buf.put_u16(value.len() as u16);
    buf.put(value.as_bytes());
}

fn write_fixed_header(buf: &mut BytesMut, packet_type: u8, flags: u8, remaining_length: usize) {
    buf.put_u8((packet_type << 4) | flags);
    buf.put(encode_remaining_length(remaining_length).as_slice());
}

fn remaining_length_for_packet(packet: &Packet) -> usize {
    match packet {
        Packet::Connack(_) => 2,
        Packet::Publish(p) => {
            let mut len = 2 + p.topic_name.len();
            if p.qos != QoS::AtMostOnce {
                len += 2;
            }
            len += p.payload.len();
            len
        }
        Packet::Puback(_)
        | Packet::Pubrec(_)
        | Packet::Pubrel(_)
        | Packet::Pubcomp(_)
        | Packet::Unsuback(_) => 2,
        Packet::Suback(s) => 2 + s.return_codes.len(),
        Packet::Pingresp => 0,
        Packet::Connect(_)
        | Packet::Subscribe(_)
        | Packet::Unsubscribe(_)
        | Packet::Pingreq
        | Packet::Disconnect => 0,
    }
}

pub fn connack(packet: &ConnackPacket) -> BytesMut {
    let mut buf = BytesMut::with_capacity(4);
    write_fixed_header(&mut buf, 2, 0, 2);
    buf.put_u8(u8::from(packet.session_present));
    buf.put_u8(packet.return_code as u8);
    buf
}

pub fn suback(packet: &SubackPacket) -> BytesMut {
    let remaining = 2 + packet.return_codes.len();
    let mut buf = BytesMut::with_capacity(5 + packet.return_codes.len());
    write_fixed_header(&mut buf, 9, 0, remaining);
    buf.put_u16(packet.packet_id.0);
    for code in &packet.return_codes {
        buf.put_u8(*code as u8);
    }
    buf
}

pub fn unsuback(packet_id: PacketId) -> BytesMut {
    let mut buf = BytesMut::with_capacity(4);
    write_fixed_header(&mut buf, 11, 0, 2);
    buf.put_u16(packet_id.0);
    buf
}

pub fn puback(packet_id: PacketId) -> BytesMut {
    simple_packet_with_id(4, packet_id)
}

pub fn pubrec(packet_id: PacketId) -> BytesMut {
    simple_packet_with_id(5, packet_id)
}

pub fn pubrel(packet_id: PacketId) -> BytesMut {
    simple_packet_with_flags(6, 0x02, packet_id)
}

pub fn pubcomp(packet_id: PacketId) -> BytesMut {
    simple_packet_with_id(7, packet_id)
}

fn simple_packet_with_id(packet_type: u8, packet_id: PacketId) -> BytesMut {
    simple_packet_with_flags(packet_type, 0, packet_id)
}

fn simple_packet_with_flags(packet_type: u8, flags: u8, packet_id: PacketId) -> BytesMut {
    let mut buf = BytesMut::with_capacity(4);
    write_fixed_header(&mut buf, packet_type, flags, 2);
    buf.put_u16(packet_id.0);
    buf
}

pub fn pingresp() -> BytesMut {
    let mut buf = BytesMut::with_capacity(2);
    write_fixed_header(&mut buf, 13, 0, 0);
    buf
}

fn publish_flags(packet: &PublishPacket) -> u8 {
    let mut flags = 0u8;
    if packet.dup {
        flags |= 0x08;
    }
    flags |= (packet.qos as u8) << 1;
    if packet.retain {
        flags |= 0x01;
    }
    flags
}

pub fn publish(packet: &PublishPacket) -> Result<BytesMut> {
    let remaining = remaining_length_for_packet(&Packet::Publish(packet.clone()));
    let mut buf = BytesMut::with_capacity(10 + packet.topic_name.len() + packet.payload.len());
    let flags = publish_flags(packet);
    write_fixed_header(&mut buf, 3, flags, remaining);
    write_utf8_string(&mut buf, &packet.topic_name);
    if packet.qos != QoS::AtMostOnce {
        let pid = packet
            .packet_id
            .ok_or_else(|| crate::error::BrokerError::Encode("QoS > 0 requires packet_id".into()))?;
        buf.put_u16(pid.0);
    }
    buf.put(&packet.payload[..]);
    Ok(buf)
}

pub fn encode(packet: &Packet) -> Result<BytesMut> {
    match packet {
        Packet::Connack(p) => Ok(connack(p)),
        Packet::Publish(p) => publish(p),
        Packet::Puback(pid) => Ok(puback(*pid)),
        Packet::Pubrec(pid) => Ok(pubrec(*pid)),
        Packet::Pubrel(pid) => Ok(pubrel(*pid)),
        Packet::Pubcomp(pid) => Ok(pubcomp(*pid)),
        Packet::Suback(p) => Ok(suback(p)),
        Packet::Unsuback(pid) => Ok(unsuback(*pid)),
        Packet::Pingresp => Ok(pingresp()),
        Packet::Connect(_)
        | Packet::Subscribe(_)
        | Packet::Unsubscribe(_)
        | Packet::Pingreq
        | Packet::Disconnect => Err(crate::error::BrokerError::Encode(format!(
            "Cannot encode client-to-server packet: {:?}",
            packet.packet_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::ConnackReturnCode;

    use super::*;

    #[test]
    fn encode_remaining_length_zero() {
        assert_eq!(encode_remaining_length(0), vec![0x00]);
    }

    #[test]
    fn encode_remaining_length_127() {
        assert_eq!(encode_remaining_length(127), vec![0x7F]);
    }

    #[test]
    fn encode_remaining_length_128() {
        assert_eq!(encode_remaining_length(128), vec![0x80, 0x01]);
    }

    #[test]
    fn encode_remaining_length_16383() {
        assert_eq!(encode_remaining_length(16383), vec![0xFF, 0x7F]);
    }

    #[test]
    fn encode_remaining_length_16384() {
        assert_eq!(encode_remaining_length(16384), vec![0x80, 0x80, 0x01]);
    }

    #[test]
    fn pingresp_encodes_correctly() {
        let result = pingresp();
        assert_eq!(&result[..], &[0xD0, 0x00]);
    }

    #[test]
    fn connack_encodes_correctly() {
        let pkt = ConnackPacket {
            session_present: false,
            return_code: ConnackReturnCode::Accepted,
        };
        let result = connack(&pkt);
        assert_eq!(&result[..], &[0x20, 0x02, 0x00, 0x00]);
    }
}
