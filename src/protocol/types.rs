use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
#[expect(clippy::enum_variant_names, reason = "Follows MQTT 3.1.1 spec naming convention")]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn try_from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketId(pub u16);

impl PacketId {
    #[expect(dead_code, reason = "Part of protocol definition")]
    pub fn to_be_bytes(self) -> [u8; 2] {
        self.0.to_be_bytes()
    }

    #[expect(dead_code, reason = "Part of protocol definition")]
    pub fn from_be_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_be_bytes(bytes))
    }
}

impl fmt::Display for PacketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

impl PacketType {
    pub fn try_from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Connect),
            2 => Some(Self::Connack),
            3 => Some(Self::Publish),
            4 => Some(Self::Puback),
            5 => Some(Self::Pubrec),
            6 => Some(Self::Pubrel),
            7 => Some(Self::Pubcomp),
            8 => Some(Self::Subscribe),
            9 => Some(Self::Suback),
            10 => Some(Self::Unsubscribe),
            11 => Some(Self::Unsuback),
            12 => Some(Self::Pingreq),
            13 => Some(Self::Pingresp),
            14 => Some(Self::Disconnect),
            _ => None,
        }
    }

    #[expect(dead_code, reason = "Used for packet validation")]
    pub fn required_flags(self) -> u8 {
        match self {
            Self::Connect => 0x00,
            Self::Connack => 0x00,
            Self::Publish => 0x00, // QoS-dependent, validated elsewhere
            Self::Puback => 0x00,
            Self::Pubrec => 0x00,
            Self::Pubrel => 0x02,
            Self::Pubcomp => 0x00,
            Self::Subscribe => 0x02,
            Self::Suback => 0x00,
            Self::Unsubscribe => 0x02,
            Self::Unsuback => 0x00,
            Self::Pingreq => 0x00,
            Self::Pingresp => 0x00,
            Self::Disconnect => 0x00,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    pub packet_type: PacketType,
    pub flags: u8,
    pub remaining_length: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnackReturnCode {
    Accepted = 0,
    #[expect(dead_code, reason = "MQTT 3.1.1 spec: protocol version mismatch")]
    UnacceptableProtocolVersion = 1,
    #[expect(dead_code, reason = "MQTT 3.1.1 spec: client ID rejected")]
    IdentifierRejected = 2,
    #[expect(dead_code, reason = "MQTT 3.1.1 spec: broker unavailable")]
    ServerUnavailable = 3,
    #[expect(dead_code, reason = "MQTT 3.1.1 spec: bad credentials")]
    BadUsernameOrPassword = 4,
    #[expect(dead_code, reason = "MQTT 3.1.1 spec: not authorized")]
    NotAuthorized = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscribeReturnCode {
    SuccessQoS0 = 0x00,
    SuccessQoS1 = 0x01,
    SuccessQoS2 = 0x02,
    Failure = 0x80,
}

#[derive(Debug, Clone)]
#[expect(dead_code, reason = "All fields defined per MQTT 3.1.1 spec; some unused in basic broker")]
pub struct ConnectPacket {
    pub protocol_name: String,
    pub protocol_level: u8,
    pub clean_session: bool,
    pub will_flag: bool,
    pub will_qos: Option<QoS>,
    pub will_retain: bool,
    pub username_flag: bool,
    pub password_flag: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub will_topic: Option<String>,
    pub will_message: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ConnackPacket {
    pub session_present: bool,
    pub return_code: ConnackReturnCode,
}

#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic_name: String,
    pub packet_id: Option<PacketId>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: PacketId,
    pub subscriptions: Vec<(String, QoS)>,
}

#[derive(Debug, Clone)]
pub struct SubackPacket {
    pub packet_id: PacketId,
    pub return_codes: Vec<SubscribeReturnCode>,
}

#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    pub packet_id: PacketId,
    pub topic_filters: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Packet {
    Connect(ConnectPacket),
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Puback(PacketId),
    Pubrec(PacketId),
    Pubrel(PacketId),
    Pubcomp(PacketId),
    Subscribe(SubscribePacket),
    Suback(SubackPacket),
    Unsubscribe(UnsubscribePacket),
    Unsuback(PacketId),
    Pingreq,
    Pingresp,
    Disconnect,
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        match self {
            Self::Connect(_) => PacketType::Connect,
            Self::Connack(_) => PacketType::Connack,
            Self::Publish(_) => PacketType::Publish,
            Self::Puback(_) => PacketType::Puback,
            Self::Pubrec(_) => PacketType::Pubrec,
            Self::Pubrel(_) => PacketType::Pubrel,
            Self::Pubcomp(_) => PacketType::Pubcomp,
            Self::Subscribe(_) => PacketType::Subscribe,
            Self::Suback(_) => PacketType::Suback,
            Self::Unsubscribe(_) => PacketType::Unsubscribe,
            Self::Unsuback(_) => PacketType::Unsuback,
            Self::Pingreq => PacketType::Pingreq,
            Self::Pingresp => PacketType::Pingresp,
            Self::Disconnect => PacketType::Disconnect,
        }
    }
}
