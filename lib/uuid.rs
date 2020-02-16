use differential_datalog::record::FromRecord;
use differential_datalog::record::IntoRecord;
use differential_datalog::record::Mutator;
use differential_datalog::record::Record;
use serde::de::Deserialize;
use serde::de::Deserializer;
use serde::ser::Serialize;
use serde::ser::Serializer;
use uuid::Error;
use uuid::Uuid;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd)]
pub struct uuid_Uuid(Uuid);

impl uuid_Uuid {
    pub fn new(addr: Uuid) -> Self {
        uuid_Uuid(addr)
    }
}

impl Deref for uuid_Uuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for uuid_Uuid {
    fn default() -> uuid_Uuid {
        uuid_nil()
    }
}

impl fmt::Display for uuid_Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", uuid_as_u128(self))
    }
}

impl fmt::Debug for uuid_Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for uuid_Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        uuid_as_u128(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for uuid_Uuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u128::deserialize(deserializer).map(|x| uuid_from_u128(&x))
    }
}

impl FromRecord for uuid_Uuid {
    fn from_record(val: &Record) -> Result<Self, String> {
        u128::from_record(val).map(|x| uuid_from_u128(&x))
    }
}

impl IntoRecord for uuid_Uuid {
    fn into_record(self) -> Record {
        uuid_as_u128(&self).into_record()
    }
}

impl Mutator<uuid_Uuid> for Record {
    fn mutate(&self, uuid: &mut uuid_Uuid) -> Result<(), String> {
        uuid_Uuid::from_record(self).map(|u| *uuid = u)
    }
}

pub fn uuid_nil() -> uuid_Uuid {
    uuid_Uuid::new(Uuid::nil())
}

pub fn uuid_nAMESPACE_DNS() -> uuid_Uuid {
    uuid_Uuid::new(Uuid::NAMESPACE_DNS)
}

pub fn uuid_nAMESPACE_OID() -> uuid_Uuid {
    uuid_Uuid::new(Uuid::NAMESPACE_OID)
}

pub fn uuid_nAMESPACE_URL() -> uuid_Uuid {
    uuid_Uuid::new(Uuid::NAMESPACE_URL)
}

pub fn uuid_nAMESPACE_X500() -> uuid_Uuid {
    uuid_Uuid::new(Uuid::NAMESPACE_X500)
}

pub fn uuid_new_v5(namespace: &uuid_Uuid, name: &std_Vec<u8>) -> uuid_Uuid {
    uuid_Uuid::new(Uuid::new_v5(namespace, &**name))
}

pub fn uuid_from_u128(v: &u128) -> uuid_Uuid {
    uuid_Uuid::new(Uuid::from_u128(*v))
}

pub fn uuid_from_u128_le(v: &u128) -> uuid_Uuid {
    uuid_Uuid::new(Uuid::from_u128_le(*v))
}

pub fn uuid_from_bytes(b: &std_Vec<u8>) -> std_Result<uuid_Uuid, uuid_Error> {
    res2std(Uuid::from_slice(&**b).map(uuid_Uuid::new))
}

pub fn uuid_parse_str(s: &String) -> std_Result<uuid_Uuid, uuid_Error> {
    res2std(Uuid::parse_str(&s).map(uuid_Uuid::new))
}

pub fn uuid_to_hyphenated_lower(uuid: &uuid_Uuid) -> String {
    uuid.to_hyphenated_ref()
        .encode_lower(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_to_hyphenated_upper(uuid: &uuid_Uuid) -> String {
    uuid.to_hyphenated_ref()
        .encode_upper(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_to_simple_lower(uuid: &uuid_Uuid) -> String {
    uuid.to_simple_ref()
        .encode_lower(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_to_simple_upper(uuid: &uuid_Uuid) -> String {
    uuid.to_simple_ref()
        .encode_upper(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_to_urn_lower(uuid: &uuid_Uuid) -> String {
    uuid.to_urn_ref()
        .encode_lower(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_to_urn_upper(uuid: &uuid_Uuid) -> String {
    uuid.to_urn_ref()
        .encode_upper(&mut Uuid::encode_buffer())
        .to_string()
}

pub fn uuid_as_u128(uuid: &uuid_Uuid) -> u128 {
    uuid.as_u128()
}

pub fn uuid_to_u128_le(uuid: &uuid_Uuid) -> u128 {
    uuid.to_u128_le()
}

pub fn uuid_is_nil(uuid: &uuid_Uuid) -> bool {
    uuid.is_nil()
}
