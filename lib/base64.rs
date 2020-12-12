use base64::decode as b64decode;
use base64::encode as b64encode;
use base64::DecodeError as b64DecodeError;

pub fn encode(s: &ddlog_std::Vec<u8>) -> String {
    b64encode(&s.vec)
}

pub fn decode(s: &String) -> ddlog_std::Result<ddlog_std::Vec<u8>, String> {
    match b64decode(s) {
        Ok(r) => ddlog_std::Result::Ok {
            res: ddlog_std::Vec::from(r),
        },
        Err(e) => match (e) {
            b64DecodeError::InvalidByte(p, b) => ddlog_std::Result::Err {
                err: format!("Invalid byte {} at position {}", b, p),
            },
            b64DecodeError::InvalidLength => ddlog_std::Result::Err {
                err: format!("Invalid length"),
            },
            b64DecodeError::InvalidLastSymbol(p, b) => ddlog_std::Result::Err {
                err: format!("Invalid last byte {} at position {}", b, p),
            },
        },
    }
}
