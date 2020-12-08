use base64::decode as b64decode;
use base64::encode as b64encode;
use base64::DecodeError as b64DecodeError;

pub fn encode(s: &crate::ddlog_std::Vec<u8>) -> String {
    b64encode(&s.x)
}

pub fn decode(s: &String) -> crate::ddlog_std::Result<crate::ddlog_std::Vec<u8>, String> {
    match b64decode(s) {
        Ok(r) => crate::ddlog_std::Result::Ok{
            res: crate::ddlog_std::Vec::from(r),
        },
        Err(e) => match (e) {
            b64DecodeError::InvalidByte(p, b) => crate::ddlog_std::Result::Err {
                err: format!("Invalid byte {} at position {}", b, p),
            },
            b64DecodeError::InvalidLength => crate::ddlog_std::Result::Err {
                err: format!("Invalid length"),
            },
            b64DecodeError::InvalidLastSymbol(p, b) => crate::ddlog_std::Result::Err {
                err: format!("Invalid last byte {} at position {}", b, p),
            },
        },
    }
}
