use crate::app::ErrorKind;

const TIMEOUT: u8 = 0;
const NOT_SUPPORTED: u8 = 10;
const TEMPORARILY_UNAVAILABLE: u8 = 11;
const MALFORMED_REQUEST: u8 = 12;
const CRASH: u8 = 13;

pub(super) fn to_code(kind: ErrorKind) -> u8 {
    match kind {
        ErrorKind::Timeout => TIMEOUT,
        ErrorKind::Unavailable => TEMPORARILY_UNAVAILABLE,
        ErrorKind::Protocol => MALFORMED_REQUEST,
        ErrorKind::Internal => CRASH,
    }
}

pub(super) fn from_code(code: u8) -> ErrorKind {
    match code {
        TIMEOUT => ErrorKind::Timeout,
        TEMPORARILY_UNAVAILABLE => ErrorKind::Unavailable,
        NOT_SUPPORTED | MALFORMED_REQUEST => ErrorKind::Protocol,
        _ => ErrorKind::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_roundtrip() {
        for kind in [
            ErrorKind::Timeout,
            ErrorKind::Unavailable,
            ErrorKind::Protocol,
            ErrorKind::Internal,
        ] {
            assert_eq!(from_code(to_code(kind)), kind);
        }
    }

    #[test]
    fn unknown_code_is_internal() {
        assert_eq!(from_code(200), ErrorKind::Internal);
    }
}
