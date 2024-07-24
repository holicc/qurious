macro_rules! generate_error_enum {
    ($($variant:ident => $reason:expr),*) => {
        #[derive(Debug)]
        pub enum Error {
            $($variant(crate::token::Token)),*,
            ParseIntError(std::num::ParseIntError,crate::token::Token),
            DuplicateColumn(String),
            UnKnownInfixOperator(String),
            ParserError(String),
        }

        impl std::fmt::Display for Error {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Error::$variant(token) => {
                        write!(f, "error: {} line: {} column: {}", $reason, token.location.line, token.location.column)
                    })*
                    Error::ParseIntError(e, token) => {
                        write!(f, "error: {} line: {} column: {}", e, token.location.line, token.location.column)
                    }
                    Error::DuplicateColumn(column) => {
                        write!(f, "error: duplicate column: {}", column)
                    }
                    Error::UnKnownInfixOperator(operator) => {
                        write!(f, "error: unknown infix operator: {}", operator)
                    }
                    Error::ParserError(msg) => {
                        write!(f, "error: {}", msg)
                    }
                }
            }
        }
    };
}

generate_error_enum! {
    UnexpectedEOF => "unexpected EOF",
    UnexpectedToken => "unexpected token",
    UnKnownDataType => "unknown data type"
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ParseIntError(e, _) => Some(e),
            _ => None,
        }
    }
}
