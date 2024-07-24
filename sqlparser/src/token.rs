use crate::datatype::DataType;
use crate::error::{Error, Result};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Keyword {
    Select,
    Insert,
    Delete,
    Create,
    Drop,
    Schema,
    Table,
    Primary,
    Key,
    Unique,
    If,
    Exists,
    With,
    From,
    Where,
    And,
    Or,
    Returning,
    Do,
    Set,
    Not,
    Null,
    Conflict,
    Order,
    Nothing,
    Update,
    By,
    Asc,
    Desc,
    Into,
    Values,
    Limit,
    Offset,
    Group,
    Distinct,
    Having,
    In,
    On,
    As,
    True,
    False,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Cross,

    /// data types
    Int,
    Integer,
    Bool,
    Boolean,
    Date,
    Datetime,
    VarChar,
    Timestamp,
    Double,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TokenType {
    ILLIGAL,
    EOF,
    // Identifiers + literals
    Ident,
    String,
    Int,

    // Operators
    Assign,
    Plus,
    Minus,
    LParen,
    RParen,
    LBrace,
    LSquareBrace,
    RSquareBrace,
    RBrace,
    Asterisk,
    Slash,
    Lt,
    Gt,
    Eq,
    NotEq,
    Lte,
    Gte,

    // Delimiters
    Comma,
    Semicolon,
    Colon,
    Bang,
    Period,

    Keyword(Keyword),
}

impl TokenType {
    pub fn lookup_ident(ident: &str) -> TokenType {
        match ident.to_lowercase().as_str() {
            "select" => TokenType::Keyword(Keyword::Select),
            "insert" => TokenType::Keyword(Keyword::Insert),
            "int" => TokenType::Keyword(Keyword::Int),
            "integer" => TokenType::Keyword(Keyword::Integer),
            "bool" => TokenType::Keyword(Keyword::Bool),
            "boolean" => TokenType::Keyword(Keyword::Boolean),
            "date" => TokenType::Keyword(Keyword::Date),
            "datetime" => TokenType::Keyword(Keyword::Datetime),
            "varchar" => TokenType::Keyword(Keyword::VarChar),
            "timestamp" => TokenType::Keyword(Keyword::Timestamp),
            "double" => TokenType::Keyword(Keyword::Double),
            "primary" => TokenType::Keyword(Keyword::Primary),
            "key" => TokenType::Keyword(Keyword::Key),
            "with" => TokenType::Keyword(Keyword::With),
            "unique" => TokenType::Keyword(Keyword::Unique),
            "delete" => TokenType::Keyword(Keyword::Delete),
            "drop" => TokenType::Keyword(Keyword::Drop),
            "create" => TokenType::Keyword(Keyword::Create),
            "schema" => TokenType::Keyword(Keyword::Schema),
            "table" => TokenType::Keyword(Keyword::Table),
            "if" => TokenType::Keyword(Keyword::If),
            "exists" => TokenType::Keyword(Keyword::Exists),
            "from" => TokenType::Keyword(Keyword::From),
            "as" => TokenType::Keyword(Keyword::As),
            "where" => TokenType::Keyword(Keyword::Where),
            "and" => TokenType::Keyword(Keyword::And),
            "or" => TokenType::Keyword(Keyword::Or),
            "not" => TokenType::Keyword(Keyword::Not),
            "order" => TokenType::Keyword(Keyword::Order),
            "by" => TokenType::Keyword(Keyword::By),
            "do" => TokenType::Keyword(Keyword::Do),
            "returning" => TokenType::Keyword(Keyword::Returning),
            "conflict" => TokenType::Keyword(Keyword::Conflict),
            "asc" => TokenType::Keyword(Keyword::Asc),
            "desc" => TokenType::Keyword(Keyword::Desc),
            "into" => TokenType::Keyword(Keyword::Into),
            "values" => TokenType::Keyword(Keyword::Values),
            "limit" => TokenType::Keyword(Keyword::Limit),
            "offset" => TokenType::Keyword(Keyword::Offset),
            "update" => TokenType::Keyword(Keyword::Update),
            "group" => TokenType::Keyword(Keyword::Group),
            "on" => TokenType::Keyword(Keyword::On),
            "set" => TokenType::Keyword(Keyword::Set),
            "in" => TokenType::Keyword(Keyword::In),
            "distinct" => TokenType::Keyword(Keyword::Distinct),
            "having" => TokenType::Keyword(Keyword::Having),
            "true" => TokenType::Keyword(Keyword::True),
            "false" => TokenType::Keyword(Keyword::False),
            "join" => TokenType::Keyword(Keyword::Join),
            "inner" => TokenType::Keyword(Keyword::Inner),
            "nothing" => TokenType::Keyword(Keyword::Nothing),
            "left" => TokenType::Keyword(Keyword::Left),
            "right" => TokenType::Keyword(Keyword::Right),
            "full" => TokenType::Keyword(Keyword::Full),
            "cross" => TokenType::Keyword(Keyword::Cross),
            "null" => TokenType::Keyword(Keyword::Null),
            "(" => TokenType::LParen,
            ")" => TokenType::RParen,
            "{" => TokenType::LBrace,
            "}" => TokenType::RBrace,
            "[" => TokenType::LSquareBrace,
            "]" => TokenType::RSquareBrace,
            "," => TokenType::Comma,
            ";" => TokenType::Semicolon,
            ":" => TokenType::Colon,
            "+" => TokenType::Plus,
            "-" => TokenType::Minus,
            "*" => TokenType::Asterisk,
            "/" => TokenType::Slash,
            "<" => TokenType::Lt,
            ">" => TokenType::Gt,
            "=" => TokenType::Eq,
            "!" => TokenType::Bang,
            "." => TokenType::Period,
            "<=" => TokenType::Lte,
            ">=" => TokenType::Gte,
            "!=" => TokenType::NotEq,
            _ => TokenType::Ident,
        }
    }
}

#[derive(Default, Debug, PartialEq, Clone)]
pub struct Location {
    pub line_str: String,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub literal: String,
    pub location: Location,
}

impl Token {
    pub fn new(token_type: TokenType, literal: String, location: Location) -> Token {
        Token {
            token_type,
            literal,
            location,
        }
    }

    pub fn datatype(&self) -> Result<DataType> {
        match self.token_type {
            TokenType::Keyword(Keyword::Int) | TokenType::Keyword(Keyword::Integer) => {
                Ok(DataType::Integer)
            }
            _ => Err(Error::UnKnownDataType(self.clone())),
        }
    }
}
