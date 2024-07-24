use std::{iter::Peekable, str::Chars};

use crate::token::{Location, Token, TokenType};

const EMPTY_CHAR: char = '\0';

pub struct Lexer<'a> {
    peekable: Peekable<Chars<'a>>,
    peeked: Option<Token>,
    lines: Vec<&'a str>,
    cur_line: usize,
    cur_pos: usize,
    cur_ch: char,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        let lines = input.lines().collect();
        let mut peekable = input.chars().peekable();
        Lexer {
            cur_ch: peekable.next().unwrap_or(EMPTY_CHAR),
            peekable,
            lines,
            peeked: None,
            cur_line: 0,
            cur_pos: 0,
        }
    }

    pub fn peek(&mut self) -> Option<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.next());
        }
        self.peeked.as_ref()
    }

    pub fn next(&mut self) -> Token {
        if let Some(tok) = self.peeked.take() {
            return tok;
        }

        self.skip_whitespace();

        let literal = char::from(self.cur_ch).to_string();
        let tok = match self.cur_ch {
            EMPTY_CHAR => Token::new(TokenType::EOF, "".to_owned(), self.location()),
            '=' => Token::new(TokenType::Eq, "=".to_owned(), self.location()),
            '!' => {
                if self.peek_char() == '=' {
                    self.read_char();
                    Token::new(TokenType::NotEq, "!=".to_owned(), self.location())
                } else {
                    Token::new(TokenType::Bang, literal, self.location())
                }
            }
            '<' => {
                if self.peek_char() == '=' {
                    self.read_char();
                    Token::new(TokenType::Lte, "<=".to_owned(), self.location())
                } else {
                    Token::new(TokenType::Lt, literal, self.location())
                }
            }
            '>' => {
                if self.peek_char() == '=' {
                    self.read_char();
                    Token::new(TokenType::Gte, ">=".to_owned(), self.location())
                } else {
                    Token::new(TokenType::Gt, literal, self.location())
                }
            }
            ';' => Token::new(TokenType::Semicolon, literal, self.location()),
            ':' => Token::new(TokenType::Colon, literal, self.location()),
            '.' => Token::new(TokenType::Period, literal, self.location()),
            '(' => Token::new(TokenType::LParen, literal, self.location()),
            ')' => Token::new(TokenType::RParen, literal, self.location()),
            ',' => Token::new(TokenType::Comma, literal, self.location()),
            '+' => Token::new(TokenType::Plus, literal, self.location()),
            '{' => Token::new(TokenType::LBrace, literal, self.location()),
            '[' => Token::new(TokenType::LSquareBrace, literal, self.location()),
            ']' => Token::new(TokenType::RSquareBrace, literal, self.location()),
            '}' => Token::new(TokenType::RBrace, literal, self.location()),
            '-' => Token::new(TokenType::Minus, literal, self.location()),
            '*' => Token::new(TokenType::Asterisk, literal, self.location()),
            '/' => Token::new(TokenType::Slash, literal, self.location()),
            '?' => Token::new(TokenType::Ident, literal, self.location()),
            '\'' => {
                let mut s = String::new();
                loop {
                    self.read_char();
                    match self.cur_ch {
                        '\'' => {
                            break;
                        }
                        EMPTY_CHAR => {
                            return Token::new(TokenType::ILLIGAL, literal, self.location())
                        }
                        _ => {
                            s.push(char::from(self.cur_ch));
                        }
                    }
                }
                Token::new(TokenType::String, s, self.location())
            }
            b if b.is_ascii_alphabetic() => {
                let literal = self.read_literal();
                let token_type = TokenType::lookup_ident(&literal);
                return Token::new(token_type, literal, self.location());
            }
            b if b.is_ascii_digit() => {
                return Token::new(TokenType::Int, self.read_number(), self.location());
            }
            _ => Token::new(TokenType::ILLIGAL, literal, self.location()),
        };
        self.read_char();
        tok
    }

    pub fn location(&self) -> Location {
        let line_str = self.lines.get(self.cur_line).cloned().unwrap_or_default();
        Location {
            line_str: line_str.to_owned(),
            line: self.cur_line,
            column: self.cur_pos,
        }
    }

    fn read_char(&mut self) {
        if let Some(ch) = self.peekable.next() {
            self.cur_ch = ch;
        } else {
            self.cur_ch = EMPTY_CHAR;
            return;
        }

        if self.cur_ch == '\n' {
            self.cur_line += 1;
        }

        self.cur_pos += 1;
    }

    fn read_literal(&mut self) -> String {
        let mut literal = String::new();
        while self.cur_ch.is_ascii_alphabetic()
            || self.cur_ch.is_ascii_alphanumeric()
            || self.cur_ch == '_'
        {
            literal.push(self.cur_ch);
            self.read_char();
        }

        literal
    }

    fn read_number(&mut self) -> String {
        let mut number = String::new();
        while self.cur_ch.is_ascii_digit() {
            number.push(self.cur_ch);
            self.read_char();
        }
        number
    }

    fn skip_whitespace(&mut self) {
        while self.cur_ch.is_ascii_whitespace() {
            self.read_char();
        }
    }

    fn peek_char(&mut self) -> char {
        self.peekable.peek().copied().unwrap_or(EMPTY_CHAR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Keyword, TokenType};

    #[test]
    fn test_single_char_token() {
        let input = "=-+(){},;*/<>!?";
        let tests = vec![
            (TokenType::Eq, "="),
            (TokenType::Minus, "-"),
            (TokenType::Plus, "+"),
            (TokenType::LParen, "("),
            (TokenType::RParen, ")"),
            (TokenType::LBrace, "{"),
            (TokenType::RBrace, "}"),
            (TokenType::Comma, ","),
            (TokenType::Semicolon, ";"),
            (TokenType::Asterisk, "*"),
            (TokenType::Slash, "/"),
            (TokenType::Lt, "<"),
            (TokenType::Gt, ">"),
            (TokenType::Bang, "!"),
        ];
        let mut l = Lexer::new(input);
        for (expected_type, expected_literal) in tests {
            let tok = l.next();
            assert_eq!(tok.token_type, expected_type);
            assert_eq!(tok.literal, expected_literal);
        }
    }

    #[test]
    fn test_two_char_token() {
        let input = "=!=<=>=";
        let tests = vec![
            (TokenType::Eq, "="),
            (TokenType::NotEq, "!="),
            (TokenType::Lte, "<="),
            (TokenType::Gte, ">="),
            (TokenType::EOF, ""),
        ];
        let mut l = Lexer::new(input);
        for (expected_type, expected_literal) in tests {
            let tok = l.next();
            assert_eq!(tok.token_type, expected_type);
            assert_eq!(tok.literal, expected_literal);
        }
    }

    #[test]
    fn test_next_token() {
        let input = "select distinct * from users as u2 where id = ? and name = ? or age = 12 group by name limit 10;";
        let tests = vec![
            (TokenType::Keyword(Keyword::Select), "select"),
            (TokenType::Keyword(Keyword::Distinct), "distinct"),
            (TokenType::Asterisk, "*"),
            (TokenType::Keyword(Keyword::From), "from"),
            (TokenType::Ident, "users"),
            (TokenType::Keyword(Keyword::As), "as"),
            (TokenType::Ident, "u2"),
            (TokenType::Keyword(Keyword::Where), "where"),
            (TokenType::Ident, "id"),
            (TokenType::Eq, "="),
            (TokenType::Ident, "?"),
            (TokenType::Keyword(Keyword::And), "and"),
            (TokenType::Ident, "name"),
            (TokenType::Eq, "="),
            (TokenType::Ident, "?"),
            (TokenType::Keyword(Keyword::Or), "or"),
            (TokenType::Ident, "age"),
            (TokenType::Eq, "="),
            (TokenType::Int, "12"),
            (TokenType::Keyword(Keyword::Group), "group"),
            (TokenType::Keyword(Keyword::By), "by"),
            (TokenType::Ident, "name"),
            (TokenType::Keyword(Keyword::Limit), "limit"),
            (TokenType::Int, "10"),
            (TokenType::Semicolon, ";"),
            (TokenType::EOF, ""),
        ];
        let mut l = Lexer::new(input);
        for (expected_type, expected_literal) in tests {
            let tok = l.next();
            assert_eq!(tok.token_type, expected_type);
            assert_eq!(tok.literal, expected_literal);
        }
    }
}
