use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    
    And,
    Or,

    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::NotEq => write!(f, "!="),
            Operator::Gt => write!(f, ">"),
            Operator::GtEq => write!(f, ">="),
            Operator::Lt => write!(f, "<"),
            Operator::LtEq => write!(f, "<="),
            Operator::And => write!(f, "AND"),
            Operator::Or => write!(f, "OR"),
            Operator::Add => write!(f, "+"),
            Operator::Sub => write!(f, "-"),
            Operator::Mul => write!(f, "*"),
            Operator::Div => write!(f, "/"),
            Operator::Mod => write!(f, "%"),
        }
    }
}
