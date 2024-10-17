use std::fmt::Display;

use crate::logical::expr::LogicalExpr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortExpr {
    pub expr: Box<LogicalExpr>,
    pub asc: bool,
}

impl Display for SortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let order = if self.asc { "ASC" } else { "DESC" };
        write!(f, "{} {order}", self.expr)
    }
}
