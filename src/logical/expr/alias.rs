use std::fmt::Display;

use super::LogicalExpr;

#[derive(Debug, Clone)]
pub struct Alias {
    expr: Box<LogicalExpr>,
    name: String,
}

impl Alias {
    pub fn new(name: String, expr: LogicalExpr) -> Self {
        Alias {
            expr: Box::new(expr),
            name,
        }
    }
}

impl Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Alias: {}", self.name)
    }
}
