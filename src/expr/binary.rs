use crate::expr::{LogicalExpr, Operator};

pub struct BinaryExpr {
    pub left: Box<dyn LogicalExpr>,
    pub op: Operator,
    pub right: Box<dyn LogicalExpr>,
}
