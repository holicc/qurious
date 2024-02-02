use arrow::record_batch::RecordBatch;

use super::PhysicalExpr;
use crate::error::{Error, Result};
use crate::types::scalar::ScalarValue;
use crate::types::{columnar::ColumnarValue, operator::Operator};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        let l = self.left.evaluate(input)?;
        let r = self.right.evaluate(input)?;

        if l.data_type() != r.data_type() {
            return Err(Error::CompareError(format!(
                "Cannot compare {:?} and {:?}",
                l.data_type(),
                r.data_type()
            )));
        }

        let (ColumnarValue::Scalar(ll), ColumnarValue::Scalar(rr)) = (&l, &r);

        let scalar = match self.op {
            // Comparison operators
            Operator::Eq => ScalarValue::Boolean(Some(ll.eq(rr))),
            Operator::NotEq => ScalarValue::Boolean(Some(!ll.eq(rr))),
            Operator::Lt => ScalarValue::Boolean(Some(ll.lt(rr))),
            Operator::LtEq => ScalarValue::Boolean(Some(ll.le(rr))),
            Operator::Gt => ScalarValue::Boolean(Some(ll.gt(rr))),
            Operator::GtEq => ScalarValue::Boolean(Some(ll.ge(rr))),
            // Boolean operators
            Operator::And => ScalarValue::and(ll, rr)?,
            Operator::Or => ScalarValue::or(ll, rr)?,
            // Arithmetic operators
            Operator::Add => ScalarValue::add(ll, rr)?,
            Operator::Sub => ScalarValue::sub(ll, rr)?,
            Operator::Mul => ScalarValue::mul(ll, rr)?,
            Operator::Div => ScalarValue::div(ll, rr)?,
            Operator::Mod => ScalarValue::modulus(ll, rr)?,
        };

        Ok(ColumnarValue::Scalar(scalar))
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
