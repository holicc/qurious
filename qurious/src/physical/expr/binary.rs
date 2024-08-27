use arrow::array::{ArrayRef, AsArray, BooleanArray, Datum};
use arrow::compute::kernels::cmp::*;
use arrow::compute::kernels::numeric::{add_wrapping, div, mul_wrapping, rem, sub_wrapping};
use arrow::compute::{and_kleene, or_kleene};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use super::PhysicalExpr;
use crate::arrow_err;
use crate::datatypes::operator::Operator;
use crate::error::{Error, Result};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        Self { left, op, right }
    }
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let l = self.left.evaluate(input)?;
        let r = self.right.evaluate(input)?;

        match self.op {
            // compare
            Operator::Eq => cmp(&l, &r, eq),
            Operator::NotEq => cmp(&l, &r, neq),
            Operator::Gt => cmp(&l, &r, gt),
            Operator::GtEq => cmp(&l, &r, gt_eq),
            Operator::Lt => cmp(&l, &r, lt),
            Operator::LtEq => cmp(&l, &r, lt_eq),
            // logic
            Operator::And => and_kleene(l.as_boolean(), r.as_boolean())
                .map(|a| Arc::new(a) as ArrayRef)
                .map_err(|e| arrow_err!(e)),
            Operator::Or => or_kleene(l.as_boolean(), r.as_boolean())
                .map(|a| Arc::new(a) as ArrayRef)
                .map_err(|e| arrow_err!(e)),
            // arithmetic
            Operator::Add => add_wrapping(&l, &r).map_err(|e| arrow_err!(e)),
            Operator::Sub => sub_wrapping(&l, &r).map_err(|e| arrow_err!(e)),
            Operator::Mul => mul_wrapping(&l, &r).map_err(|e| arrow_err!(e)),
            Operator::Div => div(&l, &r).map_err(|e| arrow_err!(e)),
            Operator::Mod => rem(&l, &r).map_err(|e| arrow_err!(e)),
        }
    }
}

fn cmp(
    l: &ArrayRef,
    r: &ArrayRef,
    f: impl Fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
) -> Result<ArrayRef, Error> {
    f(&l.as_ref(), &r.as_ref())
        .map(|a| Arc::new(a) as ArrayRef)
        .map_err(|e| arrow_err!(e))
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::{build_schema, physical::expr::Column, test_utils::build_record_i32};
    use std::sync::Arc;

    #[test]
    fn test_eq() {
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b1", 0)),
            Operator::Eq,
            Arc::new(Column::new("b2", 1)),
        ));

        let schema = Arc::new(build_schema!(("b1", DataType::Int32), ("b2", DataType::Int32)));

        let data = build_record_i32(schema, vec![vec![1, 2]]);

        let results = expr.evaluate(&data[0]).unwrap();
        let except = Arc::new(BooleanArray::from(vec![false]));

        assert_eq!(*results, *except);
    }
}
