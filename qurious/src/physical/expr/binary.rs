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
    use arrow::array::Decimal128Array;
    use arrow::datatypes::DataType;

    use super::*;
    use crate::datatypes::scalar::ScalarValue;
    use crate::physical::expr::{CastExpr, Literal};
    use crate::{build_schema, physical::expr::Column, test_utils::build_record_i32};
    use std::sync::Arc;

    #[test]
    fn test_comparison_ops() {
        let test_cases = vec![
            (Operator::Eq, vec![1, 1], vec![1, 2], vec![true, false]),
            (Operator::Gt, vec![1, 2], vec![0, 3], vec![true, false]),
            (Operator::GtEq, vec![1, 2], vec![1, 3], vec![true, false]),
            (Operator::Lt, vec![1, 2], vec![2, 2], vec![true, false]),
            (Operator::LtEq, vec![1, 2], vec![2, 2], vec![true, true]),
            (Operator::NotEq, vec![1, 2], vec![1, 3], vec![false, true]),
        ];

        for (op, left_vals, right_vals, expected) in test_cases {
            let expr = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left", 0)),
                op,
                Arc::new(Column::new("right", 1)),
            ));

            let schema = Arc::new(build_schema!(("left", DataType::Int32), ("right", DataType::Int32)));

            let data = build_record_i32(schema, vec![left_vals, right_vals]);
            let results = expr.evaluate(&data).unwrap();
            let expected_array = Arc::new(BooleanArray::from(expected));

            assert_eq!(*results, *expected_array);
        }
    }

    #[test]
    fn test_arithmetic_ops() {
        let test_cases = vec![
            (Operator::Add, vec![1, 2], vec![2, 3], vec![3, 5]),
            (Operator::Sub, vec![5, 3], vec![2, 1], vec![3, 2]),
            (Operator::Mul, vec![2, 3], vec![3, 4], vec![6, 12]),
            (Operator::Div, vec![6, 8], vec![2, 4], vec![3, 2]),
            (Operator::Mod, vec![7, 9], vec![4, 2], vec![3, 1]),
        ];

        for (op, left_vals, right_vals, expected) in test_cases {
            let expr = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left", 0)),
                op,
                Arc::new(Column::new("right", 1)),
            ));

            let schema = Arc::new(build_schema!(("left", DataType::Int32), ("right", DataType::Int32)));

            let data = build_record_i32(schema, vec![left_vals, right_vals]);
            let results = expr.evaluate(&data).unwrap();
            let expected_array = Arc::new(arrow::array::Int32Array::from(expected));

            assert_eq!(*results, *expected_array);
        }
    }

    #[test]
    fn test_logical_ops() {
        let test_cases = vec![
            (
                Operator::And,
                vec![true, true, false, false],
                vec![true, false, true, false],
                vec![true, false, false, false],
            ),
            (
                Operator::Or,
                vec![true, true, false, false],
                vec![true, false, true, false],
                vec![true, true, true, false],
            ),
        ];

        for (op, left_vals, right_vals, expected) in test_cases {
            let expr = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left", 0)),
                op,
                Arc::new(Column::new("right", 1)),
            ));

            let schema = Arc::new(build_schema!(("left", DataType::Boolean), ("right", DataType::Boolean)));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(BooleanArray::from(left_vals)),
                    Arc::new(BooleanArray::from(right_vals)),
                ],
            )
            .unwrap();

            let results = expr.evaluate(&batch).unwrap();
            let expected_array = Arc::new(BooleanArray::from(expected));

            assert_eq!(*results, *expected_array);
        }
    }

    #[test]
    fn test_nested_arithmetic() {
        // Create expression: (l_extendedprice * (1 - l_discount))
        // First create (1 - l_discount)
        let one_minus_discount = Arc::new(BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(Literal::new(ScalarValue::Int16(Some(1)))),
                DataType::Decimal128(15, 2),
            )),
            Operator::Sub,
            Arc::new(Column::new("l_discount", 1)),
        ));

        // Then create the final expression
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("l_extendedprice", 0)),
            Operator::Mul,
            one_minus_discount,
        ));

        let schema = Arc::new(build_schema!(
            ("l_extendedprice", DataType::Decimal128(15, 2)),
            ("l_discount", DataType::Decimal128(15, 2))
        ));

        // Test data:
        // l_extendedprice = 26517.75 (represented as 2651775)
        // l_discount = 0.02 (represented as 2)
        // Expected: 26517.75 * (1.00 - 0.02) = 26517.75 * 0.98 = 25987.395 = 25987.395
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal128Array::from_iter_values(vec![2651775])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ), // 26517.75
                Arc::new(
                    Decimal128Array::from_iter_values(vec![2])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ), // 0.02
            ],
        )
        .unwrap();

        let results = expr.evaluate(&batch).unwrap();
        let expected = Arc::new(
            Decimal128Array::from_iter_values(vec![259873950])
                .with_precision_and_scale(32, 4)
                .unwrap(),
        ); // 25987.395

        assert_eq!(*results, *expected);
    }
}
