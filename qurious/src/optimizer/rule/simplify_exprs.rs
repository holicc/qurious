use crate::{
    common::transformed::{Transformed, TransformedResult},
    datatypes::{operator::Operator, scalar::ScalarValue},
    error::Result,
    logical::{
        expr::{BinaryExpr, LogicalExpr},
        plan::LogicalPlan,
    },
    optimizer::rule::OptimizerRule,
};

macro_rules! impl_constant_folding {
    (
        match_val = $match_val:expr,
        cases = {
            $(
                $($scalar_type:tt) |+ => $op:path => $val_op:tt,
            )*
        }
    ) => {
        match $match_val {
            $(
                    $(
                        (
                            LogicalExpr::Literal(ScalarValue::$scalar_type(Some(a))),
                            $op,
                            LogicalExpr::Literal(ScalarValue::$scalar_type(Some(b))),
                        ) => Ok(Transformed::yes(LogicalExpr::Literal(ScalarValue::$scalar_type(Some(a $val_op b))))),
                    )*
            )*
            (LogicalExpr::Literal(ScalarValue::Boolean(Some(false))), Operator::And, _) | (_, Operator::And, LogicalExpr::Literal(ScalarValue::Boolean(Some(false))))
               => Ok(Transformed::yes(LogicalExpr::Literal(ScalarValue::Boolean(Some(false))))),
            (LogicalExpr::Literal(ScalarValue::Boolean(Some(true))), Operator::Or, _) | (_, Operator::Or, LogicalExpr::Literal(ScalarValue::Boolean(Some(true))))
               => Ok(Transformed::yes(LogicalExpr::Literal(ScalarValue::Boolean(Some(true))))),
            (left, op, right) => Ok(Transformed::no(LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))),
        }
    };
}

/// Simplify expressions in the plan
///
/// For example, constant folding the expression `1 + 1` to `2`
pub struct SimplifyExprs;

impl OptimizerRule for SimplifyExprs {
    fn name(&self) -> &str {
        "simplify_exprs"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.map_exprs(|expr| self.constant_folding(expr))
    }
}

impl SimplifyExprs {
    /// Constant folding for binary expressions
    /// This is a very simple approach, which doesn't handle more complex
    /// cases such as 1 + a - 2 (which would require rearranging the
    /// expression as 1 - 2 + a to evaluate the 1 - 2 branch).
    /// TODO: handle more complex cases
    fn constant_folding(&self, expr: LogicalExpr) -> Result<Transformed<LogicalExpr>> {
        let LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return Ok(Transformed::no(expr));
        };

        let left = self.constant_folding(*left).data()?;
        let right = self.constant_folding(*right).data()?;

        impl_constant_folding! {
            match_val = (left, op, right),
            cases = {
                Boolean => Operator::And => &&,
                Boolean => Operator::Or => ||,
                Int64 | Int32 | Int16 | Int8 | UInt64 | UInt32 | UInt16 | UInt8 => Operator::Add => + ,
                Int64 | Int32 | Int16 | Int8 | UInt64 | UInt32 | UInt16 | UInt8 => Operator::Sub => -,
                Int64 | Int32 | Int16 | Int8 | UInt64 | UInt32 | UInt16 | UInt8 => Operator::Mul => *,
                Int64 | Int32 | Int16 | Int8 | UInt64 | UInt32 | UInt16 | UInt8 => Operator::Div => /,
                Int64 | Int32 | Int16 | Int8 | UInt64 | UInt32 | UInt16 | UInt8 => Operator::Mod => %,
                Float64 | Float32 => Operator::Add => +,
                Float64 | Float32 => Operator::Sub => -,
                Float64 | Float32 => Operator::Mul => *,
                Float64 | Float32 => Operator::Div => /,
                Float64 | Float32 => Operator::Mod => %,

            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::assert_after_optimizer;

    use super::*;

    #[test]
    fn test_constant_folding_where_clause_00() {
        assert_after_optimizer(
            "SELECT * FROM users WHERE id = 1 AND name = 'John' AND false",
            vec![Box::new(SimplifyExprs)],
            vec![
                "Projection: (users.id, users.name, users.email)",
                "  Filter: Boolean(false)",
                "    TableScan: users",
            ],
        );
    }

    #[test]
    fn test_constant_folding_where_clause_01() {
        assert_after_optimizer(
            "SELECT * FROM users WHERE id = 1 + 1",
            vec![Box::new(SimplifyExprs)],
            vec![
                "Projection: (users.id, users.name, users.email)",
                "  Filter: users.id = Int64(2)",
                "    TableScan: users",
            ],
        );
    }

    #[test]
    fn test_constant_folding_boolean_and_true() {
        assert_after_optimizer(
            "SELECT TRUE AND TRUE",
            vec![Box::new(SimplifyExprs)],
            vec!["Projection: (Boolean(true))", "  Empty Relation"],
        );
    }

    #[test]
    fn test_constant_folding_boolean_and_false() {
        assert_after_optimizer(
            "SELECT TRUE AND FALSE",
            vec![Box::new(SimplifyExprs)],
            vec!["Projection: (Boolean(false))", "  Empty Relation"],
        );
    }

    #[test]
    fn test_constant_folding_boolean_or_true() {
        assert_after_optimizer(
            "SELECT TRUE OR TRUE",
            vec![Box::new(SimplifyExprs)],
            vec!["Projection: (Boolean(true))", "  Empty Relation"],
        );
    }

    #[test]
    fn test_constant_folding_boolean_or_false() {
        assert_after_optimizer(
            "SELECT TRUE OR FALSE",
            vec![Box::new(SimplifyExprs)],
            vec!["Projection: (Boolean(true))", "  Empty Relation"],
        );
    }
}
