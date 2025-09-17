use indexmap::IndexSet;

use crate::{
    common::{join_type::JoinType, table_schema::TableSchemaRef, transformed::Transformed},
    datatypes::operator::Operator,
    error::Result,
    logical::{
        expr::{BinaryExpr, LogicalExpr},
        plan::{Filter, Join, LogicalPlan},
        LogicalPlanBuilder,
    },
    optimizer::rule::OptimizerRule,
};

pub struct EliminateCrossJoin;

impl OptimizerRule for EliminateCrossJoin {
    fn name(&self) -> &str {
        "eliminate_cross_join"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) if matches!(filter.input.as_ref(), LogicalPlan::CrossJoin(_)) => {
                let LogicalPlan::CrossJoin(cross_join) = *filter.input else {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                };

                let left_schema = cross_join.left.table_schema();
                let right_schema = cross_join.right.table_schema();

                let (join_keys, remaining_predicate) = extract_join_pairs(&filter.expr, &left_schema, &right_schema);

                if join_keys.is_empty() {
                    Ok(Transformed::no(LogicalPlan::Filter(Filter {
                        input: Box::new(LogicalPlan::CrossJoin(cross_join)),
                        expr: filter.expr,
                    })))
                } else {
                    let inner_join_plan = LogicalPlan::Join(Join {
                        left: cross_join.left,
                        right: cross_join.right,
                        join_type: JoinType::Inner,
                        on: join_keys.into_iter().map(|(l, r)| (l.clone(), r.clone())).collect(),
                        filter: None,
                        schema: cross_join.schema,
                    });

                    if let Some(predicate) = remaining_predicate {
                        LogicalPlanBuilder::filter(inner_join_plan, predicate).map(Transformed::yes)
                    } else {
                        Ok(Transformed::yes(inner_join_plan))
                    }
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn extract_join_pairs<'a>(
    expr: &'a LogicalExpr,
    left_schema: &TableSchemaRef,
    right_schema: &TableSchemaRef,
) -> (IndexSet<(&'a LogicalExpr, &'a LogicalExpr)>, Option<LogicalExpr>) {
    let mut join_keys = IndexSet::new();

    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            let left_col = left.try_as_column();
            let right_col = right.try_as_column();

            if let (Some(left_col), Some(right_col)) = (left_col, right_col) {
                if (left_schema.has_column(left_col) || right_schema.has_column(left_col))
                    && (left_schema.has_column(right_col) || right_schema.has_column(right_col))
                {
                    join_keys.insert((left.as_ref(), right.as_ref()));

                    return (join_keys, None);
                }
            }

            (join_keys, Some(expr.clone()))
        }
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let (left_join_keys, left_predicate) = extract_join_pairs(left, left_schema, right_schema);
            let (right_join_keys, right_predicate) = extract_join_pairs(right, left_schema, right_schema);

            join_keys.extend(left_join_keys);
            join_keys.extend(right_join_keys);

            let predicate = match (left_predicate, right_predicate) {
                (Some(left_predicate), Some(right_predicate)) => Some(LogicalExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(left_predicate),
                    op: Operator::And,
                    right: Box::new(right_predicate),
                })),
                (l, r) => l.or(r),
            };

            (join_keys, predicate)
        }
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let (left_join_keys, left_predicate) = extract_join_pairs(left, left_schema, right_schema);
            let (right_join_keys, right_predicate) = extract_join_pairs(right, left_schema, right_schema);

            for (l, r) in left_join_keys {
                if right_join_keys.contains(&(l, r)) || right_join_keys.contains(&(r, l)) {
                    join_keys.insert((l, r));
                }
            }

            let predicate = match (left_predicate, right_predicate) {
                (Some(l), Some(r)) => Some(LogicalExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(l),
                    op: Operator::Or,
                    right: Box::new(r),
                })),
                (l, r) => l.or(r),
            };

            (join_keys, predicate)
        }

        _ => (join_keys, Some(expr.clone())),
    }
}

#[cfg(test)]
mod tests {
    use crate::{optimizer::rule::eliminate_cross_join::EliminateCrossJoin, test_utils::assert_after_optimizer};

    #[test]
    fn test_eliminate_cross_join_simple_and() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id AND users.id = 10",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = Int64(10)",
                "    Inner Join: On: (users.id, repos.owner_id)",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_eliminate_cross_join_simple_or() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id OR users.id = 10",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id OR users.id = Int64(10)",
                "    CrossJoin",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_eliminate_cross_join_and() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.name < 'a') AND (users.id = repos.owner_id and users.name = 'b')",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.name < Utf8('a') AND users.name = Utf8('b')",
                "    Inner Join: On: (users.id, repos.owner_id)",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_eliminate_cross_join_or() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.name < 'a') OR (users.id = repos.owner_id and users.name = 'b')",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.name < Utf8('a') OR users.name = Utf8('b')",
                "    Inner Join: On: (users.id, repos.owner_id)",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_eliminate_cross_join_or_with_not_valid_join_pair_case1() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.name < 'a') OR (users.id = repos.id and users.name = 'b')",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id AND users.name < Utf8('a') OR users.id = repos.id AND users.name = Utf8('b')",
                "    CrossJoin",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_eliminate_cross_join_or_with_not_valid_join_pair_case2() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.name < 'a') OR (users.id = repos.owner_id OR users.name = 'b')",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id AND users.name < Utf8('a') OR users.id = repos.owner_id OR users.name = Utf8('b')",
                "    CrossJoin",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }
}
