use crate::common::table_schema::TableSchemaRef;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::expr::Column;
use crate::utils::expr::split_conjunctive_predicates;
use crate::{
    datatypes::operator::Operator,
    logical::{
        expr::{BinaryExpr, LogicalExpr},
        plan::{Join, LogicalPlan},
    },
    optimizer::OptimizerRule,
};
use std::collections::HashSet;

/// Extract equijoin predicate from join filter.
/// extract equijoin predicate from join filter and treat equijoin predicate specially.
pub struct ExtractEquijoinPredicate;

impl OptimizerRule for ExtractEquijoinPredicate {
    fn name(&self) -> &str {
        "extract_equijoin_predicate"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform_up(|plan| {
            match plan {
                LogicalPlan::Join(Join {
                    left,
                    right,
                    join_type,
                    mut on,
                    filter: Some(filter),
                    schema,
                }) => {
                    // Extract equijoin predicates from filter
                    let (equijoin_predicates, remaining_filter) =
                        extract_equijoin_predicates(filter, left.table_schema(), right.table_schema());

                    // Combine existing join conditions with extracted equijoin predicates
                    on.extend(equijoin_predicates);

                    Ok(Transformed::yes(LogicalPlan::Join(Join {
                        left,
                        right,
                        join_type,
                        on,
                        filter: remaining_filter,
                        schema,
                    })))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }
}

/// Extract equijoin predicates from a filter expression
fn extract_equijoin_predicates(
    filter: LogicalExpr,
    left_schema: TableSchemaRef,
    right_schema: TableSchemaRef,
) -> (Vec<(LogicalExpr, LogicalExpr)>, Option<LogicalExpr>) {
    let exprs = split_conjunctive_predicates(filter);
    let mut equijoins = vec![];
    let mut filters = vec![];
    for expr in exprs {
        match expr {
            LogicalExpr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => {
                let left_columns = left.using_columns();
                let right_columns = right.using_columns();

                // Conditions like a = 10, will be added to non-equijoin.
                if left_columns.is_empty() || right_columns.is_empty() {
                    filters.push(LogicalExpr::BinaryExpr(BinaryExpr {
                        left,
                        op: Operator::Eq,
                        right,
                    }));
                } else {
                    if check_all_columns_from_schema(&left_columns, &left_schema)
                        && check_all_columns_from_schema(&right_columns, &right_schema)
                    {
                        equijoins.push((*left, *right));
                    } else if check_all_columns_from_schema(&right_columns, &left_schema)
                        && check_all_columns_from_schema(&left_columns, &right_schema)
                    {
                        equijoins.push((*right, *left));
                    }
                }
            }
            _ => filters.push(expr),
        }
    }

    (
        equijoins,
        filters.into_iter().reduce(|left, right| {
            LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::And,
                right: Box::new(right),
            })
        }),
    )
}

fn check_all_columns_from_schema(columns: &HashSet<Column>, schema: &TableSchemaRef) -> bool {
    for col in columns.iter() {
        if !schema.has_field(col.relation.as_ref(), &col.name) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{optimizer::OptimizerRule, test_utils::sql_to_plan, utils};

    fn assert_after_optimizer(sql: &str, expected: Vec<&str>) {
        let plan = sql_to_plan(sql);
        let optimizer = ExtractEquijoinPredicate;
        let plan = optimizer.optimize(plan).unwrap();
        let actual = utils::format(&plan, 0);
        let actual = actual.trim().lines().collect::<Vec<_>>();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
    }

    #[test]
    fn test_extract_equijoin_predicate_from_filter() {
        assert_after_optimizer(
            "SELECT * FROM users INNER JOIN repos ON users.id = repos.owner_id WHERE users.id = repos.id AND users.name = 'test'",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.id AND users.name = Utf8('test')",
                "    Inner Join: On: (users.id, repos.owner_id)",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_no_equijoin_predicates_in_filter() {
        assert_after_optimizer(
            "SELECT * FROM users INNER JOIN repos ON users.id > repos.owner_id WHERE users.name = 'test' AND repos.name = 'repo'",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.name = Utf8('test') AND repos.name = Utf8('repo')",
                "    Inner Join: Filter: users.id > repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_multiple_equijoin_predicates() {
        assert_after_optimizer(
            "SELECT * FROM users INNER JOIN repos ON users.id = repos.owner_id AND users.name = repos.name WHERE users.id = repos.id AND users.email = 'test@example.com'",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.id AND users.email = Utf8('test@example.com')",
                "    Inner Join: On: (users.id, repos.owner_id), (users.name, repos.name)",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }
}
