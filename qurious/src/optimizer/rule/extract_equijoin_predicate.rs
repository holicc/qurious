use crate::common::table_schema::TableSchemaRef;
use crate::error::Result;
use crate::utils::expr::{check_all_columns_from_schema, split_conjunctive_predicates};
use crate::{
    datatypes::operator::Operator,
    logical::{
        expr::{BinaryExpr, LogicalExpr},
        plan::{Join, LogicalPlan},
    },
    optimizer::rule::rule_optimizer::OptimizerRule,
};

/// Extract equijoin predicate from join filter.
/// extract equijoin predicate from join filter and treat equijoin predicate specially.
pub struct ExtractEquijoinPredicate;

impl OptimizerRule for ExtractEquijoinPredicate {
    fn name(&self) -> &str {
        "extract_equijoin_predicate"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let LogicalPlan::Join(Join {
            left,
            right,
            join_type,
            mut on,
            filter: Some(filter),
            schema,
        }) = plan
        else {
            return Ok(plan);
        };

        // Extract equijoin predicates from filter
        let (equijoin_predicates, remaining_filter) =
            extract_equijoin_predicates(filter, left.table_schema(), right.table_schema());

        // Combine existing join conditions with extracted equijoin predicates
        on.extend(equijoin_predicates);

        Ok(LogicalPlan::Join(Join {
            left,
            right,
            join_type,
            on,
            filter: remaining_filter,
            schema,
        }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::assert_after_optimizer;

    #[test]
    fn test_extract_equijoin_predicate_from_filter() {
        assert_after_optimizer(
            "SELECT * FROM users INNER JOIN repos ON users.id = repos.owner_id WHERE users.id = repos.id AND users.name = 'test'",
            Box::new(ExtractEquijoinPredicate),
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
            Box::new(ExtractEquijoinPredicate),
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
            Box::new(ExtractEquijoinPredicate),
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
