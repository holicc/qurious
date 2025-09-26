use std::{collections::HashMap, sync::Arc};

use indexmap::{Equivalent, IndexSet};

use crate::{
    common::{
        join_type::JoinType,
        table_schema::TableSchemaRef,
        transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion},
    },
    datatypes::operator::Operator,
    error::Result,
    logical::{
        expr::{BinaryExpr, LogicalExpr},
        plan::{Filter, LogicalPlan},
        LogicalPlanBuilder,
    },
    optimizer::rule::OptimizerRule,
};

/// Eliminate cross joins by rewriting them to inner joins when possible.
pub struct EliminateCrossJoin;

impl OptimizerRule for EliminateCrossJoin {
    fn name(&self) -> &str {
        "eliminate_cross_join"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(Filter { input, expr: predicate }) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut all_corss_joins = vec![];

        // collect all cross joins and filter predicates in order
        input.apply(|plan| {
            if let LogicalPlan::CrossJoin(cross_join) = plan {
                all_corss_joins.push(cross_join);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        if all_corss_joins.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Filter(Filter { input, expr: predicate })));
        }

        let mut all_join_keys = IndexSet::new();
        let mut replaced_cross_joins = HashMap::new();
        let len = all_corss_joins.len();
        // iteratively rewrite cross joins to inner joins from bottom to top
        for (index, cross_join) in all_corss_joins.into_iter().rev().enumerate() {
            let left_schema = cross_join.left.table_schema();
            let right_schema = cross_join.right.table_schema();

            let join_keys = extract_join_pairs(&predicate, &left_schema, &right_schema);

            all_join_keys.extend(join_keys.clone());

            if !join_keys.is_empty() {
                let inner_join_plan = LogicalPlanBuilder::from(Arc::unwrap_or_clone(cross_join.left.clone()))
                    .join(
                        Arc::unwrap_or_clone(cross_join.right.clone()),
                        JoinType::Inner,
                        join_keys.into_iter().collect(),
                        None,
                    )?
                    .build();

                // this index should be from the top to the bottom
                replaced_cross_joins.insert((len - 1) - index, inner_join_plan);
            }
        }

        if replaced_cross_joins.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Filter(Filter { input, expr: predicate })));
        }

        // combine all predicates and replaced cross joins
        let mut index = 0;
        let new_input = input
            .transform(|plan| {
                let result = if let Some(replaced_join) = replaced_cross_joins.remove(&index) {
                    Ok(Transformed::yes(replaced_join))
                } else {
                    Ok(Transformed::no(plan))
                };
                index += 1;
                result
            })
            .data()?;

        // remove all join keys from original predicates
        if let Some(predicate) = remove_join_keys(predicate, &all_join_keys) {
            LogicalPlanBuilder::filter(new_input, predicate).map(Transformed::yes)
        } else {
            Ok(Transformed::yes(new_input))
        }
    }
}

fn extract_join_pairs<'a>(
    expr: &'a LogicalExpr,
    left_schema: &TableSchemaRef,
    right_schema: &TableSchemaRef,
) -> IndexSet<(LogicalExpr, LogicalExpr)> {
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
                if left_schema.has_column(left_col) && right_schema.has_column(right_col) {
                    join_keys.insert((left.as_ref().clone(), right.as_ref().clone()));
                } else if right_schema.has_column(left_col) && left_schema.has_column(right_col) {
                    join_keys.insert((right.as_ref().clone(), left.as_ref().clone()));
                }
            }
        }
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let left_join_keys = extract_join_pairs(left, left_schema, right_schema);
            let right_join_keys = extract_join_pairs(right, left_schema, right_schema);

            join_keys.extend(left_join_keys);
            join_keys.extend(right_join_keys);
        }
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let left_join_keys = extract_join_pairs(left, left_schema, right_schema);
            let right_join_keys = extract_join_pairs(right, left_schema, right_schema);

            for (l, r) in left_join_keys {
                if right_join_keys.contains(&ExprPair::new(&l, &r)) || right_join_keys.contains(&ExprPair::new(&r, &l))
                {
                    join_keys.insert((l, r));
                }
            }
        }
        _ => {}
    }

    join_keys
}

fn remove_join_keys(expr: LogicalExpr, join_keys: &IndexSet<(LogicalExpr, LogicalExpr)>) -> Option<LogicalExpr> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) if join_keys.contains(&ExprPair::new(left.as_ref(), right.as_ref()))
            || join_keys.contains(&ExprPair::new(right.as_ref(), left.as_ref())) =>
        {
            None
        }
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) if op == Operator::And => {
            let l = remove_join_keys(*left, join_keys);
            let r = remove_join_keys(*right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(LogicalExpr::BinaryExpr(BinaryExpr::new(ll, op, rr))),
                (Some(ll), _) => Some(ll),
                (_, Some(rr)) => Some(rr),
                _ => None,
            }
        }
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) if op == Operator::Or => {
            let l = remove_join_keys(*left, join_keys);
            let r = remove_join_keys(*right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(LogicalExpr::BinaryExpr(BinaryExpr::new(ll, op, rr))),
                _ => None,
            }
        }
        _ => Some(expr),
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
struct ExprPair<'a>(&'a LogicalExpr, &'a LogicalExpr);

impl<'a> ExprPair<'a> {
    fn new(left: &'a LogicalExpr, right: &'a LogicalExpr) -> Self {
        Self(left, right)
    }
}

impl Equivalent<(LogicalExpr, LogicalExpr)> for ExprPair<'_> {
    fn equivalent(&self, other: &(LogicalExpr, LogicalExpr)) -> bool {
        self.0 == &other.0 && self.1 == &other.1
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

    #[test]
    fn test_tpch_03() {
        assert_after_optimizer(
            "    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
            c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-15'
      and l_shipdate > date '1995-03-15'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate
     limit 10;",
            vec![Box::new(EliminateCrossJoin)],
            vec![
                "Limit: fetch=10, skip=0",
                "  Sort: revenue DESC, orders.o_orderdate ASC",
                "    Projection: (lineitem.l_orderkey, SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS revenue, orders.o_orderdate, orders.o_shippriority)",
                "      Aggregate: group_expr=[lineitem.l_orderkey,orders.o_orderdate,orders.o_shippriority], aggregat_expr=[SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]",
                "        Filter: customer.c_mktsegment = Utf8('BUILDING') AND orders.o_orderdate < CAST(Utf8('1995-03-15') AS Date32) AND lineitem.l_shipdate > CAST(Utf8('1995-03-15') AS Date32)",
                "          Inner Join: On: (orders.o_orderkey, lineitem.l_orderkey)",
                "            Inner Join: On: (customer.c_custkey, orders.o_custkey)",
                "              TableScan: customer",
                "              TableScan: orders",
                "            TableScan: lineitem",
            ],
        );
    }
}
