use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::OptimizerRule;
use crate::common::join_type::JoinType;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::datatypes::operator::Operator;
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, Column, LogicalExpr, SubQuery};
use crate::logical::plan::{CrossJoin, Filter, LogicalPlan};
use crate::logical::LogicalPlanBuilder;

type JoinPairSet<'a> = Vec<(&'a LogicalExpr, &'a LogicalExpr)>;

/// Looks like this:
/// ```text
/// Filter(a.x = b.y AND b.xx = 100)
///  Cross Join
///   TableScan a
///   TableScan b
/// ```
///
/// After the rule is applied, the plan will look like this:
/// ```text
/// Filter(b.xx = 100)
///   InnerJoin(a.x = b.y)
///     TableScan a
///     TableScan b
/// ```
#[derive(Debug, Default, Clone)]
pub struct PushdownFilterInnerJoin;

impl OptimizerRule for PushdownFilterInnerJoin {
    fn name(&self) -> &str {
        "pushdown_filter_inner_join"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // pushdown filter to inner join
        plan.transform(|plan| {
            // rewrite sub query
            let plan = plan
                .map_exprs(|expr| {
                    expr.transform(|expr| match expr {
                        LogicalExpr::SubQuery(query) => self
                            .optimize(*query.subquery)
                            .map(|rewritten_query| {
                                LogicalExpr::SubQuery(SubQuery {
                                    subquery: Box::new(rewritten_query),
                                    outer_ref_columns: query.outer_ref_columns,
                                })
                            })
                            .map(Transformed::yes),
                        _ => Ok(Transformed::no(expr)),
                    })
                })
                .data()?;

            match plan {
                LogicalPlan::Filter(Filter { input, expr: filter })
                    if matches!(input.as_ref(), LogicalPlan::CrossJoin(_)) =>
                {
                    let mut used_join_keys = vec![];
                    // extract cross join inputs
                    let mut cross_join_inputs = extract_cross_join_inputs(*input);
                    // extract filter condition
                    let join_set = extract_join_set(&filter);
                    // remove first input as left and try to find a right then combine them into a inner join
                    let mut left = cross_join_inputs.remove(0);
                    while !cross_join_inputs.is_empty() {
                        let right = cross_join_inputs.remove(0);
                        let left_schema = left.schema();
                        let right_schema = right.schema();
                        // try to find a join condition
                        let valid_join_pairs = join_set
                            .iter()
                            .filter(|(l_k, r_k)| is_valid_join_pair(l_k, r_k, &left_schema, &right_schema))
                            .collect::<Vec<_>>();
                        // no valid join condition, just cross join
                        if valid_join_pairs.is_empty() {
                            left = LogicalPlanBuilder::from(left).cross_join(right)?.build();
                        } else {
                            used_join_keys.extend(valid_join_pairs.clone());
                            // build join on filter
                            let join_on = valid_join_pairs
                                .into_iter()
                                .map(|(l_k, r_k)| {
                                    LogicalExpr::BinaryExpr(BinaryExpr::new(
                                        (*l_k).clone(),
                                        Operator::Eq,
                                        (*r_k).clone(),
                                    ))
                                })
                                .reduce(|l, r| LogicalExpr::BinaryExpr(BinaryExpr::new(l, Operator::And, r)));

                            // find the best join condition
                            left = LogicalPlanBuilder::from(left)
                                .join_on(right, JoinType::Inner, join_on)?
                                .build();
                        }
                    }

                    match remove_join_key_from_filter(&filter, &used_join_keys) {
                        Some(expr) => Filter::try_new(left, expr)
                            .map(LogicalPlan::Filter)
                            .map(Transformed::yes),
                        None => Ok(Transformed::yes(left)),
                    }
                }
                _ => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }
}

fn remove_join_key_from_filter(filter: &LogicalExpr, used_join_keys: &JoinPairSet) -> Option<LogicalExpr> {
    if used_join_keys.is_empty() {
        return Some(filter.clone());
    }

    match filter {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if op == &Operator::Eq && used_join_keys.contains(&(left.as_ref(), right.as_ref())) {
                return None;
            }

            if op == &Operator::And {
                let l = remove_join_key_from_filter(left, used_join_keys);
                let r = remove_join_key_from_filter(right, used_join_keys);

                return match (l, r) {
                    (Some(ll), Some(rr)) => Some(LogicalExpr::BinaryExpr(BinaryExpr::new(ll, *op, rr))),
                    (Some(ll), _) => Some(ll),
                    (_, Some(rr)) => Some(rr),
                    _ => None,
                };
            }

            if op == &Operator::Or {
                let l = remove_join_key_from_filter(left, used_join_keys);
                let r = remove_join_key_from_filter(right, used_join_keys);

                return match (l, r) {
                    (Some(ll), Some(rr)) => Some(LogicalExpr::BinaryExpr(BinaryExpr::new(ll, *op, rr))),
                    _ => None,
                };
            }

            Some(filter.clone())
        }
        _ => Some(filter.clone()),
    }
}

fn extract_cross_join_inputs(plan: LogicalPlan) -> Vec<LogicalPlan> {
    let mut cross_join_inputs = vec![];
    let mut stack = vec![plan];

    while let Some(next) = stack.pop() {
        match next {
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                stack.push(Arc::unwrap_or_clone(right));
                stack.push(Arc::unwrap_or_clone(left));
            }
            _ => {
                cross_join_inputs.push(next);
            }
        }
    }

    cross_join_inputs
}

fn extract_join_set<'a>(expr: &'a LogicalExpr) -> JoinPairSet<'a> {
    let mut join_set = JoinPairSet::new();

    let mut stack = vec![expr];

    while let Some(next) = stack.pop() {
        match next {
            LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    stack.push(right);
                    stack.push(left);
                }
                Operator::Or => {
                    let left_set = extract_join_set(&left);
                    let right_set = extract_join_set(&right);

                    // only join both side have the same key
                    for (l_k, r_k) in left_set {
                        if right_set.contains(&(l_k, r_k)) || right_set.contains(&(r_k, l_k)) {
                            join_set.push((l_k, r_k));
                        }
                    }
                }
                Operator::Eq => {
                    let pair = (left.as_ref(), right.as_ref());
                    if !join_set.contains(&pair) {
                        join_set.push(pair);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    join_set
}

fn is_valid_join_pair(l_k: &LogicalExpr, r_k: &LogicalExpr, left_schema: &SchemaRef, right_schema: &SchemaRef) -> bool {
    let l_cols = l_k.column_refs();
    let r_cols = r_k.column_refs();

    (!l_cols.is_empty() && !r_cols.is_empty())
        && ((check_all_columns_from_schema(&l_cols, &left_schema)
            && check_all_columns_from_schema(&r_cols, &right_schema))
            || (check_all_columns_from_schema(&r_cols, &left_schema)
                && check_all_columns_from_schema(&l_cols, &right_schema)))
}

fn check_all_columns_from_schema(columns: &HashSet<&Column>, schema: &SchemaRef) -> bool {
    for col in columns.iter() {
        if schema.field_with_name(&col.name).is_err() {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use crate::{
        optimizer::{pushdown_filter_inner_join::PushdownFilterInnerJoin, OptimizerRule},
        test_utils::sql_to_plan,
        utils,
    };

    fn assert_after_optimizer(sql: &str, expected: Vec<&str>) {
        let plan = sql_to_plan(sql);
        let optimizer = PushdownFilterInnerJoin;
        let plan = optimizer.optimize(plan).unwrap();
        let actual = utils::format(&plan, 0);
        let actual = actual.trim().lines().collect::<Vec<_>>();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
    }

    #[test]
    fn test_not_valid_join_pair() {
        assert_after_optimizer(
            "SELECT * FROM users,repos WHERE (users.id = repos.owner_id AND users.id = 10) OR (users.name = repos.name AND repos.id = 20)",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id AND users.id = Int64(10) OR users.name = repos.name AND repos.id = Int64(20)",
                "    CrossJoin",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );

        assert_after_optimizer(
            "SELECT * FROM users,repos WHERE (users.id = repos.owner_id AND users.id = 10) OR (users.id = repos.id OR users.id = 20)",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id AND users.id = Int64(10) OR users.id = repos.id OR users.id = Int64(20)",
                "    CrossJoin",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn should_not_pushdown_filter_for_inner_join() {
        assert_after_optimizer(
            "SELECT * FROM users INNER JOIN repos ON users.id = repos.owner_id WHERE users.id = 10",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = Int64(10)",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_multiple_cross_join() {
        assert_after_optimizer(
            "SELECT * FROM users,repos,commits WHERE users.id = repos.owner_id AND repos.id = commits.repo_id",
            vec![
                "Projection: (users.email, commits.id, repos.id, users.id, commits.message, repos.name, users.name, repos.owner_id, commits.repo_id, commits.time, commits.user_id)",
                "  Inner Join: Filter: users.id = repos.owner_id AND repos.id = commits.repo_id",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
                "    TableScan: commits",
            ],
        );

        assert_after_optimizer(
            r#"
            SELECT 
                MIN(ps_supplycost) 
            FROM 
                partsupp, 
                supplier, 
                nation, 
                region 
            WHERE 
                p_partkey = ps_partkey 
                AND s_suppkey = ps_suppkey 
                AND s_nationkey = n_nationkey 
                AND n_regionkey = r_regionkey 
                AND r_name = 'EUROPE'
            "#,
            vec![
                "Projection: (MIN(partsupp.ps_supplycost))",
                "  Aggregate: group_expr=[], aggregat_expr=[MIN(partsupp.ps_supplycost)]",
                "    Filter: p_partkey = partsupp.ps_partkey AND region.r_name = Utf8('EUROPE')",
                "      Inner Join: Filter: nation.n_regionkey = region.r_regionkey",
                "        Inner Join: Filter: supplier.s_nationkey = nation.n_nationkey",
                "          Inner Join: Filter: supplier.s_suppkey = partsupp.ps_suppkey",
                "            TableScan: partsupp",
                "            TableScan: supplier",
                "          TableScan: nation",
                "        TableScan: region",
            ],
        );
    }

    #[test]
    fn test_subquery() {
        assert_after_optimizer(
            r#"
                select
                    s_acctbal,
                    s_name,
                    n_name,
                    p_partkey,
                    p_mfgr,
                    s_address,
                    s_phone,
                    s_comment
                from
                    part,
                    supplier,
                    partsupp,
                    nation,
                    region
                where
                    p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and p_size = 15
                        and p_type like '%BRASS'
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
                        and ps_supplycost = (
                                select
                                    min(ps_supplycost)
                                from
                                    partsupp,
                                    supplier,
                                    nation,
                                    region
                                where
                                        p_partkey = ps_partkey
                                    and s_suppkey = ps_suppkey
                                    and s_nationkey = n_nationkey
                                    and n_regionkey = r_regionkey
                                    and r_name = 'EUROPE'
                        )
                order by
                    s_acctbal desc,
                    n_name,
                    s_name,
                    p_partkey
                limit 10;
        "#,
            vec![
                "Limit: fetch=10, skip=0",
                "  Sort: supplier.s_acctbal DESC, nation.n_name ASC, supplier.s_name ASC, part.p_partkey ASC",
                "    Projection: (supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment)",
                "      Filter: part.p_size = Int64(15) AND part.p_type LIKE Utf8('%BRASS') AND region.r_name = Utf8('EUROPE') AND partsupp.ps_supplycost = (",
                "          Projection: (MIN(partsupp.ps_supplycost))",
                "            Aggregate: group_expr=[], aggregat_expr=[MIN(partsupp.ps_supplycost)]",
                "              Filter: part.p_partkey = partsupp.ps_partkey AND region.r_name = Utf8('EUROPE')",
                "                Inner Join: Filter: nation.n_regionkey = region.r_regionkey",
                "                  Inner Join: Filter: supplier.s_nationkey = nation.n_nationkey",
                "                    Inner Join: Filter: supplier.s_suppkey = partsupp.ps_suppkey",
                "                      TableScan: partsupp",
                "                      TableScan: supplier",
                "                    TableScan: nation",
                "                  TableScan: region",
                ")",
                "",
                "        Inner Join: Filter: nation.n_regionkey = region.r_regionkey",
                "          Inner Join: Filter: supplier.s_nationkey = nation.n_nationkey",
                "            Inner Join: Filter: part.p_partkey = partsupp.ps_partkey AND supplier.s_suppkey = partsupp.ps_suppkey",
                "              CrossJoin",
                "                TableScan: part",
                "                TableScan: supplier",
                "              TableScan: partsupp",
                "            TableScan: nation",
                "          TableScan: region",
            ]
        );
    }
    #[test]
    fn test_pushdown_filter_inner_join_or() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id or repos.owner_id = users.id",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Inner Join: Filter: users.id = repos.owner_id",
                "    TableScan: users",
                "    TableScan: repos",
            ],
        );

        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.id > 1) OR (users.id = repos.owner_id and repos.owner_id = 30)",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id > Int64(1) OR repos.owner_id = Int64(30)",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_pushdown_filter_inner_join_and() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id and users.id > 10",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id > Int64(10)",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );

        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.id > 1) AND (users.id = repos.owner_id and repos.owner_id = 30)",
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id > Int64(1) AND repos.owner_id = Int64(30)",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      TableScan: users",
                "      TableScan: repos",
            ],
        );
    }
}
