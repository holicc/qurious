use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::common::join_type::JoinType;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::datatypes::operator::Operator;
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, Column, LogicalExpr, SubQuery};
use crate::logical::plan::{CrossJoin, Filter, Join, LogicalPlan};
use crate::logical::LogicalPlanBuilder;
use crate::optimizer::rule::rule_optimizer::OptimizerRule;
use crate::utils::expr::{is_restrict_null_predicate, split_conjunctive_predicates};

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
pub struct PushdownFilterJoin;

impl OptimizerRule for PushdownFilterJoin {
    fn name(&self) -> &str {
        "pushdown_filter_join"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if let LogicalPlan::Join(join) = plan {
            return push_down_filter_to_join(join, None);
        }

        let LogicalPlan::Filter(filter) = plan else {
            return Ok(plan);
        };

        match filter.input.as_ref().clone() {
            LogicalPlan::Join(join) => {
                push_down_filter_to_join(join, Some(&filter.expr))?;
                todo!()
            }

            _ => todo!(),
        }

        todo!()
    }
}

fn push_down_filter_to_join(join: Join, parent_predicate: Option<&LogicalExpr>) -> Result<LogicalPlan> {
    let parent_filter_exprs = parent_predicate.map_or_else(Vec::new, |expr| split_conjunctive_predicates(expr.clone()));

    let join_filter_exprs = join
        .filter
        .as_ref()
        .map_or_else(Vec::new, |expr| split_conjunctive_predicates(expr.clone()));

    let infer_filter_exprs = infer_join_predicates(&join, &parent_filter_exprs, &join_filter_exprs);

    // if parent_filter_exprs.is_empty() && join_filter_exprs.is_empty() && infer_filter_exprs.is_empty() {
    //     return Ok(LogicalPlan::Join(join));
    // }

    todo!()
}

fn infer_join_predicates(
    join: &Join,
    predicates: &[LogicalExpr],
    on_filters: &[LogicalExpr],
) -> Result<Vec<LogicalExpr>> {
    let join_on_keys = join
        .on
        .iter()
        .filter_map(|(l, r)| {
            let left = l.try_as_column()?;
            let right = r.try_as_column()?;

            Some((left, right))
        })
        .collect::<Vec<_>>();

    // infer predicates from the pushed down predicates.
    let predicates = infer_join_predicates_impl::<true, true>(&join.join_type, &join_on_keys, predicates)?;

    todo!()
}

fn infer_join_predicates_impl<const ENABLE_LEFT_TO_RIGHT: bool, const ENABLE_RIGHT_TO_LEFT: bool>(
    join_type: &JoinType,
    join_on_keys: &[(&Column, &Column)],
    predicates: &[LogicalExpr],
) -> Result<Vec<LogicalExpr>> {
    let mut infered_join_predicates = Vec::new();

    for predicate in predicates {
        let mut join_cols_to_replace = HashMap::new();

        for &col in &predicate.column_refs() {
            for (l, r) in join_on_keys {
                if ENABLE_LEFT_TO_RIGHT && col == *l {
                    join_cols_to_replace.insert(col, *r);
                }
                if ENABLE_RIGHT_TO_LEFT && col == *r {
                    join_cols_to_replace.insert(col, *l);
                }
            }
        }

        if join_cols_to_replace.is_empty() {
            continue;
        }

        let is_null_predicate = is_restrict_null_predicate(&predicate, join_cols_to_replace.keys().cloned())?;

        if join_type == &JoinType::Inner || is_null_predicate {
            infered_join_predicates.push(predicate.clone());
        }
    }

    Ok(infered_join_predicates)
}

#[cfg(test)]
mod tests {
    use crate::{optimizer::rule::pushdown_filter_join::PushdownFilterJoin, test_utils::assert_after_optimizer};

    #[test]
    fn test_not_valid_join_pair() {
        assert_after_optimizer(
            "SELECT * FROM users,repos WHERE (users.id = repos.owner_id AND users.id = 10) OR (users.name = repos.name AND repos.id = 20)",
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Inner Join: Filter: users.id = repos.owner_id",
                "    TableScan: users",
                "    TableScan: repos",
            ],
        );

        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE (users.id = repos.owner_id and users.id > 1) OR (users.id = repos.owner_id and repos.owner_id = 30)",
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
            Box::new(PushdownFilterJoin),
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
