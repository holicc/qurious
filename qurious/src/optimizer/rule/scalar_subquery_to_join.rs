use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::common::join_type::JoinType;
use crate::common::table_schema::TableSchemaRef;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion};
use crate::datatypes::operator::Operator;
use crate::error::Error;
use crate::error::Result;
use crate::internal_err;
use crate::logical::expr::alias::Alias;
use crate::logical::expr::{BinaryExpr, Column, LogicalExpr, SubQuery};
use crate::logical::plan::LogicalPlan;
use crate::logical::LogicalPlanBuilder;
use crate::optimizer::rule::OptimizerRule;
use crate::utils::alias::AliasGenerator;
use crate::utils::expr::split_conjunctive_predicates;

const SCALAR_SUBQUERY_ALIAS_PREFIX: &str = "__scalar_sq";

/// Convert scalar subquery to join
///
/// ```sql
/// SELECT a FROM t1 WHERE t1.a = (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a);
/// ```
///
/// After the rule is applied, the plan will look like this:
/// ```text
/// SELECT a FROM t1 LEFT JOIN (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a) AS t2 ON t1.a = t2.a WHERE t1.a = t2.b;
/// ```
#[derive(Default)]
pub struct ScalarSubqueryToJoin {
    id_generator: AliasGenerator,
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let new_plan = plan
            .transform(|plan| match plan {
                LogicalPlan::Filter(filter) => {
                    if !contains_scalar_subquery(&filter.expr) {
                        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                    }

                    let (subqueries, rewritten_expr) = extract_subquery_exprs(filter.expr.clone(), &self.id_generator)?;
                    let mut cur_input = filter.input.as_ref().clone();

                    // iterate through all subqueries in predicate, turning each into a left join
                    for (subquery, subquery_alias) in subqueries {
                        let (correlated_exprs, new_subquery_plan) =
                            find_correlated_exprs(subquery.subquery.as_ref().clone())?;

                        let new_subquery_plan = LogicalPlanBuilder::from(new_subquery_plan)
                            .alias(&subquery_alias)?
                            .build();

                        cur_input = match filter.input.as_ref() {
                            LogicalPlan::EmptyRelation(_) => new_subquery_plan,
                            _ => {
                                let mut all_correlated_cols = BTreeSet::new();
                                correlated_exprs
                                    .correlated_subquery_cols_map
                                    .values()
                                    .for_each(|cols| all_correlated_cols.extend(cols.clone()));

                                let join_filter = correlated_exprs
                                    .join_filters
                                    .into_iter()
                                    .reduce(LogicalExpr::and)
                                    .map_or(Ok(Some(LogicalExpr::Literal(true.into()))), |expr| {
                                        expr.transform(|expr| match expr {
                                            LogicalExpr::Column(col) if all_correlated_cols.contains(&col) => {
                                                Ok(Transformed::yes(LogicalExpr::Column(
                                                    col.with_relation(subquery_alias.clone()),
                                                )))
                                            }
                                            _ => Ok(Transformed::no(expr)),
                                        })
                                        .data()
                                        .map(Some)
                                    })?;

                                LogicalPlanBuilder::from(cur_input)
                                    .join_on(new_subquery_plan, JoinType::Left, join_filter)?
                                    .build()
                            }
                        };
                    }

                    Ok(Transformed::yes(LogicalPlanBuilder::filter(cur_input, rewritten_expr)?))
                }
                LogicalPlan::SubqueryAlias(subquery_alias) => self
                    .rewrite(Arc::unwrap_or_clone(subquery_alias.input))
                    .and_then(|new_plan| {
                        LogicalPlanBuilder::from(new_plan)
                            .alias(&subquery_alias.alias.to_qualified_name())
                            .map(LogicalPlanBuilder::build)
                    })
                    .map(Transformed::yes),
                _ => Ok(Transformed::no(plan)),
            })
            .data()?;

        new_plan
            .map_children(|child_plan| self.rewrite(child_plan).map(Transformed::yes))
            .data()
    }
}

#[derive(Default, Debug, Clone)]
struct CorrelatedExprs {
    join_filters: Vec<LogicalExpr>,
    /// mapping from the plan to its holding correlated columns
    correlated_subquery_cols_map: HashMap<LogicalPlan, BTreeSet<Column>>,
}

fn find_correlated_exprs(subquery_plan: LogicalPlan) -> Result<(CorrelatedExprs, LogicalPlan)> {
    let mut correlated_exprs = CorrelatedExprs::default();

    let new_plan = subquery_plan
        .transform_up(|plan| {
            let plan_schema = plan.table_schema();

            match &plan {
                LogicalPlan::Filter(filter) => {
                    let predicate_exprs = split_conjunctive_predicates(filter.expr.clone());
                    let (join_filters, remaining_subquery_filters) = find_join_filters(&predicate_exprs);

                    let correlated_subquery_cols = collect_subquery_cols(&join_filters, &plan_schema)?;

                    for join_filter in join_filters {
                        if !correlated_exprs.join_filters.contains(&join_filter) {
                            correlated_exprs.join_filters.push(join_filter);
                        }
                    }

                    let new_plan = if let Some(subquery_filter) =
                        remaining_subquery_filters.into_iter().reduce(LogicalExpr::and)
                    {
                        LogicalPlanBuilder::filter(filter.input.as_ref().clone(), subquery_filter)?
                    } else {
                        filter.input.as_ref().clone()
                    };

                    correlated_exprs
                        .correlated_subquery_cols_map
                        .insert(new_plan.clone(), correlated_subquery_cols);

                    Ok(Transformed::yes(new_plan))
                }
                LogicalPlan::Aggregate(aggregate) if !correlated_exprs.join_filters.is_empty() => {
                    let mut local_correlated_cols = BTreeSet::new();

                    collect_local_correlated_cols(
                        &plan,
                        &correlated_exprs.correlated_subquery_cols_map,
                        &mut local_correlated_cols,
                    );

                    let missing_group_epxrs = collect_missing_exprs(&aggregate.group_expr, &local_correlated_cols);

                    // adding missing group by columns to the aggregate
                    LogicalPlanBuilder::from(aggregate.input.as_ref().clone())
                        .aggregate(missing_group_epxrs, aggregate.aggr_expr.clone())
                        .map(|builder| Transformed::yes(builder.build()))
                }
                LogicalPlan::Projection(projection) if !correlated_exprs.join_filters.is_empty() => {
                    let mut local_correlated_cols = BTreeSet::new();

                    collect_local_correlated_cols(
                        &plan,
                        &correlated_exprs.correlated_subquery_cols_map,
                        &mut local_correlated_cols,
                    );

                    // adding missing select expressions to the projection
                    let missing_select_exprs = collect_missing_exprs(&projection.exprs, &local_correlated_cols);

                    LogicalPlanBuilder::from(projection.input.as_ref().clone())
                        .add_project(missing_select_exprs)
                        .map(|builder| Transformed::yes(builder.build()))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })
        .data()?;

    Ok((correlated_exprs, new_plan))
}

fn collect_missing_exprs(group_exprs: &[LogicalExpr], local_correlated_cols: &BTreeSet<Column>) -> Vec<LogicalExpr> {
    let mut missing_exprs = vec![];

    for group_expr in group_exprs {
        if !missing_exprs.contains(group_expr) {
            missing_exprs.push(group_expr.clone());
        }
    }

    for col in local_correlated_cols {
        let col_expr = LogicalExpr::Column(col.clone());
        if !missing_exprs.contains(&col_expr) {
            missing_exprs.push(col_expr);
        }
    }

    missing_exprs
}

fn collect_local_correlated_cols(
    plan: &LogicalPlan,
    all_cols_map: &HashMap<LogicalPlan, BTreeSet<Column>>,
    local_cols: &mut BTreeSet<Column>,
) {
    for child in plan.children().unwrap_or_default() {
        if let Some(cols) = all_cols_map.get(child) {
            local_cols.extend(cols.clone());
        }
        // SubqueryAlias is treated as the leaf node
        if !matches!(child, LogicalPlan::SubqueryAlias(_)) {
            collect_local_correlated_cols(child, all_cols_map, local_cols);
        }
    }
}

fn collect_subquery_cols(exprs: &[LogicalExpr], schema: &TableSchemaRef) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.column_refs().into_iter() {
            if schema.has_field(col.relation.as_ref(), &col.name) {
                using_cols.push(col.clone());
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

fn contains_scalar_subquery(expr: &LogicalExpr) -> bool {
    let mut contains = false;
    expr.apply(|expr| {
        if let LogicalExpr::SubQuery(_) = expr {
            contains = true;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("[SHOULD NOT HAPPEN] contains scalar subquery");

    contains
}

fn find_join_filters(filter_exprs: &[LogicalExpr]) -> (Vec<LogicalExpr>, Vec<LogicalExpr>) {
    let mut join_filters = vec![];
    let mut remaining_subquery_filters = vec![];
    for filter_expr in filter_exprs {
        if filter_expr.contains_outer_ref_columns()
            && !matches!(filter_expr, LogicalExpr::BinaryExpr(BinaryExpr{ left,op:Operator::Eq,right}) if left == right)
        {
            join_filters.push(filter_expr.clone());
        } else {
            remaining_subquery_filters.push(filter_expr.clone());
        }
    }

    (join_filters, remaining_subquery_filters)
}

fn extract_subquery_exprs(
    expr: LogicalExpr,
    alias_generator: &AliasGenerator,
) -> Result<(Vec<(SubQuery, String)>, LogicalExpr)> {
    let mut subqueries = vec![];

    let rewritten_expr = expr
        .transform_down(|expr| {
            match expr {
                LogicalExpr::SubQuery(subquery) => {
                    let subquery_alias = alias_generator.next(SCALAR_SUBQUERY_ALIAS_PREFIX);
                    // this is the name of the column that will be used to reference the subquery
                    // e.g. SELECT a FROM t1 WHERE t1.a = (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a)
                    // will be rewritten as SELECT a FROM t1 WHERE t1.a = __scalar_sq_0.MIN(b)
                    let scalar_expr = subquery
                        .subquery
                        .head_output_expr()?
                        .map_or(internal_err!("subquery has no output,but it should"), Ok)?;

                    subqueries.push((subquery, subquery_alias.clone()));

                    match scalar_expr {
                        LogicalExpr::Alias(Alias { name, .. }) => Ok(Transformed::yes(LogicalExpr::Column(
                            Column::new(name, subquery_alias.into(), false),
                        ))),
                        LogicalExpr::Column(col) => {
                            Ok(Transformed::yes(LogicalExpr::Column(col.with_relation(subquery_alias))))
                        }
                        _ => todo!(),
                    }
                }
                _ => Ok(Transformed::no(expr)),
            }
        })
        .data()?;

    Ok((subqueries, rewritten_expr))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::assert_after_optimizer;

    #[test]
    fn test_contains_scalar_subquery() {
        assert_after_optimizer(
            "SELECT customer.c_custkey FROM customer WHERE customer.c_custkey < (SELECT MAX(orders.o_custkey) FROM orders)",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Projection: (customer.c_custkey)",
                "  Filter: customer.c_custkey < __scalar_sq_0.MAX(orders.o_custkey)",
                "    Left Join: Filter: Boolean(true)",
                "      TableScan: customer",
                "      SubqueryAlias: __scalar_sq_0",
                "        Projection: (MAX(orders.o_custkey))",
                "          Aggregate: group_expr=[], aggregat_expr=[MAX(orders.o_custkey)]",
                "            TableScan: orders",
            ],
        );
    }

    #[test]
    fn test_scalar_subquery_with_filter() {
        assert_after_optimizer(
            "SELECT customer.c_custkey FROM customer WHERE 1 < (SELECT max(orders.o_custkey) FROM orders WHERE orders.o_custkey = customer.c_custkey)",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Projection: (customer.c_custkey)",
                "  Filter: Int64(1) < __scalar_sq_0.MAX(orders.o_custkey)",
                "    Left Join: Filter: __scalar_sq_0.o_custkey = customer.c_custkey",
                "      TableScan: customer",
                "      SubqueryAlias: __scalar_sq_0",
                "        Projection: (MAX(orders.o_custkey), orders.o_custkey)",
                "          Aggregate: group_expr=[orders.o_custkey], aggregat_expr=[MAX(orders.o_custkey)]",
                "            TableScan: orders",
            ]
        );
    }

    #[test]
    fn test_multiple_subqueries() {
        assert_after_optimizer(
            "SELECT customer.c_custkey FROM customer WHERE 1 < (SELECT MAX(orders.o_custkey) FROM orders) AND 1 < (SELECT MIN(orders.o_custkey) FROM orders)",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Projection: (customer.c_custkey)",
                "  Filter: Int64(1) < __scalar_sq_0.MAX(orders.o_custkey) AND Int64(1) < __scalar_sq_1.MIN(orders.o_custkey)",
                "    Left Join: Filter: Boolean(true)",
                "      Left Join: Filter: Boolean(true)",
                "        TableScan: customer",
                "        SubqueryAlias: __scalar_sq_0",
                "          Projection: (MAX(orders.o_custkey))",
                "            Aggregate: group_expr=[], aggregat_expr=[MAX(orders.o_custkey)]",
                "              TableScan: orders",
                "      SubqueryAlias: __scalar_sq_1",
                "        Projection: (MIN(orders.o_custkey))",
                "          Aggregate: group_expr=[], aggregat_expr=[MIN(orders.o_custkey)]",
                "            TableScan: orders",
            ]
        );
    }

    #[test]
    fn test_recursive_subqueries() {
        assert_after_optimizer(
            "SELECT customer.c_custkey FROM customer WHERE customer.c_acctbal < (SELECT SUM(orders.o_totalprice) FROM orders WHERE orders.o_custkey = customer.c_custkey AND orders.o_totalprice < (SELECT SUM(lineitem.l_extendedprice) FROM lineitem WHERE lineitem.l_orderkey = orders.o_orderkey))",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Projection: (customer.c_custkey)",
                "  Filter: customer.c_acctbal < __scalar_sq_0.SUM(orders.o_totalprice)",
                "    Left Join: Filter: __scalar_sq_0.o_custkey = customer.c_custkey",
                "      TableScan: customer",
                "      SubqueryAlias: __scalar_sq_0",
                "        Projection: (SUM(orders.o_totalprice), orders.o_custkey)",
                "          Aggregate: group_expr=[orders.o_custkey], aggregat_expr=[SUM(orders.o_totalprice)]",
                "            Filter: orders.o_totalprice < __scalar_sq_1.SUM(lineitem.l_extendedprice)",
                "              Left Join: Filter: __scalar_sq_1.l_orderkey = orders.o_orderkey",
                "                TableScan: orders",
                "                SubqueryAlias: __scalar_sq_1",
                "                  Projection: (SUM(lineitem.l_extendedprice), lineitem.l_orderkey)",
                "                    Aggregate: group_expr=[lineitem.l_orderkey], aggregat_expr=[SUM(lineitem.l_extendedprice)]",
                "                      TableScan: lineitem",
            ]
        );
    }

    #[test]
    fn test_with_subquery_filters() {
        assert_after_optimizer(
            "SELECT customer.c_custkey FROM customer WHERE customer.c_custkey = (SELECT MAX(orders.o_custkey) FROM orders WHERE orders.o_custkey = customer.c_custkey AND orders.o_orderkey = 1)",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Projection: (customer.c_custkey)",
                "  Filter: customer.c_custkey = __scalar_sq_0.MAX(orders.o_custkey)",
                "    Left Join: Filter: __scalar_sq_0.o_custkey = customer.c_custkey",
                "      TableScan: customer",
                "      SubqueryAlias: __scalar_sq_0",
                "        Projection: (MAX(orders.o_custkey), orders.o_custkey)",
                "          Aggregate: group_expr=[orders.o_custkey], aggregat_expr=[MAX(orders.o_custkey)]",
                "            Filter: orders.o_orderkey = Int64(1)",
                "              TableScan: orders",
            ]
        );
    }

    #[test]
    fn test_tpch_q3() {
        assert_after_optimizer(
            "SELECT
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
    FROM
        part,
        supplier,
        partsupp,
        nation,
        region
    WHERE
            p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND p_size = 15
      AND p_type like '%BRASS'
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'EUROPE'
      AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
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
    )
    ORDER BY
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
    LIMIT 10;",
            Box::new(ScalarSubqueryToJoin::default()),
            vec![
                "Limit: fetch=10, skip=0",
                "  Sort: supplier.s_acctbal DESC, nation.n_name ASC, supplier.s_name ASC, part.p_partkey ASC",
                "    Projection: (supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment)",
                "      Filter: part.p_partkey = partsupp.ps_partkey AND supplier.s_suppkey = partsupp.ps_suppkey AND part.p_size = Int64(15) AND part.p_type LIKE Utf8('%BRASS') AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = Utf8('EUROPE') AND partsupp.ps_supplycost = __scalar_sq_0.MIN(partsupp.ps_supplycost)",
                "        Left Join: Filter: part.p_partkey = __scalar_sq_0.ps_partkey",
                "          CrossJoin",
                "            CrossJoin",
                "              CrossJoin",
                "                CrossJoin",
                "                  TableScan: part",
                "                  TableScan: supplier",
                "                TableScan: partsupp",
                "              TableScan: nation",
                "            TableScan: region",
                "          SubqueryAlias: __scalar_sq_0",
                "            Projection: (MIN(partsupp.ps_supplycost), partsupp.ps_partkey)",
                "              Aggregate: group_expr=[partsupp.ps_partkey], aggregat_expr=[MIN(partsupp.ps_supplycost)]",
                "                Filter: supplier.s_suppkey = partsupp.ps_suppkey AND supplier.s_nationkey = nation.n_nationkey AND nation.n_regionkey = region.r_regionkey AND region.r_name = Utf8('EUROPE')",
                "                  CrossJoin",
                "                    CrossJoin",
                "                      CrossJoin",
                "                        TableScan: partsupp",
                "                        TableScan: supplier",
                "                      TableScan: nation",
                "                    TableScan: region",
            ],
        );
    }
}
