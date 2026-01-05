use std::collections::{BTreeSet, HashMap};

use crate::common::join_type::JoinType;
use crate::common::table_schema::TableSchemaRef;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion};
use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, Column, Exists, LogicalExpr};
use crate::logical::plan::{Filter, LogicalPlan};
use crate::logical::LogicalPlanBuilder;
use crate::optimizer::rule::rule_optimizer::OptimizerRule;
use crate::utils::alias::AliasGenerator;
use crate::utils::expr::{check_all_columns_from_schema, replace_cols_by_name, split_conjunctive_predicates};

const PREDICATE_SUBQUERY_ALIAS_PREFIX: &str = "__predicate_sq";

/// Decorrelate predicate subqueries (IN/EXISTS) to left semi/anti joins
///
/// This rule rewrites predicate subqueries into semi/anti joins:
/// - `EXISTS (subquery)` -> `LeftSemi` join
/// - `NOT EXISTS (subquery)` -> `LeftAnti` join
/// - `col IN (subquery)` -> `LeftSemi` join (if subquery returns single column)
/// - `col NOT IN (subquery)` -> `LeftAnti` join (if subquery returns single column)
pub struct DecorrelatePredicateSubquery {
    id_generator: AliasGenerator,
}

impl Default for DecorrelatePredicateSubquery {
    fn default() -> Self {
        Self {
            id_generator: AliasGenerator::default(),
        }
    }
}

impl OptimizerRule for DecorrelatePredicateSubquery {
    fn name(&self) -> &str {
        "decorrelate_predicate_subquery"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(Filter { input, expr: predicate }) = plan else {
            return Ok(Transformed::no(plan));
        };

        if !contains_predicate_subquery(&predicate) {
            return Ok(Transformed::no(LogicalPlan::Filter(Filter { input, expr: predicate })));
        }

        let (subqueries, rewritten_expr) = extract_predicate_subqueries(predicate, &self.id_generator)?;
        let rewritten_expr = remove_true_conjuncts(rewritten_expr);

        if subqueries.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Filter(Filter {
                input,
                expr: rewritten_expr,
            })));
        }

        let mut cur_input = input.as_ref().clone();

        // Iterate through all predicate subqueries, turning each into a semi/anti join
        for (subquery_info, subquery_alias) in subqueries {
            let (correlated_exprs, new_subquery_plan) = find_correlated_exprs(subquery_info.subquery.as_ref().clone())?;

            // IMPORTANT:
            // - `subquery_schema` is the schema BEFORE applying `SubqueryAlias`, so it has original qualifiers
            //   (e.g. `orders.o_custkey`). We must use it to detect which side of a predicate belongs to the subquery.
            // - after aliasing, the right-side schema qualifiers become the alias (e.g. `__predicate_sq_0.o_custkey`).
            let subquery_schema = new_subquery_plan.table_schema();
            let new_subquery_plan = LogicalPlanBuilder::from(new_subquery_plan)
                .alias(&subquery_alias)?
                .build();

            let join_type = if subquery_info.negated {
                JoinType::LeftAnti
            } else {
                JoinType::LeftSemi
            };

            // Build join conditions from correlated expressions
            let mut join_on = vec![];
            let mut join_filter_parts = vec![];
            let right_schema = subquery_schema;
            let subquery_alias_replace_map = build_subquery_alias_replace_map(&right_schema, &subquery_alias);

            for join_filter_expr in &correlated_exprs.join_filters {
                // Try to extract equijoin conditions
                if let LogicalExpr::BinaryExpr(BinaryExpr {
                    left,
                    op: crate::datatypes::operator::Operator::Eq,
                    right,
                }) = join_filter_expr
                {
                    // Check if this is an equijoin condition
                    let left_cols = left.using_columns();
                    let right_cols = right.using_columns();

                    if !left_cols.is_empty() && !right_cols.is_empty() {
                        let left_schema = cur_input.table_schema();

                        // Case 1: left is outer, right is subquery
                        let all_left_from_outer = check_all_columns_from_schema(&left_cols, &left_schema);
                        let all_right_from_subquery = check_all_columns_from_schema(&right_cols, &right_schema);
                        if all_left_from_outer && all_right_from_subquery {
                            let subquery_expr =
                                replace_cols_by_name(right.as_ref().clone(), &subquery_alias_replace_map)?;
                            join_on.push((*left.clone(), subquery_expr));
                            continue;
                        }

                        // Case 2: left is subquery, right is outer (swapped)
                        let all_left_from_subquery = check_all_columns_from_schema(&left_cols, &right_schema);
                        let all_right_from_outer = check_all_columns_from_schema(&right_cols, &left_schema);
                        if all_left_from_subquery && all_right_from_outer {
                            let subquery_expr =
                                replace_cols_by_name(left.as_ref().clone(), &subquery_alias_replace_map)?;
                            join_on.push((*right.clone(), subquery_expr));
                            continue;
                        }
                    }
                }

                // Non-equijoin conditions go to filter
                let transformed_filter = replace_cols_by_name(join_filter_expr.clone(), &subquery_alias_replace_map)?;

                join_filter_parts.push(transformed_filter);
            }

            let join_filter = join_filter_parts.into_iter().reduce(LogicalExpr::and).or_else(|| {
                if join_on.is_empty() {
                    Some(LogicalExpr::Literal(true.into()))
                } else {
                    None
                }
            });

            cur_input = LogicalPlanBuilder::from(cur_input)
                .join(new_subquery_plan, join_type, join_on, join_filter)?
                .build();
        }

        // Apply remaining filter if any
        if rewritten_expr != LogicalExpr::Literal(true.into()) {
            LogicalPlanBuilder::filter(cur_input, rewritten_expr).map(Transformed::yes)
        } else {
            Ok(Transformed::yes(cur_input))
        }
    }
}

#[derive(Debug, Clone)]
struct PredicateSubqueryInfo {
    subquery: Box<LogicalPlan>,
    negated: bool,
}

fn is_true_literal(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::Literal(ScalarValue::Boolean(Some(true))))
}

fn remove_true_conjuncts(expr: LogicalExpr) -> LogicalExpr {
    let mut parts = split_conjunctive_predicates(expr);
    parts.retain(|e| !is_true_literal(e));
    parts
        .into_iter()
        .reduce(LogicalExpr::and)
        .unwrap_or_else(|| LogicalExpr::Literal(true.into()))
}

fn build_subquery_alias_replace_map(subquery_schema: &TableSchemaRef, alias: &str) -> HashMap<String, LogicalExpr> {
    subquery_schema
        .columns()
        .into_iter()
        .map(|c| {
            let key = c.qualified_name();
            let value = LogicalExpr::Column(c.with_relation(alias.to_string()));
            (key, value)
        })
        .collect()
}

fn contains_predicate_subquery(expr: &LogicalExpr) -> bool {
    let mut contains = false;
    expr.apply(|expr| {
        match expr {
            LogicalExpr::Exists(_) => {
                contains = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            LogicalExpr::SubQuery(_) => {
                // Check if this is used in an IN context
                // For now, we'll handle EXISTS explicitly
                // IN subqueries might be represented differently
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("[SHOULD NOT HAPPEN] contains predicate subquery");

    contains
}

fn extract_predicate_subqueries(
    expr: LogicalExpr,
    alias_generator: &AliasGenerator,
) -> Result<(Vec<(PredicateSubqueryInfo, String)>, LogicalExpr)> {
    let mut subqueries = vec![];

    let rewritten_expr = expr
        .transform_down(|expr| {
            match expr {
                LogicalExpr::Exists(Exists { subquery, negated }) => {
                    let subquery_alias = alias_generator.next(PREDICATE_SUBQUERY_ALIAS_PREFIX);
                    subqueries.push((
                        PredicateSubqueryInfo {
                            subquery: subquery.clone(),
                            negated,
                        },
                        subquery_alias.clone(),
                    ));
                    // EXISTS is replaced with true literal, the actual filtering is done by the join
                    Ok(Transformed::yes(LogicalExpr::Literal(true.into())))
                }
                _ => Ok(Transformed::no(expr)),
            }
        })
        .data()?;

    Ok((subqueries, rewritten_expr))
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

                    for join_filter in &join_filters {
                        if !correlated_exprs.join_filters.contains(join_filter) {
                            correlated_exprs.join_filters.push(join_filter.clone());
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

                    let missing_group_exprs = collect_missing_exprs(&aggregate.group_expr, &local_correlated_cols);

                    LogicalPlanBuilder::from(aggregate.input.as_ref().clone())
                        .aggregate(missing_group_exprs, aggregate.aggr_expr.clone())
                        .map(|builder| Transformed::yes(builder.build()))
                }
                LogicalPlan::Projection(projection) if !correlated_exprs.join_filters.is_empty() => {
                    let mut local_correlated_cols = BTreeSet::new();

                    collect_local_correlated_cols(
                        &plan,
                        &correlated_exprs.correlated_subquery_cols_map,
                        &mut local_correlated_cols,
                    );

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

fn find_join_filters(filter_exprs: &[LogicalExpr]) -> (Vec<LogicalExpr>, Vec<LogicalExpr>) {
    let mut join_filters = vec![];
    let mut remaining_subquery_filters = vec![];
    for filter_expr in filter_exprs {
        if filter_expr.contains_outer_ref_columns()
            && !matches!(
                filter_expr,
                LogicalExpr::BinaryExpr(BinaryExpr {
                    left,
                    op: crate::datatypes::operator::Operator::Eq,
                    right
                }) if left == right
            )
        {
            join_filters.push(filter_expr.clone());
        } else {
            remaining_subquery_filters.push(filter_expr.clone());
        }
    }

    (join_filters, remaining_subquery_filters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::assert_after_optimizer;

    #[test]
    fn test_exists_subquery() {
        assert_after_optimizer(
            "SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM orders WHERE orders.o_custkey = customer.c_custkey)",
            vec![Box::new(DecorrelatePredicateSubquery::default())],
            vec![
                "Projection: (customer.c_custkey, customer.c_name, customer.c_address, customer.c_nationkey, customer.c_phone, customer.c_acctbal, customer.c_mktsegment, customer.c_comment, customer.c_rev)",
                "  Left Semi Join: On: (customer.c_custkey, __predicate_sq_0.o_custkey)",
                "    TableScan: customer",
                "    SubqueryAlias: __predicate_sq_0",
                "      Projection: (Int64(1), orders.o_custkey)",
                "        TableScan: orders",
            ],
        );
    }

    #[test]
    fn test_not_exists_subquery() {
        assert_after_optimizer(
            "SELECT * FROM customer WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.o_custkey = customer.c_custkey)",
            vec![Box::new(DecorrelatePredicateSubquery::default())],
            vec![
                "Projection: (customer.c_custkey, customer.c_name, customer.c_address, customer.c_nationkey, customer.c_phone, customer.c_acctbal, customer.c_mktsegment, customer.c_comment, customer.c_rev)",
                "  Left Anti Join: On: (customer.c_custkey, __predicate_sq_0.o_custkey)",
                "    TableScan: customer",
                "    SubqueryAlias: __predicate_sq_0",
                "      Projection: (Int64(1), orders.o_custkey)",
                "        TableScan: orders",
            ],
        );
    }

    #[test]
    fn test_exists_with_additional_filter() {
        assert_after_optimizer(
            "SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM orders WHERE orders.o_custkey = customer.c_custkey AND orders.o_totalprice > 1000) AND customer.c_acctbal > 0",
            vec![Box::new(DecorrelatePredicateSubquery::default())],
            vec![
                "Projection: (customer.c_custkey, customer.c_name, customer.c_address, customer.c_nationkey, customer.c_phone, customer.c_acctbal, customer.c_mktsegment, customer.c_comment, customer.c_rev)",
                "  Filter: customer.c_acctbal > Int64(0)",
                "    Left Semi Join: On: (customer.c_custkey, __predicate_sq_0.o_custkey)",
                "      TableScan: customer",
                "      SubqueryAlias: __predicate_sq_0",
                "        Projection: (Int64(1), orders.o_custkey)",
                "          Filter: orders.o_totalprice > Int64(1000)",
                "            TableScan: orders",
            ],
        );
    }

    #[test]
    fn test_exists_swapped_eq_sides_still_becomes_join_on() {
        // correlated predicate is written as: outer = subquery
        assert_after_optimizer(
            "SELECT * FROM customer WHERE EXISTS (SELECT 1 FROM orders WHERE customer.c_custkey = orders.o_custkey)",
            vec![Box::new(DecorrelatePredicateSubquery::default())],
            vec![
                "Projection: (customer.c_custkey, customer.c_name, customer.c_address, customer.c_nationkey, customer.c_phone, customer.c_acctbal, customer.c_mktsegment, customer.c_comment, customer.c_rev)",
                "  Left Semi Join: On: (customer.c_custkey, __predicate_sq_0.o_custkey)",
                "    TableScan: customer",
                "    SubqueryAlias: __predicate_sq_0",
                "      Projection: (Int64(1), orders.o_custkey)",
                "        TableScan: orders",
            ],
        );
    }
}
