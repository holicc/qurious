use crate::common::join_type::JoinType;
use crate::common::table_schema::qualified_name;
use crate::common::transformed::Transformed;
use crate::error::Result;
use crate::logical::expr::{Column, LogicalExpr};
use crate::logical::plan::{Filter, Join, LogicalPlan, SubqueryAlias};
use crate::logical::LogicalPlanBuilder;
use crate::optimizer::rule::rule_optimizer::OptimizerRule;
use crate::utils::expr::{conjunction, replace_col, replace_cols_by_name, split_conjunctive_predicates};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct PushdownFilter;

impl OptimizerRule for PushdownFilter {
    fn name(&self) -> &str {
        "pushdown_filter"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Join(join) = plan {
            return self.push_join(join, None);
        }

        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        match *filter.input {
            LogicalPlan::TableScan(mut scan) => {
                if let Some(table_scan_filter) = scan.filter {
                    scan.filter = Some(table_scan_filter.and(filter.expr));
                } else {
                    scan.filter = Some(filter.expr);
                }

                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            LogicalPlan::Join(join) => self.push_join(join, Some(&filter.expr)),
            LogicalPlan::CrossJoin(cross_join) => self.push_join(
                Join {
                    left: cross_join.left,
                    right: cross_join.right,
                    join_type: JoinType::Inner,
                    on: vec![],
                    filter: None,
                    schema: cross_join.schema,
                },
                Some(&filter.expr),
            ),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let input_schema = subquery_alias.input.table_schema();
                let mut replace_map = HashMap::new();

                for (i, (quanlifier, field)) in input_schema.iter().enumerate() {
                    let (sub_quanlifier, sub_field) = subquery_alias.schema.qualified_field(i);

                    replace_map.insert(
                        qualified_name(sub_quanlifier, sub_field.name()),
                        LogicalExpr::Column(Column::new(field.name(), quanlifier.cloned(), false)),
                    );
                }

                let new_expr = replace_cols_by_name(filter.expr, &replace_map)?;
                let new_input =
                    LogicalPlanBuilder::filter(Arc::unwrap_or_clone(subquery_alias.input), new_expr).map(Arc::new)?;

                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(SubqueryAlias {
                    input: new_input,
                    alias: subquery_alias.alias,
                    schema: subquery_alias.schema,
                })))
            }
            _ => Ok(Transformed::no(LogicalPlan::Filter(filter))),
        }
    }
}

impl PushdownFilter {
    fn push_join(&self, mut join: Join, parent_predicate: Option<&LogicalExpr>) -> Result<Transformed<LogicalPlan>> {
        let left_schema = join.left.table_schema();
        let right_schema = join.right.table_schema();
        let predicates =
            parent_predicate.map_or_else(Vec::new, |predicate| split_conjunctive_predicates(predicate.clone()));
        let join_keys = join
            .on
            .iter()
            .flat_map(|(l, r)| {
                let left_col = l.try_as_column()?;
                let right_col = r.try_as_column()?;

                Some((left_col, right_col))
            })
            .collect::<Vec<_>>();

        let (mut push_left, mut push_right, mut keep_predicates) = (Vec::new(), Vec::new(), Vec::new());

        for expr in predicates {
            let (mut ref_left, mut ref_right) = (false, false);

            for &col in &expr.column_refs() {
                if left_schema.has_column(col) {
                    ref_left = true;
                }
                if right_schema.has_column(col) {
                    ref_right = true;
                }

                for (l, r) in join_keys.iter() {
                    if col == *l {
                        push_right.push(replace_col(expr.clone(), col, r)?);
                        break;
                    }
                    if col == *r {
                        push_left.push(replace_col(expr.clone(), col, l)?);
                        break;
                    }
                }
            }

            match (ref_left, ref_right) {
                (true, true) => keep_predicates.push(expr),
                (true, false) => push_left.push(expr),
                (false, true) => push_right.push(expr),
                (false, false) => {
                    push_left.push(expr.clone());
                    push_right.push(expr);
                }
            }
        }

        if let Some(expr) = conjunction(push_left) {
            join.left = Filter::try_new(Arc::unwrap_or_clone(join.left), expr)
                .map(LogicalPlan::Filter)
                .map(Arc::new)?;
        }

        if let Some(expr) = conjunction(push_right) {
            join.right = Filter::try_new(Arc::unwrap_or_clone(join.right), expr)
                .map(LogicalPlan::Filter)
                .map(Arc::new)?;
        }

        let join = LogicalPlan::Join(join);

        if let Some(expr) = conjunction(keep_predicates) {
            return Ok(Transformed::yes(LogicalPlanBuilder::filter(join, expr)?));
        }

        Ok(Transformed::yes(join))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        optimizer::rule::{
            eliminate_cross_join::EliminateCrossJoin, pushdown_filter_join::PushdownFilter, ExtractEquijoinPredicate,
        },
        test_utils::assert_after_optimizer,
    };

    #[test]
    fn test_filter_before_projection() {
        assert_after_optimizer(
            "SELECT id,name FROM users WHERE id = 1",
            vec![Box::new(PushdownFilter)],
            vec![
                "Projection: (users.id, users.name)",
                "  TableScan: users, full_filter=[users.id = Int64(1)]",
            ],
        );
    }

    #[test]
    fn test_filter_with_limit() {
        assert_after_optimizer(
            "SELECT id,name FROM users WHERE id = 1 LIMIT 10",
            vec![Box::new(PushdownFilter)],
            vec![
                "Limit: fetch=10, skip=0",
                "  Projection: (users.id, users.name)",
                "    TableScan: users, full_filter=[users.id = Int64(1)]",
            ],
        );
    }

    #[test]
    fn test_filter_with_alias() {
        assert_after_optimizer(
            "SELECT id, name FROM users a WHERE a.id = 1",
            vec![Box::new(PushdownFilter)],
            vec![
                "Projection: (users.id, users.name)",
                "  SubqueryAlias: a",
                "    TableScan: users, full_filter=[users.id = Int64(1)]",
            ],
        );
    }

    #[test]
    fn test_filter_with_inner_join() {
        assert_after_optimizer(
            "SELECT a.id,a.name,b.name as b_name FROM users a INNER JOIN repos b ON a.id = b.owner_id WHERE a.id > 1",
            vec![Box::new(PushdownFilter)],
            vec![
                "Projection: (a.id, a.name, b.name AS b_name)",
                "  Inner Join: Filter: a.id = b.owner_id",
                "    SubqueryAlias: a",
                "      TableScan: users, full_filter=[users.id > Int64(1)]",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_filter_with_inner_join_01() {
        assert_after_optimizer(
            "SELECT a.id,a.name,b.name as b_name FROM users a INNER JOIN repos b ON a.id = b.owner_id WHERE a.id > 1",
            vec![Box::new(ExtractEquijoinPredicate), Box::new(PushdownFilter)],
            vec![
                "Projection: (a.id, a.name, b.name AS b_name)",
                "  Inner Join: On: (a.id, b.owner_id)",
                "    SubqueryAlias: a",
                "      TableScan: users, full_filter=[users.id > Int64(1)]",
                "    SubqueryAlias: b",
                "      TableScan: repos, full_filter=[repos.owner_id > Int64(1)]",
            ],
        );
    }

    #[test]
    fn test_cross_join() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id AND users.id = 10",
            vec![Box::new(PushdownFilter)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Filter: users.id = repos.owner_id",
                "    Inner Join:",
                "      TableScan: users, full_filter=[users.id = Int64(10)]",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_cross_join_01() {
        assert_after_optimizer(
            "SELECT * FROM users, repos WHERE users.id = repos.owner_id AND users.id = 10",
            vec![Box::new(EliminateCrossJoin), Box::new(PushdownFilter)],
            vec![
                "Projection: (users.email, repos.id, users.id, repos.name, users.name, repos.owner_id)",
                "  Inner Join: On: (users.id, repos.owner_id)",
                "    TableScan: users, full_filter=[users.id = Int64(10)]",
                "    TableScan: repos, full_filter=[repos.owner_id = Int64(10)]",
            ],
        );
    }
}
