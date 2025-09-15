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

#[derive(Debug, Default, Clone)]
pub struct PushdownFilter;

impl OptimizerRule for PushdownFilter {
    fn name(&self) -> &str {
        "pushdown_filter"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => self.push_filter(filter),
            LogicalPlan::Join(join) => self.push_join(join),
            _ => Ok(Transformed::no(plan)),
        }
    }
}

impl PushdownFilter {
    fn push_filter(&self, filter: Filter) -> Result<Transformed<LogicalPlan>> {
        match *filter.input {
            LogicalPlan::TableScan(mut scan) => {
                if let Some(table_scan_filter) = scan.filter {
                    scan.filter = Some(table_scan_filter.and(filter.expr));
                } else {
                    scan.filter = Some(filter.expr);
                }

                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            _ => Ok(Transformed::no(LogicalPlan::Filter(filter))),
        }
    }

    fn push_join(&self, plan: Join) -> Result<Transformed<LogicalPlan>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{optimizer::rule::pushdown_filter_join::PushdownFilter, test_utils::assert_after_optimizer};

    #[test]
    fn test_filter_before_projection() {
        assert_after_optimizer(
            "SELECT id,name FROM users WHERE id = 1",
            Box::new(PushdownFilter),
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
            Box::new(PushdownFilter),
            vec![
                "Limit: fetch=10, skip=0",
                "  Projection: (users.id, users.name)",
                "    TableScan: users, full_filter=[users.id = Int64(1)]",
            ],
        );
    }
}
