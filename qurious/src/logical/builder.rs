use arrow::datatypes::Schema;
use std::sync::Arc;

use super::{
    expr::{LogicalExpr, SortExpr},
    plan::{Aggregate, EmptyRelation, Filter, Join, Limit, LogicalPlan, Projection, Sort, TableScan},
};
use crate::{common::join_type::JoinType, provider::table::TableProvider};
use crate::{common::table_relation::TableRelation, error::Result};

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder { plan }
    }

    pub fn project(input: LogicalPlan, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<LogicalPlan> {
        Projection::try_new(input, exprs.into_iter().map(|exp| exp.into()).collect()).map(LogicalPlan::Projection)
    }

    pub fn filter(input: LogicalPlan, predicate: LogicalExpr) -> Result<LogicalPlan> {
        Filter::try_new(input, predicate).map(LogicalPlan::Filter)
    }

    pub fn having(self, predicate: LogicalExpr) -> Result<Self> {
        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Filter(Filter::try_new(self.plan, predicate)?.into()),
        })
    }

    pub fn add_project(self, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<Self> {
        Projection::try_new(self.plan, exprs.into_iter().map(|exp| exp.into()).collect())
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Projection(s)))
    }

    pub fn empty(produce_one_row: bool) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation {
                schema: Arc::new(Schema::empty()),
                produce_one_row,
            }),
        }
    }

    pub fn scan(
        relation: impl Into<TableRelation>,
        table_source: Arc<dyn TableProvider>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        TableScan::try_new(relation.into(), table_source, None, filter)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::TableScan(s)))
    }

    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let left_fields = self.plan.schema().fields.clone();
        let right_fields = right.schema().fields.clone();

        // left then right
        let schema = Schema::new(
            left_fields
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                join_type: JoinType::Inner,
                filter: None,
                schema: Arc::new(schema),
            }),
        })
    }

    pub fn join_on(self, right: LogicalPlan, join_type: JoinType, on: LogicalExpr) -> Result<Self> {
        let left_fields = self.plan.schema().fields.clone();
        let right_fields = right.schema().fields.clone();

        // left then right
        let schema = Schema::new(
            left_fields
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                join_type,
                filter: Some(on),
                schema: Arc::new(schema),
            }),
        })
    }

    pub fn aggregate(self, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<LogicalExpr>) -> Result<Self> {
        Aggregate::try_new(self.plan, group_expr, aggr_expr)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Aggregate(s)))
    }

    pub fn sort(self, order_by: Vec<SortExpr>) -> Result<Self> {
        // TODO
        // we need check if the column is ambiguous and columns is present in the schema
        // if not present in the schema we should add missing columns to the schema and project them
        // let missing_columns = order_by
        //     .iter()
        //     .flat_map(|sort|sort.expr.column_refs())
        //     .filter(|col| !self.plan.has_column(col))
        //     .collect::<Vec<_>>();

        // if !missing_columns.is_empty() {
        //     todo!("Add missing columns to the schema and project them");
        // }

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Sort(Sort {
                exprs: order_by,
                input: Box::new(self.plan),
            }),
        })
    }

    pub fn limit(self, fetch: Option<usize>, skip: usize) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::Limit(Limit {
                input: Box::new(self.plan),
                fetch,
                skip,
            }),
        }
    }
}
