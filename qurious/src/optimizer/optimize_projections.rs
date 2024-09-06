use std::collections::HashSet;

use arrow::datatypes::Schema;

use super::OptimizerRule;
use crate::error::{Result,Error};
use crate::internal_err;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::{Aggregate, LogicalPlan, TableScan};

#[derive(Default)]
pub struct OptimizeProjections {}

impl OptimizerRule for OptimizeProjections {
    fn name(&self) -> &str {
        "optimize_projections"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        optimize_projections(plan, vec![])
    }
}

fn optimize_projections(plan: &LogicalPlan, necessary_projection: Vec<String>) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(projection) => {
            // if inner LogicalPlan has the same schema as the projection, then the projection is unnecessary
            // nomatter the projection columns order
            if is_projection_unnecessary(&projection.input, &projection.exprs)? {
                let LogicalPlan::Projection(_) = projection.input.as_ref() else {
                    return optimize_projections(&projection.input, necessary_projection);
                };

                Ok(Some(projection.input.as_ref().clone()))
            } else {
                Ok(Some(plan.clone()))
            }
        }
        LogicalPlan::TableScan(TableScan {
            relation,
            source,
            projected_schema,
            filter,
            ..
        }) => Ok(Some(LogicalPlan::TableScan(TableScan {
            relation: relation.clone(),
            source: source.clone(),
            projections: Some(necessary_projection),
            projected_schema: projected_schema.clone(),
            filter: filter.clone(),
        }))),
        // re-write the projection for the aggregate add projection to input of the aggregate
        // so that the input only has the columns that are required for the aggregate
        LogicalPlan::Aggregate(agg) => {
            let input_schema = agg.input.schema();
            let group_cols = agg.group_expr.clone().into_iter().map(|group_by| group_by.as_column());
            let agg_aggr_cols = agg.aggr_expr.clone().into_iter().map(|aggr| aggr.expr.as_column());
            let mut projections = HashSet::new();

            for col in group_cols.chain(agg_aggr_cols) {
                let LogicalExpr::Column(col) = col? else {
                    return internal_err!("Aggregate expression should be a column");
                };
                let _index = input_schema.index_of(&col.name)?;
                projections.insert(col.name);
            }

            optimize_projections(&agg.input, projections.into_iter().collect())
                .map(Box::new)
                .map(|input| {
                    input.map(|input| {
                        LogicalPlan::Aggregate(Aggregate {
                            input: Box::new(input),
                            group_expr: agg.group_expr.clone(),
                            aggr_expr: agg.aggr_expr.clone(),
                            schema: agg.schema.clone(),
                        })
                    })
                })
        }
        _ => Ok(None),
    }
}

/// Check if the projection is unnecessary
/// A projection is unnecessary if all the expressions are trivial and the input schema is the same as the output schema
///
/// # Parameters
/// * `plan` - The input plan reference of the projection
/// * `exprs` - The expressions of the projection
///
/// # Returns
/// * `bool` - True if the projection is unnecessary, false otherwise
fn is_projection_unnecessary(plan: &LogicalPlan, exprs: &Vec<LogicalExpr>) -> Result<bool> {
    let input_schema = plan.schema();
    let output_schema = exprs
        .iter()
        .map(|e| e.field(plan))
        .collect::<Result<Vec<_>>>()
        .map(|fields| Schema::new(fields))?;

    // Extract and sort fields from both schemas
    let mut input_fields = input_schema.fields().to_vec();
    let mut output_fields = output_schema.fields().to_vec();

    input_fields.sort_by(|a, b| a.name().cmp(b.name()));
    output_fields.sort_by(|a, b| a.name().cmp(b.name()));

    Ok(exprs.iter().all(is_expr_trivial) && input_fields == output_fields)
}

fn is_expr_trivial(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Column(_) => true,
        LogicalExpr::Literal(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::build_schema;
    use crate::error::Result;
    use crate::logical::expr::{AggregateExpr, AggregateOperator, Column, LogicalExpr};
    use crate::optimizer::optimize_projections::OptimizeProjections;
    use crate::optimizer::OptimizerRule;
    use crate::{
        datasource::memory::MemoryTable,
        logical::{
            expr::column,
            plan::{LogicalPlan, TableScan},
            LogicalPlanBuilder,
        },
        utils,
    };
    use std::sync::Arc;

    fn test_table_scan() -> LogicalPlan {
        let relation = "test";
        let schema = build_schema!(("a", DataType::Int32), ("b", DataType::Int32), ("c", DataType::Int32));
        let table_source = Arc::new(MemoryTable::try_new(Arc::new(schema), vec![]).unwrap());
        let filter = None;

        LogicalPlan::TableScan(TableScan::try_new(relation, table_source, None, filter).unwrap())
    }

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        let optimizer = OptimizeProjections::default();
        let plan = optimizer.optimize(&plan)?.unwrap();

        assert_eq!(utils::format(&plan, 0), expected);

        Ok(())
    }

    #[test]
    fn redundant_projection() -> Result<()> {
        let projection = LogicalPlanBuilder::from(test_table_scan())
            .add_project(vec![column("a"), column("b"), column("c")])?
            .add_project(vec![column("a"), column("c"), column("b")])?
            .build();

        assert_optimized_plan_equal(projection, "Projection: (a, b, c)\n  TableScan: test\n")
    }

    #[test]
    fn unnecessary_group_by() -> Result<()> {
        let projection = LogicalPlanBuilder::from(test_table_scan())
            .aggregate(vec![column("a"), column("a")], vec![])?
            .add_project(vec![column("a")])?
            .build();

        assert_optimized_plan_equal(
            projection,
            "Aggregate: group_expr=[a], aggregat_expr=[]\n  TableScan: test Projection: [a]\n",
        )
    }

    #[test]
    fn no_group_by() -> Result<()> {
        let projection = LogicalPlanBuilder::from(test_table_scan())
            .aggregate(
                vec![],
                vec![AggregateExpr {
                    op: AggregateOperator::Sum,
                    expr: Box::new(LogicalExpr::Column(Column::new("a", "test".into()))),
                }],
            )?
            .build();

        assert_optimized_plan_equal(
            projection,
            "Aggregate: group_expr=[], aggregat_expr=[SUM(test.a)]\n  TableScan: test Projection: [a]\n",
        )
    }
}
