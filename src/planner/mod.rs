pub mod sql;

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{
    common::TableRelation,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    logical::{
        expr::{AggregateOperator, BinaryExpr, Column, LogicalExpr},
        plan::{Aggregate, Filter, Join, LogicalPlan, Projection, TableScan},
    },
    physical::{
        self,
        expr::{AggregateExpr, PhysicalExpr},
        plan::PhysicalPlan,
    },
};

pub trait QueryPlanner: Debug {
    fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>>;

    fn create_physical_expr(&self, schema: &SchemaRef, expr: &LogicalExpr) -> Result<Arc<dyn PhysicalExpr>>;
}

#[derive(Debug)]
pub struct DefaultQueryPlanner;

impl DefaultQueryPlanner {
    fn physical_plan_projection(&self, projection: &Projection) -> Result<Arc<dyn PhysicalPlan>> {
        let physical_plan = self.create_physical_plan(&projection.input)?;
        let exprs = projection
            .exprs
            .iter()
            .map(|e| self.create_physical_expr(&projection.schema, e))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(physical::plan::Projection::new(
            projection.schema.clone(),
            physical_plan,
            exprs,
        )))
    }

    fn physical_plan_filter(&self, filter: &Filter) -> Result<Arc<dyn PhysicalPlan>> {
        Ok(Arc::new(physical::plan::Filter::new(
            self.create_physical_plan(&filter.input)?,
            self.create_physical_expr(&filter.schema(), &filter.expr)?,
        )))
    }

    fn physical_plan_aggregate(&self, aggregate: &Aggregate) -> Result<Arc<dyn PhysicalPlan>> {
        let input = self.create_physical_plan(&aggregate.input)?;
        let group_expr = aggregate
            .group_expr
            .iter()
            .map(|e| self.create_physical_expr(&aggregate.schema, e))
            .collect::<Result<Vec<_>>>()?;

        let aggr_expr = aggregate
            .aggr_expr
            .iter()
            .map(|e| {
                self.create_physical_expr(&aggregate.schema, &e.expr)
                    .map(|expr| match e.op {
                        AggregateOperator::Sum => todo!(),
                        AggregateOperator::Min => todo!(),
                        AggregateOperator::Max => {
                            Arc::new(physical::expr::MaxAggregateExpr::new(expr)) as Arc<dyn AggregateExpr>
                        }
                        AggregateOperator::Avg => todo!(),
                        AggregateOperator::Count => todo!(),
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(physical::plan::HashAggregate::new(
            aggregate.schema.clone(),
            input,
            group_expr,
            aggr_expr,
        )))
    }

    fn physical_plan_table_scan(&self, table_scan: &TableScan) -> Result<Arc<dyn PhysicalPlan>> {
        Ok(Arc::new(physical::plan::Scan::new(
            table_scan.schema(),
            table_scan.source.clone(),
            table_scan.projections.clone(),
        )) as Arc<dyn PhysicalPlan>)
    }

    fn physical_expr_column(&self, schema: &SchemaRef, column: &Column) -> Result<Arc<dyn PhysicalExpr>> {
        schema
            .index_of(&column.name)
            .map_err(|e| Error::ArrowError(e))
            .map(|index| Arc::new(physical::expr::Column::new(&column.name, index)) as Arc<dyn PhysicalExpr>)
    }

    fn physical_expr_literal(&self, value: &ScalarValue) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(physical::expr::Literal::new(value.clone())))
    }

    fn physical_expr_binary(&self, schema: &SchemaRef, binary_expr: &BinaryExpr) -> Result<Arc<dyn PhysicalExpr>> {
        let left = self.create_physical_expr(schema, &binary_expr.left)?;
        let right = self.create_physical_expr(schema, &binary_expr.right)?;
        Ok(Arc::new(physical::expr::BinaryExpr::new(left, binary_expr.op.clone(), right)) as Arc<dyn PhysicalExpr>)
    }

    fn physical_plan_join(&self, join: &Join) -> Result<Arc<dyn PhysicalPlan>> {
        todo!()
    }
}

impl QueryPlanner for DefaultQueryPlanner {
    fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>> {
        match plan {
            LogicalPlan::Projection(p) => self.physical_plan_projection(p),
            LogicalPlan::Filter(f) => self.physical_plan_filter(f),
            LogicalPlan::Aggregate(a) => self.physical_plan_aggregate(a),
            LogicalPlan::TableScan(t) => self.physical_plan_table_scan(t),
            LogicalPlan::EmptyRelation(_) => todo!(),
            LogicalPlan::CrossJoin(_) => todo!(),
            LogicalPlan::SubqueryAlias(_) => todo!(),
            LogicalPlan::Join(join) => self.physical_plan_join(join),
        }
    }

    fn create_physical_expr(&self, input_schema: &SchemaRef, expr: &LogicalExpr) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(c) => self.physical_expr_column(input_schema, c),
            LogicalExpr::Literal(v) => self.physical_expr_literal(v),
            LogicalExpr::BinaryExpr(b) => self.physical_expr_binary(input_schema, b),
            _ => unimplemented!("unsupported logical expression: {:?}", expr),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableSchemaInfo {
    pub(crate) schema: SchemaRef,
    pub(crate) alias: Option<String>,
}

/// Normalize the columns in the expression using the provided schemas and check for ambiguity
pub(crate) fn normalize_col_with_schemas_and_ambiguity_check(
    expr: LogicalExpr,
    schemas: &HashMap<TableRelation<'_>, TableSchemaInfo>,
) -> Result<LogicalExpr> {
    match expr {
        LogicalExpr::Column(mut col) => {
            if col.relation.is_some() {
                return Ok(LogicalExpr::Column(col));
            }

            // nomalize the column name
            let idents: Vec<String> = col.name.split('.').map(|a| a.to_string()).collect();
            // is compound table name
            match idents.len() {
                1 => {
                    // find the first schema that has the column
                    // should match without relation
                    let mut matched = vec![];

                    for (relation, table_info) in schemas {
                        if table_info.schema.field_with_name(&col.name).is_ok() {
                            matched.push(relation);
                        }
                    }

                    if matched.len() == 1 {
                        col.relation = Some(matched.pop().unwrap().clone().to_owned());
                        return Ok(LogicalExpr::Column(col));
                    } else if matched.len() > 1 {
                        return Err(Error::InternalError(format!("Column \"{}\" is ambiguous", col.name)));
                    }
                }
                2 => {
                    let relation = idents[0].as_str().into();
                    let col_name = idents[1].clone();
                    if let Some(table_info) = schemas.get(&relation) {
                        if table_info.schema.field_with_name(&col_name).is_ok() {
                            col.relation = Some(relation.to_owned());
                            // nomarlize the column name
                            col.name = col_name;
                            return Ok(LogicalExpr::Column(col));
                        }
                    }
                }
                _ => {}
            }

            Err(Error::InternalError(format!(
                "Column \"{}\" not found in any table",
                col.name
            )))
        }
        _ => Ok(expr),
    }
}
