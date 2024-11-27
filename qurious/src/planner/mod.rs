pub mod sql;

use std::{fmt::Debug, sync::Arc};

use arrow::{
    compute::SortOptions,
    datatypes::{SchemaBuilder, SchemaRef},
};

use crate::{
    arrow_err,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    internal_err,
    logical::{
        expr::{alias::Alias, AggregateOperator, BinaryExpr, CastExpr, Column, Function, Like, LogicalExpr},
        plan::{
            Aggregate, CrossJoin, EmptyRelation, Filter, Join, LogicalPlan, Projection, Sort, SubqueryAlias, TableScan,
            Values,
        },
    },
    physical::{
        self,
        expr::{IsNotNull, IsNull, Negative, PhysicalExpr},
        plan::{ColumnIndex, JoinFilter, JoinSide, PhysicalPlan},
    },
};

pub trait QueryPlanner: Debug + Send + Sync {
    fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>>;

    fn create_physical_expr(&self, input_schema: &SchemaRef, expr: &LogicalExpr) -> Result<Arc<dyn PhysicalExpr>>;
}

#[derive(Default, Debug)]
pub struct DefaultQueryPlanner;

impl QueryPlanner for DefaultQueryPlanner {
    fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>> {
        match plan {
            LogicalPlan::Projection(p) => self.physical_plan_projection(p),
            LogicalPlan::Filter(f) => self.physical_plan_filter(f),
            LogicalPlan::Aggregate(a) => self.physical_plan_aggregate(a),
            LogicalPlan::TableScan(t) => self.physical_plan_table_scan(t),
            LogicalPlan::EmptyRelation(v) => self.physical_empty_relation(v),
            LogicalPlan::CrossJoin(j) => self.physical_plan_cross_join(j),
            LogicalPlan::Join(join) => self.physical_plan_join(join),
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => self.create_physical_plan(input),
            LogicalPlan::Sort(sort) => self.physical_plan_sort(sort),
            LogicalPlan::Limit(limit) => Ok(Arc::new(physical::plan::Limit::new(
                self.create_physical_plan(&limit.input)?,
                limit.fetch,
                limit.skip,
            ))),
            LogicalPlan::Values(Values { values, schema }) => values
                .iter()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| self.create_physical_expr(schema, e))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()
                .map(|exprs| Arc::new(physical::plan::Values::new(schema.clone(), exprs)) as Arc<dyn PhysicalPlan>),

            stmt => Err(Error::InternalError(format!(
                "[{}] Statement not supported here should be handled in ExecuteSession",
                stmt
            ))),
        }
    }

    fn create_physical_expr(&self, input_schema: &SchemaRef, expr: &LogicalExpr) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(c) => self.physical_expr_column(input_schema, c),
            LogicalExpr::Literal(v) => self.physical_expr_literal(v),
            LogicalExpr::BinaryExpr(b) => self.physical_expr_binary(input_schema, b),
            LogicalExpr::Cast(c) => self.physical_expr_cast(input_schema, c),
            LogicalExpr::Alias(Alias { expr, .. }) => self.create_physical_expr(input_schema, expr),
            LogicalExpr::AggregateExpr(a) => self.create_physical_expr(input_schema, &a.as_column()?),
            LogicalExpr::Function(f) => self.physical_expr_function(input_schema, f),
            LogicalExpr::IsNull(f) => self
                .create_physical_expr(input_schema, f)
                .map(|expr| Arc::new(IsNull::new(expr)) as Arc<dyn PhysicalExpr>),
            LogicalExpr::IsNotNull(f) => self
                .create_physical_expr(input_schema, f)
                .map(|expr| Arc::new(IsNotNull::new(expr)) as Arc<dyn PhysicalExpr>),
            LogicalExpr::Negative(neg) => self
                .create_physical_expr(input_schema, neg)
                .map(|expr| Arc::new(Negative::new(expr)) as Arc<dyn PhysicalExpr>),
            LogicalExpr::Like(like) => self.physical_expr_like(input_schema, like),
            LogicalExpr::SubQuery(plan) => self
                .create_physical_plan(plan)
                .map(|plan| Arc::new(physical::expr::SubQuery { plan }) as Arc<dyn PhysicalExpr>),
            _ => unimplemented!("unsupported logical expression: {}", expr),
        }
    }
}

impl DefaultQueryPlanner {
    // Physical plan functions
    fn physical_plan_projection(&self, projection: &Projection) -> Result<Arc<dyn PhysicalPlan>> {
        let physical_plan = self.create_physical_plan(&projection.input)?;

        let input_schema = physical_plan.schema();
        let exprs: Vec<Arc<dyn PhysicalExpr>> = projection
            .exprs
            .iter()
            .map(|e| self.create_physical_expr(&input_schema, e))
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
            .map(|e| self.create_physical_expr(&aggregate.input.schema(), e))
            .collect::<Result<Vec<_>>>()?;

        let aggr_expr = aggregate
            .aggr_expr
            .iter()
            .map(|e| {
                let LogicalExpr::AggregateExpr(agg_expr) = e else {
                    return internal_err!("LogicalExpr should be AggregateExpr, but got {:?}", e);
                };
                // use base plan for aggregate expr
                self.create_physical_expr(&aggregate.input.schema(), &agg_expr.expr)
                    .and_then(|expr| {
                        // get AggreateExpr return datatype
                        let return_type = e.data_type(&aggregate.input.schema())?;
                        match agg_expr.op {
                            AggregateOperator::Sum => {
                                Ok(Arc::new(physical::expr::SumAggregateExpr::new(expr, return_type))
                                    as Arc<dyn physical::expr::AggregateExpr>)
                            }
                            AggregateOperator::Max => {
                                Ok(Arc::new(physical::expr::MaxAggregateExpr::new(expr, return_type))
                                    as Arc<dyn physical::expr::AggregateExpr>)
                            }
                            AggregateOperator::Min => {
                                Ok(Arc::new(physical::expr::MinAggregateExpr::new(expr, return_type))
                                    as Arc<dyn physical::expr::AggregateExpr>)
                            }
                            AggregateOperator::Count => Ok(Arc::new(physical::expr::CountAggregateExpr::new(expr))
                                as Arc<dyn physical::expr::AggregateExpr>),
                            AggregateOperator::Avg => Ok(Arc::new(physical::expr::AvgAggregateExpr::new(
                                expr,
                                agg_expr.expr.data_type(&aggregate.input.schema())?,
                                return_type,
                            ))),
                        }
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        // not group by
        if group_expr.is_empty() {
            return Ok(Arc::new(physical::plan::NoGroupingAggregate::new(
                aggregate.schema.clone(),
                input,
                aggr_expr,
            )));
        }

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

    fn physical_plan_cross_join(&self, cross_join: &CrossJoin) -> Result<Arc<dyn PhysicalPlan>> {
        let left = self.create_physical_plan(cross_join.left.as_ref())?;
        let right = self.create_physical_plan(cross_join.right.as_ref())?;
        Ok(Arc::new(physical::plan::CrossJoin::new(left, right)))
    }

    fn physical_plan_join(&self, join: &Join) -> Result<Arc<dyn PhysicalPlan>> {
        let left = self.create_physical_plan(join.left.as_ref())?;
        let right = self.create_physical_plan(join.right.as_ref())?;

        let using_columns = join.filter.using_columns();

        let ls = left.schema();
        let li = using_columns
            .iter()
            .filter_map(|c| ls.fields().find(&c.name))
            .map(|(i, f)| (f.clone(), (i, JoinSide::Left)));

        let rs = right.schema();
        let ri = using_columns
            .iter()
            .filter_map(|c| rs.fields().find(&c.name))
            .map(|(i, f)| (f.clone(), (i, JoinSide::Right)));

        let (filter_schema, column_indices): (SchemaBuilder, Vec<ColumnIndex>) = li.chain(ri).unzip();
        let filter_schema = Arc::new(filter_schema.finish());
        let filter_expr = self.create_physical_expr(&filter_schema, &join.filter)?;

        let join_filter = JoinFilter {
            expr: filter_expr,
            schema: filter_schema,
            column_indices,
        };

        physical::plan::Join::try_new(left, right, join.join_type, Some(join_filter))
            .map(|j| Arc::new(j) as Arc<dyn PhysicalPlan>)
    }

    fn physical_empty_relation(&self, empty: &EmptyRelation) -> Result<Arc<dyn PhysicalPlan>> {
        Ok(Arc::new(physical::plan::EmptyRelation::new(
            empty.schema.clone(),
            empty.produce_one_row,
        )))
    }

    fn physical_plan_sort(&self, sort: &Sort) -> Result<Arc<dyn PhysicalPlan>> {
        let input = self.create_physical_plan(&sort.input)?;
        sort.exprs
            .iter()
            .map(|expr| {
                let options = SortOptions {
                    descending: !expr.asc,
                    nulls_first: true,
                };
                let expr = self.create_physical_expr(&input.schema(), &expr.expr)?;
                Ok(physical::plan::PhyscialSortExpr::new(expr, options))
            })
            .collect::<Result<_>>()
            .map(|exprs| Arc::new(physical::plan::Sort::new(exprs, input)) as Arc<dyn PhysicalPlan>)
    }
}

impl DefaultQueryPlanner {
    fn physical_expr_like(&self, schema: &SchemaRef, like: &Like) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = self.create_physical_expr(schema, &like.expr)?;
        let pattern = self.create_physical_expr(schema, &like.pattern)?;
        Ok(Arc::new(physical::expr::Like::new(like.negated, expr, pattern)))
    }

    // Physical expression functions
    fn physical_expr_column(&self, schema: &SchemaRef, column: &Column) -> Result<Arc<dyn PhysicalExpr>> {
        schema
            .index_of(&column.name)
            .map_err(|e| arrow_err!(e))
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

    fn physical_expr_cast(&self, schema: &SchemaRef, cast: &CastExpr) -> Result<Arc<dyn PhysicalExpr>> {
        self.create_physical_expr(schema, &cast.expr)
            .map(|expr| Arc::new(physical::expr::CastExpr::new(expr, cast.data_type.clone())) as Arc<dyn PhysicalExpr>)
    }

    fn physical_expr_function(&self, schema: &SchemaRef, function: &Function) -> Result<Arc<dyn PhysicalExpr>> {
        let args = function
            .args
            .iter()
            .map(|e| self.create_physical_expr(schema, e))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(physical::expr::Function::new(function.func.clone(), args)))
    }
}
