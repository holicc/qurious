pub mod sql;

use std::{fmt::Debug, sync::Arc};

use arrow::{
    compute::SortOptions,
    datatypes::{SchemaBuilder, SchemaRef},
};

use crate::{
    common::table_relation::TableRelation,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    execution::session::TableRegistryRef,
    logical::{
        expr::{alias::Alias, AggregateExpr, AggregateOperator, BinaryExpr, CastExpr, Column, LogicalExpr, SortExpr},
        plan::{
            Aggregate, CreateMemoryTable, CrossJoin, DdlStatement, DmlOperator, DmlStatement, DropTable, EmptyRelation,
            Filter, Join, LogicalPlan, Projection, Sort, SubqueryAlias, TableScan, Values,
        },
    },
    physical::{
        self,
        expr::PhysicalExpr,
        plan::{ColumnIndex, JoinFilter, JoinSide, PhysicalPlan},
    },
};

pub trait QueryPlanner: Debug + Send + Sync {
    fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>>;
}

#[derive(Debug)]
pub struct DefaultQueryPlanner {
    table_registry: TableRegistryRef,
}

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
            // DDL
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable { schema, name, input })) => {
                self.create_physical_plan(input).map(|input| {
                    Arc::new(physical::plan::ddl::CreateTable::new(
                        name.clone(),
                        schema.clone(),
                        input,
                    )) as Arc<dyn PhysicalPlan>
                })
            }
            LogicalPlan::Ddl(DdlStatement::DropTable(DropTable { name, if_exists })) => {
                Ok(Arc::new(physical::plan::ddl::DropTable::new(name.clone(), *if_exists)) as Arc<dyn PhysicalPlan>)
            }
            // DML
            LogicalPlan::Dml(DmlStatement {
                relation, op, input, ..
            }) => {
                let source = self
                    .table_registry
                    .read()
                    .map_err(|e| Error::InternalError(e.to_string()))
                    .and_then(|x| x.get_table_source(&relation.to_quanlify_name()))?;
                let input = self.create_physical_plan(input)?;

                match op {
                    DmlOperator::Insert => source.insert_into(input),
                    DmlOperator::Update => source.update(input),
                    DmlOperator::Delete => source.delete(input),
                }
            }
        }
    }
}

impl DefaultQueryPlanner {
    pub fn new(table_registry: TableRegistryRef) -> Self {
        Self { table_registry }
    }

    pub fn create_physical_expr(&self, input_schema: &SchemaRef, expr: &LogicalExpr) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(c) => self.physical_expr_column(input_schema, c),
            LogicalExpr::Literal(v) => self.physical_expr_literal(v),
            LogicalExpr::BinaryExpr(b) => self.physical_expr_binary(input_schema, b),
            LogicalExpr::Cast(c) => self.physical_expr_cast(input_schema, c),
            _ => unimplemented!("unsupported logical expression: {:?}", expr),
        }
    }
}

impl DefaultQueryPlanner {
    fn physical_plan_projection(&self, projection: &Projection) -> Result<Arc<dyn PhysicalPlan>> {
        let physical_plan = self.create_physical_plan(&projection.input)?;
        let schema = physical_plan.schema();
        let exprs = projection
            .exprs
            .iter()
            .map(|e| self.create_physical_expr(&schema, e))
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
                        AggregateOperator::Max => Arc::new(physical::expr::MaxAggregateExpr::new(expr))
                            as Arc<dyn physical::expr::AggregateExpr>,
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

    fn physical_expr_cast(&self, schema: &SchemaRef, cast: &CastExpr) -> Result<Arc<dyn PhysicalExpr>> {
        self.create_physical_expr(schema, &cast.expr)
            .map(|expr| Arc::new(physical::expr::CastExpr::new(expr, cast.data_type.clone())) as Arc<dyn PhysicalExpr>)
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

/// Normalize the columns in the expression using the provided schemas and check for ambiguity
pub(crate) fn normalize_col_with_schemas_and_ambiguity_check(
    expr: LogicalExpr,
    schemas: &[&[(&TableRelation, SchemaRef)]],
) -> Result<LogicalExpr> {
    match expr {
        LogicalExpr::AggregateExpr(AggregateExpr { op, expr }) => {
            normalize_col_with_schemas_and_ambiguity_check(*expr, schemas).map(|transform| {
                LogicalExpr::AggregateExpr(AggregateExpr {
                    op,
                    expr: Box::new(transform),
                })
            })
        }
        LogicalExpr::SortExpr(SortExpr { expr, asc }) => normalize_col_with_schemas_and_ambiguity_check(*expr, schemas)
            .map(|transform| {
                LogicalExpr::SortExpr(SortExpr {
                    expr: Box::new(transform),
                    asc,
                })
            }),
        LogicalExpr::Alias(Alias { expr, name }) => {
            normalize_col_with_schemas_and_ambiguity_check(*expr, schemas).map(|col| col.alias(name))
        }
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = normalize_col_with_schemas_and_ambiguity_check(*left, schemas)?;
            let right = normalize_col_with_schemas_and_ambiguity_check(*right, schemas)?;
            Ok(LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        }
        LogicalExpr::Column(col) => col
            .normalize_col_with_schemas_and_ambiguity_check(schemas)
            .map(LogicalExpr::Column),
        _ => Ok(expr),
    }
}
