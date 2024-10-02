use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Field, Schema, TimeUnit};
use sqlparser::ast::{
    Assignment, BinaryOperator, Cte, Expression, From, FunctionArgument, Literal, Order, Select, SelectItem, Statement,
};

use crate::{
    common::{join_type::JoinType, table_relation::TableRelation},
    datasource::file::csv::CsvReadOptions,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    functions::UserDefinedFunction,
    internal_err,
    logical::{
        expr::*,
        plan::{
            self, CreateMemoryTable, DdlStatement, DmlStatement, DropTable, Filter, LogicalPlan, SubqueryAlias, Values,
        },
        LogicalPlanBuilder,
    },
    provider::table::TableProvider,
    utils::normalize_ident,
};

use self::alias::Alias;

use crate::planner::normalize_col_with_schemas_and_ambiguity_check;

pub struct SqlQueryPlanner<'a> {
    ctes: HashMap<String, Arc<LogicalPlan>>,
    udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
}

impl<'a> SqlQueryPlanner<'a> {
    pub fn new(
        relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
        udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    ) -> Self {
        SqlQueryPlanner {
            ctes: HashMap::default(),
            relations,
            udfs,
        }
    }

    pub fn create_logical_plan(
        stmt: Statement,
        relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
        udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    ) -> Result<LogicalPlan> {
        let mut planner = SqlQueryPlanner {
            ctes: HashMap::default(),
            relations,
            udfs,
        };

        match stmt {
            Statement::Select(select) => planner.select_to_plan(*select),
            Statement::CreateTable {
                table,
                check_exists,
                columns,
                query,
            } => {
                let schema = Arc::new(Schema::new(
                    columns
                        .into_iter()
                        .map(|col| {
                            let name = col.name.clone();
                            let data_type = sql_to_arrow_data_type(&col.datatype)?;
                            Ok(Field::new(&name, data_type, col.nullable))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ));

                let input = if let Some(query) = query {
                    planner.select_to_plan(query)?
                } else {
                    LogicalPlan::EmptyRelation(plan::EmptyRelation {
                        schema: schema.clone(),
                        produce_one_row: false,
                    })
                };

                planner.create_table_to_plan(input, table, schema, check_exists)
            }
            Statement::DropTable { table, check_exists } => planner.drop_table_to_plan(table, check_exists),
            Statement::Insert {
                table,
                alias,
                columns,
                values,
                on_conflict,
                returning,
                query,
            } => {
                if alias.is_some() {
                    return Err(Error::InternalError(
                        "Alias is not supported for INSERT statement".to_owned(),
                    ));
                }
                if on_conflict.is_some() {
                    return Err(Error::InternalError(
                        "ON CONFLICT is not supported for INSERT statement".to_owned(),
                    ));
                }
                if returning.is_some() {
                    return Err(Error::InternalError(
                        "RETURNING is not supported for INSERT statement".to_owned(),
                    ));
                }
                if query.is_none() && values.is_empty() {
                    return Err(Error::InternalError(
                        "INSERT statement requires VALUES or SELECT clause".to_owned(),
                    ));
                }

                let columns = if let Some(col) = columns {
                    if col.is_empty() {
                        return Err(Error::InternalError(
                            "INSERT statement requires at least one column".to_owned(),
                        ));
                    }
                    // make sure values have the same length
                    let len = values.first().map(|v| v.len()).ok_or(Error::InternalError(
                        "INSERT statement requires at least one value".to_owned(),
                    ))?;
                    if values.iter().any(|v| v.len() != len) {
                        return Err(Error::InternalError(
                            "INSERT statement requires all VALUES to have the same length".to_owned(),
                        ));
                    }
                    if col.len() != len {
                        return Err(Error::InternalError(
                            "INSERT statement requires all VALUES to have the same length as columns".to_owned(),
                        ));
                    }
                    col
                } else {
                    vec![]
                };

                planner.insert_to_plan(table, columns, values, query)
            }
            Statement::Delete { table, r#where } => planner.delete_to_plan(table, r#where),
            Statement::Update {
                table,
                assignments,
                r#where,
            } => planner.update_to_plan(table, assignments, r#where),
            _ => todo!(),
        }
    }
}

impl<'a> SqlQueryPlanner<'a> {
    fn update_to_plan(
        &mut self,
        table: String,
        assignments: Vec<Assignment>,
        r#where: Option<Expression>,
    ) -> Result<LogicalPlan> {
        let table_source = self.get_table_source(&table)?;
        let table_schema = table_source.schema();
        let input = LogicalPlanBuilder::scan(table.clone(), table_source, None)
            .map(|builder| builder.build())
            .and_then(|plan| self.filter_expr(plan, r#where))?;
        let relations = input.relation();

        let mut assign_map: HashMap<String, LogicalExpr> = assignments
            .into_iter()
            .map(|assign| {
                let name = assign.target.to_string();
                let value = self
                    .sql_to_expr(assign.value)
                    .and_then(|e| normalize_col_with_schemas_and_ambiguity_check(e, &[&relations]))?;
                Ok((name, value))
            })
            .collect::<Result<_>>()?;

        // zip table relation with schema field
        let exprs = relations
            .into_iter()
            .map(|(r, s)| s.fields().iter().map(|f| (r.clone(), f.clone())).collect::<Vec<_>>())
            .flatten()
            .map(|(r, f)| match assign_map.remove(f.name()) {
                Some(expr) => expr.cast_to(f.data_type()).alias(f.name()),
                None => LogicalExpr::Column(Column {
                    name: f.name().to_owned(),
                    relation: Some(r.clone()),
                }),
            })
            .collect::<Vec<_>>();

        let plan = LogicalPlanBuilder::project(input, exprs)?;

        Ok(LogicalPlan::Dml(DmlStatement {
            relation: table.into(),
            op: plan::DmlOperator::Update,
            schema: table_schema,
            input: Box::new(plan),
        }))
    }

    fn delete_to_plan(&mut self, table: String, r#where: Option<Expression>) -> Result<LogicalPlan> {
        let table_source = self.get_table_source(&table)?;
        let table_schema = table_source.schema();
        let plan = LogicalPlanBuilder::scan(table.clone(), table_source, None)?.build();
        let plan = self.filter_expr(plan, r#where)?;

        Ok(LogicalPlan::Dml(DmlStatement {
            relation: table.into(),
            op: plan::DmlOperator::Delete,
            schema: table_schema,
            input: Box::new(plan),
        }))
    }

    fn insert_to_plan(
        &mut self,
        table: String,
        columns: Vec<Expression>,
        values: Vec<Vec<Expression>>,
        query: Option<Select>,
    ) -> Result<LogicalPlan> {
        let table_source = self.get_table_source(&table)?;
        let table_schema = table_source.schema();
        let input = if let Some(query) = query {
            let plan = self.select_to_plan(query)?;
            // check INSERT's input plan schema is compatible with table schema
            if plan.schema().fields().len() != table_schema.fields().len() {
                return Err(Error::InternalError(format!(
                    "INSERT statement requires the same number of columns as the table: {}",
                    table
                )));
            }

            plan
        } else {
            let mut column_indices: HashMap<&String, usize> = HashMap::default();
            let mut value_indices = vec![None; table_schema.fields().len()];
            let row = values.first().ok_or(Error::InternalError(
                "INSERT statement requires at least one value".to_owned(),
            ))?;
            // check if columns are specified in the INSERT statement
            // if not, we will use the default values
            if columns.is_empty() {
                row.iter().enumerate().for_each(|(i, _)| value_indices[i] = Some(i));
            } else {
                for (c_index, col) in columns.iter().enumerate() {
                    let name = match col {
                        Expression::Identifier(name) => &name.value,
                        _ => {
                            return Err(Error::InternalError(
                                "INSERT statement requires column name to be an identifier".to_owned(),
                            ))
                        }
                    };

                    column_indices.insert(name, c_index);

                    let index = table_schema.index_of(name)?;
                    if value_indices[index].is_some() {
                        return Err(Error::InternalError(format!(
                            "Column [{}] is specified more than once",
                            name
                        )));
                    }

                    value_indices[index] = Some(index);
                }
            }
            // convert values to logical plan
            let plan = self.values_to_plan(values)?;
            let schema = plan.schema();
            // check if all columns have values, if not, fill with default values
            let exprs = value_indices
                .into_iter()
                .enumerate()
                .map(|(i, value_index)| {
                    let target_field = table_schema.field(i);
                    match value_index {
                        Some(v) => {
                            // the index of `v` is the index of the table_schema field
                            // not the index of the `values` plan schema field
                            // so we need to transform it to the `values` plan schema field index
                            let target_field = table_schema.field(v);
                            let v = column_indices.get(target_field.name()).unwrap_or(&v);

                            Ok(column(schema.field(*v).name())
                                .cast_to(target_field.data_type())
                                .alias(target_field.name()))
                        }
                        None => {
                            let default_value = table_source.get_column_default(target_field.name());
                            if !target_field.is_nullable() && default_value.is_none() {
                                return Err(Error::InternalError(format!(
                                    "Column [{}] does not have a default value and does not allow NULLs",
                                    target_field.name()
                                )));
                            }
                            Ok(LogicalExpr::Literal(default_value.unwrap_or(ScalarValue::Null))
                                .cast_to(target_field.data_type())
                                .alias(target_field.name()))
                        }
                    }
                })
                .collect::<Result<Vec<LogicalExpr>>>()?;
            // convert values to expressions
            LogicalPlanBuilder::project(plan, exprs)?
        };

        Ok(LogicalPlan::Dml(DmlStatement {
            relation: table.into(),
            op: plan::DmlOperator::Insert,
            schema: table_schema,
            input: Box::new(input),
        }))
    }

    fn drop_table_to_plan(&mut self, table: String, check_exists: bool) -> Result<LogicalPlan> {
        let relation: TableRelation = table.clone().into();

        if !check_exists && !self.relations.contains_key(&relation) {
            return Err(Error::TableNotFound(table));
        }

        Ok(LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
            name: table,
            if_exists: check_exists,
        })))
    }

    fn create_table_to_plan(
        &mut self,
        input: LogicalPlan,
        table: String,
        schema: Arc<Schema>,
        check_exists: bool,
    ) -> Result<LogicalPlan> {
        if check_exists {
            if self.relations.contains_key(&table.clone().into()) {
                return Err(Error::InternalError(format!("table [{}] already exists", table)));
            }
        }

        Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            schema,
            name: table,
            input: Box::new(input),
        })))
    }

    fn values_to_plan(&self, values: Vec<Vec<Expression>>) -> Result<LogicalPlan> {
        let rows = values
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|expr| self.sql_to_expr(expr))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        // get the schema from the first row
        let row = rows.first().ok_or(Error::InternalError("Empty values".to_owned()))?;
        let empty_plan = LogicalPlan::EmptyRelation(plan::EmptyRelation {
            schema: Arc::new(Schema::empty()),
            produce_one_row: false,
        });
        let schema = row
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                let name = format!("column{}", i + 1);
                let data_type = expr.field(&empty_plan).map(|f| f.data_type().clone())?;
                Ok(Field::new(&name, data_type, true))
            })
            .collect::<Result<Vec<_>>>()
            .map(|fields| Arc::new(Schema::new(fields)))?;

        Ok(LogicalPlan::Values(Values { schema, values: rows }))
    }

    fn select_to_plan(&mut self, select: Select) -> Result<LogicalPlan> {
        // process `with` clause
        if let Some(with) = select.with {
            self.cte_tables(with.cte_tables)?;
        }
        // process `from` clause
        let plan = self.table_scan_to_plan(select.from)?;
        let empty_from = match plan {
            LogicalPlan::EmptyRelation(_) => true,
            _ => false,
        };
        // process the WHERE clause
        let mut plan = self.filter_expr(plan, select.r#where)?;
        // process the SELECT expressions
        let column_exprs = self.column_exprs(&plan, empty_from, select.columns)?;
        // get aggregate expressions
        let aggr_exprs = find_aggregate_exprs(&column_exprs);
        // process the HAVING clause
        let having = if let Some(having_expr) = select.having {
            Some(self.sql_to_expr(having_expr)?)
        } else {
            None
        };
        // process the GROUP BY clause or process aggregation in SELECT
        if select.group_by.is_some() || !aggr_exprs.is_empty() {
            plan = self.aggregate_plan(
                plan,
                &column_exprs,
                aggr_exprs,
                select.group_by.unwrap_or_default(),
                having,
            )?;
        } else {
            plan = LogicalPlanBuilder::project(plan, column_exprs)?;
        }

        // process the ORDER BY clause
        let plan = if let Some(order_by) = select.order_by {
            self.order_by_expr(order_by)
                .and_then(|exprs| LogicalPlanBuilder::from(plan).sort(exprs))?
                .build()
        } else {
            plan
        };

        if select.limit.is_none() && select.offset.is_none() {
            return Ok(plan);
        }

        // process the LIMIT clause
        let fetch = select.limit.and_then(|l| {
            self.sql_to_expr(l)
                .and_then(|v| get_expr_value(v).map(|v| v as usize))
                .ok()
        });
        let skip = select
            .offset
            .and_then(|o| {
                self.sql_to_expr(o)
                    .and_then(|v| get_expr_value(v).map(|v| v as usize))
                    .ok()
            })
            .unwrap_or_default();

        Ok(LogicalPlanBuilder::from(plan).limit(fetch, skip).build())
    }

    fn table_scan_to_plan(&mut self, mut froms: Vec<From>) -> Result<LogicalPlan> {
        match froms.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()),
            1 => {
                let (plan, alias) = match froms.remove(0) {
                    From::Table { name, alias } => {
                        let relation: TableRelation = name.clone().into();

                        // try to get ctes table first and the from table registey
                        let scan = if let Some(plan) = self.get_cte_table(&name) {
                            plan
                        } else {
                            let source = self.get_table_source(&name)?;
                            LogicalPlanBuilder::scan(relation.clone(), source, None)?.build()
                        };

                        (scan, alias)
                    }
                    From::TableFunction { name, args, alias } => (self.table_func_to_plan(name, args)?, alias),
                    From::Join {
                        left,
                        right,
                        on,
                        join_type,
                    } => {
                        let left = self.table_scan_to_plan(vec![*left])?;
                        let right = self.table_scan_to_plan(vec![*right])?;

                        if sqlparser::ast::JoinType::Cross == join_type {
                            return LogicalPlanBuilder::from(left)
                                .cross_join(right)
                                .map(|builder| builder.build());
                        }

                        let filter_expr = on
                            .ok_or(Error::InternalError("Join clause requires an ON clause".to_owned()))
                            .and_then(|expr| self.sql_to_expr(expr))?;

                        (
                            LogicalPlanBuilder::from(left)
                                .join_on(right, JoinType::from(join_type), filter_expr)?
                                .build(),
                            None,
                        )
                    }
                    _ => todo!(),
                };

                if let Some(alias) = alias {
                    self.apply_table_alias(plan, alias)
                } else {
                    Ok(plan)
                }
            }
            _ => {
                let mut plans = froms.into_iter().map(|f| self.table_scan_to_plan(vec![f]));
                let mut left = LogicalPlanBuilder::from(plans.next().expect("")?);

                for right in plans {
                    left = left.cross_join(right?)?;
                }

                Ok(left.build())
            }
        }
    }

    fn table_func_to_plan(&mut self, name: String, mut args: Vec<FunctionArgument>) -> Result<LogicalPlan> {
        let (table_name, provider) = match name.to_lowercase().as_str() {
            "read_csv" | "read_parquet" | "read_json" => {
                let path = parse_file_path(&mut args)?;
                let relation = TableRelation::parse_file_path(&path);
                let provider = self
                    .relations
                    .get(&relation)
                    .cloned()
                    .ok_or(Error::TableNotFound(path))?;

                (relation, provider)
            }
            _ => todo!(),
        };

        LogicalPlanBuilder::scan(table_name, provider, None).map(|builder| builder.build())
    }

    fn filter_expr(&self, mut plan: LogicalPlan, expr: Option<Expression>) -> Result<LogicalPlan> {
        if let Some(filter) = expr {
            let filter_expr = self
                .sql_to_expr(filter)
                .and_then(|e| normalize_col_with_schemas_and_ambiguity_check(e, &[&plan.relation()]))?;
            // we should parse filter first and then apply it to the table scan
            match &mut plan {
                LogicalPlan::TableScan(table) => {
                    table.filter = Some(filter_expr.clone());
                }
                _ => {}
            }

            Filter::try_new(plan, filter_expr).map(LogicalPlan::Filter)
        } else {
            Ok(plan)
        }
    }

    fn apply_table_alias(&mut self, plan: LogicalPlan, alias: String) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan(mut table) => {
                let relation = self
                    .relations
                    .remove(&table.relation)
                    .ok_or(Error::TableNotFound(format!(
                        "Can't apply table alias: {} to relation: {}, because relation not exists",
                        alias, table.relation
                    )))?;
                let new_relation: TableRelation = alias.clone().into();
                self.relations.insert(new_relation.clone(), relation);
                // apply alias to table scan plan
                table.relation = new_relation;

                Ok(LogicalPlan::TableScan(table))
            }
            _ => Ok(plan),
        }
    }

    fn aggregate_plan(
        &self,
        input: LogicalPlan,
        select_exprs: &[LogicalExpr],
        aggr_exprs: Vec<AggregateExpr>,
        group_by: Vec<Expression>,
        _having_expr: Option<LogicalExpr>,
    ) -> Result<LogicalPlan> {
        let group_exprs = group_by
            .into_iter()
            .map(|expr| {
                self.sql_to_expr(expr)
                    .and_then(|expr| normalize_col_with_schemas_and_ambiguity_check(expr, &[&input.relation()]))
            })
            .collect::<Result<Vec<_>>>()?;

        let agg_and_group_by_column_exprs = aggr_exprs
            .iter()
            .map(AggregateExpr::as_column)
            .chain(group_exprs.iter().map(LogicalExpr::as_column))
            .collect::<Result<Vec<_>>>()?;
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| {
                // if expr is one of the group by columns or aggregate columns, we should convert to column
                if agg_and_group_by_column_exprs.iter().any(|e| e == expr) {
                    return expr.as_column();
                }
                Ok(expr.clone())
            })
            .collect::<Result<Vec<_>>>()?;

        for col_expr in select_exprs_post_aggr.iter().flat_map(|f| match find_column_exprs(f) {
            Ok(v) => v.into_iter().map(|i| Ok(i)).collect(),
            Err(e) => vec![Err(e)],
        }) {
            let col_expr = col_expr?;
            if agg_and_group_by_column_exprs.iter().all(|e| e != &col_expr) {
                return Err(Error::InternalError(format!(
                    "column [{}] must appear in the GROUP BY clause or be used in an aggregate function",
                    col_expr
                )));
            }
        }

        LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggr_exprs)?
            .add_project(select_exprs_post_aggr)
            .map(|plan| plan.build())
    }

    fn order_by_expr(&self, order_by: Vec<(Expression, Order)>) -> Result<Vec<SortExpr>> {
        order_by
            .into_iter()
            .map(|(expr, order)| {
                self.sql_to_expr(expr).map(|expr| SortExpr {
                    expr: Box::new(expr),
                    asc: order == Order::Asc,
                })
            })
            .collect()
    }

    fn cte_tables(&mut self, ctes: Vec<Cte>) -> Result<()> {
        for cte in ctes {
            let plan = self
                .select_to_plan(*cte.query)
                .and_then(|plan| self.apply_table_alias(plan, cte.alias.clone()))
                .map(|plan| {
                    let schema = plan.schema();
                    Arc::new(LogicalPlan::SubqueryAlias(SubqueryAlias {
                        input: Arc::new(plan),
                        alias: cte.alias.clone(),
                        schema,
                    }))
                })?;

            self.ctes.insert(cte.alias, plan);
        }

        Ok(())
    }

    fn column_exprs(&self, plan: &LogicalPlan, empty_from: bool, columns: Vec<SelectItem>) -> Result<Vec<LogicalExpr>> {
        columns
            .into_iter()
            .flat_map(|col| match self.sql_select_item_to_expr(plan, col, empty_from) {
                Ok(vec) => vec
                    .into_iter()
                    .map(|expr| normalize_col_with_schemas_and_ambiguity_check(expr, &[&plan.relation()]))
                    .collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<LogicalExpr>>>()
    }

    fn sql_to_expr(&self, expr: Expression) -> Result<LogicalExpr> {
        match expr {
            Expression::CompoundIdentifier(mut idents) => {
                if idents.len() != 2 {
                    return Err(Error::InternalError(format!(
                        "CompoundIdentifier should have two parts, but got {}",
                        idents.len()
                    )));
                }
                Ok(LogicalExpr::Column(Column {
                    name: normalize_ident(idents.remove(1)),
                    relation: Some(idents.remove(0).value.into()),
                }))
            }
            Expression::Identifier(ident) => Ok(column(&normalize_ident(ident))),
            Expression::Literal(lit) => match lit {
                Literal::Int(i) => Ok(LogicalExpr::Literal(ScalarValue::Int64(Some(i)))),
                Literal::Float(f) => Ok(LogicalExpr::Literal(ScalarValue::Float64(Some(f)))),
                Literal::String(s) => Ok(LogicalExpr::Literal(ScalarValue::Utf8(Some(s)))),
                Literal::Boolean(b) => Ok(LogicalExpr::Literal(ScalarValue::Boolean(Some(b)))),
                Literal::Null => Ok(LogicalExpr::Literal(ScalarValue::Null)),
            },
            Expression::BinaryOperator(op) => self.parse_binary_op(op),
            Expression::Function(name, args) => {
                let exprs = args
                    .into_iter()
                    .map(|expr| self.sql_function_args_to_expr(expr))
                    .collect::<Result<Vec<_>>>()?;

                self.handle_function(&name, exprs)
            }
            Expression::Cast { expr, data_type } => {
                let expr = self.sql_to_expr(*expr)?;
                Ok(expr.cast_to(&sql_to_arrow_data_type(&data_type)?))
            }
            Expression::TypedString { data_type, value } => Ok(LogicalExpr::Cast(CastExpr {
                expr: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some(value)))),
                data_type: sql_to_arrow_data_type(&data_type)?,
            })),
            Expression::Extract { field, expr } => self.handle_function(
                "EXTRACT",
                vec![
                    LogicalExpr::Literal(ScalarValue::Utf8(Some(field.to_string()))),
                    self.sql_to_expr(*expr)?,
                ],
            ),
            Expression::IsNull(expr) => self.sql_to_expr(*expr).map(|expr| LogicalExpr::IsNull(Box::new(expr))),
            Expression::IsNotNull(expr) => self
                .sql_to_expr(*expr)
                .map(|expr| LogicalExpr::IsNotNull(Box::new(expr))),
            Expression::UnaryOperator { op, expr } => self.sql_to_expr(*expr).map(|expr| match op {
                sqlparser::ast::UnaryOperator::Minus => LogicalExpr::Negative(Box::new(expr)),
                _ => todo!("UnaryOperator: {:?}", expr),
            }),
            _ => todo!("sql_to_expr: {:?}", expr),
        }
    }

    fn handle_function(&self, name: &str, mut exprs: Vec<LogicalExpr>) -> Result<LogicalExpr> {
        if let Some(udf) = self.udfs.get(name.to_uppercase().as_str()) {
            return Ok(LogicalExpr::Function(Function {
                func: udf.clone(),
                args: exprs,
            }));
        }

        if let Ok(op) = name.try_into() {
            return Ok(LogicalExpr::AggregateExpr(AggregateExpr {
                op,
                expr: Box::new(exprs.pop().ok_or(Error::InternalError(
                    "Aggregate function should have at least one expr".to_string(),
                ))?),
            }));
        }

        internal_err!("Unknown function: {}", name)
    }

    fn sql_function_args_to_expr(&self, expr: Expression) -> Result<LogicalExpr> {
        match expr {
            Expression::Identifier(ident) if ident.value == "*" => Ok(LogicalExpr::Wildcard),
            _ => self.sql_to_expr(expr),
        }
    }

    fn parse_binary_op(&self, op: BinaryOperator) -> Result<LogicalExpr> {
        Ok(match op {
            BinaryOperator::Eq(l, r) => eq(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::NotEq(l, r) => not_eq(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Gt(l, r) => gt(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Gte(l, r) => gt_eq(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Lt(l, r) => lt(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Lte(l, r) => lt_eq(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Or(l, r) => or(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::And(l, r) => and(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Sub(l, r) => sub(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Mul(l, r) => mul(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Add(l, r) => add(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            BinaryOperator::Div(l, r) => div(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
        })
    }

    fn sql_select_item_to_expr(
        &self,
        plan: &LogicalPlan,
        item: SelectItem,
        empty_relation: bool,
    ) -> Result<Vec<LogicalExpr>> {
        match item {
            SelectItem::UnNamedExpr(expr) => self.sql_to_expr(expr).map(|v| vec![v]),
            SelectItem::ExprWithAlias(expr, alias) => self
                .sql_to_expr(expr)
                .map(|col| vec![LogicalExpr::Alias(Alias::new(alias, col))]),
            SelectItem::Wildcard => {
                if empty_relation {
                    return Err(Error::InternalError(
                        "SELECT * with no tables specified is not valid".to_owned(),
                    ));
                }
                // expand schema
                let mut using_columns: HashMap<TableRelation, HashSet<String>> = HashMap::default();
                let mut eval_stack = vec![plan];
                while let Some(next_plan) = eval_stack.pop() {
                    match next_plan {
                        LogicalPlan::TableScan(table) => {
                            using_columns.insert(
                                table.relation.clone(),
                                table
                                    .projected_schema
                                    .fields()
                                    .iter()
                                    .map(|f| f.name().clone())
                                    .collect::<HashSet<_>>(),
                            );
                        }
                        LogicalPlan::SubqueryAlias(sub_query) => {
                            let relation = sub_query.alias.clone().into();
                            using_columns.insert(
                                relation,
                                sub_query
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|f| f.name().clone())
                                    .collect::<HashSet<_>>(),
                            );
                        }
                        p => {
                            if let Some(child) = p.children() {
                                eval_stack.extend(child.iter());
                            }
                        }
                    }
                }

                let mut cols = using_columns
                    .into_iter()
                    .flat_map(|(relation, cols)| {
                        cols.into_iter()
                            .map(|name| Column {
                                name,
                                relation: Some(relation.to_owned()),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();

                cols.sort();

                Ok(cols.into_iter().map(LogicalExpr::Column).collect())
            }
            SelectItem::QualifiedWildcard(idents) => {
                if empty_relation {
                    return Err(Error::InternalError(
                        "SELECT * with no tables specified is not valid".to_owned(),
                    ));
                }
                // expand schema
                let quanlified_prefix = idents.join(".").into();

                if self.relations.contains_key(&quanlified_prefix) {
                    return // expand schema
                        plan.schema()
                            .flattened_fields()
                            .into_iter()
                            .map(|field| Ok(column(field.name())))
                            .collect();
                }

                Err(Error::InternalError(format!(
                    "Invalid qualified wildcard: {}",
                    quanlified_prefix
                )))
            }
        }
    }

    fn get_table_source(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        self.relations
            .get(&table_name.into())
            .map(|a| a.clone())
            .ok_or(Error::TableNotFound(table_name.to_owned()))
    }

    fn get_cte_table(&self, name: &str) -> Option<LogicalPlan> {
        self.ctes.get(name).map(|a| a.as_ref().clone())
    }
}

fn find_column_exprs(expr: &LogicalExpr) -> Result<Vec<LogicalExpr>> {
    match expr {
        LogicalExpr::Alias(alias) => find_column_exprs(&alias.expr),
        LogicalExpr::AggregateExpr(aggr) => Ok(vec![aggr.as_column()?]),
        LogicalExpr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            let mut left = find_column_exprs(left)?;
            let mut right = find_column_exprs(right)?;
            left.append(&mut right);
            Ok(left)
        }
        _ => Ok(vec![expr.clone()]),
    }
}

fn find_aggregate_exprs(exprs: &Vec<LogicalExpr>) -> Vec<AggregateExpr> {
    exprs
        .iter()
        .flat_map(|expr| match expr {
            LogicalExpr::AggregateExpr(aggr) => vec![(aggr.clone())],
            LogicalExpr::Alias(alias) => find_aggregate_exprs(&vec![alias.expr.as_ref().clone()]),
            LogicalExpr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                let mut left = find_aggregate_exprs(&vec![left.as_ref().clone()]);
                let mut right = find_aggregate_exprs(&vec![right.as_ref().clone()]);
                left.append(&mut right);
                left
            }
            _ => vec![],
        })
        .collect::<Vec<_>>()
}

pub(crate) fn parse_file_path(args: &mut Vec<FunctionArgument>) -> Result<String> {
    if args.len() == 0 {
        return Err(Error::InternalError(
            "table function requires at least one argument".to_owned(),
        ));
    }

    match args.remove(0).value {
        Expression::Literal(Literal::String(s)) => Ok(s),
        _ => {
            return Err(Error::InternalError(
                "read_csv function requires the first argument to be a string".to_owned(),
            ))
        }
    }
}

pub(crate) fn parse_csv_options(mut args: Vec<FunctionArgument>) -> Result<CsvReadOptions> {
    let mut options = CsvReadOptions::default();

    let extract_literal = |expr: Expression| -> Result<u8> {
        match expr {
            Expression::Literal(Literal::String(s)) => {
                if s.len() != 1 {
                    return Err(Error::InternalError("Expected a single character".to_owned()));
                }
                Ok(s.as_bytes()[0])
            }
            _ => Err(Error::InternalError("Expected a string literal".to_owned())),
        }
    };
    let extract_value = |expr: Expression| -> Result<Literal> {
        match expr {
            Expression::Literal(lit) => Ok(lit),
            _ => Err(Error::InternalError("Expected a boolean literal".to_owned())),
        }
    };

    while let Some(arg) = args.pop() {
        let opt_name = &arg
            .id
            .ok_or(Error::InternalError(format!(
                "Parse CsvOptions error, expected identifier, but it's empty"
            )))?
            .value
            .to_lowercase();
        let value = arg.value;

        match opt_name.as_str() {
            "delim" => options.delimiter = extract_literal(value)?,
            "escape" => options.escape = extract_literal(value).ok(),
            "quote" => options.quote = extract_literal(value).ok(),
            "header" => {
                options.has_header = extract_value(value).and_then(|a| {
                    a.try_into()
                        .map_err(|e| Error::InternalError(format!("Parse CsvOptions error, {}", e)))
                })?
            }
            "columns" => todo!(),
            _ => {
                return Err(Error::InternalError(format!(
                    "Unknown option {} for read_csv function",
                    opt_name
                )))
            }
        }
    }

    Ok(options)
}

fn sql_to_arrow_data_type(data_type: &sqlparser::datatype::DataType) -> Result<arrow::datatypes::DataType> {
    match data_type {
        sqlparser::datatype::DataType::Integer => Ok(arrow::datatypes::DataType::Int64),
        sqlparser::datatype::DataType::Boolean => Ok(arrow::datatypes::DataType::Boolean),
        sqlparser::datatype::DataType::Float => Ok(arrow::datatypes::DataType::Float64),
        sqlparser::datatype::DataType::String =>Ok( arrow::datatypes::DataType::Utf8),
        sqlparser::datatype::DataType::Date => Ok(arrow::datatypes::DataType::Date32),
        sqlparser::datatype::DataType::Timestamp =>Ok(arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None)),
        sqlparser::datatype::DataType::Int16 => Ok(arrow::datatypes::DataType::Int16),
        sqlparser::datatype::DataType::Decimal(precision, scale) => match (precision,scale){
            (Some(precision),Some(scale)) if *precision == 0 || *precision > 76 || (*scale as i8).unsigned_abs() > (*precision as u8) => internal_err!("Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 76`, and `scale <= precision`."),
            (Some(precision),Some(scale)) if *precision > 38 && *precision <= 76  => Ok(arrow::datatypes::DataType::Decimal256(*precision, *scale)),
            (Some(precision),Some(scale)) if *precision <= 38 => Ok(arrow::datatypes::DataType::Decimal128(*precision, *scale)),
            (Some(precision),None) if *precision == 0 || *precision > 76 => internal_err!("Decimal(precision = {precision}) should satisfy `0 < precision <= 76`."),
            (Some(precision),None) if *precision <= 38 => Ok(arrow::datatypes::DataType::Decimal128(*precision, 0)),
            (Some(precision),None) if *precision > 38 && *precision <= 76 => Ok(arrow::datatypes::DataType::Decimal256(*precision, 0)),
            (None,None) => Ok(arrow::datatypes::DataType::Decimal128(38, 10)),
            _ => internal_err!("Cannot specify only scale for decimal data type")
        },
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use sqlparser::parser::Parser;

    use crate::{
        build_mem_datasource,
        common::table_relation::TableRelation,
        datasource::file::{self, csv::CsvReadOptions},
        datatypes::scalar::ScalarValue,
        functions::all_builtin_functions,
        utils,
    };

    use super::SqlQueryPlanner;

    #[test]
    fn test_udf() {
        quick_test(
            "SELECT my_udf(1, 2) AS result",
            "Internal Error: Unknown function: my_udf",
        );

        quick_test(
            "SELECT EXTRACT(YEAR FROM DATE '2022-09-08')",
            "Projection: (EXTRACT(Utf8('YEAR'), CAST(Utf8('2022-09-08') AS Date32)))\n  Empty Relation\n",
        );
    }

    #[test]
    fn test_aggregate() {
        quick_test(
            "SELECT SUM(id) FROM tbl ",
            "Projection: (SUM(tbl.id))\n  Aggregate: group_expr=[], aggregat_expr=[SUM(tbl.id)]\n    TableScan: tbl\n",
        );
    }

    #[test]
    fn test_insert() {
        quick_test(
            "INSERT INTO tbl VALUES (1), (2), (3);",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(column1 AS Int32) AS id, CAST(Utf8('default_name') AS Utf8) AS name, CAST(null AS Int32) AS age)\n    Values: [[Int64(1)], [Int64(2)], [Int64(3)]]\n",
        );
        // insert with not exists column
        quick_test("INSERT INTO tbl(noexists,id,name) VALUES (1,1,'')", "Arrow Error: Schema error: Unable to get field named \"noexists\". Valid fields: [\"id\", \"name\", \"age\"]");
        // insert the result of a query into a table
        quick_test(
            "INSERT INTO tbl SELECT * FROM other_tbl;",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (other_tbl.age, other_tbl.id, other_tbl.name)\n    TableScan: other_tbl\n",
        );
        // insert values into the "i" column, inserting the default value into other columns
        quick_test(
            "INSERT INTO tbl(id,age) VALUES (1,10), (2,12), (3,13);",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(column1 AS Int32) AS id, CAST(Utf8('default_name') AS Utf8) AS name, CAST(column2 AS Int32) AS age)\n    Values: [[Int64(1), Int64(10)], [Int64(2), Int64(12)], [Int64(3), Int64(13)]]\n",
        );
    }

    #[test]
    fn test_update() {
        quick_test("UPDATE tbl SET id=0 WHERE id IS NULL;", "Dml: op=[Update] table=[tbl]\n  Projection: (CAST(Int64(0) AS Int32) AS id, tbl.name, tbl.age)\n    Filter: tbl.id IS NULL\n      TableScan: tbl\n");

        quick_test("UPDATE tbl SET id = 1, name = 2;", "Dml: op=[Update] table=[tbl]\n  Projection: (CAST(Int64(1) AS Int32) AS id, CAST(Int64(2) AS Utf8) AS name, tbl.age)\n    TableScan: tbl\n");
    }

    #[test]
    fn test_delete() {
        quick_test("DELETE FROM tblx", "Table Not Found: tblx");

        quick_test(
            "DELETE FROM tbl",
            "Dml: op=[Delete From] table=[tbl]\n  TableScan: tbl\n",
        );

        quick_test(
            "DELETE FROM tbl WHERE id = 1",
            "Dml: op=[Delete From] table=[tbl]\n  Filter: tbl.id = Int64(1)\n    TableScan: tbl\n",
        );
    }

    #[test]
    fn test_drop_table() {
        quick_test("DROP TABLE tblx;", "Table Not Found: tblx");

        quick_test("DROP TABLE IF EXISTS tbl;", "DropTable: [tbl]\n");
    }

    #[test]
    fn test_create_table() {
        // create a table with two integer columns (i and j)
        quick_test(
            "CREATE TABLE t1(i INTEGER, j INTEGER);",
            "CreateMemoryTable: [t1]\n  Empty Relation\n",
        );
        // create a table from the result of a query
        quick_test(
            "CREATE TABLE t1 AS SELECT 42 AS i, 84 AS j;",
            "CreateMemoryTable: [t1]\n  Projection: (Int64(42) AS i, Int64(84) AS j)\n    Empty Relation\n",
        );
        // create a table from a CSV file using AUTO-DETECT (i.e., automatically detecting column names and types)
        quick_test(
            "CREATE TABLE t1 AS SELECT * FROM read_csv('./tests/testdata/file/case1.csv');", 
    "CreateMemoryTable: [t1]\n  Projection: (tmp_table(b563e59).id, tmp_table(b563e59).localtion, tmp_table(b563e59).name)\n    TableScan: tmp_table(b563e59)\n"
        );
        // omit 'SELECT *'
        quick_test(
            "CREATE TABLE t1 AS FROM read_csv('./tests/testdata/file/case1.csv');", 
            "CreateMemoryTable: [t1]\n  Projection: (tmp_table(b563e59).id, tmp_table(b563e59).localtion, tmp_table(b563e59).name)\n    TableScan: tmp_table(b563e59)\n"
        );
    }

    #[test]
    fn test_read_parquet() {
        quick_test(
            "select * from read_parquet('./tests/testdata/file/case2.parquet') where counter_id = '1'",
            "Projection: (tmp_table(17b774f).counter_id, tmp_table(17b774f).currency, tmp_table(17b774f).market, tmp_table(17b774f).type)\n  Filter: tmp_table(17b774f).counter_id = Utf8('1')\n    TableScan: tmp_table(17b774f)\n",
        );
    }

    #[test]
    fn test_read_csv() {
        quick_test(
            "SELECT * FROM read_csv('./tests/testdata/file/case1.csv')",
            "Projection: (tmp_table(b563e59).id, tmp_table(b563e59).localtion, tmp_table(b563e59).name)\n  TableScan: tmp_table(b563e59)\n",
        );
    }

    #[test]
    fn test_empty_relation() {
        quick_test("SELECT 1", "Projection: (Int64(1))\n  Empty Relation\n");

        quick_test("SELECT -1", "Projection: (- Int64(1))\n  Empty Relation\n");
    }

    #[test]
    fn test_select_column() {
        quick_test(
            "SELECT id,name FROM person",
            "Projection: (person.id, person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT person.id,person.name FROM person",
            "Projection: (person.id, person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT id as a,name as b FROM person",
            "Projection: (person.id AS a, person.name AS b)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT address FROM person",
            "Internal Error: Column \"address\" not found in any table",
        );

        quick_test(
            "SELECT id,id FROM person",
            "Projection: (person.id, person.id)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person",
            "Projection: (person.age, person.first_name, person.id, person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT *,id FROM person",
            "Projection: (person.age, person.first_name, person.id, person.name, person.id)\n  TableScan: person\n",
        );

        quick_test("SELECT t.id FROM person as t", "Projection: (t.id)\n  TableScan: t\n");

        quick_test(
            "SELECT t.* FROM person as t",
            "Projection: (t.id, t.name, t.first_name, t.age)\n  TableScan: t\n",
        );
    }

    #[test]
    fn test_where() {
        quick_test(
            "SELECT id,name FROM person WHERE id = 1",
            "Projection: (person.id, person.name)\n  Filter: person.id = Int64(1)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person WHERE id = 2",
            "Projection: (person.age, person.first_name, person.id, person.name)\n  Filter: person.id = Int64(2)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person as t WHERE t.id = 2",
            "Projection: (t.age, t.first_name, t.id, t.name)\n  Filter: t.id = Int64(2)\n    TableScan: t\n",
        );
    }

    #[test]
    fn test_join() {
        quick_test(
            "SELECT p.id FROM person as p,a,b",
            "Projection: (p.id)\n  CrossJoin\n    CrossJoin\n      TableScan: p\n      TableScan: a\n    TableScan: b\n",
        );

        quick_test(
            "SELECT p.id,a.id FROM person as p,a,b",
            "Projection: (p.id, a.id)\n  CrossJoin\n    CrossJoin\n      TableScan: p\n      TableScan: a\n    TableScan: b\n",
        );

        quick_test(
            "SELECT * FROM person,b,a",
            "Projection: (person.age, person.first_name, a.id, b.id, person.id, a.name, b.name, person.name)\n  CrossJoin\n    CrossJoin\n      TableScan: person\n      TableScan: b\n    TableScan: a\n",
        );

        quick_test("SELECT id FROM person,b", "Internal Error: Column \"id\" is ambiguous");

        quick_test(
            "SELECT * FROM person,b WHERE id = 1",
            "Internal Error: Column \"id\" is ambiguous",
        );

        quick_test(
            "SELECT * FROM person as p,a WHERE p.id = 1",
            "Projection: (p.age, p.first_name, a.id, p.id, a.name, p.name)\n  Filter: p.id = Int64(1)\n    CrossJoin\n      TableScan: p\n      TableScan: a\n",
        );

        quick_test(
            "SELECT person.id, person.first_name \
        FROM person LEFT JOIN orders \
        ON person.age > 10",
            "Projection: (person.id, person.first_name)\n  Left Join: Filter: person.age > Int64(10)\n    TableScan: person\n    TableScan: orders\n",
        );

        quick_test(
            "SELECT person.id, person.first_name \
        FROM person RIGHT JOIN orders \
        ON person.age > 10",
            "Projection: (person.id, person.first_name)\n  Right Join: Filter: person.age > Int64(10)\n    TableScan: person\n    TableScan: orders\n",
        );

        quick_test(
            "SELECT person.id, person.first_name \
        FROM person INNER JOIN orders \
        ON person.age > 10",
            "Projection: (person.id, person.first_name)\n  Inner Join: Filter: person.age > Int64(10)\n    TableScan: person\n    TableScan: orders\n",
        );

        quick_test(
            "SELECT person.id, person.first_name \
        FROM person FULL JOIN orders \
        ON person.age > 10",
            "Projection: (person.id, person.first_name)\n  Full Join: Filter: person.age > Int64(10)\n    TableScan: person\n    TableScan: orders\n",
        );
    }

    #[test]
    fn test_with() {
        quick_test(
            "WITH t1 AS (SELECT * FROM person) SELECT * FROM t1",
            "Projection: (t1.age, t1.first_name, t1.id, t1.name)\n  SubqueryAlias: t1\n    Projection: (person.age, person.first_name, person.id, person.name)\n      TableScan: person\n",
        );
    }

    #[test]
    fn test_group_by() {
        let sql = "SELECT name,max(name) FROM person GROUP BY name";
        let expected = "Projection: (person.name, MAX(person.name))\n  Aggregate: group_expr=[person.name], aggregat_expr=[MAX(person.name)]\n    TableScan: person\n";
        quick_test(sql, expected);

        let sql = "SELECT name, COUNT(*) FROM person GROUP BY name";
        let expected = "Projection: (person.name, COUNT(*))\n  Aggregate: group_expr=[person.name], aggregat_expr=[COUNT(*)]\n    TableScan: person\n";
        quick_test(sql, expected);

        let sql = "SELECT * FROM person GROUP BY name";
        let expected = "Internal Error: column [person.age] must appear in the GROUP BY clause or be used in an aggregate function";
        quick_test(sql, expected);
    }

    #[test]
    fn test_order_by() {
        quick_test(
            "SELECT name FROM person ORDER BY name",
            "Sort: person.name ASC\n  Projection: (person.name)\n    TableScan: person\n",
        );

        // FIXME
        // quick_test(
        //     "SELECT name as a FROM person ORDER BY a",
        //     "Sort: a ASC\n  Projection: (person.name)\n    TableScan: person\n",
        // );
    }

    #[test]
    fn test_limit() {
        quick_test(
            "select id from person where person.id > 100 LIMIT 5;",
            "Limit: fetch=5, skip=0\n  Projection: (person.id)\n    Filter: person.id > Int64(100)\n      TableScan: person\n",
        );

        quick_test("SELECT id FROM person WHERE person.id > 100 OFFSET 10 LIMIT 5;", 
        "Limit: fetch=5, skip=10\n  Projection: (person.id)\n    Filter: person.id > Int64(100)\n      TableScan: person\n");

        quick_test(
            "SELECT id FROM person WHERE person.id > 100 OFFSET 5;",
            "Limit: fetch=None, skip=5\n  Projection: (person.id)\n    Filter: person.id > Int64(100)\n      TableScan: person\n",
        )
    }

    fn quick_test(sql: &str, expected: &str) {
        let mut tables = HashMap::new();

        tables.insert(
            "person".into(),
            build_mem_datasource!(
                ("id", DataType::Int32, false),
                ("name", DataType::Utf8, false),
                ("first_name", DataType::Utf8, false),
                ("age", DataType::Int32, false)
            ),
        );
        tables.insert(
            "a".into(),
            build_mem_datasource!(("id", DataType::Int32, false), ("name", DataType::Utf8, false),),
        );
        tables.insert(
            "b".into(),
            build_mem_datasource!(("id", DataType::Int32, false), ("name", DataType::Utf8, false),),
        );
        tables.insert(
            "orders".into(),
            build_mem_datasource!(
                ("id", DataType::Int32, false),
                ("name", DataType::Utf8, false),
                ("age", DataType::Int32, false)
            ),
        );

        let mut default_values = HashMap::new();
        default_values.insert("name".into(), ScalarValue::from("default_name"));

        tables.insert(
            "tbl".into(),
            build_mem_datasource!(
                default_values,
                [
                    ("id", DataType::Int32, false),
                    ("name", DataType::Utf8, false),
                    ("age", DataType::Int32, true),
                ]
            ),
        );

        tables.insert(
            "other_tbl".into(),
            build_mem_datasource!(
                ("id", DataType::Int32, false),
                ("name", DataType::Utf8, false),
                ("age", DataType::Int32, true),
            ),
        );

        tables.insert(
            TableRelation::parse_file_path("./tests/testdata/file/case1.csv"),
            file::csv::read_csv("./tests/testdata/file/case1.csv", CsvReadOptions::default()).unwrap(),
        );

        tables.insert(
            TableRelation::parse_file_path("./tests/testdata/file/case2.parquet"),
            file::parquet::read_parquet("./tests/testdata/file/case2.parquet").unwrap(),
        );

        let stmt = Parser::new(sql).parse().unwrap();
        let udfs = all_builtin_functions()
            .into_iter()
            .map(|udf| (udf.name().to_uppercase().to_string(), udf))
            .collect();
        let plan = SqlQueryPlanner::create_logical_plan(stmt, tables, &udfs);
        match plan {
            Ok(plan) => assert_eq!(utils::format(&plan, 0), expected),
            Err(err) => assert_eq!(err.to_string(), expected),
        }
    }
}
