use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{
    Assignment, BinaryOperator, Cte, Expression, From, FunctionArgument, Literal, Order, Select, SelectItem, Statement,
};

use crate::{
    common::{join_type::JoinType, table_relation::TableRelation},
    datasource::{
        file::{
            csv::{self, CsvReadOptions},
            parquet::read_parquet,
        },
        DataSource,
    },
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    logical::{
        expr::*,
        plan::{
            self, CreateMemoryTable, DdlStatement, DmlStatement, DropTable, Filter, LogicalPlan, SubqueryAlias, Values,
        },
        LogicalPlanBuilder,
    },
    utils,
};

use self::alias::Alias;

use crate::planner::normalize_col_with_schemas_and_ambiguity_check;

pub struct SqlQueryPlanner {
    ctes: HashMap<String, Arc<LogicalPlan>>,
    relations: HashMap<TableRelation, Arc<dyn DataSource>>,
}

impl SqlQueryPlanner {
    pub fn create_logical_plan(
        stmt: Statement,
        relations: HashMap<TableRelation, Arc<dyn DataSource>>,
    ) -> Result<LogicalPlan> {
        let mut planner = SqlQueryPlanner {
            ctes: HashMap::default(),
            relations,
        };

        match stmt {
            Statement::Select(select) => planner.select_to_plan(*select),
            Statement::CreateTable {
                table,
                check_exists,
                columns,
                query,
            } => {
                let input = if let Some(query) = query {
                    planner.select_to_plan(query)?
                } else {
                    LogicalPlan::EmptyRelation(plan::EmptyRelation {
                        schema: Arc::new(Schema::empty()),
                        produce_one_row: true,
                    })
                };

                planner.create_table_to_plan(input, table, columns, check_exists)
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
                            Ok(
                                LogicalExpr::Literal(default_value.cloned().unwrap_or(ScalarValue::Null))
                                    .cast_to(target_field.data_type())
                                    .alias(target_field.name()),
                            )
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
        columns: Vec<sqlparser::ast::Column>,
        check_exists: bool,
    ) -> Result<LogicalPlan> {
        if check_exists {
            if self.relations.contains_key(&table.clone().into()) {
                return Err(Error::InternalError(format!("table [{}] already exists", table)));
            }
        }

        let schema = Arc::new(Schema::new(
            columns
                .into_iter()
                .map(|col| {
                    let name = col.name.clone();
                    let data_type = sql_to_arrow_data_type(&col.datatype);
                    Ok(Field::new(&name, data_type, col.nullable))
                })
                .collect::<Result<Vec<_>>>()?,
        ));

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
        let plan = self.filter_expr(plan, select.r#where)?;
        // process the SELECT expressions
        let column_exprs = self.column_exprs(&plan, empty_from, select.columns)?;
        // process the HAVING clause
        let having = if let Some(having_expr) = select.having {
            Some(self.sql_to_expr(having_expr)?)
        } else {
            None
        };
        // process the GROUP BY clause
        let plan = if let Some(group_by) = select.group_by {
            self.aggregate_plan(plan, &column_exprs, group_by, having)?
        } else {
            plan
        };

        let plan = LogicalPlanBuilder::project(plan, column_exprs)?;

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
                // handle cross join
                let mut plans = froms.into_iter().map(|f| self.table_scan_to_plan(vec![f]));

                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);

                for right in plans {
                    left = left.cross_join(right?)?;
                }

                Ok(left.build())
            }
        }
    }

    fn table_func_to_plan(&mut self, name: String, args: Vec<FunctionArgument>) -> Result<LogicalPlan> {
        let (table_name, source) = match name.to_lowercase().as_str() {
            "read_csv" => {
                let (path, options) = self.parse_csv_options(args)?;
                ("tmp_csv_table", csv::read_csv(path, options)?)
            }
            "read_parquet" => {
                let path = match args.get(0) {
                    Some(FunctionArgument {
                        value: Expression::Literal(Literal::String(s)),
                        ..
                    }) => s,
                    _ => {
                        return Err(Error::InternalError(
                            "read_parquet function requires the first argument to be a string".to_owned(),
                        ))
                    }
                };

                ("tmp_parquet_table", read_parquet(path)?)
            }
            "read_json" => todo!(),
            _ => todo!(),
        };

        LogicalPlanBuilder::scan(table_name, source.clone(), None).map(|builder| builder.build())
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

    fn parse_csv_options(&self, mut args: Vec<FunctionArgument>) -> Result<(String, CsvReadOptions)> {
        if args.len() == 0 {
            return Err(Error::InternalError(
                "read_csv function requires at least one argument".to_owned(),
            ));
        }

        // first argument is the path
        let path = match args.remove(0).value {
            Expression::Literal(Literal::String(s)) => s,
            _ => {
                return Err(Error::InternalError(
                    "read_csv function requires the first argument to be a string".to_owned(),
                ))
            }
        };

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

        Ok((path, options))
    }

    fn aggregate_plan(
        &self,
        input: LogicalPlan,
        column_exprs: &[LogicalExpr],
        group_by: Vec<Expression>,
        _having_expr: Option<LogicalExpr>,
    ) -> Result<LogicalPlan> {
        // get aggregate expressions
        let aggr_exprs = column_exprs
            .iter()
            .filter_map(|expr| match expr {
                LogicalExpr::AggregateExpr(aggr) => Some(aggr.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let group_exprs = group_by
            .into_iter()
            .map(|expr| {
                self.sql_to_expr(expr)
                    .and_then(|expr| normalize_col_with_schemas_and_ambiguity_check(expr, &[&input.relation()]))
            })
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggr_exprs)
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
                    name: idents.remove(1).value,
                    relation: Some(idents.remove(0).value.into()),
                }))
            }
            Expression::Identifier(ident) => Ok(column(&ident.value)),
            Expression::Literal(lit) => match lit {
                Literal::Int(i) => Ok(LogicalExpr::Literal(ScalarValue::Int64(Some(i)))),
                Literal::Float(f) => Ok(LogicalExpr::Literal(ScalarValue::Float64(Some(f)))),
                Literal::String(s) => Ok(LogicalExpr::Literal(ScalarValue::Utf8(Some(s)))),
                Literal::Boolean(b) => Ok(LogicalExpr::Literal(ScalarValue::Boolean(Some(b)))),
                Literal::Null => Ok(LogicalExpr::Literal(ScalarValue::Null)),
            },
            Expression::BinaryOperator(op) => self.parse_binary_op(op),
            Expression::Function(name, args) => {
                let mut exprs = args
                    .into_iter()
                    .map(|expr| self.sql_function_args_to_expr(expr))
                    .collect::<Result<Vec<_>>>()?;

                match name.to_uppercase().as_str() {
                    "VERSION" => {
                        if !exprs.is_empty() {
                            return Err(Error::InternalError(format!(
                                "VERSION function should not have any arguments"
                            )));
                        }
                        Ok(LogicalExpr::Literal(ScalarValue::Utf8(Some(utils::version()))))
                    }
                    _ => Ok(LogicalExpr::AggregateExpr(AggregateExpr {
                        op: name.into(),
                        expr: Box::new(exprs.pop().ok_or(Error::InternalError(format!(
                            "Aggreate function should have at latest one expr"
                        )))?),
                    })),
                }
            }
            _ => todo!(),
        }
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
            BinaryOperator::Gt(l, r) => gt(self.sql_to_expr(*l)?, self.sql_to_expr(*r)?),
            _ => todo!(),
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
                            .all_fields()
                            .into_iter()
                            .map(|field| {
                                Ok(column(field.name()))
                                // normalize_col_with_schemas_and_ambiguity_check(
                                //     column(field.name()),
                                //     &self.relations,
                                // )
                            })
                            .collect();
                }

                Err(Error::InternalError(format!(
                    "Invalid qualified wildcard: {}",
                    quanlified_prefix
                )))
            }
        }
    }
}

impl SqlQueryPlanner {
    pub fn new(relations: HashMap<TableRelation, Arc<dyn DataSource>>) -> Self {
        SqlQueryPlanner {
            ctes: HashMap::default(),
            relations,
        }
    }

    fn get_table_source(&self, table_name: &str) -> Result<Arc<dyn DataSource>> {
        self.relations
            .get(&table_name.into())
            .map(|a| a.clone())
            .ok_or(Error::TableNotFound(table_name.to_owned()))
    }

    fn get_cte_table(&self, name: &str) -> Option<LogicalPlan> {
        self.ctes.get(name).map(|a| a.as_ref().clone())
    }
}

fn sql_to_arrow_data_type(data_type: &sqlparser::datatype::DataType) -> arrow::datatypes::DataType {
    match data_type {
        sqlparser::datatype::DataType::Integer => arrow::datatypes::DataType::Int32,
        sqlparser::datatype::DataType::Boolean => arrow::datatypes::DataType::Boolean,
        sqlparser::datatype::DataType::Float => arrow::datatypes::DataType::Float64,
        sqlparser::datatype::DataType::String => arrow::datatypes::DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use sqlparser::parser::Parser;

    use crate::{build_mem_datasource, datatypes::scalar::ScalarValue, utils};

    use super::SqlQueryPlanner;

    #[test]
    fn test_insert() {
        quick_test(
            "INSERT INTO tbl VALUES (1), (2), (3);",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(column1 AS Int32) AS id, CAST(Utf8(default_name) AS Utf8) AS name, CAST(null AS Int32) AS age)\n    Values: [[Int64(1)], [Int64(2)], [Int64(3)]]\n",
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
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(column1 AS Int32) AS id, CAST(Utf8(default_name) AS Utf8) AS name, CAST(column2 AS Int32) AS age)\n    Values: [[Int64(1), Int64(10)], [Int64(2), Int64(12)], [Int64(3), Int64(13)]]\n",
        );
    }

    #[test]
    fn test_update() {
        quick_test("UPDATE tbl SET id=0 WHERE id IS NULL;", "Dml: op=[Update] table=[tbl]\n  Projection: (CAST(Int64(0) AS Int32) AS id, tbl.name, tbl.age)\n    Filter: tbl.id\n      TableScan: tbl\n");

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
        quick_test("CREATE TABLE t1 AS SELECT * FROM read_csv('./tests/testdata/file/case1.csv');", "CreateMemoryTable: [t1]\n  Projection: (tmp_csv_table.id, tmp_csv_table.localtion, tmp_csv_table.name)\n    TableScan: tmp_csv_table\n");
        // omit 'SELECT *'
        quick_test("CREATE TABLE t1 AS FROM read_csv('./tests/testdata/file/case1.csv');", "CreateMemoryTable: [t1]\n  Projection: (tmp_csv_table.id, tmp_csv_table.localtion, tmp_csv_table.name)\n    TableScan: tmp_csv_table\n");
    }

    #[test]
    fn test_read_parquet() {
        quick_test(
            "select * from read_parquet('./tests/testdata/file/case2.parquet') where counter_id = '1'",
            "Projection: (tmp_parquet_table.counter_id, tmp_parquet_table.currency, tmp_parquet_table.market, tmp_parquet_table.type)\n  Filter: tmp_parquet_table.counter_id = CAST(Utf8(1) AS LargeUtf8)\n    TableScan: tmp_parquet_table\n",
        );
    }

    #[test]
    fn test_table_function() {
        quick_test(
            "SELECT * FROM read_csv('tests/testdata/file/case1.csv')",
            "Projection: (tmp_csv_table.id, tmp_csv_table.localtion, tmp_csv_table.name)\n  TableScan: tmp_csv_table\n",
        );
    }

    #[test]
    fn test_empty_relation() {
        quick_test("SELECT 1", "Projection: (Int64(1))\n  Empty Relation\n");
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
        let expected = "Arrow Error: Schema error: Unable to get field named \"age\". Valid fields: [\"name\"]";
        quick_test(sql, expected);
    }

    #[test]
    fn test_order_by() {
        quick_test(
            "SELECT name FROM person ORDER BY name",
            "Sort: person.name ASC\n  Projection: (person.name)\n    TableScan: person\n",
        );
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

        let stmt = Parser::new(sql).parse().unwrap();
        let plan = SqlQueryPlanner::create_logical_plan(stmt, tables);
        match plan {
            Ok(plan) => assert_eq!(utils::format(&plan, 0), expected),
            Err(err) => assert_eq!(err.to_string(), expected),
        }
    }
}
