use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit};
use sqlparser::ast::{
    Assignment, BinaryOperator, CopyOption, CopySource, CopyTarget, Cte, Expression, From, FunctionArgument, Ident,
    Literal, Order, Select, SelectItem, Statement,
};

use crate::{
    common::{
        join_type::JoinType,
        table_relation::TableRelation,
        transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion},
    },
    datasource::file::{self, csv::CsvReadOptions},
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
    utils::{get_file_type, normalize_ident},
};

use self::alias::Alias;

#[derive(Default, Debug)]
struct Context {
    ctes: HashMap<String, LogicalPlan>,
    relations: HashMap<TableRelation, SchemaRef>,
    /// table alias -> original table name
    table_aliase: HashMap<String, TableRelation>,
    columns_alias: HashMap<String, LogicalExpr>,
}

pub struct SqlQueryPlanner<'a> {
    contexts: Vec<Context>,
    // TODO move those field to PlannerContext trait
    udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
}

// export the public functions
impl<'a> SqlQueryPlanner<'a> {
    pub fn new(
        relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
        udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    ) -> Self {
        SqlQueryPlanner {
            contexts: vec![Context::default()],
            relations,
            udfs,
        }
    }

    pub fn create_logical_plan(
        stmt: Statement,
        relations: HashMap<TableRelation, Arc<dyn TableProvider>>,
        udfs: &'a HashMap<String, Arc<dyn UserDefinedFunction>>,
    ) -> Result<LogicalPlan> {
        let mut planner = SqlQueryPlanner::new(relations, udfs);

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
            Statement::ShowTables => {
                // Handle the ShowTables statement explicitly
                // This could involve returning an appropriate error or handling it in a way that aligns with the application's logic
                internal_err!("ShowTables statement is not supported in this context")
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
            } => {
                if to {
                    planner.copy_to_plan(source, target, options)
                } else {
                    planner.copy_from_plan(source, target, options)
                }
            }
            _ => todo!(),
        }
    }
}

// functions for Context
impl<'a> SqlQueryPlanner<'a> {
    fn add_relation(&mut self, relation: TableRelation, schema: SchemaRef, alias: Option<String>) -> Result<()> {
        let context = self.current_context();
        context.relations.insert(relation.clone(), schema);

        if let Some(alias) = alias {
            if context.table_aliase.contains_key(&alias) {
                return internal_err!("Table alias {} already exists", alias);
            }
            context.table_aliase.insert(alias, relation);
        }

        Ok(())
    }

    fn get_table_source(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        self.relations
            .get(&table_name.into())
            .map(|a| a.clone())
            .ok_or(Error::TableNotFound(table_name.to_owned()))
    }

    fn get_cte_table(&self, name: &str) -> Option<LogicalPlan> {
        self.contexts
            .iter()
            .rev()
            .find_map(|context| context.ctes.get(name).cloned())
    }

    fn add_cte_table(&mut self, name: String, plan: LogicalPlan) -> Result<()> {
        let context = self.current_context();
        if context.ctes.contains_key(&name) {
            return internal_err!("CTE with name {} already exists", name);
        }

        context.ctes.insert(name, plan);
        Ok(())
    }

    fn current_context(&mut self) -> &mut Context {
        self.contexts.last_mut().expect("Context stack is empty")
    }

    fn new_context_scope<U, F>(&mut self, f: F) -> Result<U>
    where
        F: FnOnce(&mut Self) -> Result<U>,
    {
        self.contexts.push(Context::default());
        let result = f(self);
        self.contexts.pop();
        result
    }

    fn get_relation(&self, column_name: &str) -> Result<Option<TableRelation>> {
        // find from relations
        let mut matched = self
            .contexts
            .iter()
            .flat_map(|ctx| &ctx.relations)
            .filter_map(|(relation, provider)| provider.column_with_name(column_name).map(|_| relation.clone()))
            .collect::<Vec<_>>();

        match matched.len() {
            0 => Ok(None),
            1 => Ok(matched.pop()),
            _ => internal_err!("Column \"{}\" is ambiguous", column_name,),
        }
    }

    /// find the relation of the table
    fn find_relation(&self, table: &TableRelation) -> Option<TableRelation> {
        self.contexts.iter().rev().find_map(|ctx| {
            ctx.relations
                .contains_key(table)
                .then(|| table.clone())
                .or(ctx.table_aliase.get(&table.to_string()).cloned())
        })
    }

    fn add_column_alias(&mut self, name: String, expr: LogicalExpr) -> Result<()> {
        let context = self.current_context();
        if context.columns_alias.contains_key(&name) {
            return internal_err!("Column alias {} already exists", name);
        }
        context.columns_alias.insert(name, expr);
        Ok(())
    }

    fn get_column_alias(&self, name: &str) -> Option<LogicalExpr> {
        self.contexts
            .iter()
            .rev()
            .find_map(|ctx| ctx.columns_alias.get(name).cloned())
    }
}

// functions for Binder
impl<'a> SqlQueryPlanner<'a> {
    fn copy_to_plan(
        &mut self,
        _source: CopySource,
        _target: CopyTarget,
        _options: Vec<CopyOption>,
    ) -> Result<LogicalPlan> {
        todo!()
    }

    fn copy_from_plan(
        &mut self,
        source: CopySource,
        target: CopyTarget,
        options: Vec<CopyOption>,
    ) -> Result<LogicalPlan> {
        let (relation, table_source, columns) = match source {
            CopySource::Table { table_name, columns } => {
                let table_name = table_name.to_string();
                let table_source = self.get_table_source(&table_name)?;

                if columns.is_empty() {
                    (table_name.into(), table_source, vec![])
                } else {
                    (table_name.into(), table_source, columns)
                }
            }
            CopySource::Query(_) => return internal_err!("COPY FROM query is not supported"),
        };

        let file_path = match target {
            CopyTarget::File { file } => file,
        };
        let option_map = options
            .into_iter()
            .map(|option| match option {
                CopyOption::Format(ident) => ("format", ident.to_string()),
                CopyOption::Delimiter(ch) => ("delimiter", ch.to_string()),
                CopyOption::Header(_) => ("has_header", "true".to_owned()),
            })
            .collect::<HashMap<_, _>>();
        let file_extension = option_map
            .get("format")
            .cloned()
            .or(get_file_type(&file_path).map(|s| s.to_string()))
            .unwrap_or_default();
        let target_relation = TableRelation::parse_file_path(&file_path);
        let input = match file_extension.to_lowercase().as_str() {
            "csv" => {
                let mut csv_options = CsvReadOptions::default();
                csv_options.delimiter = option_map.get("delimiter").map(|s| s.as_bytes()[0]).unwrap_or(b',');
                csv_options.has_header = option_map.get("has_header").map(|s| s == "true").unwrap_or(true);

                file::csv::read_csv(file_path, csv_options)
                    .and_then(|table| LogicalPlanBuilder::scan(target_relation.clone(), table, None))
                    .map(|builder| builder.build())?
            }
            _ => return internal_err!("COPY FROM only supports csv files"),
        };

        self.insert_plan(relation, table_source, input, columns)
    }

    fn update_to_plan(
        &mut self,
        table: String,
        assignments: Vec<Assignment>,
        r#where: Option<Expression>,
    ) -> Result<LogicalPlan> {
        let table_source = self.get_table_source(&table)?;
        let table_schema = table_source.schema();
        let relation: TableRelation = table.into();

        self.add_relation(relation.clone(), table_schema.clone(), None)?;

        let input = LogicalPlanBuilder::scan(relation.clone(), table_source, None)
            .map(|builder| builder.build())
            .and_then(|plan| self.filter_expr(plan, r#where))?;

        // update set a = 1
        // key: target_column value: expr
        let mut assign_map: HashMap<String, LogicalExpr> = assignments
            .into_iter()
            .map(|assign| {
                let name = assign.target.to_string();
                let value = self.sql_to_expr(assign.value)?;
                Ok((name, value))
            })
            .collect::<Result<_>>()?;

        // zip table relation with schema field
        let exprs = table_schema
            .fields()
            .iter()
            .map(|f| {
                assign_map
                    .remove(f.name())
                    .map(|expr| expr.cast_to(f.data_type()).alias(f.name()))
                    .unwrap_or(LogicalExpr::Column(Column::new(f.name(), Some(relation.clone()))))
            })
            .collect::<Vec<_>>();

        let plan = LogicalPlanBuilder::project(input, exprs)?;

        Ok(LogicalPlan::Dml(DmlStatement {
            relation,
            op: plan::DmlOperator::Update,
            schema: table_schema,
            input: Box::new(plan),
        }))
    }

    fn delete_to_plan(&mut self, table: String, r#where: Option<Expression>) -> Result<LogicalPlan> {
        let table_source = self.get_table_source(&table)?;
        let table_schema = table_source.schema();
        let relation: TableRelation = table.into();

        self.add_relation(relation.clone(), table_schema.clone(), None)?;

        let plan = LogicalPlanBuilder::scan(relation.clone(), table_source, None)?.build();
        let plan = self.filter_expr(plan, r#where)?;

        Ok(LogicalPlan::Dml(DmlStatement {
            relation,
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
        // build source value plan
        let source = if let Some(query) = query {
            self.select_to_plan(query)?
        } else {
            self.values_to_plan(values)?
        };

        self.insert_plan(
            table.into(),
            table_source,
            source,
            columns
                .into_iter()
                .map(|expr| match expr {
                    Expression::Identifier(name) => Ok(name),
                    _ => internal_err!(
                        "INSERT statement requires column name to be an identifier, but got: {}",
                        expr
                    ),
                })
                .collect::<Result<_>>()?,
        )
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
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
        // process the WHERE clause
        let plan = self.filter_expr(plan, select.r#where)?;
        // process the SELECT expressions
        let column_exprs = self.column_exprs(&plan, empty_from, select.columns)?;
        // sort exprs
        let sort_exprs = self.order_by_exprs(select.order_by.unwrap_or_default())?;
        // process the HAVING clause
        let having = select.having.map(|expr| self.sql_to_expr(expr)).transpose()?;
        // get aggregate expressions
        let aggr_exprs = find_aggregate_exprs(column_exprs.iter().chain(having.iter()));
        // process the GROUP BY clause or process aggregation in SELECT
        let (mut plan, select_exprs_post_aggr, having_expr_post_aggr) =
            if select.group_by.is_some() || !aggr_exprs.is_empty() {
                let group_by_exprs = select
                    .group_by
                    .unwrap_or_default()
                    .into_iter()
                    .map(|expr| {
                        let col = self.sql_to_expr(expr)?;

                        col.transform(|expr| match expr {
                            LogicalExpr::Column(col) => {
                                if col.relation.is_none() {
                                    if let Some(data) = self.get_column_alias(&col.name) {
                                        return Ok(Transformed::yes(data));
                                    }
                                }
                                Ok(Transformed::no(LogicalExpr::Column(col)))
                            }
                            _ => Ok(Transformed::no(expr)),
                        })
                        .data()
                    })
                    .collect::<Result<_>>()?;

                let having = having
                    .map(|expr| {
                        expr.transform(|expr| match expr {
                            LogicalExpr::Column(col) => {
                                if col.relation.is_none() {
                                    if let Some(data) = self.get_column_alias(&col.name) {
                                        return Ok(Transformed::yes(data));
                                    }
                                }
                                Ok(Transformed::no(LogicalExpr::Column(col)))
                            }
                            _ => Ok(Transformed::no(expr)),
                        })
                        .data()
                    })
                    .transpose()?;

                self.aggregate_plan(plan, column_exprs.clone(), aggr_exprs, group_by_exprs, having)?
            } else {
                match having {
                    Some(having_expr) => {
                        // // allow scalar having
                        // having_expr.apply(|expr| {

                        // })
                        return internal_err!(
                    "HAVING clause [{having_expr}] requires a GROUP BY clause or be used in an aggregate function"
                );
                    }
                    None => (plan, column_exprs, None),
                }
            };
        // process the HAVE clause
        if let Some(having_expr) = having_expr_post_aggr {
            plan = LogicalPlanBuilder::from(plan)
                .having(having_expr)
                .map(|builder| builder.build())?;
        }
        // do the final projection
        plan = LogicalPlanBuilder::project(plan, select_exprs_post_aggr)?;
        // process the ORDER BY clause
        let plan = if !sort_exprs.is_empty() {
            LogicalPlanBuilder::from(plan)
                .sort(sort_exprs)
                .map(|builder| builder.build())?
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

                        self.add_relation(relation, scan.schema(), alias.clone())?;

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

                self.add_relation(relation.clone(), provider.schema(), None)?;

                (relation, provider)
            }
            _ => todo!(),
        };

        LogicalPlanBuilder::scan(table_name, provider, None).map(|builder| builder.build())
    }

    fn filter_expr(&self, mut plan: LogicalPlan, expr: Option<Expression>) -> Result<LogicalPlan> {
        if let Some(filter) = expr {
            let filter_expr = self.sql_to_expr(filter)?;
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

    fn apply_table_alias(&mut self, input: LogicalPlan, alias: String) -> Result<LogicalPlan> {
        SubqueryAlias::try_new(input, &alias).map(LogicalPlan::SubqueryAlias)
    }

    fn insert_plan(
        &mut self,
        target_relation: TableRelation,
        target_table_provider: Arc<dyn TableProvider>,
        value_source: LogicalPlan,
        columns: Vec<Ident>,
    ) -> Result<LogicalPlan> {
        let table_schema = target_table_provider.schema();
        // if value_indices[i] = Some(j), it means that the value of the i-th target table's column is
        // derived from the j-th output of the source.
        //
        // if value_indices[i] = None, it means that the value of the i-th target table's column is
        // not provided, and should be filled with a default value later.
        let (fields, value_indices) = if columns.is_empty() {
            (
                table_schema.fields().iter().map(|f| f.as_ref()).collect::<Vec<_>>(),
                (0..table_schema.fields().len()).map(Some).collect::<Vec<_>>(),
            )
        } else {
            let mut value_indices = vec![None; table_schema.fields().len()];
            let fields = columns
                .into_iter()
                .enumerate()
                .map(|(i, name)| {
                    let col_name = normalize_ident(name);
                    let index = table_schema.index_of(&col_name)?;

                    if value_indices[index].is_some() {
                        return internal_err!("Column [{}] is specified more than once", col_name);
                    }
                    value_indices[index] = Some(i);
                    Ok(table_schema.field(index))
                })
                .collect::<Result<Vec<_>>>()?;

            (fields, value_indices)
        };
        // check if the source has the same number of columns as the target table
        if value_source.schema().fields().len() != fields.len() {
            return internal_err!(
                "statement requires the {} columns, but got {} columns",
                fields.len(),
                value_source.schema().fields().len(),
            );
        }
        // check if all columns have values, if not, fill with default values
        let source_schema = value_source.schema();
        let exprs = value_indices
            .into_iter()
            .enumerate()
            .map(|(i, value_index)| {
                let target_field = table_schema.field(i);
                match value_index {
                    Some(v) => {
                        let target_field = table_schema.field(v);
                        Ok(column(source_schema.field(v).name())
                            .cast_to(target_field.data_type())
                            .alias(target_field.name()))
                    }
                    None => {
                        let default_value = target_table_provider.get_column_default(target_field.name());
                        if !target_field.is_nullable() && default_value.is_none() {
                            return internal_err!(
                                "Column [{}] does not have a default value and does not allow NULLs",
                                target_field.name()
                            );
                        }
                        Ok(LogicalExpr::Literal(default_value.unwrap_or(ScalarValue::Null))
                            .cast_to(target_field.data_type())
                            .alias(target_field.name()))
                    }
                }
            })
            .collect::<Result<Vec<LogicalExpr>>>()?;

        LogicalPlanBuilder::project(value_source, exprs).map(|input| {
            LogicalPlan::Dml(DmlStatement {
                relation: target_relation,
                op: plan::DmlOperator::Insert,
                schema: table_schema,
                input: Box::new(input),
            })
        })
    }

    fn aggregate_plan(
        &self,
        input: LogicalPlan,
        select_exprs: Vec<LogicalExpr>,
        aggr_exprs: Vec<LogicalExpr>,
        group_exprs: Vec<LogicalExpr>,
        having: Option<LogicalExpr>,
    ) -> Result<(LogicalPlan, Vec<LogicalExpr>, Option<LogicalExpr>)> {
        let agg_and_group_by_column_exprs = aggr_exprs.iter().chain(group_exprs.iter()).collect::<Vec<_>>();

        let select_exprs_post_aggr = select_exprs
            .into_iter()
            .map(|expr| expr.rebase_expr(&agg_and_group_by_column_exprs))
            .collect::<Result<Vec<_>>>()?;
        let having_expr_post_aggr = having
            .map(|expr| expr.rebase_expr(&agg_and_group_by_column_exprs))
            .transpose()?;

        let agg_and_group_columns = agg_and_group_by_column_exprs
            .iter()
            .map(|expr| expr.as_column())
            .collect::<Result<Vec<_>>>()?;

        let mut check_columns = select_exprs_post_aggr
            .iter()
            .flat_map(find_columns_exprs)
            .collect::<Vec<_>>();

        if let Some(having_expr) = &having_expr_post_aggr {
            check_columns.extend(find_columns_exprs(having_expr));
        }

        for col_expr in check_columns {
            if !agg_and_group_columns.contains(&col_expr) {
                return internal_err!("column [{}] must appear in the GROUP BY clause or be used in an aggregate function, validate columns: [{}]",
                    col_expr,
                    agg_and_group_columns
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", "));
            }
        }

        let plan = LogicalPlanBuilder::from(input)
            .aggregate(group_exprs, aggr_exprs)
            .map(|plan| plan.build())?;

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
    }

    fn order_by_exprs(&self, order_by: Vec<(Expression, Order)>) -> Result<Vec<SortExpr>> {
        order_by
            .into_iter()
            .map(|(sort_expr, order)| {
                self.sql_to_expr(sort_expr).map(|expr| SortExpr {
                    expr: Box::new(expr),
                    asc: order == Order::Asc,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn cte_tables(&mut self, ctes: Vec<Cte>) -> Result<()> {
        ctes.into_iter().try_for_each(|cte| {
            self.new_context_scope(|planner| planner.select_to_plan(*cte.query))
                .and_then(|plan| self.apply_table_alias(plan, cte.alias.clone()))
                .and_then(|plan| self.add_cte_table(cte.alias, plan))
        })
    }

    fn column_exprs(
        &mut self,
        plan: &LogicalPlan,
        empty_from: bool,
        columns: Vec<SelectItem>,
    ) -> Result<Vec<LogicalExpr>> {
        columns
            .into_iter()
            .flat_map(|col| match self.sql_select_item_to_expr(plan, col, empty_from) {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
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

                let name = normalize_ident(idents.remove(1));
                let relation = idents.remove(0).value.into();
                if self.find_relation(&relation).is_none() {
                    return internal_err!(
                        "Column [\"{}\"] not found in table [\"{}\"] or table not exists",
                        name,
                        relation
                    );
                }

                Ok(LogicalExpr::Column(Column {
                    name,
                    relation: Some(relation),
                }))
            }
            Expression::Identifier(ident) => {
                let col_name = normalize_ident(ident);
                self.get_relation(&col_name)
                    .map(|relation| LogicalExpr::Column(Column::new(col_name, relation)))
            }
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

    fn handle_function(&self, name: &str, mut args: Vec<LogicalExpr>) -> Result<LogicalExpr> {
        if let Some(udf) = self.udfs.get(name.to_uppercase().as_str()) {
            return Ok(LogicalExpr::Function(Function {
                func: udf.clone(),
                args,
            }));
        }

        if let Ok(op) = name.try_into() {
            return Ok(LogicalExpr::AggregateExpr(AggregateExpr {
                op,
                expr: Box::new(args.pop().ok_or(Error::InternalError(
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
        &mut self,
        plan: &LogicalPlan,
        item: SelectItem,
        empty_relation: bool,
    ) -> Result<Vec<LogicalExpr>> {
        match item {
            SelectItem::UnNamedExpr(expr) => self.sql_to_expr(expr).map(|v| vec![v]),
            SelectItem::ExprWithAlias(expr, alias) => {
                let col = self.sql_to_expr(expr)?;
                self.add_column_alias(alias.clone(), col.clone())?;
                Ok(vec![LogicalExpr::Alias(Alias::new(alias, col))])
            }
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

                if self.find_relation(&quanlified_prefix).is_some() {
                    return plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            Ok(LogicalExpr::Column(Column::new(
                                field.name(),
                                Some(quanlified_prefix.clone()),
                            )))
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

fn find_columns_exprs(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    let mut columns = vec![];
    expr.apply(|nested_expr| {
        if let LogicalExpr::Column(_) = nested_expr {
            columns.push(nested_expr.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("[find_columns_exprs] should not fail");

    columns
}

fn find_aggregate_exprs<'a>(exprs: impl IntoIterator<Item = &'a LogicalExpr>) -> Vec<LogicalExpr> {
    exprs
        .into_iter()
        .flat_map(|expr| {
            let mut exprs = vec![];
            expr.apply(|nested_expr| {
                if let LogicalExpr::AggregateExpr(_) = nested_expr {
                    if !exprs.contains(nested_expr) {
                        exprs.push(nested_expr.clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("[find_aggregate_exprs] should not fail");

            exprs
        })
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

fn sql_to_arrow_data_type(data_type: &sqlparser::datatype::DataType) -> Result<arrow::datatypes::DataType> {
    match data_type {
        sqlparser::datatype::DataType::Integer => Ok(arrow::datatypes::DataType::Int64),
        sqlparser::datatype::DataType::Boolean => Ok(arrow::datatypes::DataType::Boolean),
        sqlparser::datatype::DataType::Float => Ok(arrow::datatypes::DataType::Float64),
        sqlparser::datatype::DataType::String => Ok(arrow::datatypes::DataType::Utf8),
        sqlparser::datatype::DataType::Date => Ok(arrow::datatypes::DataType::Date32),
        sqlparser::datatype::DataType::Timestamp => {
            Ok(arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        sqlparser::datatype::DataType::Int16 => Ok(arrow::datatypes::DataType::Int16),
        sqlparser::datatype::DataType::Int64 => Ok(arrow::datatypes::DataType::Int64),
        sqlparser::datatype::DataType::Decimal(precision, scale) => match (precision, scale) {
            // Check for invalid precision and scale
            (Some(precision), Some(scale))
                if *precision == 0 || *precision > 76 || (*scale as i64).abs() > *precision as i64 =>
            {
                internal_err!("Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 76`, and `scale <= precision`.")
            }
            // Decimal256 for precision > 38
            (Some(precision), Some(scale)) if *precision > 38 => {
                Ok(arrow::datatypes::DataType::Decimal256(*precision, *scale))
            }
            // Decimal128 for precision <= 38
            (Some(precision), Some(scale)) => Ok(arrow::datatypes::DataType::Decimal128(*precision, *scale)),
            // Handle precision without scale
            (Some(precision), None) if *precision == 0 || *precision > 76 => {
                internal_err!("Decimal(precision = {precision}) should satisfy `0 < precision <= 76`.")
            }
            (Some(precision), None) if *precision > 38 => Ok(arrow::datatypes::DataType::Decimal256(*precision, 0)),
            (Some(precision), None) => Ok(arrow::datatypes::DataType::Decimal128(*precision, 0)),
            // Default case for Decimal without precision and scale
            (None, None) => Ok(arrow::datatypes::DataType::Decimal128(38, 10)),
            // Invalid case where only scale is specified
            _ => internal_err!("Cannot specify only scale for decimal data type"),
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
    fn test_copy() {
        quick_test("COPY schools FROM './tests/testdata/file/case1.csv';", "Dml: op=[Insert Into] table=[schools]\n  Projection: (CAST(id AS Int64) AS id, CAST(name AS Utf8) AS name, CAST(location AS Utf8) AS location)\n    TableScan: tmp_table(b563e59)\n");

        quick_test(
            "COPY schools FROM './tests/testdata/file/case1.csv' (FORMAT CSV, HEADER, DELIMITER ',')",
            "Dml: op=[Insert Into] table=[schools]\n  Projection: (CAST(id AS Int64) AS id, CAST(name AS Utf8) AS name, CAST(location AS Utf8) AS location)\n    TableScan: tmp_table(b563e59)\n",
        );
    }

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
            "Internal Error: statement requires the 3 columns, but got 1 columns",
        );
        // insert with not exists column
        quick_test("INSERT INTO tbl(noexists,id,name) VALUES (1,1,'')", "Arrow Error: Schema error: Unable to get field named \"noexists\". Valid fields: [\"id\", \"name\", \"age\"]");
        // insert the result of a query into a table
        quick_test(
            "INSERT INTO tbl SELECT * FROM other_tbl;",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(age AS Int32) AS id, CAST(id AS Utf8) AS name, CAST(name AS Int32) AS age)\n    Projection: (other_tbl.age, other_tbl.id, other_tbl.name)\n      TableScan: other_tbl\n",
        );
        // insert values into the "i" column, inserting the default value into other columns
        quick_test(
            "INSERT INTO tbl(id,age) VALUES (1,10), (2,12), (3,13);",
            "Dml: op=[Insert Into] table=[tbl]\n  Projection: (CAST(column1 AS Int32) AS id, CAST(Utf8('default_name') AS Utf8) AS name, CAST(column2 AS Utf8) AS name)\n    Values: [[Int64(1), Int64(10)], [Int64(2), Int64(12)], [Int64(3), Int64(13)]]\n",
        );
    }

    #[test]
    fn test_update() {
        quick_test("UPDATE tbl SET id = 0 WHERE id IS NULL;", "Dml: op=[Update] table=[tbl]\n  Projection: (CAST(Int64(0) AS Int32) AS id, tbl.name, tbl.age)\n    Filter: tbl.id IS NULL\n      TableScan: tbl\n");

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
    "CreateMemoryTable: [t1]\n  Projection: (tmp_table(b563e59).id, tmp_table(b563e59).location, tmp_table(b563e59).name)\n    TableScan: tmp_table(b563e59)\n"
        );
        // omit 'SELECT *'
        quick_test(
            "CREATE TABLE t1 AS FROM read_csv('./tests/testdata/file/case1.csv');", 
            "CreateMemoryTable: [t1]\n  Projection: (tmp_table(b563e59).id, tmp_table(b563e59).location, tmp_table(b563e59).name)\n    TableScan: tmp_table(b563e59)\n"
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
            "Projection: (tmp_table(b563e59).id, tmp_table(b563e59).location, tmp_table(b563e59).name)\n  TableScan: tmp_table(b563e59)\n",
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
            "SELECT person.id,a.name as c FROM person as a",
            "Projection: (person.id, a.name AS c)\n  SubqueryAlias: a\n    TableScan: person\n",
        );

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
            "Arrow Error: Schema error: Unable to get field named \"address\". Valid fields: [\"id\", \"name\", \"first_name\", \"age\"]",
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

        quick_test(
            "SELECT t.id FROM person as t",
            "Projection: (t.id)\n  SubqueryAlias: t\n    TableScan: person\n",
        );

        quick_test(
            "SELECT t.* FROM person as t",
            "Projection: (t.id, t.name, t.first_name, t.age)\n  SubqueryAlias: t\n    TableScan: person\n",
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
            "Projection: (t.age, t.first_name, t.id, t.name)\n  Filter: t.id = Int64(2)\n    SubqueryAlias: t\n      TableScan: person\n",
        );
    }

    #[test]
    fn test_join() {
        quick_test(
            "SELECT p.id FROM person as p,a,b",
            "Projection: (p.id)\n  CrossJoin\n    CrossJoin\n      SubqueryAlias: p\n        TableScan: person\n      TableScan: a\n    TableScan: b\n",
        );

        quick_test(
            "SELECT p.id,a.id FROM person as p,a,b",
            "Projection: (p.id, a.id)\n  CrossJoin\n    CrossJoin\n      SubqueryAlias: p\n        TableScan: person\n      TableScan: a\n    TableScan: b\n",
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
            "Projection: (p.age, p.first_name, a.id, p.id, a.name, p.name)\n  Filter: p.id = Int64(1)\n    CrossJoin\n      SubqueryAlias: p\n        TableScan: person\n      TableScan: a\n",
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
        quick_test("SELECT name FROM person HAVING count(name) > 1", "Internal Error: column [person.name] must appear in the GROUP BY clause or be used in an aggregate function, validate columns: [COUNT(person.name)]");

        quick_test(
            "SELECT name FROM person WHERE name = 'abc' GROUP BY name HAVING count(name) > 1",
            "Projection: (person.name)\n  Filter: COUNT(person.name) > Int64(1)\n    Aggregate: group_expr=[person.name], aggregat_expr=[COUNT(person.name)]\n      Filter: person.name = Utf8('abc')\n        TableScan: person\n",
        );

        quick_test("SELECT name,max(name) FROM person GROUP BY name", "Projection: (person.name, MAX(person.name))\n  Aggregate: group_expr=[person.name], aggregat_expr=[MAX(person.name)]\n    TableScan: person\n");

        quick_test("SELECT name, COUNT(*) FROM person GROUP BY name", "Projection: (person.name, COUNT(*))\n  Aggregate: group_expr=[person.name], aggregat_expr=[COUNT(*)]\n    TableScan: person\n");

        quick_test("SELECT * FROM person GROUP BY name", "Internal Error: column [person.age] must appear in the GROUP BY clause or be used in an aggregate function, validate columns: [person.name]");
    }

    #[test]
    fn test_order_by() {
        quick_test(
            "SELECT name FROM person ORDER BY name asc, age desc",
            "Sort: person.name ASC, person.age DESC\n  Projection: (person.name)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT name FROM person ORDER BY name",
            "Sort: person.name ASC\n  Projection: (person.name)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT name as a FROM person ORDER BY a",
            "Sort: a ASC\n  Projection: (person.name AS a)\n    TableScan: person\n",
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
            "schools".into(),
            build_mem_datasource!(
                ("id", DataType::Int64, false),
                ("name", DataType::Utf8, false),
                ("location", DataType::Utf8, false)
            ),
        );

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

        let mut parser = Parser::new(sql);
        let stmt = parser.parse().unwrap();
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
