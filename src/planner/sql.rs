use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use sqlparser::{
    ast::{Assignment, BinaryOperator, Expression, From, Literal, SelectItem, Statement},
    parser::Parser,
};

use crate::{
    common::{OwnedTableRelation, TableRelation},
    datasource::file::csv::{self, CsvReadOptions},
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    execution::registry::TableRegistry,
    logical::{
        expr::*,
        plan::{Filter, LogicalPlan},
        LogicalPlanBuilder,
    },
};

use self::alias::Alias;

use super::{normalize_col_with_schemas_and_ambiguity_check, TableSchemaInfo};

pub struct SqlQueryPlanner<'a> {
    table_registry: &'a mut dyn TableRegistry,
}

impl<'a> SqlQueryPlanner<'a> {
    pub fn new(table_registry: &'a mut dyn TableRegistry) -> Self {
        SqlQueryPlanner { table_registry }
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan> {
        let stmts = Parser::new(sql)
            .parse()
            .map_err(|e| Error::SQLParseError(e))?;

        let mut context = PlannerContext::default();

        match stmts {
            Statement::Select {
                distinct,
                columns,
                from,
                r#where,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // process `from` clause
                let mut empty_from = false;
                let plan = if let Some(f) = from {
                    self.table_scan_to_plan(f, &mut context)?
                } else {
                    empty_from = true;
                    LogicalPlanBuilder::empty().build()
                };
                // process the WHERE clause
                let plan = self.filter_expr(plan, r#where, &mut context)?;
                // process the SELECT expressions
                let column_exprs = self.column_exprs(&plan, empty_from, columns, &context)?;

                LogicalPlanBuilder::project(plan, column_exprs)
            }
            _ => todo!(),
        }
    }

    fn create_logical_expr(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn select_to_plan(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn table_scan_to_plan(&mut self, from: From, ctx: &mut PlannerContext) -> Result<LogicalPlan> {
        let (plan, alias) = match from {
            From::Table { name, alias } => {
                let relation: TableRelation = name.clone().into();
                let scan = self
                    .table_registry
                    .get_table_source(&name)
                    .map(|table_source| {
                        LogicalPlanBuilder::scan(relation.clone(), table_source, None).build()
                    })?;

                ctx.relations.push(TableSchemaInfo {
                    relation,
                    schema: scan.schema(),
                    alias: alias.clone(),
                });

                (scan, alias)
            }
            From::TableFunction { name, args, alias } => {
                (self.table_func_to_plan(name, args)?, alias)
            }
            _ => todo!(),
        };

        if let Some(alias) = alias {
            self.apply_expr_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    fn table_func_to_plan(&mut self, name: String, args: Vec<Assignment>) -> Result<LogicalPlan> {
        match name.to_lowercase().as_str() {
            "read_csv" => {
                let (path, options) = self.parse_csv_options(args)?;
                let table_name = "tmp_csv_table";
                let table_srouce = csv::read_csv(path, options)?;
                let plan = LogicalPlanBuilder::scan(table_name, table_srouce.clone(), None).build();
                // register the table to the table registry
                // TODO: we should use a unique name for the table and apply the alias
                self.table_registry
                    .register_table(table_name, table_srouce)?;

                Ok(plan)
            }
            "read_json" => todo!(),

            _ => todo!(),
        }
    }

    fn parse_csv_options(&self, mut args: Vec<Assignment>) -> Result<(String, CsvReadOptions)> {
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
                        return Err(Error::InternalError(
                            "Expected a single character".to_owned(),
                        ));
                    }
                    Ok(s.as_bytes()[0])
                }
                _ => Err(Error::InternalError("Expected a string literal".to_owned())),
            }
        };
        let extract_value = |expr: Expression| -> Result<Literal> {
            match expr {
                Expression::Literal(lit) => Ok(lit),
                _ => Err(Error::InternalError(
                    "Expected a boolean literal".to_owned(),
                )),
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
                        a.try_into().map_err(|e| {
                            Error::InternalError(format!("Parse CsvOptions error, {}", e))
                        })
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
}

impl<'a> SqlQueryPlanner<'a> {
    fn apply_expr_alias(&self, plan: LogicalPlan, alias: String) -> Result<LogicalPlan> {
        let fields = plan.schema().fields().clone();
        let exprs = fields.into_iter().map(|field| {
            LogicalExpr::Alias(Alias::new(
                alias.clone(),
                LogicalExpr::Column(Column::new(field.name(), None::<OwnedTableRelation>)),
            ))
        });

        LogicalPlanBuilder::project(plan, exprs)
    }

    fn column_exprs(
        &self,
        plan: &LogicalPlan,
        empty_from: bool,
        columns: Vec<SelectItem>,
        ctx: &PlannerContext,
    ) -> Result<Vec<LogicalExpr>> {
        let schema = plan.schema();
        columns
            .into_iter()
            .flat_map(
                |col| match self.sql_select_item_to_expr(col, empty_from, &schema, ctx) {
                    Ok(vec) => vec.into_iter().map(Ok).collect(),
                    Err(err) => vec![Err(err)],
                },
            )
            .collect::<Result<Vec<LogicalExpr>>>()
    }

    fn filter_expr(
        &self,
        mut plan: LogicalPlan,
        expr: Option<Expression>,
        ctx: &PlannerContext,
    ) -> Result<LogicalPlan> {
        if let Some(filter) = expr {
            let filter_expr = self.sql_to_expr(filter, ctx)?;

            // we should parse filter first and then apply it to the table scan
            match &mut plan {
                LogicalPlan::TableScan(table) => {
                    table.filter = Some(filter_expr.clone());
                }
                _ => {}
            }

            Ok(LogicalPlan::Filter(Filter::new(plan, filter_expr)))
        } else {
            Ok(plan)
        }
    }

    fn sql_to_expr(&self, expr: Expression, ctx: &PlannerContext) -> Result<LogicalExpr> {
        match expr {
            Expression::Identifier(ident) => normalize_col_with_schemas_and_ambiguity_check(
                LogicalExpr::Column(Column::new(ident, None::<OwnedTableRelation>)),
                &ctx.relations,
            ),
            Expression::Literal(lit) => match lit {
                Literal::Int(i) => Ok(LogicalExpr::Literal(ScalarValue::Int64(Some(i)))),
                Literal::Float(f) => Ok(LogicalExpr::Literal(ScalarValue::Float64(Some(f)))),
                Literal::String(s) => Ok(LogicalExpr::Literal(ScalarValue::Utf8(Some(s)))),
                Literal::Boolean(b) => Ok(LogicalExpr::Literal(ScalarValue::Boolean(Some(b)))),
                Literal::Null => Ok(LogicalExpr::Literal(ScalarValue::Null)),
            },
            Expression::BinaryOperator(op) => self.parse_binary_op(op, ctx),
            _ => todo!("{:?}", expr),
        }
    }

    fn parse_binary_op(&self, op: BinaryOperator, ctx: &PlannerContext) -> Result<LogicalExpr> {
        Ok(match op {
            BinaryOperator::Eq(l, r) => eq(self.sql_to_expr(*l, ctx)?, self.sql_to_expr(*r, ctx)?),
            _ => todo!(),
        })
    }

    fn sql_select_item_to_expr(
        &self,
        item: SelectItem,
        empty_relation: bool,
        schema: &SchemaRef,
        ctx: &PlannerContext,
    ) -> Result<Vec<LogicalExpr>> {
        match item {
            SelectItem::UnNamedExpr(expr) => self
                .sql_to_expr(expr, ctx)
                .and_then(|expr| {
                    normalize_col_with_schemas_and_ambiguity_check(expr, &ctx.relations)
                })
                .map(|v| vec![v]),
            SelectItem::ExprWithAlias(expr, alias) => self
                .sql_to_expr(expr, ctx)
                .map(|col| vec![LogicalExpr::Alias(Alias::new(alias, col))]),
            SelectItem::Wildcard => {
                if empty_relation {
                    return Err(Error::InternalError(
                        "SELECT * with no tables specified is not valid".to_owned(),
                    ));
                }
                // expand schema
                schema
                    .all_fields()
                    .into_iter()
                    .map(|field| {
                        normalize_col_with_schemas_and_ambiguity_check(
                            column(field.name()),
                            &ctx.relations,
                        )
                    })
                    .collect()
            }
            SelectItem::QualifiedWildcard(_) => {
                todo!()
            }
        }
    }
}

#[derive(Debug, Default)]
struct PlannerContext<'a> {
    relations: Vec<TableSchemaInfo<'a>>,
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, SchemaBuilder};

    use crate::{
        datasource::{memory::MemoryDataSource, DataSource},
        error::Result,
        execution::registry::TableRegistry,
        utils,
    };

    use super::SqlQueryPlanner;

    struct TestTableRegistry {
        tables: HashMap<String, Box<dyn DataSource>>,
    }

    impl TestTableRegistry {
        fn new() -> Self {
            TestTableRegistry {
                tables: HashMap::new(),
            }
        }
    }

    impl TableRegistry for TestTableRegistry {
        fn register_table(&mut self, _name: &str, _table: Arc<dyn DataSource>) -> Result<()> {
            Ok(())
        }

        fn get_table_source(&self, _name: &str) -> Result<Arc<dyn DataSource>> {
            let mut schema = SchemaBuilder::default();

            schema.push(Field::new("id", DataType::Int32, false));
            schema.push(Field::new("name", DataType::Utf8, false));

            Ok(Arc::new(MemoryDataSource::new(
                Arc::new(schema.finish()),
                vec![],
            )))
        }
    }

    #[test]
    fn test_table_function() {
        quick_test(
            "SELECT * FROM read_csv('tests/testdata/file/case1.csv')",
            "Projection: (tmp_csv_table.id,tmp_csv_table.name,tmp_csv_table.localtion)\n  TableScan: tmp_csv_table\n",
        );
    }

    #[test]
    fn test_empty_relation() {
        quick_test("SELECT 1", "Projection: (Int64(1))\n  Empty Relation\n");
    }

    #[test]
    fn test_select_column() {
        quick_test(
            "SELECT id,name FROM t",
            "Projection: (t.id,t.name)\n  TableScan: t\n",
        );

        quick_test(
            "SELECT age FROM t",
            "Internal Error: Column \"age\" not found in any table",
        );

        quick_test(
            "SELECT id,id FROM t",
            "Projection: (t.id,t.id)\n  TableScan: t\n",
        );

        quick_test(
            "SELECT * FROM person",
            "Projection: (person.id,person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT *,id FROM person",
            "Projection: (person.id,person.name,person.id)\n  TableScan: person\n",
        );

        quick_test("SELECT t.id FROM person as t", "Projection: (t.id)\n  Projection: (Alias: t.id,Alias: t.name)\n    TableScan: person\n");
    }

    #[test]
    fn test_where() {
        quick_test(
            "SELECT id,name FROM t WHERE id = 1",
            "Projection: (t.id,t.name)\n  Filter: t.id = Int64(1)\n    TableScan: t\n",
        )
    }

    fn quick_test(sql: &str, expected: &str) {
        let mut registry = TestTableRegistry::new();
        let mut planner = SqlQueryPlanner::new(&mut registry);
        match planner.create_logical_plan(sql) {
            Ok(plan) => assert_eq!(utils::format(&plan, 0), expected),
            Err(err) => assert_eq!(err.to_string(), expected),
        }
    }
}
