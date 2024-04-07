use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use sqlparser::{
    ast::{
        Assignment, BinaryOperator, Cte, Expression, From, Literal, Select, SelectItem, Statement,
    },
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
        plan::{Filter, LogicalPlan, SubqueryAlias},
        LogicalPlanBuilder,
    },
};

use self::alias::Alias;

use super::{normalize_col_with_schemas_and_ambiguity_check, TableSchemaInfo};

#[derive(Debug, Default, Clone)]
struct PlannerContext<'a> {
    ctes: HashMap<String, Arc<LogicalPlan>>,
    relations: HashMap<TableRelation<'a>, TableSchemaInfo>,
}

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
            Statement::Select(select) => self.select_to_plan(*select, &mut context),
            _ => todo!(),
        }
    }

    fn create_logical_expr(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn select_to_plan(
        &mut self,
        select: Select,
        mut context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if let Some(with) = select.with {
            self.cte_tables(&mut context, with.cte_tables)?;
        }

        // process `from` clause
        let plan = self.table_scan_to_plan(select.from, &mut context)?;
        let empty_from = match plan {
            LogicalPlan::EmptyRelation(_) => true,
            _ => false,
        };
        // process the WHERE clause
        let plan = self.filter_expr(plan, select.r#where, &mut context)?;
        // process the SELECT expressions
        let column_exprs = self.column_exprs(&plan, empty_from, select.columns, &context)?;

        LogicalPlanBuilder::project(plan, column_exprs)
    }

    fn table_scan_to_plan(
        &mut self,
        mut froms: Vec<From>,
        ctx: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match froms.len() {
            0 => Ok(LogicalPlanBuilder::empty().build()),
            1 => {
                let (plan, alias) = match froms.remove(0) {
                    From::Table { name, alias } => {
                        let relation: TableRelation = name.clone().into();

                        // try to get ctes table first and the from table registey
                        let scan = ctx
                            .ctes
                            .get(&name)
                            .map(|a| a.as_ref().clone())
                            .ok_or(Error::InternalError(format!(
                                "Can't get cte tables: {}",
                                name
                            )))
                            .or(self.table_registry.get_table_source(&name).and_then(
                                |table_source| {
                                    LogicalPlanBuilder::scan(relation.clone(), table_source, None)
                                        .map(|l| l.build())
                                },
                            ))?;

                        // put relation into context
                        // we will use it in normalize_col_with_schemas_and_ambiguity_check
                        ctx.relations.insert(
                            relation,
                            TableSchemaInfo {
                                schema: scan.schema(),
                                alias: alias.clone(),
                            },
                        );

                        (scan, alias)
                    }
                    From::TableFunction { name, args, alias } => {
                        (self.table_func_to_plan(ctx, name, args)?, alias)
                    }
                    _ => todo!(),
                };

                if let Some(alias) = alias {
                    self.apply_table_alias(ctx, plan, alias)
                } else {
                    Ok(plan)
                }
            }
            _ => {
                // handle cross join
                let mut plans = froms
                    .into_iter()
                    .map(|f| self.table_scan_to_plan(vec![f], ctx));

                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);

                for right in plans {
                    left = left.cross_join(right?)?;
                }

                Ok(left.build())
            }
        }
    }

    fn table_func_to_plan(
        &mut self,
        ctx: &mut PlannerContext,
        name: String,
        args: Vec<Assignment>,
    ) -> Result<LogicalPlan> {
        match name.to_lowercase().as_str() {
            "read_csv" => {
                let (path, options) = self.parse_csv_options(args)?;
                let table_name = "tmp_csv_table";
                let table_srouce = csv::read_csv(path, options)?;
                let plan =
                    LogicalPlanBuilder::scan(table_name, table_srouce.clone(), None)?.build();
                // register the table to the table registry
                // TODO: we should use a unique name for the table and apply the alias
                self.table_registry
                    .register_table(table_name, table_srouce)?;

                ctx.relations.insert(
                    table_name.into(),
                    TableSchemaInfo {
                        schema: plan.schema(),
                        alias: None,
                    },
                );

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
    fn cte_tables(&mut self, ctx: &mut PlannerContext, ctes: Vec<Cte>) -> Result<()> {
        for cte in ctes {
            let plan = self
                .select_to_plan(*cte.query, &mut ctx.clone())
                .and_then(|plan| self.apply_table_alias(ctx, plan, cte.alias.clone()))
                .map(|plan| {
                    let schema = plan.schema();
                    Arc::new(LogicalPlan::SubqueryAlias(SubqueryAlias {
                        input: Arc::new(plan),
                        alias: cte.alias.clone(),
                        schema,
                    }))
                })?;

            ctx.relations.insert(
                cte.alias.clone().into(),
                TableSchemaInfo {
                    schema: plan.schema(),
                    alias: Some(cte.alias.clone()),
                },
            );

            ctx.ctes.insert(cte.alias, plan);
        }

        Ok(())
    }

    fn apply_table_alias(
        &self,
        ctx: &mut PlannerContext,
        plan: LogicalPlan,
        alias: String,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan(mut table) => {
                let mut relation =
                    ctx.relations
                        .remove(&table.relation)
                        .ok_or(Error::TableNotFound(format!(
                        "Can't apply table alias: {} to relation: {}, because relation not exists",
                        alias, table.relation
                    )))?;

                let new_relation: TableRelation = alias.clone().into();
                // replace table relation in context with alias
                relation.alias = Some(alias);
                ctx.relations.insert(new_relation.clone(), relation);
                // apply alias to table scan plan
                table.relation = new_relation;

                Ok(LogicalPlan::TableScan(table))
            }
            _ => Ok(plan),
        }
    }

    fn column_exprs(
        &self,
        plan: &LogicalPlan,
        empty_from: bool,
        columns: Vec<SelectItem>,
        ctx: &PlannerContext,
    ) -> Result<Vec<LogicalExpr>> {
        columns
            .into_iter()
            .flat_map(
                |col| match self.sql_select_item_to_expr(ctx, plan, col, empty_from) {
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
        ctx: &PlannerContext,
        plan: &LogicalPlan,
        item: SelectItem,
        empty_relation: bool,
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
                let mut using_columns: HashMap<TableRelation<'_>, HashSet<String>> =
                    HashMap::default();
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

                if ctx.relations.contains_key(&quanlified_prefix) {
                    return // expand schema
                        plan.schema()
                            .all_fields()
                            .into_iter()
                            .map(|field| {
                                normalize_col_with_schemas_and_ambiguity_check(
                                    column(field.name()),
                                    &ctx.relations,
                                )
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

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, SchemaBuilder};

    use crate::{
        datasource::{memory::MemoryDataSource, DataSource},
        error::{Error, Result},
        execution::registry::TableRegistry,
        test_utils::build_mem_datasource,
        utils,
    };

    use super::SqlQueryPlanner;

    struct TestTableRegistry {
        tables: HashMap<String, Arc<dyn DataSource>>,
    }

    impl TestTableRegistry {
        fn new() -> Self {
            let mut tables = HashMap::new();

            tables.insert(
                "person".to_owned(),
                build_mem_datasource(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ],
                    vec![],
                ),
            );

            tables.insert(
                "a".to_owned(),
                build_mem_datasource(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ],
                    vec![],
                ),
            );

            tables.insert(
                "b".to_owned(),
                build_mem_datasource(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ],
                    vec![],
                ),
            );

            TestTableRegistry { tables }
        }
    }

    impl TableRegistry for TestTableRegistry {
        fn register_table(&mut self, _name: &str, _table: Arc<dyn DataSource>) -> Result<()> {
            Ok(())
        }

        fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
            self.tables
                .get(name)
                .cloned()
                .ok_or(Error::TableNotFound(name.to_owned()))
        }
    }

    #[test]
    fn test_table_function() {
        quick_test(
            "SELECT * FROM read_csv('tests/testdata/file/case1.csv')",
            "Projection: (tmp_csv_table.id,tmp_csv_table.localtion,tmp_csv_table.name)\n  TableScan: tmp_csv_table\n",
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
            "Projection: (person.id,person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT person.id,person.name FROM person",
            "Projection: (person.id,person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT id as a,name as b FROM person",
            "Projection: (person.id AS a,person.name AS b)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT age FROM person",
            "Internal Error: Column \"age\" not found in any table",
        );

        quick_test(
            "SELECT id,id FROM person",
            "Projection: (person.id,person.id)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person",
            "Projection: (person.id,person.name)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT *,id FROM person",
            "Projection: (person.id,person.name,person.id)\n  TableScan: person\n",
        );

        quick_test(
            "SELECT t.id FROM person as t",
            "Projection: (t.id)\n  TableScan: t\n",
        );

        quick_test(
            "SELECT t.* FROM person as t",
            "Projection: (t.id,t.name)\n  TableScan: t\n",
        );
    }

    #[test]
    fn test_where() {
        quick_test(
            "SELECT id,name FROM person WHERE id = 1",
            "Projection: (person.id,person.name)\n  Filter: person.id = Int64(1)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person WHERE id = 2",
            "Projection: (person.id,person.name)\n  Filter: person.id = Int64(2)\n    TableScan: person\n",
        );

        quick_test(
            "SELECT * FROM person as t WHERE t.id = 2",
            "Projection: (t.id,t.name)\n  Filter: t.id = Int64(2)\n    TableScan: t\n",
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
            "Projection: (p.id,a.id)\n  CrossJoin\n    CrossJoin\n      TableScan: p\n      TableScan: a\n    TableScan: b\n",
        );

        quick_test(
            "SELECT * FROM person,b,a",
            "Projection: (a.id,b.id,person.id,a.name,b.name,person.name)\n  CrossJoin\n    CrossJoin\n      TableScan: person\n      TableScan: b\n    TableScan: a\n",
        );

        quick_test(
            "SELECT id FROM person,b",
            "Internal Error: Column \"id\" is ambiguous",
        );

        quick_test(
            "SELECT * FROM person,b WHERE id = 1",
            "Internal Error: Column \"id\" is ambiguous",
        );

        quick_test(
            "SELECT * FROM person as p,a WHERE p.id = 1",
            "Projection: (a.id,p.id,a.name,p.name)\n  Filter: p.id = Int64(1)\n    CrossJoin\n      TableScan: p\n      TableScan: a\n",
        );
        // TODO left join
        quick_test(
            "SELECT person.id, person.first_name \
        FROM person LEFT JOIN orders \
        ON person.age > 10",
            "Projection: person.id, person.first_name\
        \n  Left Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders",
        );
        // TODO right join
        quick_test(
            "SELECT person.id, person.first_name \
        FROM person RIGHT JOIN orders \
        ON person.age > 10",
            "Projection: person.id, person.first_name\
        \n  Right Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders",
        );
        // TODO inner join
        quick_test(
            "SELECT person.id, person.first_name \
        FROM person INNER JOIN orders \
        ON person.age > 10",
            "Projection: person.id, person.first_name\
        \n  Inner Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders",
        );
        // TODO full join
        quick_test(
            "SELECT person.id, person.first_name \
        FROM person FULL JOIN orders \
        ON person.age > 10",
            "Projection: person.id, person.first_name\
        \n  Full Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders",
        );
    }

    #[test]
    fn test_with() {
        quick_test(
            "WITH t1 AS (SELECT * FROM person) SELECT * FROM t1",
            "Projection: (t1.id,t1.name)\n  SubqueryAlias: t1\n    Projection: (person.id,person.name)\n      TableScan: person\n",
        );
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
