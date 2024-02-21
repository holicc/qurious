use std::sync::Arc;

use sqlparser::{
    ast::{Expression, From, Literal, Statement},
    parser::Parser,
};

use crate::{
    common::TableRelation,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    execution::registry::TableRegistry,
    logical::{
        expr::{column, Column, LogicalExpr},
        plan::LogicalPlan,
        LogicalPlanBuilder,
    },
};

pub struct SqlQueryPlanner {
    table_registry: Arc<dyn TableRegistry>,
}

impl SqlQueryPlanner {
    pub fn new(table_registry: Arc<dyn TableRegistry>) -> Self {
        SqlQueryPlanner { table_registry }
    }

    pub fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let stmts = Parser::new(sql)
            .parse()
            .map_err(|e| Error::SQLParseError(e))?;

        match stmts {
            Statement::CreateTable {
                table,
                check_exists,
                columns,
            } => todo!(),
            Statement::CreateSchema {
                schema,
                check_exists,
            } => todo!(),
            Statement::DropTable {
                table,
                check_exists,
            } => todo!(),
            Statement::DropSchema {
                schema,
                check_exists,
            } => todo!(),
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
                let (plan, relation) = if let Some(f) = from {
                    self.table_scan_to_plan(f)?
                } else {
                    empty_from = true;
                    (LogicalPlanBuilder::empty().build(), None)
                };

                let column_exprs = columns
                    .into_iter()
                    .map(|(col, alias)| match col {
                        Expression::Identifier(ident) => {
                            // normalize column name with table name
                            plan.schema()
                                .field_with_name(&ident)
                                .map_err(|e| Error::ArrowError(e))
                                .and(Ok(LogicalExpr::Column(Column::new(
                                    ident,
                                    relation.clone(),
                                ))))
                        }
                        Expression::Literal(lit) => match lit {
                            Literal::Int(i) => {
                                Ok(LogicalExpr::Literal(ScalarValue::Int64(Some(i))))
                            }
                            Literal::Float(f) => {
                                Ok(LogicalExpr::Literal(ScalarValue::Float64(Some(f))))
                            }
                            Literal::String(s) => {
                                Ok(LogicalExpr::Literal(ScalarValue::Utf8(Some(s))))
                            }
                            Literal::Boolean(b) => {
                                Ok(LogicalExpr::Literal(ScalarValue::Boolean(Some(b))))
                            }
                            Literal::Null => Ok(LogicalExpr::Literal(ScalarValue::Null)),
                        },
                        _ => todo!(),
                    })
                    .collect::<Result<Vec<_>>>()?;
                // process the SELECT expressions
                LogicalPlanBuilder::project(plan, column_exprs)
            }
            Statement::Insert {
                table,
                columns,
                values,
                on_conflict,
                returning,
            } => todo!(),
            Statement::Update {
                table,
                assignments,
                r#where,
            } => todo!(),
            Statement::Delete { table, r#where } => todo!(),
        }
    }

    fn create_logical_expr(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn select_to_plan(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn table_scan_to_plan(&self, from: From) -> Result<(LogicalPlan, Option<TableRelation>)> {
        match from {
            From::Table { name, alias } => {
                let builder = self
                    .table_registry
                    .get_table_source(&name)
                    .map(|table_source| LogicalPlanBuilder::scan(&name, table_source))?;

                if let Some(alias) = alias {
                    Ok((self.apply_table_alias(builder.build(), alias)?, None))
                } else {
                    Ok((builder.build(), Some(name.into())))
                }
            }
            From::TableFunction { name, args, alias } => todo!(),
            From::SubQuery { query, alias } => todo!(),
            From::Join {
                left,
                right,
                on,
                join_type,
            } => todo!(),
        }
    }

    fn apply_table_alias(&self, plan: LogicalPlan, alias: String) -> Result<LogicalPlan> {
        todo!("apply_table_alias")
    }

    fn project(
        plan: LogicalPlan,
        columns: Vec<(Expression, Option<String>)>,
    ) -> Result<LogicalPlan> {
        let fields = columns.into_iter().map(|(expr, alias)| {
            LogicalExpr::Column(Column {
                name: expr.to_string(),
                relation: None,
            })
        });

        LogicalPlanBuilder::project(plan, fields)
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

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
    fn test_empty_relation() {
        quick_test("SELECT 1", "");
    }

    #[test]
    fn test_project_plan() {
        quick_test(
            "SELECT id,name FROM t",
            "Projection: (t.id,t.name)\n  TableScan: t\n",
        );
    }

    fn quick_test(sql: &str, expected: &str) {
        let planner = SqlQueryPlanner::new(Arc::new(TestTableRegistry::new()));
        let plan = planner.create_logical_plan(sql).unwrap();

        assert_eq!(utils::format(&plan, 0), expected)
    }
}
