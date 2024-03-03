use std::{rc::Rc, sync::Arc};

use arrow::datatypes::SchemaRef;
use sqlparser::{
    ast::{BinaryOperator, Expression, From, Literal, SelectItem, Statement},
    parser::Parser,
};

use crate::{
    common::TableRelation,
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
    execution::registry::{ImmutableHashMapTableRegistry, TableRegistry},
    logical::{
        expr::*,
        plan::{Filter, LogicalPlan},
        LogicalPlanBuilder,
    },
};

use self::alias::Alias;

pub struct SqlQueryPlanner {
    table_registry: Box<dyn TableRegistry>,
}

impl SqlQueryPlanner {
    pub fn new(table_registry: Box<dyn TableRegistry>) -> Self {
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
                // TODO we should parse filter first and then apply it to the table scan

                // process `from` clause
                let (plan, relation) = if let Some(f) = from {
                    self.table_scan_to_plan(f)?
                } else {
                    (LogicalPlanBuilder::empty().build(), None)
                };

                // process the WHERE clause
                let plan = self.filter_expr(plan, r#where, &relation)?;

                // process the SELECT expressions
                let column_exprs = self.column_exprs(&plan, &relation, columns)?;

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
                alias: None,
                relation: None,
            })
        });

        LogicalPlanBuilder::project(plan, fields)
    }
}

impl SqlQueryPlanner {
    fn column_exprs(
        &self,
        plan: &LogicalPlan,
        relation: &Option<TableRelation>,
        columns: Vec<SelectItem>,
    ) -> Result<Vec<LogicalExpr>> {
        let mut result = vec![];
        let schema = plan.schema();

        for col in columns {
            let mut exprs = self.sql_select_item_to_expr(col, &schema, relation)?;
            for expr in &mut exprs {
                if let LogicalExpr::Column(col) = expr {
                    // check does schema has the field
                    plan.schema()
                        .field_with_name(&col.name)
                        .map_err(|e| Error::ArrowError(e))?;
                    // set relation to column expression
                    col.relation = relation.clone();
                }
            }
            result.extend(exprs);
        }

        Ok(result)
    }

    fn filter_expr(
        &self,
        plan: LogicalPlan,
        expr: Option<Expression>,
        relation: &Option<TableRelation>,
    ) -> Result<LogicalPlan> {
        if let Some(filter) = expr {
            self.sql_to_expr(filter, relation)
                .map(|exp| LogicalPlan::Filter(Filter::new(plan, exp)))
        } else {
            Ok(plan)
        }
    }

    fn sql_to_expr(
        &self,
        expr: Expression,
        relation: &Option<TableRelation>,
    ) -> Result<LogicalExpr> {
        Ok(match expr {
            Expression::Identifier(ident) => {
                LogicalExpr::Column(Column::new(ident, None, relation.clone()))
            }
            Expression::Literal(lit) => match lit {
                Literal::Int(i) => LogicalExpr::Literal(ScalarValue::Int64(Some(i))),
                Literal::Float(f) => LogicalExpr::Literal(ScalarValue::Float64(Some(f))),
                Literal::String(s) => LogicalExpr::Literal(ScalarValue::Utf8(Some(s))),
                Literal::Boolean(b) => LogicalExpr::Literal(ScalarValue::Boolean(Some(b))),
                Literal::Null => LogicalExpr::Literal(ScalarValue::Null),
            },
            Expression::BinaryOperator(op) => self.parse_binary_op(op, relation)?,
            _ => todo!("{:?}", expr),
        })
    }

    fn parse_binary_op(
        &self,
        op: BinaryOperator,
        relation: &Option<TableRelation>,
    ) -> Result<LogicalExpr> {
        Ok(match op {
            BinaryOperator::Eq(l, r) => eq(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::NotEq(l, r) => not_eq(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::And(l, r) => and(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Or(l, r) => or(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Gt(l, r) => gt(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Gte(l, r) => gt_eq(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Lt(l, r) => lt(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Lte(l, r) => lt_eq(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Add(l, r) => add(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Sub(l, r) => sub(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Mul(l, r) => mul(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Div(l, r) => div(
                self.sql_to_expr(*l, relation)?,
                self.sql_to_expr(*r, relation)?,
            ),
            BinaryOperator::Neg(_) => todo!(),
            BinaryOperator::Pos(_) => todo!(),
            BinaryOperator::Not(l) => todo!(),
        })
    }

    fn sql_select_item_to_expr(
        &self,
        item: SelectItem,
        schema: &SchemaRef,
        relation: &Option<TableRelation>,
    ) -> Result<Vec<LogicalExpr>> {
        match item {
            SelectItem::UnNamedExpr(expr) => self.sql_to_expr(expr, &relation).map(|v| vec![v]),
            SelectItem::ExprWithAlias(expr, alias) => self
                .sql_to_expr(expr, relation)
                .map(|col| vec![LogicalExpr::Alias(Alias::new(alias, col))]),
            SelectItem::Wildcard => {
                if relation.is_none() {
                    return Err(Error::InternalError(
                        "SELECT * with no tables specified is not valid".to_owned(),
                    ));
                }
                // expand schema
                Ok(schema
                    .all_fields()
                    .into_iter()
                    .map(|field| {
                        LogicalExpr::Column(Column::new(field.name(), None, relation.clone()))
                    })
                    .collect())
            }
            SelectItem::QualifiedWildcard(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, SchemaBuilder};

    use crate::{
        datasource::{memory::MemoryDataSource, DataSource},
        error::Result,
        execution::registry::TableRegistry,
        logical::plan,
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
        quick_test("SELECT 1", "Projection: (Int64(1))\n  Empty Relation\n");
    }

    #[test]
    fn test_select_column() {
        quick_test(
            "SELECT id,name FROM t",
            "Projection: (t.id,t.name)\n  TableScan: t\n",
        );

        quick_test("SELECT age FROM t", "Arrow Error: Schema error: Unable to get field named \"age\". Valid fields: [\"id\", \"name\"]");

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
    }

    #[test]
    fn test_where() {
        quick_test(
            "SELECT id,name FROM t WHERE id = 1",
            "Projection: (t.id,t.name)\n  Filter: t.id == Int64(1)\n    TableScan: t\n",
        )
    }

    fn quick_test(sql: &str, expected: &str) {
        let planner = SqlQueryPlanner::new(Box::new(TestTableRegistry::new()));
        match planner.create_logical_plan(sql) {
            Ok(plan) => assert_eq!(utils::format(&plan, 0), expected),
            Err(err) => assert_eq!(err.to_string(), expected),
        }
    }
}
