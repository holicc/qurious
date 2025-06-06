use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

use super::{
    expr::{LogicalExpr, SortExpr},
    plan::{Aggregate, CrossJoin, EmptyRelation, Filter, Join, Limit, LogicalPlan, Projection, Sort, TableScan},
};
use crate::{
    common::table_relation::TableRelation,
    error::{Error, Result},
};
use crate::{
    common::{
        join_type::JoinType,
        table_schema::{TableSchema, TableSchemaRef},
    },
    provider::table::TableProvider,
};

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        LogicalPlanBuilder { plan }
    }

    pub fn project(input: LogicalPlan, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<LogicalPlan> {
        Projection::try_new(input, exprs.into_iter().map(|exp| exp.into()).collect()).map(LogicalPlan::Projection)
    }

    pub fn filter(input: LogicalPlan, predicate: LogicalExpr) -> Result<LogicalPlan> {
        Filter::try_new(input, predicate).map(LogicalPlan::Filter)
    }

    pub fn having(self, predicate: LogicalExpr) -> Result<Self> {
        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Filter(Filter::try_new(self.plan, predicate)?.into()),
        })
    }

    pub fn add_project(self, exprs: impl IntoIterator<Item = impl Into<LogicalExpr>>) -> Result<Self> {
        Projection::try_new(self.plan, exprs.into_iter().map(|exp| exp.into()).collect())
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Projection(s)))
    }

    pub fn empty(produce_one_row: bool) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::EmptyRelation(EmptyRelation {
                schema: Arc::new(Schema::empty()),
                produce_one_row,
            }),
        }
    }

    pub fn scan(
        relation: impl Into<TableRelation>,
        table_source: Arc<dyn TableProvider>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        TableScan::try_new(relation.into(), table_source, filter)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::TableScan(s)))
    }

    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let schema = TableSchema::merge(vec![self.plan.table_schema(), right.table_schema()])?;
        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::CrossJoin(CrossJoin {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                schema: Arc::new(schema),
            }),
        })
    }

    pub fn join_on(self, right: LogicalPlan, join_type: JoinType, filter: Option<LogicalExpr>) -> Result<Self> {
        let schema = build_join_schema(join_type, &self.plan.table_schema(), &right.table_schema())?;
        let on = vec![];

        if join_type != JoinType::Inner && filter.is_none() && on.is_empty() {
            return Err(Error::InternalError(format!("join condition should not be empty")));
        }

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                join_type,
                on,
                filter,
                schema,
            }),
        })
    }

    pub fn aggregate(self, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<LogicalExpr>) -> Result<Self> {
        Aggregate::try_new(self.plan, group_expr, aggr_expr)
            .map(|s| LogicalPlanBuilder::from(LogicalPlan::Aggregate(s)))
    }

    pub fn sort(self, order_by: Vec<SortExpr>) -> Result<Self> {
        // TODO
        // we need check if the column is ambiguous and columns is present in the schema
        // if not present in the schema we should add missing columns to the schema and project them
        // let missing_columns = order_by
        //     .iter()
        //     .flat_map(|sort|sort.expr.column_refs())
        //     .filter(|col| !self.plan.has_column(col))
        //     .collect::<Vec<_>>();

        // if !missing_columns.is_empty() {
        //     todo!("Add missing columns to the schema and project them");
        // }

        Ok(LogicalPlanBuilder {
            plan: LogicalPlan::Sort(Sort {
                exprs: order_by,
                input: Box::new(self.plan),
            }),
        })
    }

    pub fn limit(self, fetch: Option<usize>, skip: usize) -> Self {
        LogicalPlanBuilder {
            plan: LogicalPlan::Limit(Limit {
                input: Box::new(self.plan),
                fetch,
                skip,
            }),
        }
    }
}

fn build_join_schema(join_type: JoinType, left: &TableSchemaRef, right: &TableSchemaRef) -> Result<TableSchemaRef> {
    let left_fields = left.iter();
    let right_fields = right.iter();

    let qualified_fields = match join_type {
        // left then right, right set to nullable
        JoinType::Left => left_fields
            .map(|(a, b)| (a.cloned(), b.clone()))
            .chain(nullify_fields(right_fields))
            .collect::<Vec<_>>(),
        // right then left, left set to nullable
        JoinType::Right => right_fields
            .map(|(a, b)| (a.cloned(), b.clone()))
            .chain(nullify_fields(left_fields))
            .collect::<Vec<_>>(),
        // left then right
        JoinType::Inner => left_fields
            .map(|(a, b)| (a.cloned(), b.clone()))
            .chain(right_fields.map(|(a, b)| (a.cloned(), b.clone())))
            .collect::<Vec<_>>(),
        // left then right, both set to nullable
        JoinType::Full => nullify_fields(left_fields)
            .into_iter()
            .chain(nullify_fields(right_fields))
            .collect(),
    };

    TableSchema::try_new(qualified_fields).map(Arc::new)
}

fn nullify_fields<'a>(
    fields: impl Iterator<Item = (Option<&'a TableRelation>, &'a Arc<Field>)>,
) -> Vec<(Option<TableRelation>, Arc<Field>)> {
    fields
        .map(|(rel, field)| (rel.cloned(), Arc::new(field.as_ref().clone().with_nullable(true))))
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use crate::{test_utils::sql_to_plan, utils};

    fn assert_plan(sql: &str, expected: Vec<&str>) {
        let plan = sql_to_plan(sql);
        let actual = utils::format(&plan, 0);
        let actual = actual.trim().lines().collect::<Vec<_>>();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
    }

    #[test]
    fn test_inner_join() {
        assert_plan(
            "SELECT * FROM users a JOIN repos b ON a.id = b.owner_id",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Inner Join: Filter: users.id = repos.owner_id",
                "    SubqueryAlias: a",
                "      TableScan: users",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_left_join() {
        assert_plan(
            "SELECT * FROM users a LEFT JOIN repos b ON a.id = b.owner_id",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Left Join: Filter: users.id = repos.owner_id",
                "    SubqueryAlias: a",
                "      TableScan: users",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_right_join() {
        assert_plan(
            "SELECT * FROM users a RIGHT JOIN repos b ON a.id = b.owner_id",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Right Join: Filter: users.id = repos.owner_id",
                "    SubqueryAlias: a",
                "      TableScan: users",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_full_join() {
        assert_plan(
            "SELECT * FROM users a FULL JOIN repos b ON a.id = b.owner_id",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Full Join: Filter: users.id = repos.owner_id",
                "    SubqueryAlias: a",
                "      TableScan: users",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_join_with_where() {
        assert_plan(
            "SELECT * FROM users a JOIN repos b ON a.id = b.owner_id WHERE a.name = 'test'",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Filter: users.name = Utf8('test')",
                "    Inner Join: Filter: users.id = repos.owner_id",
                "      SubqueryAlias: a",
                "        TableScan: users",
                "      SubqueryAlias: b",
                "        TableScan: repos",
            ],
        );
    }

    #[test]
    fn test_join_with_multiple_conditions() {
        assert_plan(
            "SELECT * FROM users a JOIN repos b ON a.id = b.owner_id AND a.name = b.name",
            vec![
                "Projection: (a.email, a.id, b.id, a.name, b.name, b.owner_id)",
                "  Inner Join: Filter: users.id = repos.owner_id AND users.name = repos.name",
                "    SubqueryAlias: a",
                "      TableScan: users",
                "    SubqueryAlias: b",
                "      TableScan: repos",
            ],
        );
    }
}
