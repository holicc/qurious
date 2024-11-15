use crate::{
    common::{join_type::JoinType, table_relation::TableRelation},
    datatypes::{operator::Operator, scalar::ScalarValue},
    error::{Error, Result},
    internal_err,
    logical::{
        expr::{AggregateExpr, AggregateOperator, BinaryExpr, LogicalExpr},
        plan::{Filter, LogicalPlan},
        LogicalPlanBuilder,
    },
};
use arrow::datatypes::DataType;
use egg::*;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    str::FromStr,
    sync::Arc,
};

use super::OptimizerRule;

type EGraph = egg::EGraph<Node, ()>;
type Rewrite = egg::Rewrite<Node, ()>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct DataValue(ScalarValue);

impl Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{}", v),
            ScalarValue::Boolean(None) => write!(f, "null"),
            ScalarValue::Int64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int64(None) => write!(f, "null"),
            ScalarValue::Int32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int32(None) => write!(f, "null"),
            ScalarValue::Int16(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int16(None) => write!(f, "null"),
            ScalarValue::Int8(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int8(None) => write!(f, "null"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt64(None) => write!(f, "null"),
            ScalarValue::UInt32(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt32(None) => write!(f, "null"),
            ScalarValue::UInt16(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt16(None) => write!(f, "null"),
            ScalarValue::UInt8(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt8(None) => write!(f, "null"),
            ScalarValue::Float64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float64(None) => write!(f, "null"),
            ScalarValue::Float32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float32(None) => write!(f, "null"),
            ScalarValue::Decimal128(Some(v), _, _) => write!(f, "{}", v),
            ScalarValue::Decimal128(None, _, _) => write!(f, "null"),
            ScalarValue::Decimal256(Some(v), _, _) => write!(f, "{}", v),
            ScalarValue::Decimal256(None, _, _) => write!(f, "null"),
            ScalarValue::Utf8(Some(v)) => write!(f, "\"{}\"", v),
            ScalarValue::Utf8(None) => write!(f, "null"),
        }
    }
}

impl FromStr for DataValue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(ScalarValue::from_str(s)?))
    }
}

impl From<ScalarValue> for DataValue {
    fn from(value: ScalarValue) -> Self {
        Self(value)
    }
}

impl From<u64> for DataValue {
    fn from(value: u64) -> Self {
        Self(ScalarValue::UInt64(Some(value)))
    }
}

pub struct EgraphOptimizer {
    rules: Vec<Rewrite>,
}

impl Default for EgraphOptimizer {
    fn default() -> Self {
        Self {
            rules: vec![rewrite!("pushdown-filter-inner-join";
                "(filter ?cond (join inner ?on ?left ?right))" =>
                "(join inner (and ?on ?cond) ?left ?right)"
            )],
        }
    }
}

impl OptimizerRule for EgraphOptimizer {
    fn name(&self) -> &str {
        "egraph_optimizer"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if matches!(plan, LogicalPlan::Ddl(_) | LogicalPlan::Dml(_)) {
            return Ok(plan);
        }
        let mut egraph = EGraph::default();
        let mut context = EgraphContext::default();

        let root = context.build_egraph(&mut egraph, plan)?;
        let runner = Runner::default().with_egraph(egraph).run(&self.rules);
        let extractor = Extractor::new(&runner.egraph, AstSize);
        let best = extractor.find_best_node(root);

        context.rebuild_plan(&best)
    }
}

define_language! {
    pub enum Node{
        // values
        Constant(DataValue),  // null, true, 1, 1.0, "hello", ...
        DataType(DataType),
        Column(String),         // $1.2, $2.1, ...
        Table(TableRelation), // $1, $2, ...

        "list" = List(Box<[Id]>),       // (list ...)

        // binary operations
        "+" = Add([Id; 2]),
        "-" = Sub([Id; 2]),
        "*" = Mul([Id; 2]),
        "/" = Div([Id; 2]),
        "%" = Mod([Id; 2]),
        "||" = StringConcat([Id; 2]),
        ">" = Gt([Id; 2]),
        "<" = Lt([Id; 2]),
        ">=" = GtEq([Id; 2]),
        "<=" = LtEq([Id; 2]),
        "=" = Eq([Id; 2]),
        "<>" = NotEq([Id; 2]),
        "and" = And([Id; 2]),
        "or" = Or([Id; 2]),
        "xor" = Xor([Id; 2]),
        "like" = Like([Id; 2]),

        // unary operations
        "-" = Neg(Id),
        "not" = Not(Id),
        "isnull" = IsNull(Id),

        "if" = If([Id; 3]), // (if cond then else)

        "cast" = Cast([Id; 2]), // cast: (type expr)

        // aggregations
        "max" = Max(Id),
        "min" = Min(Id),
        "sum" = Sum(Id),
        "avg" = Avg(Id),
        "count" = Count(Id),

        // logical plan
        "values" = Values(Box<[Id]>), // values: ([expr..]..)
        "scan" = TableScan(Id), // scan: (table, [column...], filter)
        "project" = Project([Id; 2]), // project: (input, [expr...])
        "filter" = Filter([Id; 2]), // filter: (cond, input)
        "sort" = Sort([Id; 2]), // sort: (input, [expr...])
            "desc" = Desc(Id),
        "limit" = Limit([Id; 3]), // limit: (input, limit)
        "empty_scan" = EmptyTableScan, // empty table scan
        "join" = Join([Id;4]), // join: (join_type cond left right)
        "apply" = Apply([Id; 3]),
            "inner" = Inner,
            "left_outer" = LeftOuter,
            "right_outer" = RightOuter,
            "full_outer" = FullOuter,
            "semi" = Semi,
            "anti" = Anti,

        "agg" = Agg([Id; 3]), // agg: (input [agg_expr...] [group_expr...])

        Symbol(Symbol),
    }
}

struct CostFn<'a> {
    egraph: &'a EGraph,
}

impl CostFunction<Node> for CostFn<'_> {
    type Cost = f32;

    fn cost<C>(&mut self, enode: &Node, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let id = &self.egraph.lookup(enode.clone()).unwrap();
        let mut costs = |i: &Id| costs(*i);
        // let rows = |i: &Id| self.egraph[*i].data.rows;
        // let cols = |i: &Id| self.egraph[*i].data.schema.len() as f32;
        let nlogn = |x: f32| x * (x + 1.0).log2();
        // The cost of build output chunks of a plan.
        // let build = || rows(id) * cols(id);
        // The cost of an operation in hash table.
        let hash = |size: f32| (size + 1.0).log2() * 0.01;

        match enode {
            Node::Filter([exprs, c]) => costs(exprs) + costs(c),
            // each operator has a cost of 0.1
            _ => enode.fold(0.1, |sum, id| sum + costs(&id)),
        }
    }
}

#[derive(Default)]
struct EgraphContext {
    plans: HashMap<Id, LogicalPlan>,
    exprs: HashMap<Id, Vec<LogicalExpr>>,
    table_ids: HashMap<TableRelation, usize>,
    column_ids: HashMap<TableRelation, HashMap<String, usize>>,
    table_aliases: HashMap<String, TableRelation>,
    column_aliases: HashMap<String, (TableRelation, String)>,
    next_table_id: usize,
}

impl EgraphContext {
    fn add_table_alias(&mut self, alias: String, table: TableRelation) {
        self.table_aliases.insert(alias, table);
    }

    fn add_column_alias(&mut self, alias: String, table: TableRelation, column: String) {
        self.column_aliases.insert(alias, (table, column));
    }

    fn resolve_table(&self, table: &TableRelation) -> TableRelation {
        if let Some(real_table) = self.table_aliases.get(&table.to_string()) {
            real_table.clone()
        } else {
            table.clone()
        }
    }

    fn resolve_column(&self, table: &TableRelation, column: &str) -> (TableRelation, String) {
        if let Some((real_table, real_column)) = self.column_aliases.get(column) {
            return (real_table.clone(), real_column.clone());
        }

        (self.resolve_table(table), column.to_string())
    }

    fn get_table_id(&mut self, table: &TableRelation) -> usize {
        let real_table = self.resolve_table(table);
        if let Some(&id) = self.table_ids.get(&real_table) {
            id
        } else {
            let id = self.next_table_id;
            self.table_ids.insert(real_table, id);
            self.next_table_id += 1;
            id
        }
    }

    fn get_column_id(&mut self, table: &TableRelation, column: &str) -> usize {
        let (real_table, real_column) = self.resolve_column(table, column);
        let column_map = self.column_ids.entry(real_table).or_default();

        if let Some(&id) = column_map.get(&real_column) {
            id
        } else {
            let id = column_map.len();
            column_map.insert(real_column, id);
            id
        }
    }

    fn build_egraph(&mut self, egraph: &mut EGraph, plan: LogicalPlan) -> Result<Id> {
        let id = match plan.clone() {
            LogicalPlan::Limit(limit) => {
                let input = self.build_egraph(egraph, *limit.input)?;
                let limit_id = egraph.add(Node::Constant(DataValue(ScalarValue::UInt64(
                    limit.fetch.map(|v| v as u64),
                ))));
                let offset_id = egraph.add(Node::Constant(DataValue::from(limit.skip as u64)));
                egraph.add(Node::Limit([input, limit_id, offset_id]))
            }
            LogicalPlan::EmptyRelation(_) => egraph.add(Node::EmptyTableScan),
            LogicalPlan::Filter(filter) => {
                let input = self.build_egraph(egraph, *filter.input)?;
                let cond = self.build_expr(egraph, filter.expr)?;
                egraph.add(Node::Filter([cond, input]))
            }
            LogicalPlan::Projection(projection) => {
                let input = self.build_egraph(egraph, *projection.input)?;
                let exprs = projection
                    .exprs
                    .clone()
                    .into_iter()
                    .map(|expr| self.build_expr(egraph, expr))
                    .collect::<Result<Vec<_>>>()
                    .map(|exprs| egraph.add(Node::List(exprs.into())))?;

                self.exprs.insert(exprs, projection.exprs);

                egraph.add(Node::Project([input, exprs]))
            }
            LogicalPlan::TableScan(table_scan) => {
                let table_id = self.get_table_id(&table_scan.relation);
                egraph.add(Node::Table(format!("${}", table_id).into()))
            }
            LogicalPlan::Join(join) => {
                let join_type = egraph.add(match join.join_type {
                    JoinType::Inner => Node::Inner,
                    JoinType::Left => Node::LeftOuter,
                    JoinType::Right => Node::RightOuter,
                    JoinType::Full => Node::FullOuter,
                });
                let cond = join
                    .filter
                    .map(|expr| self.build_expr(egraph, expr))
                    .transpose()?
                    .unwrap_or(egraph.add(Node::Constant(DataValue(ScalarValue::Boolean(Some(true))))));
                let left = self.build_egraph(egraph, Arc::unwrap_or_clone(join.left))?;
                let right = self.build_egraph(egraph, Arc::unwrap_or_clone(join.right))?;
                egraph.add(Node::Join([join_type, cond, left, right]))
            }
            LogicalPlan::Aggregate(aggregate) => {
                let input = self.build_egraph(egraph, *aggregate.input)?;
                let agg_exprs = aggregate
                    .aggr_expr
                    .into_iter()
                    .map(|expr| self.build_expr(egraph, expr))
                    .collect::<Result<Vec<_>>>()
                    .map(|ids| egraph.add(Node::List(ids.into())))?;
                let group_exprs = aggregate
                    .group_expr
                    .into_iter()
                    .map(|expr| self.build_expr(egraph, expr))
                    .collect::<Result<Vec<_>>>()
                    .map(|ids| egraph.add(Node::List(ids.into())))?;
                egraph.add(Node::Agg([input, agg_exprs, group_exprs]))
            }
            LogicalPlan::Values(values) => {
                let mut bound_values = Vec::with_capacity(values.values.len());
                for row in values.values {
                    let ids = row
                        .into_iter()
                        .map(|expr| self.build_expr(egraph, expr))
                        .collect::<Result<Vec<_>>>()?;
                    bound_values.extend(ids);
                }
                egraph.add(Node::Values(bound_values.into()))
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.add_table_alias(
                    subquery_alias.alias.to_string(),
                    subquery_alias
                        .input
                        .as_ref()
                        .relation()
                        .ok_or(Error::InternalError(format!(
                            "Subquery alias must have a table relation in optimizer"
                        )))?,
                );
                self.build_egraph(egraph, subquery_alias.input.as_ref().clone())?
            }
            LogicalPlan::Sort(sort) => {
                let input = self.build_egraph(egraph, *sort.input)?;
                let exprs = sort
                    .exprs
                    .into_iter()
                    .map(|expr| {
                        let expr_id = self.build_expr(egraph, *expr.expr)?;
                        if expr.asc {
                            Ok(expr_id)
                        } else {
                            Ok(egraph.add(Node::Desc(expr_id)))
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|ids| egraph.add(Node::List(ids.into())))?;
                egraph.add(Node::Sort([input, exprs]))
            }
            _ => unreachable!("ddl and dml no need to optimize should skip"),
        };

        self.plans.insert(id, plan);
        Ok(id)
    }

    fn build_expr(&mut self, egraph: &mut EGraph, expr: LogicalExpr) -> Result<Id> {
        let id = match expr.clone() {
            LogicalExpr::Literal(value) => egraph.add(Node::Constant(DataValue(value))),
            LogicalExpr::Column(col) => {
                let relation = col.relation.clone().ok_or_else(|| {
                    Error::InternalError("Column must have a table reference in optimizer".to_string())
                })?;
                let table_id = self.get_table_id(&relation);
                let column_id = self.get_column_id(&relation, &col.name);
                egraph.add(Node::Column(format!("${}.{}", table_id, column_id)))
            }
            LogicalExpr::BinaryExpr(binary) => {
                let left = self.build_expr(egraph, *binary.left)?;
                let right = self.build_expr(egraph, *binary.right)?;

                let node = match binary.op {
                    Operator::Add => Node::Add([left, right]),
                    Operator::Sub => Node::Sub([left, right]),
                    Operator::Mul => Node::Mul([left, right]),
                    Operator::Div => Node::Div([left, right]),
                    Operator::Mod => Node::Mod([left, right]),
                    Operator::Gt => Node::Gt([left, right]),
                    Operator::Lt => Node::Lt([left, right]),
                    Operator::GtEq => Node::GtEq([left, right]),
                    Operator::LtEq => Node::LtEq([left, right]),
                    Operator::Eq => Node::Eq([left, right]),
                    Operator::NotEq => Node::NotEq([left, right]),
                    Operator::And => Node::And([left, right]),
                    Operator::Or => Node::Or([left, right]),
                };
                egraph.add(node)
            }
            LogicalExpr::AggregateExpr(agg) => {
                let expr = self.build_expr(egraph, *agg.expr)?;
                let node = match agg.op {
                    AggregateOperator::Max => Node::Max(expr),
                    AggregateOperator::Min => Node::Min(expr),
                    AggregateOperator::Sum => Node::Sum(expr),
                    AggregateOperator::Avg => Node::Avg(expr),
                    AggregateOperator::Count => Node::Count(expr),
                };
                egraph.add(node)
            }
            LogicalExpr::Alias(alias) => {
                let inner_expr = *alias.expr.clone();
                if let LogicalExpr::Column(col) = &inner_expr {
                    if let Some(relation) = &col.relation {
                        self.add_column_alias(alias.name.clone(), relation.clone(), col.name.clone());
                    }
                }
                self.build_expr(egraph, inner_expr)?
            }
            LogicalExpr::SortExpr(sort_expr) => {
                let expr_id = self.build_expr(egraph, *sort_expr.expr)?;
                if sort_expr.asc {
                    expr_id
                } else {
                    egraph.add(Node::Desc(expr_id))
                }
            }
            LogicalExpr::Cast(cast_expr) => {
                let expr_id = self.build_expr(egraph, *cast_expr.expr)?;
                let type_id = egraph.add(Node::DataType(cast_expr.data_type));
                egraph.add(Node::Cast([type_id, expr_id]))
            }
            LogicalExpr::Wildcard => {
                todo!("Wildcard should be expanded before optimization")
            }
            LogicalExpr::Function(_function) => {
                todo!("Function calls not yet supported in optimizer")
            }
            LogicalExpr::IsNull(expr) => {
                let expr_id = self.build_expr(egraph, *expr)?;
                egraph.add(Node::IsNull(expr_id))
            }
            LogicalExpr::IsNotNull(expr) => {
                let expr_id = self.build_expr(egraph, *expr)?;
                let is_null = egraph.add(Node::IsNull(expr_id));
                egraph.add(Node::Not(is_null))
            }
            LogicalExpr::Negative(expr) => {
                let expr_id = self.build_expr(egraph, *expr)?;
                egraph.add(Node::Neg(expr_id))
            }
        };

        self.exprs.insert(id, vec![expr]);
        Ok(id)
    }

    fn rebuild_plan(&mut self, node: &Node) -> Result<LogicalPlan> {
        match node {
            Node::Project([input, columns]) => {
                let input_plan = self
                    .plans
                    .remove(input)
                    .ok_or(Error::InternalError(format!("Input plan not found in rebuild_plan")))?;
                let exprs = self
                    .exprs
                    .remove(columns)
                    .ok_or(Error::InternalError(format!("Columns not found in rebuild_plan")))?;

                LogicalPlanBuilder::from(input_plan)
                    .add_project(exprs)
                    .map(|build| build.build())
            }
            _ => Err(Error::InternalError(format!(
                "Unsupported node type in rebuild_plan: {:?}",
                node
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::Schema;
    use egg::{AstDepth, AstSize, EGraph, Extractor, Language, RecExpr, Runner};
    use sqlparser::parser::Parser;

    use crate::{
        build_mem_datasource,
        common::table_relation::TableRelation,
        error::Result,
        logical::plan::{EmptyRelation, LogicalPlan},
        optimizer::{
            egg::{CostFn, EgraphOptimizer},
            OptimizerRule,
        },
        planner::sql::SqlQueryPlanner,
    };

    use super::{EgraphContext, Node};

    fn sql_to_plan(sql: &str) -> LogicalPlan {
        let mut tables = HashMap::new();

        // Add test tables
        tables.insert(
            "users".into(),
            build_mem_datasource!(
                ("id", DataType::Int64, false),
                ("name", DataType::Utf8, false),
                ("email", DataType::Utf8, false)
            ),
        );

        tables.insert(
            "repos".into(),
            build_mem_datasource!(
                ("id", DataType::Int64, false),
                ("name", DataType::Utf8, false),
                ("owner_id", DataType::Int64, false)
            ),
        );

        tables.insert(
            "commits".into(),
            build_mem_datasource!(
                ("id", DataType::Int64, false),
                ("repo_id", DataType::Int64, false),
                ("user_id", DataType::Int64, false),
                ("time", DataType::Date32, false),
                ("message", DataType::Utf8, true)
            ),
        );

        let stmt = Parser::new(sql).parse().unwrap();
        let udsf = HashMap::default();
        SqlQueryPlanner::create_logical_plan(stmt, tables, &udsf).unwrap()
    }

    fn build_egg_expr(sql: &str) -> RecExpr<Node> {
        let mut egraph = EGraph::default();
        let mut ctx = EgraphContext::default();
        let plan = sql_to_plan(sql);
        let root = ctx.build_egraph(&mut egraph, plan).unwrap();
        let extract = Extractor::new(&egraph, AstSize);
        let (_, best) = extract.find_best(root);

        best
    }

    fn assert_expr_eq(sql: &str, expected: &str) {
        let expr = build_egg_expr(sql);
        let actual = expr.to_string();
        assert_eq!(actual, expected);
    }

    fn assert_after_optimizer(sql: &str, expected: &str) {
        let optimizer = EgraphOptimizer::default();
        let plan = sql_to_plan(sql);

        let mut ctx = EgraphContext::default();
        let mut graph = EGraph::default();

        let root = ctx.build_egraph(&mut graph, plan).unwrap();
        let runner = Runner::default().with_egraph(graph).run(&optimizer.rules);

        runner.print_report();

        let cost_fn = CostFn { egraph: &runner.egraph };
        let extractor = Extractor::new(&runner.egraph, cost_fn);
        let (_, best) = extractor.find_best(root);

        assert_eq!(&best.to_string(), expected);
    }

    #[test]
    fn test_column_ids() {
        let mut ctx = EgraphContext::default();
        let table1: TableRelation = "users".into();
        let table2: TableRelation = "repos".into();

        // Test that column IDs start from 0 for each table
        assert_eq!(ctx.get_column_id(&table1, "id"), 0);
        assert_eq!(ctx.get_column_id(&table1, "name"), 1);
        assert_eq!(ctx.get_column_id(&table1, "email"), 2);

        assert_eq!(ctx.get_column_id(&table2, "id"), 0);
        assert_eq!(ctx.get_column_id(&table2, "name"), 1);
        assert_eq!(ctx.get_column_id(&table2, "owner_id"), 2);

        // Test that IDs are consistent
        assert_eq!(ctx.get_column_id(&table1, "id"), 0);
        assert_eq!(ctx.get_column_id(&table2, "id"), 0);
    }

    #[test]
    fn test_build_egraph() {
        assert_expr_eq("SELECT name FROM users", "(project $0 (list $0.0))");
        assert_expr_eq(
            "SELECT users.name, users.email FROM users",
            "(project $0 (list $0.0 $0.1))",
        );
        assert_expr_eq(
            "SELECT users.name FROM users, repos",
            "(project (join inner true $0 $1) (list $0.0))",
        );
        assert_expr_eq(
            "SELECT users.name, repos.name FROM users, repos",
            "(project (join inner true $0 $1) (list $0.0 $1.0))",
        );
    }

    #[test]
    fn test_aliases() {
        assert_expr_eq("SELECT u.name, u.email FROM users u", "(project $0 (list $0.0 $0.1))");
        assert_expr_eq("SELECT name as user_name FROM users", "(project $0 (list $0.0))");
        assert_expr_eq(
            "SELECT u.name as user_name, r.name as repo_name FROM users u, repos r",
            "(project (join inner true $0 $1) (list $0.0 $1.0))",
        );
    }

    #[test]
    fn test_rebuild_projection() -> Result<()> {
        let plan = sql_to_plan("SELECT id, name FROM users");
        let optimizer = EgraphOptimizer::default();
        let root = optimizer.optimize(plan.clone())?;

        assert!(matches!(root, LogicalPlan::Projection(_)));
        assert_eq!(format!("{:?}", plan), format!("{:?}", root));
        Ok(())
    }

    #[test]
    fn test_filter_inner_join() {
        // (project (filter (= $0.0 $1.0) (join inner true $0 $1)) (list $0.1 $1.1 $0.0 $1.2 $0.2 $1.0))
        assert_after_optimizer(
            "SELECT * FROM users u, repos r WHERE u.id = r.owner_id",
            "(proje (join inner true $0 $1) (list $0.0 $1.0))",
        );
    }
}
