// use std::collections::HashSet;

// use super::OptimizerRule;
// use crate::error::Result;
// use crate::logical::expr::LogicalExpr;
// use crate::logical::plan::{Aggregate, Filter, LogicalPlan, Projection, TableScan};

// pub struct ProjectionPushDownRule;

// impl OptimizerRule for ProjectionPushDownRule {
//     fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
//         self.push_down(plan, HashSet::new()).map(Some)
//     }

//     fn name(&self) -> &str {
//         "projection_push_down_rule"
//     }
// }

// impl ProjectionPushDownRule {
//     fn extract_columns(&self, exprs: &Vec<LogicalExpr>, input: &LogicalPlan, accum: &mut HashSet<String>) {
//         for expr in exprs {
//             self.extract_column(expr, input, accum);
//         }
//     }

//     fn extract_column(&self, expr: &LogicalExpr, input: &LogicalPlan, accum: &mut HashSet<String>) {
//         match expr {
//             LogicalExpr::Column(c) => {
//                 accum.insert(c.name.clone());
//             }
//             LogicalExpr::BinaryExpr(b) => {
//                 self.extract_column(&b.left, input, accum);
//                 self.extract_column(&b.right, input, accum);
//             }
//             LogicalExpr::AggregateExpr(a) => {
//                 self.extract_column(&a.expr, input, accum);
//             }
//             _ => {}
//         }
//     }

//     fn push_down(&self, plan: &LogicalPlan, mut columns: HashSet<String>) -> Result<LogicalPlan> {
//         match plan {
//             LogicalPlan::Projection(p) => {
//                 self.extract_columns(&p.exprs, plan, &mut columns);

//                 self.push_down(plan, columns)
//                     .and_then(|input| Projection::try_new(input, p.exprs.clone()).map(|p| LogicalPlan::Projection(p)))
//             }
//             LogicalPlan::Filter(f) => {
//                 self.extract_column(&f.expr, plan, &mut columns);

//                 self.push_down(&f.input, columns.clone()).and_then(|input| {
//                     let expr = f.expr.clone();
//                     Ok(LogicalPlan::Filter(Filter {
//                         input: Box::new(input),
//                         expr,
//                     }))
//                 })
//             }
//             LogicalPlan::Aggregate(a) => {
//                 self.extract_columns(&a.group_expr, plan, &mut columns);
//                 self.extract_columns(
//                     &a.aggr_expr.iter().map(|e| *e.expr.clone()).collect(),
//                     plan,
//                     &mut columns,
//                 );

//                 self.push_down(&a.input, columns.clone()).and_then(|input| {
//                     let group_expr = a.group_expr.clone();
//                     let aggr_expr = a.aggr_expr.clone();

//                     Aggregate::try_new(input, group_expr, aggr_expr).map(|a| LogicalPlan::Aggregate(a))
//                 })
//             }
//             LogicalPlan::TableScan(s) => TableScan::try_new(
//                 s.relation.clone(),
//                 s.source.clone(),
//                 Some(columns.into_iter().collect()),
//                 None,
//             )
//             .map(LogicalPlan::TableScan),
//             _ => todo!(),
//         }
//     }
// }
