use arrow::datatypes::FieldRef;

use crate::{
    common::table_relation::TableRelation,
    datatypes::operator::Operator,
    error::Result,
    logical::{
        expr::{alias::Alias, BinaryExpr, Column, LogicalExpr},
        plan::LogicalPlan,
    },
};

pub fn exprs_to_fields(exprs: &[LogicalExpr], plan: &LogicalPlan) -> Result<Vec<(Option<TableRelation>, FieldRef)>> {
    exprs
        .iter()
        .map(|expr| expr.field(plan).map(|field| (expr.qualified_name(), field)))
        .collect()
}

pub fn split_conjunctive_predicates(filter: LogicalExpr) -> Vec<LogicalExpr> {
    split_conjunction_impl(filter, vec![])
}

fn split_conjunction_impl(filter: LogicalExpr, mut exprs: Vec<LogicalExpr>) -> Vec<LogicalExpr> {
    match filter {
        LogicalExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let exprs = split_conjunction_impl(*left, exprs);
            split_conjunction_impl(*right, exprs)
        }
        LogicalExpr::Alias(Alias { expr, .. }) => split_conjunction_impl(*expr, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

pub fn is_restrict_null_predicate<'a>(
    predicate: &'a LogicalExpr,
    join_cols_of_predicates: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, LogicalExpr::Column(_)) {
        return Ok(true);
    }

    todo!()
}
