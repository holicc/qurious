use std::collections::{HashMap, HashSet};

use arrow::datatypes::FieldRef;

use crate::{
    common::{
        table_relation::TableRelation,
        table_schema::TableSchemaRef,
        transformed::{TransformNode, Transformed, TransformedResult},
    },
    datatypes::operator::Operator,
    error::Result,
    logical::{
        expr::{alias::Alias, BinaryExpr, Column, LogicalExpr},
        plan::LogicalPlan,
    },
};

pub fn replace_col(expr: LogicalExpr, col: &Column, new_col: &Column) -> Result<LogicalExpr> {
    expr.transform_up(|expr| {
        if matches!(&expr, LogicalExpr::Column(c) if c == col) {
            Ok(Transformed::yes(LogicalExpr::Column((*new_col).to_owned())))
        } else {
            Ok(Transformed::no(expr))
        }
    })
    .data()
}

pub fn replace_cols(expr: LogicalExpr, replace_map: &HashMap<&Column, &Column>) -> Result<LogicalExpr> {
    expr.transform_up(|expr| {
        if let LogicalExpr::Column(col) = &expr {
            match replace_map.get(&col) {
                Some(expr) => Ok(Transformed::yes(LogicalExpr::Column((*expr).to_owned()))),
                None => Ok(Transformed::no(expr)),
            }
        } else {
            Ok(Transformed::no(expr))
        }
    })
    .data()
}

pub fn replace_cols_by_name(e: LogicalExpr, replace_map: &HashMap<String, LogicalExpr>) -> Result<LogicalExpr> {
    e.transform_up(|expr| {
        Ok(if let LogicalExpr::Column(c) = &expr {
            match replace_map.get(&c.qualified_name()) {
                Some(new_c) => Transformed::yes(new_c.clone()),
                None => Transformed::no(expr),
            }
        } else {
            Transformed::no(expr)
        })
    })
    .data()
}

pub fn exprs_to_fields(exprs: &[LogicalExpr], plan: &LogicalPlan) -> Result<Vec<(Option<TableRelation>, FieldRef)>> {
    exprs
        .iter()
        .map(|expr| expr.field(plan).map(|field| (expr.qualified_name(), field)))
        .collect()
}

pub fn split_conjunctive_predicates(filter: LogicalExpr) -> Vec<LogicalExpr> {
    split_conjunction_impl(filter, vec![])
}

pub fn conjunction(filters: impl IntoIterator<Item = LogicalExpr>) -> Option<LogicalExpr> {
    filters.into_iter().reduce(LogicalExpr::and)
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

pub fn check_all_columns_from_schema(columns: &HashSet<Column>, schema: &TableSchemaRef) -> bool {
    for col in columns.iter() {
        if !schema.has_field(col.relation.as_ref(), &col.name) {
            return false;
        }
    }
    true
}
