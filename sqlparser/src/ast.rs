use std::fmt::{Display, Formatter};

use crate::{datatype::DataType, error::Error};

#[derive(Clone, PartialEq, Debug)]
pub enum Statement {
    CreateTable {
        query: Option<Select>,
        table: String,
        check_exists: bool,
        columns: Vec<Column>,
    },
    CreateSchema {
        schema: String,
        check_exists: bool,
    },
    DropTable {
        table: String,
        check_exists: bool,
    },
    DropSchema {
        schema: String,
        check_exists: bool,
    },
    Select(Box<Select>),
    Insert {
        table: String,
        alias: Option<String>,
        columns: Option<Vec<Expression>>,
        values: Vec<Vec<Expression>>,
        on_conflict: Option<OnConflict>,
        returning: Option<Vec<SelectItem>>,
        query: Option<Select>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        r#where: Option<Expression>,
    },
    Delete {
        table: String,
        r#where: Option<Expression>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub struct Select {
    pub with: Option<With>,
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectItem>,
    pub from: Vec<From>,
    pub r#where: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<(Expression, Order)>>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl Display for Select {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT ")?;
        if let Some(w) = &self.with {
            write!(f, "WITH {}", w)?;
        }
        if let Some(d) = &self.distinct {
            match d {
                Distinct::ALL => write!(f, "ALL ")?,
                Distinct::DISTINCT(e) => write!(
                    f,
                    "DISTINCT {}",
                    e.iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?,
            }
        }
        write!(
            f,
            "{}",
            self.columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )?;
        if !self.from.is_empty() {
            write!(
                f,
                " FROM {}",
                self.from
                    .iter()
                    .map(|from| from.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        if let Some(w) = &self.r#where {
            write!(f, " WHERE {}", w)?;
        }
        if let Some(g) = &self.group_by {
            write!(
                f,
                " GROUP BY {}",
                g.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        if let Some(h) = &self.having {
            write!(f, " HAVING {}", h)?;
        }
        if let Some(o) = &self.order_by {
            write!(
                f,
                " ORDER BY {}",
                o.iter()
                    .map(|(e, o)| format!("{} {}", e, o))
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        if let Some(l) = &self.limit {
            write!(f, " LIMIT {}", l)?;
        }
        if let Some(o) = &self.offset {
            write!(f, " OFFSET {}", o)?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl Display for With {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.cte_tables
                .iter()
                .map(|cte| cte.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Cte {
    pub alias: String,
    pub query: Box<Select>,
}

impl Display for Cte {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AS ({})", self.alias, self.query)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SelectItem {
    /// expression without alias eg. `SELECT 1`
    UnNamedExpr(Expression),
    /// expression with alias eg. `SELECT 1 AS one`
    ExprWithAlias(Expression, String),
    /// `SELECT *`
    Wildcard,
    /// `SELECT table.*`
    QualifiedWildcard(Vec<String>),
}

impl Display for SelectItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectItem::UnNamedExpr(e) => write!(f, "{}", e),
            SelectItem::ExprWithAlias(e, a) => write!(f, "{} AS {}", e, a),
            SelectItem::Wildcard => write!(f, "*"),
            SelectItem::QualifiedWildcard(t) => write!(f, "{}.*", t.join(".")),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub index: bool,
    pub unique: bool,
    pub references: Option<String>,
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.datatype)?;
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if self.nullable {
            write!(f, " NULL")?;
        } else {
            write!(f, " NOT NULL")?;
        }
        if self.index {
            write!(f, " INDEX")?;
        }
        if self.unique {
            write!(f, " UNIQUE")?;
        }
        if let Some(r) = &self.references {
            write!(f, " REFERENCES {}", r)?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum OnConflict {
    DoNothing,
    /// `ON CONFLICT (constraint) DO UPDATE SET values[expressions]`
    DoUpdate {
        constraints: Vec<Ident>,
        values: Vec<Expression>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub struct Ident {
    pub value: String,
    pub quote_style: Option<char>,
}

impl std::convert::From<String> for Ident {
    fn from(value: String) -> Self {
        Ident {
            value,
            quote_style: None,
        }
    }
}

impl std::convert::From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
        }
    }
}

impl Display for Ident {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.quote_style {
            Some(q) => write!(f, "{}{}{}", q, self.value, q),
            None => write!(f, "{}", self.value),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Order {
    Asc,
    Desc,
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflict::DoNothing => write!(f, "DO NOTHING"),
            OnConflict::DoUpdate {
                constraints,
                values,
            } => {
                write!(
                    f,
                    "DO UPDATE SET {}",
                    values
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                if !constraints.is_empty() {
                    write!(
                        f,
                        " WHERE {}",
                        constraints
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl Display for Order {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Order::Asc => write!(f, "ASC"),
            Order::Desc => write!(f, "DESC"),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Select(select) => write!(f, "{}", select),
            Statement::Insert {
                table,
                alias,
                columns,
                values,
                on_conflict,
                returning,
                query,
            } => {
                write!(f, "INSERT INTO {} ", table,)?;
                if let Some(a) = alias {
                    write!(f, "AS {}", a)?;
                }
                if let Some(c) = columns {
                    write!(
                        f,
                        "({}) ",
                        c.iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                write!(
                    f,
                    "VALUES {}",
                    values
                        .iter()
                        .map(|v| {
                            format!(
                                "({})",
                                v.iter()
                                    .map(|e| e.to_string())
                                    .collect::<Vec<String>>()
                                    .join(", ")
                            )
                        })
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                if let Some(o) = on_conflict {
                    write!(f, " {}", o)?;
                }
                if let Some(r) = returning {
                    write!(
                        f,
                        " RETURNING {}",
                        r.iter()
                            .map(|r| r.to_string())
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                if let Some(q) = query {
                    write!(f, " {}", q)?;
                }
                Ok(())
            }
            Statement::Update {
                table,
                assignments,
                r#where,
            } => {
                write!(f, "UPDATE {} SET ", table)?;
                for (i, a) in assignments.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(
                        f,
                        "{} = {}",
                        a.target
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                            .join("."),
                        a.value
                    )?;
                }
                if let Some(w) = r#where {
                    write!(f, " WHERE {}", w)?;
                }
                Ok(())
            }
            Statement::Delete { table, r#where } => {
                write!(f, "DELETE FROM {}", table)?;
                if let Some(w) = r#where {
                    write!(f, " WHERE {}", w)?;
                }
                Ok(())
            }
            Statement::CreateSchema {
                schema,
                check_exists,
            } => {
                write!(f, "CREATE SCHEMA ")?;
                if *check_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{}", schema)
            }
            Statement::DropSchema {
                schema,
                check_exists,
            } => {
                write!(f, "DROP SCHEMA ")?;
                if *check_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{}", schema)
            }
            Statement::CreateTable {
                query,
                table,
                check_exists,
                columns,
            } => {
                write!(f, "CREATE TABLE ")?;
                if *check_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write!(f, "{} (", table)?;
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", c)?;
                }
                write!(f, ")")?;
                if let Some(q) = query {
                    write!(f, " AS {}", q)?;
                }
                Ok(())
            }
            Statement::DropTable {
                table,
                check_exists,
            } => {
                write!(f, "DROP TABLE ")?;
                if *check_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{}", table)
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug)]

pub enum Distinct {
    ALL,
    DISTINCT(Vec<Expression>),
}

#[derive(Clone, PartialEq, Debug)]

pub enum From {
    Table {
        name: String,
        alias: Option<String>,
    },
    TableFunction {
        name: String,
        args: Vec<FunctionArgument>,
        alias: Option<String>,
    },
    SubQuery {
        query: Box<Statement>,
        alias: Option<String>,
    },
    Join {
        left: Box<From>,
        right: Box<From>,
        on: Option<Expression>,
        join_type: JoinType,
    },
}

impl Display for From {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            From::Table { name, alias } => match alias {
                Some(a) => write!(f, "{} AS {}", name, a),
                None => write!(f, "{}", name),
            },
            From::TableFunction { name, args, alias } => match alias {
                Some(a) => write!(
                    f,
                    "{}({}) AS {}",
                    name,
                    args.iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    a
                ),
                None => write!(
                    f,
                    "{}({})",
                    name,
                    args.iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                ),
            },
            From::SubQuery { query, alias } => match alias {
                Some(a) => write!(f, "({}) AS {}", query, a),
                None => write!(f, "({})", query),
            },
            From::Join {
                left,
                right,
                on,
                join_type,
            } => {
                write!(f, "{}", left)?;
                match join_type {
                    JoinType::Cross => write!(f, " CROSS JOIN ")?,
                    JoinType::Inner => write!(f, " INNER JOIN ")?,
                    JoinType::Left => write!(f, " LEFT JOIN ")?,
                    JoinType::Full => write!(f, " FULL JOIN ")?,
                    JoinType::Right => write!(f, " RIGHT JOIN ")?,
                }
                write!(f, "{}", right)?;
                if let Some(on) = on {
                    write!(f, " ON {}", on)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Full,
    Right,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Expression {
    /// single identifier eg. `table` or `column`
    Identifier(Ident),
    /// multiple identifiers eg. `table.column`
    CompoundIdentifier(Vec<Ident>),
    Literal(Literal),
    BinaryOperator(BinaryOperator),
    Function(String, Vec<Expression>),
    Struct(Vec<StructField>),
    Array(Vec<Expression>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        field: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubQuery {
        field: Box<Expression>,
        query: Box<Statement>,
        negated: bool,
    },
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Literal(l) => write!(f, "{}", l),
            Expression::BinaryOperator(o) => write!(f, "{}", o),
            Expression::Function(n, args) => {
                write!(
                    f,
                    "{}({})",
                    n,
                    args.iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            Expression::InList {
                field,
                list,
                negated,
            } => {
                write!(
                    f,
                    "{} {} IN ({})",
                    field,
                    if *negated { "NOT" } else { "" },
                    list.iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            Expression::InSubQuery {
                field,
                query,
                negated,
            } => {
                write!(
                    f,
                    "{} {} IN ({})",
                    field,
                    if *negated { "NOT" } else { "" },
                    query
                )
            }
            Expression::Identifier(i) => write!(f, "{}", i),
            Expression::Struct(s) => write!(
                f,
                "({})",
                s.iter()
                    .map(|field| format!("{} = {}", field.name, field.value))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Expression::Array(a) => write!(
                f,
                "[{}]",
                a.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Expression::CompoundIdentifier(idents) => write!(
                f,
                "{}",
                idents
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(".")
            ),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct StructField {
    pub name: Expression,
    pub value: Expression,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Assignment {
    pub target: Vec<Ident>,
    pub value: Expression,
}

#[derive(Clone, PartialEq, Debug)]
pub struct FunctionArgument {
    pub id: Option<Ident>,
    pub value: Expression,
}

impl Display for FunctionArgument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.id {
            Some(id) => write!(f, "{} = {}", id, self.value),
            None => write!(f, "{}", self.value),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Int(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::String(s) => write!(f, "{}", s),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::Null => write!(f, "null"),
        }
    }
}

impl TryInto<i64> for Literal {
    type Error = Error;

    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            Literal::Int(i) => Ok(i),
            _ => Err(Error::ParserError(format!(
                "{} cannot be converted to i64",
                self.to_string()
            ))),
        }
    }
}

impl TryInto<f64> for Literal {
    type Error = Error;

    fn try_into(self) -> Result<f64, Self::Error> {
        match self {
            Literal::Float(f) => Ok(f),
            _ => Err(Error::ParserError(format!(
                "{} cannot be converted to f64",
                self.to_string()
            ))),
        }
    }
}

impl TryInto<String> for Literal {
    type Error = Error;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Literal::String(s) => Ok(s),
            _ => Err(Error::ParserError(format!(
                "{} cannot be converted to String",
                self.to_string()
            ))),
        }
    }
}

impl TryInto<bool> for Literal {
    type Error = Error;

    fn try_into(self) -> Result<bool, Self::Error> {
        match self {
            Literal::Boolean(b) => Ok(b),
            _ => Err(Error::ParserError(format!(
                "{} cannot be converted to bool",
                self.to_string()
            ))),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum BinaryOperator {
    // Logical
    Not(Box<Expression>),
    Eq(Box<Expression>, Box<Expression>),
    NotEq(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Gt(Box<Expression>, Box<Expression>),
    Gte(Box<Expression>, Box<Expression>),
    Lt(Box<Expression>, Box<Expression>),
    Lte(Box<Expression>, Box<Expression>),

    // Mathematical
    Add(Box<Expression>, Box<Expression>),
    Sub(Box<Expression>, Box<Expression>),
    Mul(Box<Expression>, Box<Expression>),
    Div(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),
    Pos(Box<Expression>),
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Not(e) => write!(f, "NOT {}", e),
            BinaryOperator::Sub(l, r) => write!(f, "{} - {}", l, r),
            BinaryOperator::Neg(e) => write!(f, "-{}", e),
            BinaryOperator::Pos(r) => write!(f, "+{}", r),
            BinaryOperator::Add(l, r) => write!(f, "{} + {}", l, r),
            BinaryOperator::Mul(l, r) => write!(f, "{} * {}", l, r),
            BinaryOperator::Div(l, r) => write!(f, "{} / {}", l, r),
            BinaryOperator::Eq(l, r) => write!(f, "{} = {}", l, r),
            BinaryOperator::NotEq(l, r) => write!(f, "{} != {}", l, r),
            BinaryOperator::And(l, r) => write!(f, "{} AND {}", l, r),
            BinaryOperator::Or(l, r) => write!(f, "{} OR {}", l, r),
            BinaryOperator::Gt(l, r) => write!(f, "{} > {}", l, r),
            BinaryOperator::Gte(l, r) => write!(f, "{} >= {}", l, r),
            BinaryOperator::Lt(l, r) => write!(f, "{} < {}", l, r),
            BinaryOperator::Lte(l, r) => write!(f, "{} <= {}", l, r),
        }
    }
}
