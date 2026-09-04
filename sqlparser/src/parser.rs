use crate::{
    ast::{
        self, Assignment, CopyOption, CopySource, CopyTarget, Cte, DateTimeField, Expression, FunctionArgument, Ident,
        ObjectName, OnConflict, Order, Select, SelectItem, Statement, StructField, With,
    },
    datatype::DataType,
    error::{Error, Result},
    lexer::Lexer,
    token::{Keyword, Token, TokenType},
};

#[derive(Debug, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub alias: Option<String>,
    pub args: Vec<FunctionArgument>,
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,

    pub tables: Vec<TableInfo>,
    pub ctes: Vec<TableInfo>,
}

impl<'a> Parser<'a> {
    pub fn new(sql: &'a str) -> Parser<'a> {
        Parser {
            lexer: Lexer::new(sql),
            tables: Vec::new(),
            ctes: Vec::new(),
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let statement = self.parse_statement()?;

        // Whatever the statement did not consume has to be an error. Without this a helper that
        // leaves its tokens behind makes the statement quietly end at that point instead, so a
        // trailing GROUP BY / ORDER BY / LIMIT is dropped and the query silently returns the
        // wrong rows -- which is how several parser bugs here stayed invisible.
        self.next_if_token(TokenType::Semicolon);

        match self.peek() {
            Ok(token) if token.token_type != TokenType::EOF => {
                let token = self.lexer.next();
                Err(Error::UnexpectedToken(token))
            }
            _ => Ok(statement),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Select) => self.parse_select_statement(),
            TokenType::Keyword(Keyword::With) => self.parse_with_statment(),
            TokenType::Keyword(Keyword::Insert) => self.parse_insert_statement(),
            TokenType::Keyword(Keyword::Update) => self.parse_update_statement(),
            TokenType::Keyword(Keyword::Delete) => self.parse_delete_statement(),
            TokenType::Keyword(Keyword::Create) => self.parse_create_statement(),
            TokenType::Keyword(Keyword::Drop) => self.parse_drop_statement(),
            TokenType::Keyword(Keyword::Copy) => self.parse_copy_statement(),
            TokenType::Keyword(Keyword::Show) => self.parse_show(),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_show(&mut self) -> Result<Statement> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Tables) => Ok(Statement::ShowTables),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_copy_statement(&mut self) -> Result<Statement> {
        let token = self.peek()?;
        let source = match token.token_type {
            TokenType::Keyword(Keyword::Select) => {
                self.parse_select().map(|query| CopySource::Query(Box::new(query)))?
            }
            _ => {
                let table = self.parse_object_name()?;

                let mut columns = vec![];
                if self.next_if_token(TokenType::LParen).is_some() {
                    columns = self.parse_comma_separated(Parser::parse_ident)?;
                    self.next_except(TokenType::RParen)?;
                }

                self.tables.push(TableInfo {
                    name: table.to_string(),
                    alias: None,
                    args: vec![],
                });

                CopySource::Table {
                    table_name: table,
                    columns,
                }
            }
        };

        let to = if self.next_if_token(TokenType::Keyword(Keyword::From)).is_some() {
            false
        } else if self.next_if_token(TokenType::Keyword(Keyword::To)).is_some() {
            true
        } else {
            return Err(Error::UnexpectedToken(self.peek()?.clone()));
        };

        let target = self.parse_literal_string().map(|file| CopyTarget::File { file })?;
        let mut options = vec![];
        if self.next_if_token(TokenType::LParen).is_some() {
            options = self.parse_comma_separated(Parser::parse_copy_option)?;
            self.next_except(TokenType::RParen)?;
        }

        Ok(Statement::Copy {
            source,
            to,
            target,
            options,
        })
    }

    fn parse_drop_statement(&mut self) -> Result<Statement> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Schema) => {
                let check_exists = self.parse_if_exists()?;
                let schema = self.next_ident()?;

                Ok(Statement::DropSchema { schema, check_exists })
            }
            TokenType::Keyword(Keyword::Table) => {
                let check_exists = self.parse_if_exists()?;
                let table = self.next_ident()?;

                self.add_relation_table(TableInfo {
                    name: table.clone(),
                    alias: None,
                    args: vec![],
                });

                Ok(Statement::DropTable { table, check_exists })
            }
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Schema) => self.parse_create_schema(),
            TokenType::Keyword(Keyword::Table) => self.parse_create_table(),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        let check_exists = self.parse_if_not_exists()?;
        let table = self.next_ident()?;
        let mut columns = Vec::new();
        // parse table columns
        if self.next_if_token(TokenType::LParen).is_some() {
            loop {
                if self.next_if_token(TokenType::RParen).is_some() {
                    break;
                }
                let mut nullable = true;
                let name = self.next_ident()?;
                let datatype = self.parse_data_type()?;
                let primary_key = if self.next_if_token(TokenType::Keyword(Keyword::Primary)).is_some() {
                    self.next_except(TokenType::Keyword(Keyword::Key))?;

                    nullable = false;
                    true
                } else {
                    false
                };
                let unique = self.next_if_token(TokenType::Keyword(Keyword::Unique)).is_some();

                if self.next_if_token(TokenType::Keyword(Keyword::Not)).is_some() {
                    self.next_except(TokenType::Keyword(Keyword::Null))?;

                    nullable = false;
                } else if self.next_if_token(TokenType::Keyword(Keyword::Null)).is_some() {
                    // An explicit `NULL` marker. It used to be left in the stream and ignored,
                    // which only looked harmless because nullable already defaults to true.
                    if primary_key {
                        return Err(Error::ParserError(format!(
                            "column `{name}` is a PRIMARY KEY and cannot be declared NULL"
                        )));
                    }
                    nullable = true;
                }

                columns.push(ast::Column {
                    name,
                    datatype,
                    nullable,
                    unique,
                    references: None,
                    primary_key,
                    index: false,
                });

                if self.next_if_token(TokenType::Comma).is_none() {
                    // No comma, so the list ends here and the paren has to close it. Breaking
                    // without consuming it left the `)` in the stream, and everything after the
                    // column list was then silently ignored -- `CREATE TABLE t(a int) AS SELECT
                    // ...` quietly built an empty table.
                    self.next_except(TokenType::RParen)?;
                    break;
                }
            }
        }
        // parse query
        let query = if self.next_if_token(TokenType::Keyword(Keyword::As)).is_some() {
            if self.next_if_token(TokenType::Keyword(Keyword::Select)).is_some() {
                Some(self.parse_select()?)
            } else {
                self.next_except(TokenType::Keyword(Keyword::From))?;
                let table = self.parse_table_reference()?;
                Some(Select {
                    with: None,
                    distinct: None,
                    columns: vec![SelectItem::Wildcard],
                    from: vec![table],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                })
            }
        } else {
            None
        };

        Ok(Statement::CreateTable {
            query,
            table,
            columns,
            check_exists,
        })
    }

    fn parse_create_schema(&mut self) -> Result<Statement> {
        let check_exists: bool = self.parse_if_not_exists()?;
        let schema = self.next_ident()?;

        Ok(Statement::CreateSchema { schema, check_exists })
    }

    fn parse_delete_statement(&mut self) -> Result<Statement> {
        self.next_except(TokenType::Keyword(Keyword::From))?;

        let table = self.next_ident()?;

        self.add_relation_table(TableInfo {
            name: table.clone(),
            alias: None,
            args: vec![],
        });

        let r#where = if self.next_if_token(TokenType::Keyword(Keyword::Where)).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };

        Ok(Statement::Delete { table, r#where })
    }

    fn parse_update_statement(&mut self) -> Result<Statement> {
        let table = self.next_ident()?;

        // Collect the target the same way DELETE does; without this the session never resolves it
        // and every UPDATE fails with "Table Not Found".
        self.add_relation_table(TableInfo {
            name: table.clone(),
            alias: None,
            args: vec![],
        });

        self.next_except(TokenType::Keyword(Keyword::Set))?;

        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;

        let r#where = if self.next_if_token(TokenType::Keyword(Keyword::Where)).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };

        Ok(Statement::Update {
            table,
            assignments,
            r#where,
        })
    }

    fn parse_insert_statement(&mut self) -> Result<Statement> {
        self.next_except(TokenType::Keyword(Keyword::Into))?;

        let table = self.next_ident()?;
        // Not `parse_table_alias`: a parenthesised list after an INSERT target is the column list,
        // not a set of alias columns.
        let alias = self.parse_alias()?;

        self.add_relation_table(TableInfo {
            name: table.clone(),
            alias: alias.clone(),
            args: vec![],
        });

        let columns = if self.next_if_token(TokenType::LParen).is_some() {
            let mut columns = vec![];
            loop {
                columns.push(self.parse_expression(0)?);
                if self.next_if_token(TokenType::Comma).is_none() {
                    break;
                }
            }
            self.next_except(TokenType::RParen)?;

            Some(columns)
        } else {
            None
        };
        let query = if self.next_if_token(TokenType::Keyword(Keyword::Select)).is_some() {
            Some(self.parse_select()?)
        } else if self.next_if_token(TokenType::Keyword(Keyword::From)).is_some() {
            let table = self.parse_table_reference()?;
            Some(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![table],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })
        } else {
            None
        };

        let values = if self.next_if_token(TokenType::Keyword(Keyword::Values)).is_some() {
            self.parse_values()?
        } else {
            vec![]
        };

        let on_conflict = if self.next_if_token(TokenType::Keyword(Keyword::On)).is_some() {
            Some(self.parse_on_conflict()?)
        } else {
            None
        };

        let returning = if self.next_if_token(TokenType::Keyword(Keyword::Returning)).is_some() {
            self.parse_columns().ok()
        } else {
            None
        };

        Ok(Statement::Insert {
            query,
            table,
            alias,
            columns,
            values,
            on_conflict,
            returning,
        })
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        self.parse_select().map(|s| Statement::Select(Box::new(s)))
    }

    fn parse_with_statment(&mut self) -> Result<Statement> {
        let with = self.parse_cte_with()?;
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Select) => self.parse_select().map(|mut select| {
                select.with = Some(with);
                Statement::Select(Box::new(select))
            }),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_select(&mut self) -> Result<Select> {
        let distinct = self.parse_distinct()?;

        let columns = self.parse_columns()?;

        let from = if self.next_if_token(TokenType::Keyword(Keyword::From)).is_none() {
            vec![]
        } else {
            self.parse_from_statment()?
        };

        let r#where = if self.next_if_token(TokenType::Keyword(Keyword::Where)).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };

        let group_by = if self.next_if_token(TokenType::Keyword(Keyword::Group)).is_some() {
            Some(self.parse_group_by()?)
        } else {
            None
        };

        let having = if self.next_if_token(TokenType::Keyword(Keyword::Having)).is_some() {
            Some(self.parse_expression(0)?)
        } else {
            None
        };

        let order_by = if self.next_if_token(TokenType::Keyword(Keyword::Order)).is_some() {
            Some(self.parse_order_by()?)
        } else {
            None
        };

        let mut limit = None;
        let mut offset = None;

        for _ in 0..2 {
            if self.next_if_token(TokenType::Keyword(Keyword::Limit)).is_some() {
                limit = Some(self.parse_expression(0)?);
            }

            if self.next_if_token(TokenType::Keyword(Keyword::Offset)).is_some() {
                offset = Some(self.parse_expression(0)?)
            }
        }

        Ok(Select {
            with: None,
            distinct,
            columns,
            from,
            r#where,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_cte_with(&mut self) -> Result<With> {
        let mut ctes = vec![];
        loop {
            let cte_table_name = self.parse_ident()?.value;

            // `WITH name (c1, c2, ...) AS (...)` names the CTE's output columns.
            let mut cte_columns = vec![];
            if self.next_if_token(TokenType::LParen).is_some() {
                while self.next_if_token(TokenType::RParen).is_none() {
                    cte_columns.push(self.next_ident()?);
                    self.next_if_token(TokenType::Comma);
                }
            }

            self.next_except(TokenType::Keyword(Keyword::As))?;

            self.next_except(TokenType::LParen)?;

            let token = self.next_token()?;
            match token.token_type {
                TokenType::Keyword(Keyword::Select) => ctes.push(Cte {
                    alias: cte_table_name.clone(),
                    query: Box::new(self.parse_select()?),
                    columns: cte_columns,
                }),
                _ => return Err(Error::UnexpectedToken(token)),
            }

            self.add_cte_table(TableInfo {
                name: cte_table_name,
                alias: None,
                args: vec![],
            });

            self.next_except(TokenType::RParen)?;

            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }

        Ok(With {
            recursive: false,
            cte_tables: ctes,
        })
    }

    fn parse_copy_option(&mut self) -> Result<CopyOption> {
        let token = self.next_token()?;

        match token.token_type {
            TokenType::Keyword(Keyword::Format) => self.parse_ident().map(CopyOption::Format),
            TokenType::Keyword(Keyword::Header) => Ok(CopyOption::Header(true)),
            TokenType::Keyword(Keyword::Delimiter) => self.parse_literal_char().map(CopyOption::Delimiter),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_on_conflict(&mut self) -> Result<OnConflict> {
        self.next_except(TokenType::Keyword(Keyword::Conflict))?;

        let mut constraints = vec![];
        self.next_except(TokenType::LParen)?;
        loop {
            constraints.push(self.parse_ident()?);
            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }
        self.next_except(TokenType::RParen)?;
        self.next_except(TokenType::Keyword(Keyword::Do))?;

        if self.next_if_token(TokenType::Keyword(Keyword::Nothing)).is_some() {
            Ok(OnConflict::DoNothing)
        } else {
            self.next_except(TokenType::Keyword(Keyword::Update))?;
            self.next_except(TokenType::Keyword(Keyword::Set))?;

            let mut values = Vec::new();
            loop {
                values.push(self.parse_expression(0)?);
                if self.next_if_token(TokenType::Comma).is_none() {
                    break;
                }
            }

            Ok(OnConflict::DoUpdate { constraints, values })
        }
    }

    fn parse_values(&mut self) -> Result<Vec<Vec<Expression>>> {
        let mut values = Vec::new();
        loop {
            if self.next_if_token(TokenType::Comma).is_some() {
                continue;
            }
            let mut row = Vec::new();

            self.next_except(TokenType::LParen)?;

            while self.next_if_token(TokenType::RParen).is_none() {
                row.push(self.parse_expression(0)?);
                self.next_if_token(TokenType::Comma);
            }
            values.push(row);
            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }
        Ok(values)
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        let mut check_exists = false;
        if self.next_if_token(TokenType::Keyword(Keyword::If)).is_some() {
            self.next_except(TokenType::Keyword(Keyword::Not))?;
            self.next_except(TokenType::Keyword(Keyword::Exists))?;

            check_exists = true;
        }
        Ok(check_exists)
    }

    fn parse_if_exists(&mut self) -> Result<bool> {
        let mut check_exists = false;
        if self.next_if_token(TokenType::Keyword(Keyword::If)).is_some() {
            self.next_except(TokenType::Keyword(Keyword::Exists))?;

            check_exists = true;
        }
        Ok(check_exists)
    }

    fn parse_order_by(&mut self) -> Result<Vec<(Expression, Order)>> {
        self.next_except(TokenType::Keyword(Keyword::By))?;

        let mut order_fields = vec![];
        loop {
            let expr = self.parse_expression(0)?;
            let mut order = ast::Order::Asc;

            if self.next_if_token(TokenType::Keyword(Keyword::Asc)).is_some() {
                order = ast::Order::Asc;
            }
            if self.next_if_token(TokenType::Keyword(Keyword::Desc)).is_some() {
                order = ast::Order::Desc;
            }

            order_fields.push((expr, order));

            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }

        Ok(order_fields)
    }

    fn parse_group_by(&mut self) -> Result<Vec<Expression>> {
        self.next_except(TokenType::Keyword(Keyword::By))?;

        let mut group_by = Vec::new();
        while self.next_if_token(TokenType::Semicolon).is_none() {
            group_by.push(self.parse_expression(0)?);
            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }

        Ok(group_by)
    }

    fn parse_distinct(&mut self) -> Result<Option<ast::Distinct>> {
        if self.next_if_token(TokenType::Keyword(Keyword::Distinct)).is_some() {
            if self.next_if_token(TokenType::Keyword(Keyword::On)).is_some() {
                self.next_except(TokenType::LParen)?;

                let mut columns = Vec::new();
                while self.next_if_token(TokenType::RParen).is_none() {
                    columns.push(self.parse_expression(0)?);
                    self.next_if_token(TokenType::Comma);
                }

                Ok(Some(ast::Distinct::DISTINCT(columns)))
            } else {
                Ok(Some(ast::Distinct::ALL))
            }
        } else {
            Ok(None)
        }
    }

    fn parse_columns(&mut self) -> Result<Vec<SelectItem>> {
        let mut columns = Vec::new();

        loop {
            if self.next_if_token(TokenType::Comma).is_some() {
                continue;
            }
            let expr = self.parse_expression(0)?;
            let alias = self.parse_alias()?;

            let col = match expr {
                Expression::CompoundIdentifier(ref idents) => {
                    if idents.last().filter(|a| a.value == "*").is_some() {
                        SelectItem::QualifiedWildcard(
                            idents
                                .iter()
                                .filter_map(|i| if i.value == "*" { None } else { Some(i.value.clone()) })
                                .collect(),
                        )
                    } else if let Some(alias) = alias {
                        SelectItem::ExprWithAlias(expr, alias)
                    } else {
                        SelectItem::UnNamedExpr(expr)
                    }
                }
                Expression::Identifier(ref ident) => match alias {
                    Some(alias) => SelectItem::ExprWithAlias(expr, alias),
                    None if ident.value == "*" => SelectItem::Wildcard,
                    None => SelectItem::UnNamedExpr(expr),
                },
                Expression::Literal(_)
                | Expression::BinaryOperator(_)
                | Expression::Function { .. }
                | Expression::InSubQuery { .. } => match alias {
                    Some(a) => SelectItem::ExprWithAlias(expr, a),
                    None => SelectItem::UnNamedExpr(expr),
                },
                _ => {
                    if let Some(alias) = alias {
                        SelectItem::ExprWithAlias(expr, alias)
                    } else {
                        SelectItem::UnNamedExpr(expr)
                    }
                }
            };

            columns.push(col);

            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_from_statment(&mut self) -> Result<Vec<ast::From>> {
        let relation = self.parse_table_reference()?;
        let mut table_ref = vec![relation];
        loop {
            if let Some(join_type) = self.parse_join_type()? {
                let right = self.parse_table_reference()?;
                let on = if join_type == ast::JoinType::Cross {
                    None
                } else {
                    self.next_except(TokenType::Keyword(Keyword::On))?;
                    Some(self.parse_expression(0)?)
                };

                let left = table_ref.pop().ok_or(Error::ParserError("no left table".to_string()))?;

                table_ref.push(ast::From::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                    join_type,
                });
            } else if self.next_if_token(TokenType::Comma).is_some() {
                table_ref.push(self.parse_table_reference()?);
            } else {
                break;
            }
        }

        Ok(table_ref)
    }

    fn parse_join_type(&mut self) -> Result<Option<ast::JoinType>> {
        let token = self.peek()?;
        let join_type = match token.token_type {
            TokenType::Keyword(Keyword::Left) => ast::JoinType::Left,
            TokenType::Keyword(Keyword::Right) => ast::JoinType::Right,
            TokenType::Keyword(Keyword::Full) => ast::JoinType::Full,
            TokenType::Keyword(Keyword::Cross) => ast::JoinType::Cross,
            TokenType::Keyword(Keyword::Inner) | TokenType::Keyword(Keyword::Join) => ast::JoinType::Inner,
            _ => return Ok(None),
        };
        // consumer keyword token,such as: left \ right \ full \ cross \ inner
        if token.token_type != TokenType::Keyword(Keyword::Join) {
            self.lexer.next();
        }
        // `OUTER` is optional noise in `LEFT/RIGHT/FULL [OUTER] JOIN`
        self.next_if_token(TokenType::Keyword(Keyword::Outer));
        // consumer next keyword token 'join'
        self.next_except(TokenType::Keyword(Keyword::Join))?;

        Ok(Some(join_type))
    }

    fn parse_table_reference(&mut self) -> Result<ast::From> {
        if self.next_if_token(TokenType::LParen).is_some() {
            self.next_except(TokenType::Keyword(Keyword::Select))?;
            let subquery = self.parse_select_statement()?;
            self.next_except(TokenType::RParen)?;

            let (alias, columns) = self.parse_table_alias()?;

            return Ok(ast::From::SubQuery {
                query: Box::new(subquery),
                alias,
                columns,
            });
        }

        let mut table_name = self.next_token().map(|i| i.literal)?;
        let mut is_table_function = false;
        let mut args = Vec::new();

        while let Some(preiod) = self.next_if_token(TokenType::Period) {
            table_name.push_str(&preiod.literal);
            table_name.push_str(&self.next_ident()?);
        }

        // parse table function
        if self.next_if_token(TokenType::LParen).is_some() {
            is_table_function = true;
            while self.next_if_token(TokenType::RParen).is_none() {
                args.push(self.parse_function_argument()?);
                self.next_if_token(TokenType::Comma);
            }
        }

        let (alias, alias_columns) = self.parse_table_alias()?;
        // Consumed above so the tokens cannot be left behind, but only a derived table can act on
        // them; renaming a base table's columns would need a projection the planner does not build.
        if !alias_columns.is_empty() {
            return Err(Error::ParserError(format!(
                "column aliases are only supported on a derived table, not on `{table_name}`"
            )));
        }

        self.add_relation_table(TableInfo {
            name: table_name.clone(),
            alias: alias.clone(),
            args: args.clone(),
        });

        let table = if is_table_function {
            ast::From::TableFunction {
                name: table_name,
                args,
                alias,
            }
        } else {
            ast::From::Table {
                name: table_name,
                alias,
            }
        };

        Ok(table)
    }

    /// An alias on a table factor, optionally followed by names for its columns:
    /// `AS alias (c1, c2, ...)`.
    ///
    /// The column list has to be consumed even where it is not supported, otherwise the leftover
    /// tokens make the enclosing statement stop early and silently drop its remaining clauses.
    fn parse_table_alias(&mut self) -> Result<(Option<String>, Vec<String>)> {
        let alias = self.parse_alias()?;

        let mut columns = vec![];
        if alias.is_some() && self.next_if_token(TokenType::LParen).is_some() {
            while self.next_if_token(TokenType::RParen).is_none() {
                columns.push(self.next_ident()?);
                self.next_if_token(TokenType::Comma);
            }
        }

        Ok((alias, columns))
    }

    fn parse_alias(&mut self) -> Result<Option<String>> {
        if self.next_if_token(TokenType::Keyword(Keyword::As)).is_some() {
            self.next_ident().map(Some)
        } else if let Some(ident) = self.next_if_token(TokenType::Ident) {
            Ok(Some(ident.literal))
        } else {
            Ok(None)
        }
    }

    fn parse_in_expr(&mut self, lhs: Expression, negated: bool) -> Result<Expression> {
        self.next_except(TokenType::LParen)?;

        if self.next_if_token(TokenType::Keyword(Keyword::Select)).is_some() {
            let query = self.parse_select_statement()?;
            // The subquery's closing paren must be consumed here; leaving it in the stream makes
            // the enclosing statement stop early and silently drop its remaining clauses.
            self.next_except(TokenType::RParen)?;

            Ok(Expression::InSubQuery {
                field: Box::new(lhs),
                query: Box::new(query),
                negated,
            })
        } else {
            let mut list = Vec::new();
            while self.next_if_token(TokenType::RParen).is_none() {
                list.push(self.parse_expression(0)?);
                self.next_if_token(TokenType::Comma);
            }
            Ok(Expression::InList {
                field: Box::new(lhs),
                list,
                negated,
            })
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::String | TokenType::Keyword(Keyword::VarChar) => Ok(DataType::String),
            TokenType::Int | TokenType::Keyword(Keyword::Int) | TokenType::Keyword(Keyword::Integer) => {
                Ok(DataType::Integer)
            }
            TokenType::Keyword(Keyword::SmallInt) => Ok(DataType::Int16),
            TokenType::Keyword(Keyword::BigInt) => Ok(DataType::Int64),
            TokenType::Float | TokenType::Keyword(Keyword::Double) => Ok(DataType::Float),
            TokenType::Keyword(Keyword::Bool) | TokenType::Keyword(Keyword::Boolean) => Ok(DataType::Boolean),
            TokenType::Keyword(Keyword::Date) => Ok(DataType::Date),
            TokenType::Keyword(Keyword::Decimal) => {
                let (precision, scale) = if self.next_if_token(TokenType::LParen).is_some() {
                    let precision = self
                        .next_token()?
                        .literal
                        .parse()
                        .map_err(|e| Error::ParseIntError(e, token.clone()))?;
                    self.next_except(TokenType::Comma)?;
                    let scale = self
                        .next_token()?
                        .literal
                        .parse()
                        .map_err(|e| Error::ParseIntError(e, token.clone()))?;
                    self.next_except(TokenType::RParen)?;
                    (Some(precision), Some(scale))
                } else {
                    (None, None)
                };
                Ok(DataType::Decimal(precision, scale))
            }
            _ => Err(Error::ParserError(format!(
                "[parse_data_type] unexpected token {:?}",
                token
            ))),
        }
    }

    fn parse_date_time_field(&mut self) -> Result<DateTimeField> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::Keyword(Keyword::Year) => Ok(DateTimeField::Year),
            TokenType::Keyword(Keyword::Month) => Ok(DateTimeField::Month),
            TokenType::Keyword(Keyword::Day) => Ok(DateTimeField::Day),
            TokenType::Keyword(Keyword::Hour) => Ok(DateTimeField::Hour),
            TokenType::Keyword(Keyword::Minute) => Ok(DateTimeField::Minute),
            TokenType::Keyword(Keyword::Second) => Ok(DateTimeField::Second),
            _ => Err(Error::ParserError(format!(
                "[parse_date_time_field] unexpected token {:?}",
                token
            ))),
        }
    }

    fn parse_expression(&mut self, precedence: u8) -> Result<Expression> {
        let mut lhs = if let Some(prefix) = self.next_if_operator::<PrefixOperator>(precedence) {
            prefix.build(self.parse_expression(prefix.precedence())?)
        } else {
            self.parse_expression_atom()?
        };

        let negated = self.next_if_token(TokenType::Keyword(Keyword::Not)).is_some();
        loop {
            if self.get_nexr_precedence() <= precedence {
                break;
            }
            let infix = self
                .next_if_operator::<InfixOperator>(precedence)
                .ok_or(Error::ParserError(format!(
                    "[parse_expression] unexpected token {:?}",
                    self.peek()?
                )))?;
            lhs = match infix {
                InfixOperator::Like => Expression::Like {
                    negated,
                    left: Box::new(lhs),
                    right: self.parse_expression(infix.precedence()).map(Box::new)?,
                },
                InfixOperator::Between => {
                    let low = self.parse_expression(infix.precedence())?;
                    self.next_except(TokenType::Keyword(Keyword::And))?;
                    let high = self.parse_expression(infix.precedence())?;

                    Expression::Between {
                        negated,
                        expr: Box::new(lhs),
                        low: Box::new(low),
                        high: Box::new(high),
                    }
                }
                InfixOperator::In => self.parse_in_expr(lhs, negated)?,
                InfixOperator::DoubleColon => self.parse_data_type().map(|dt| Expression::Cast {
                    expr: Box::new(lhs),
                    data_type: dt,
                })?,
                InfixOperator::Is => {
                    // `IS [NOT] NULL`: the `NOT` is optional but `NULL` is then mandatory, so both
                    // have to be consumed here. Leaving `NULL` in the stream would make the
                    // enclosing statement stop early and silently drop its remaining clauses.
                    let is_negated = self.next_if_token(TokenType::Keyword(Keyword::Not)).is_some();
                    self.next_except(TokenType::Keyword(Keyword::Null))?;

                    if is_negated {
                        Expression::IsNotNull(Box::new(lhs))
                    } else {
                        Expression::IsNull(Box::new(lhs))
                    }
                }
                _ => infix.build(lhs, self.parse_expression(infix.precedence())?)?,
            };
        }

        Ok(lhs)
    }

    fn parse_expression_atom(&mut self) -> Result<Expression> {
        let token = self.next_token()?;
        let literal = token.literal.clone();
        match token.token_type {
            TokenType::Keyword(Keyword::Select) => {
                self.parse_select().map(|query| Expression::SubQuery(Box::new(query)))
            }
            TokenType::Keyword(Keyword::Not) => {
                // `NOT` can either be part of `NOT EXISTS (...)` (handled via the Exists node with
                // a dedicated negated flag), or it can be a general unary operator.
                if self.next_if_token(TokenType::Keyword(Keyword::Exists)).is_some() {
                    self.parse_exists_expr(true)
                } else {
                    Ok(Expression::UnaryOperator {
                        op: ast::UnaryOperator::Not,
                        // NOT has lower precedence than comparison operators, but higher than AND/OR.
                        // Parsing the RHS at precedence 2 yields:
                        // - `NOT a = b` => `NOT (a = b)`
                        // - `NOT a AND b` => `(NOT a) AND b`
                        expr: Box::new(self.parse_expression(2)?),
                    })
                }
            }
            TokenType::Keyword(Keyword::Extract) => {
                self.next_except(TokenType::LParen)?;

                let field = self.parse_date_time_field()?;
                self.next_except(TokenType::Keyword(Keyword::From))?;
                let expr = self.parse_expression(0)?;

                self.next_except(TokenType::RParen)?;
                Ok(Expression::Extract {
                    field,
                    expr: Box::new(expr),
                })
            }
            TokenType::Keyword(Keyword::Cast) => {
                // CAST(<expr> AS <data_type>)
                self.next_except(TokenType::LParen)?;
                let expr = self.parse_expression(0)?;
                self.next_except(TokenType::Keyword(Keyword::As))?;
                let data_type = self.parse_data_type()?;
                self.next_except(TokenType::RParen)?;
                Ok(Expression::Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }
            TokenType::Keyword(Keyword::Substring) => self.parse_substring_expr(),
            TokenType::Keyword(Keyword::Case) => self.parse_case_expr(),
            TokenType::Keyword(Keyword::Interval) => self.parse_interval(),
            TokenType::Asterisk => Ok(ast::Expression::Identifier("*".into())),
            TokenType::Float => literal
                .parse()
                .map(|f| ast::Expression::Literal(ast::Literal::Float(f)))
                .map_err(|e| Error::ParseFloatError(e, token)),
            TokenType::Int => literal
                .parse()
                .map(|i| ast::Expression::Literal(ast::Literal::Int(i)))
                .map_err(|e| Error::ParseIntError(e, token)),
            TokenType::String => Ok(ast::Expression::Literal(ast::Literal::String(literal))),
            TokenType::Keyword(Keyword::True) => Ok(ast::Expression::Literal(ast::Literal::Boolean(true))),
            TokenType::Keyword(Keyword::False) => Ok(ast::Expression::Literal(ast::Literal::Boolean(false))),
            TokenType::Keyword(Keyword::Null) => Ok(ast::Expression::Literal(ast::Literal::Null)),
            TokenType::Keyword(Keyword::Exists) => self.parse_exists_expr(false),
            TokenType::LParen => {
                let expr = self.parse_expression(0)?;
                self.next_except(TokenType::RParen)?;
                Ok(expr)
            }
            TokenType::LBrace => {
                let mut fields = vec![];
                while self.next_if_token(TokenType::RBrace).is_none() {
                    let name = self.parse_expression(0)?;
                    self.next_except(TokenType::Colon)?;
                    let value = self.parse_expression(0)?;
                    fields.push(StructField { name, value });
                    self.next_if_token(TokenType::Comma);
                }
                Ok(ast::Expression::Struct(fields))
            }
            TokenType::LSquareBrace => {
                let mut list = vec![];
                while self.next_if_token(TokenType::RSquareBrace).is_none() {
                    list.push(self.parse_expression(0)?);
                    self.next_if_token(TokenType::Comma);
                }
                Ok(ast::Expression::Array(list))
            }
            // `LEFT` and `RIGHT` are join keywords, but they are also standard function names. In
            // expression position, followed by a paren, only the function reading is possible.
            TokenType::Keyword(Keyword::Left) | TokenType::Keyword(Keyword::Right)
                if self.peek().map(|t| t.token_type == TokenType::LParen).unwrap_or(false) =>
            {
                self.next_except(TokenType::LParen)?;
                self.parse_function_call(literal)
            }
            TokenType::Ident => {
                // parse function
                if self.next_if_token(TokenType::LParen).is_some() {
                    self.parse_function_call(literal)
                } else {
                    let mut idents: Vec<Ident> = vec![literal.into()];

                    while self.next_if_token(TokenType::Period).is_some() {
                        idents.push(self.next_ident().map(|s| s.into())?);
                    }
                    if idents.len() > 1 {
                        Ok(ast::Expression::CompoundIdentifier(idents))
                    } else {
                        Ok(ast::Expression::Identifier(idents.remove(0)))
                    }
                }
            }
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn parse_case_expr(&mut self) -> Result<Expression> {
        // CASE [operand] WHEN cond THEN result ... [ELSE result] END
        // Note: `CASE` token already consumed.
        let mut operand: Option<Box<Expression>> = None;
        if !matches!(self.peek()?.token_type, TokenType::Keyword(Keyword::When)) {
            operand = Some(Box::new(self.parse_expression(0)?));
        }

        let mut when_then = vec![];
        while self.next_if_token(TokenType::Keyword(Keyword::When)).is_some() {
            let when_expr = self.parse_expression(0)?;
            self.next_except(TokenType::Keyword(Keyword::Then))?;
            let then_expr = self.parse_expression(0)?;
            when_then.push((when_expr, then_expr));
        }

        let else_expr = if self.next_if_token(TokenType::Keyword(Keyword::Else)).is_some() {
            Some(Box::new(self.parse_expression(0)?))
        } else {
            None
        };

        self.next_except(TokenType::Keyword(Keyword::End))?;
        Ok(Expression::Case {
            operand,
            when_then,
            else_expr,
        })
    }

    /// `SUBSTRING(<expr> FROM <start> [FOR <length>])`, and the equivalent
    /// `SUBSTRING(<expr>, <start> [, <length>])`.
    ///
    /// Both spellings produce the same plain function call, so the engine only needs one
    /// `substring` implementation.
    fn parse_substring_expr(&mut self) -> Result<Expression> {
        self.next_except(TokenType::LParen)?;

        let mut args = vec![self.parse_expression(0)?];

        // `FROM` and `,` are interchangeable separators here, as are `FOR` and `,`.
        if self.next_if_token(TokenType::Keyword(Keyword::From)).is_some()
            || self.next_if_token(TokenType::Comma).is_some()
        {
            args.push(self.parse_expression(0)?);

            if self.next_if_token(TokenType::Keyword(Keyword::For)).is_some()
                || self.next_if_token(TokenType::Comma).is_some()
            {
                args.push(self.parse_expression(0)?);
            }
        }

        self.next_except(TokenType::RParen)?;

        Ok(Expression::Function {
            name: "substring".to_owned(),
            args,
            distinct: false,
        })
    }

    /// The arguments of a call whose name and opening paren have already been consumed.
    fn parse_function_call(&mut self, name: String) -> Result<Expression> {
        // `DISTINCT` is only meaningful for aggregates, but accepting it here keeps the call syntax
        // in one place; the planner rejects it for other functions.
        let distinct = self.next_if_token(TokenType::Keyword(Keyword::Distinct)).is_some();

        let mut args = Vec::new();
        while self.next_if_token(TokenType::RParen).is_none() {
            args.push(self.parse_expression(0)?);
            self.next_if_token(TokenType::Comma);
        }

        Ok(Expression::Function { name, args, distinct })
    }

    fn parse_exists_expr(&mut self, negated: bool) -> Result<Expression> {
        self.next_except(TokenType::LParen)?;
        // `parse_statement`, not `parse`: the subquery is followed by its closing paren, so the
        // token stream is deliberately not exhausted here.
        let subquery = self.parse_statement()?;
        let Statement::Select(select) = subquery else {
            return Err(Error::ParserError(format!(
                "[parse_exists_expr] unexpected token {:?}",
                self.peek()?
            )));
        };
        self.next_except(TokenType::RParen)?;
        Ok(Expression::Exists {
            subquery: select,
            negated,
        })
    }

    fn parse_interval(&mut self) -> Result<Expression> {
        let expr = self.parse_expression(0).map(Box::new)?;
        Ok(Expression::Interval {
            expr,
            field: self.next_token()?.try_into()?,
        })
    }

    fn parse_ident(&mut self) -> Result<Ident> {
        self.next_except(TokenType::Ident).map(|ident| Ident {
            value: ident.literal,
            quote_style: None,
        })
    }

    fn parse_literal_string(&mut self) -> Result<String> {
        self.next_except(TokenType::String).map(|s| s.literal)
    }

    fn parse_literal_char(&mut self) -> Result<char> {
        let token = self.next_token()?;
        if token.token_type != TokenType::String || token.literal.len() != 1 {
            return Err(Error::UnexpectedToken(token));
        }
        Ok(token.literal.chars().next().unwrap())
    }

    fn parse_assignment(&mut self) -> Result<Assignment> {
        let target = self.parse_comma_separated(Parser::parse_ident).map(ObjectName)?;
        self.next_except(TokenType::Eq)?;
        let value = self.parse_expression(0)?;
        Ok(Assignment { target, value })
    }

    fn parse_object_name(&mut self) -> Result<ObjectName> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_ident()?);
            if self.next_if_token(TokenType::Period).is_none() {
                break;
            }
        }

        Ok(ObjectName(idents))
    }

    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Self) -> Result<T>,
    {
        let mut items = vec![];
        loop {
            items.push(f(self)?);

            if self.next_if_token(TokenType::Comma).is_none() {
                break;
            }
        }

        Ok(items)
    }

    fn parse_function_argument(&mut self) -> Result<FunctionArgument> {
        let token = self.next_token()?;
        match token.token_type {
            TokenType::String => Ok(FunctionArgument {
                id: None,
                value: Expression::Literal(ast::Literal::String(token.literal)),
            }),
            TokenType::Ident | TokenType::Keyword(Keyword::Header) => {
                self.next_except(TokenType::Eq)?;
                Ok(FunctionArgument {
                    id: Some(Ident {
                        value: token.literal,
                        quote_style: None,
                    }),
                    value: self.parse_expression(0)?,
                })
            }
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        let token = self.lexer.next();
        match token.token_type {
            TokenType::EOF => Err(Error::UnexpectedEOF(token)),
            TokenType::ILLIGAL => Err(Error::UnexpectedToken(token)),
            _ => Ok(token),
        }
    }

    fn next_except(&mut self, except: TokenType) -> Result<Token> {
        let token = self.lexer.next();
        if token.token_type == except {
            return Ok(token);
        }

        Err(Error::UnexpectedToken(token))
    }

    fn next_ident(&mut self) -> Result<String> {
        let token = self.lexer.next();
        match token.token_type {
            TokenType::Asterisk | TokenType::Ident | TokenType::Keyword(_) => Ok(token.literal),
            TokenType::EOF => Err(Error::UnexpectedEOF(token)),
            _ => Err(Error::UnexpectedToken(token)),
        }
    }

    fn next_if_operator<O: Operator>(&mut self, precedence: u8) -> Option<O> {
        self.lexer
            .peek()
            .and_then(|t| O::from(t))
            .filter(|op| op.precedence() >= precedence)?;
        O::from(&self.lexer.next())
    }

    fn get_nexr_precedence(&mut self) -> u8 {
        self.lexer
            .peek()
            .and_then(|t| Operator::from(t))
            .map(|op: InfixOperator| op.precedence())
            .unwrap_or_default()
    }

    fn next_if_token(&mut self, token: TokenType) -> Option<Token> {
        self.lexer.peek().filter(|t| t.token_type == token)?;
        Some(self.lexer.next())
    }

    fn peek(&mut self) -> Result<&Token> {
        let localtion = self.lexer.location();
        self.lexer.peek().ok_or(Error::UnexpectedEOF(Token {
            token_type: TokenType::EOF,
            literal: "".to_owned(),
            location: localtion,
        }))
    }
}

impl<'a> Parser<'a> {
    fn add_relation_table(&mut self, table: TableInfo) {
        if !self.tables.contains(&table) && !self.ctes.contains(&table) {
            self.tables.push(table);
        }
    }

    fn add_cte_table(&mut self, table: TableInfo) {
        if !self.ctes.contains(&table) {
            self.ctes.push(table);
        }
    }
}

trait Operator: Sized {
    fn from(token: &Token) -> Option<Self>;

    fn precedence(&self) -> u8;
}

enum PrefixOperator {
    Plus,
    Minus,
    Not,
    Date,
    Timestamp,
}

impl Operator for PrefixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token.token_type {
            TokenType::Plus => Some(PrefixOperator::Plus),
            TokenType::Minus => Some(PrefixOperator::Minus),
            TokenType::Bang => Some(PrefixOperator::Not),
            TokenType::Keyword(Keyword::Date) => Some(PrefixOperator::Date),
            TokenType::Keyword(Keyword::Timestamp) => Some(PrefixOperator::Timestamp),
            _ => None,
        }
    }

    fn precedence(&self) -> u8 {
        9
    }
}

impl PrefixOperator {
    fn build(&self, rhs: Expression) -> Expression {
        match self {
            PrefixOperator::Plus => Expression::UnaryOperator {
                op: ast::UnaryOperator::Plus,
                expr: Box::new(rhs),
            },
            PrefixOperator::Minus => Expression::UnaryOperator {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(rhs),
            },
            PrefixOperator::Not => Expression::UnaryOperator {
                op: ast::UnaryOperator::Not,
                expr: Box::new(rhs),
            },
            PrefixOperator::Date => Expression::TypedString {
                data_type: DataType::Date,
                value: rhs.to_string(),
            },
            PrefixOperator::Timestamp => Expression::TypedString {
                data_type: DataType::Timestamp,
                value: rhs.to_string(),
            },
        }
    }
}

#[derive(Debug)]
enum InfixOperator {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    NotEq,
    And,
    Or,
    In,
    Between,
    DoubleColon,
    Is,
    Like,
}

impl Operator for InfixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token.token_type {
            TokenType::Plus => Some(InfixOperator::Add),
            TokenType::Minus => Some(InfixOperator::Sub),
            TokenType::Asterisk => Some(InfixOperator::Mul),
            TokenType::Slash => Some(InfixOperator::Div),
            TokenType::Gt => Some(InfixOperator::Gt),
            TokenType::Gte => Some(InfixOperator::Gte),
            TokenType::Lt => Some(InfixOperator::Lt),
            TokenType::Lte => Some(InfixOperator::Lte),
            TokenType::Eq => Some(InfixOperator::Eq),
            TokenType::NotEq => Some(InfixOperator::NotEq),
            TokenType::DoubleColon => Some(InfixOperator::DoubleColon),
            TokenType::Keyword(Keyword::And) => Some(InfixOperator::And),
            TokenType::Keyword(Keyword::Or) => Some(InfixOperator::Or),
            TokenType::Keyword(Keyword::In) => Some(InfixOperator::In),
            TokenType::Keyword(Keyword::Between) => Some(InfixOperator::Between),
            TokenType::Keyword(Keyword::Is) => Some(InfixOperator::Is),
            TokenType::Keyword(Keyword::Like) => Some(InfixOperator::Like),
            _ => None,
        }
    }

    fn precedence(&self) -> u8 {
        match self {
            InfixOperator::Or => 1,
            InfixOperator::And => 2,
            InfixOperator::Eq | InfixOperator::NotEq | InfixOperator::Like | InfixOperator::Between => 3,
            InfixOperator::Gt | InfixOperator::Gte | InfixOperator::Lt | InfixOperator::Lte => 4,
            InfixOperator::Add | InfixOperator::Sub => 5,
            InfixOperator::Mul | InfixOperator::Div => 6,
            InfixOperator::In => 7,
            InfixOperator::DoubleColon | InfixOperator::Is => 8,
        }
    }
}

impl InfixOperator {
    pub fn build(&self, lhr: Expression, rhs: Expression) -> Result<Expression> {
        macro_rules! build_binary_operator {
            ($($variant: ident),+ $(,)?) => {
                match self {
                    $(InfixOperator::$variant => Ok(Expression::BinaryOperator(ast::BinaryOperator::$variant(
                        Box::new(lhr),
                        Box::new(rhs),
                    ))),)+
                    _ => Err(Error::UnKnownInfixOperator(format!("{:?}", self)))
                }
            };
        }

        build_binary_operator!(Add, Sub, Mul, Div, Gt, Gte, Lt, Lte, Eq, NotEq, And, Or)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Parser;
    use crate::ast::{
        self, Assignment, BinaryOperator, CopyOption, CopySource, CopyTarget, DateTimeField, Expression,
        FunctionArgument, Ident, Select, SelectItem, Statement,
    };
    use crate::datatype::{DataType, IntervalFields};
    use crate::error::Result;
    use crate::parser::TableInfo;

    fn assert_stmt_eq(sql: &str, stmt: Statement) {
        let result = parse_stmt(sql).unwrap();
        assert_eq!(result, stmt, "Runing SQL: {}", sql);
    }

    #[test]
    fn test_exists_expr() {
        assert_stmt_eq(
            "SELECT * FROM users WHERE EXISTS (SELECT * FROM commits WHERE commits.user_id = users.id)",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: Some(Expression::Exists {
                    subquery: Box::new(Select {
                        with: None,
                        distinct: None,
                        columns: vec![SelectItem::Wildcard],
                        from: vec![ast::From::Table {
                            name: "commits".to_owned(),
                            alias: None,
                        }],
                        r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "commits".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "user_id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "users".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                        ))),
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                    }),
                    negated: false,
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE NOT EXISTS (SELECT * FROM commits WHERE commits.user_id = users.id)",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: Some(Expression::Exists {
                    subquery: Box::new(Select {
                        with: None,
                        distinct: None,
                        columns: vec![SelectItem::Wildcard],
                        from: vec![ast::From::Table {
                            name: "commits".to_owned(),
                            alias: None,
                        }],
                        r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "commits".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "user_id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "users".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                        ))),
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                    }),
                    negated: true,
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_not_is_not_silently_dropped_for_interval_or_literals() -> Result<()> {
        // Regression test: `NOT` must not be unconditionally consumed and discarded.
        // It should either negate EXISTS via the Exists node, or become a unary operator.
        assert_eq!(
            parse_expr("NOT INTERVAL '1' DAY")?,
            Expression::UnaryOperator {
                op: ast::UnaryOperator::Not,
                expr: Box::new(Expression::Interval {
                    expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                    field: crate::datatype::IntervalFields::Day,
                }),
            }
        );

        assert_eq!(
            parse_expr("NOT TRUE")?,
            Expression::UnaryOperator {
                op: ast::UnaryOperator::Not,
                expr: Box::new(Expression::Literal(ast::Literal::Boolean(true))),
            }
        );

        Ok(())
    }

    #[test]
    fn test_join_multiple_table() {
        assert_stmt_eq(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c on c.id = b.id",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    left: Box::new(ast::From::Join {
                        left: Box::new(ast::From::Table {
                            name: "a".to_owned(),
                            alias: None,
                        }),
                        right: Box::new(ast::From::Table {
                            name: "b".to_owned(),
                            alias: None,
                        }),
                        on: Some(Expression::BinaryOperator(BinaryOperator::Eq(
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "a".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "b".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                        ))),
                        join_type: ast::JoinType::Left,
                    }),
                    right: Box::new(ast::From::Table {
                        name: "c".to_owned(),
                        alias: None,
                    }),
                    on: Some(Expression::BinaryOperator(BinaryOperator::Eq(
                        Box::new(Expression::CompoundIdentifier(vec![
                            Ident {
                                value: "c".to_owned(),
                                quote_style: None,
                            },
                            Ident {
                                value: "id".to_owned(),
                                quote_style: None,
                            },
                        ])),
                        Box::new(Expression::CompoundIdentifier(vec![
                            Ident {
                                value: "b".to_owned(),
                                quote_style: None,
                            },
                            Ident {
                                value: "id".to_owned(),
                                quote_style: None,
                            },
                        ])),
                    ))),
                    join_type: ast::JoinType::Left,
                }],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM a,b LEFT JOIN c ON a.id = c.id LEFT JOIN d ON b.id = d.id",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![
                    ast::From::Table {
                        name: "a".to_owned(),
                        alias: None,
                    },
                    ast::From::Join {
                        left: Box::new(ast::From::Join {
                            left: Box::new(ast::From::Table {
                                name: "b".to_owned(),
                                alias: None,
                            }),
                            right: Box::new(ast::From::Table {
                                name: "c".to_owned(),
                                alias: None,
                            }),
                            on: Some(Expression::BinaryOperator(BinaryOperator::Eq(
                                Box::new(Expression::CompoundIdentifier(vec![
                                    Ident {
                                        value: "a".to_owned(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "id".to_owned(),
                                        quote_style: None,
                                    },
                                ])),
                                Box::new(Expression::CompoundIdentifier(vec![
                                    Ident {
                                        value: "c".to_owned(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "id".to_owned(),
                                        quote_style: None,
                                    },
                                ])),
                            ))),
                            join_type: ast::JoinType::Left,
                        }),
                        right: Box::new(ast::From::Table {
                            name: "d".to_owned(),
                            alias: None,
                        }),
                        on: Some(Expression::BinaryOperator(BinaryOperator::Eq(
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "b".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                            Box::new(Expression::CompoundIdentifier(vec![
                                Ident {
                                    value: "d".to_owned(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "id".to_owned(),
                                    quote_style: None,
                                },
                            ])),
                        ))),
                        join_type: ast::JoinType::Left,
                    },
                ],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_like() {
        assert_stmt_eq(
            "SELECT * FROM users WHERE name LIKE '%Joe%'",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: Some(Expression::Like {
                    negated: false,
                    left: Box::new(Expression::Identifier(Ident {
                        value: "name".to_owned(),
                        quote_style: None,
                    })),
                    right: Box::new(Expression::Literal(ast::Literal::String("%Joe%".to_owned()))),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_filter_sub_query() {
        assert_stmt_eq(
            "SELECT * FROM users WHERE users.id = (SELECT MIN(user_id) FROM commits)",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(Expression::CompoundIdentifier(vec![
                        Ident {
                            value: "users".to_owned(),
                            quote_style: None,
                        },
                        Ident {
                            value: "id".to_owned(),
                            quote_style: None,
                        },
                    ])),
                    Box::new(Expression::SubQuery(Box::new(Select {
                        with: None,
                        distinct: None,
                        columns: vec![SelectItem::UnNamedExpr(Expression::Function {
                            name: "MIN".to_owned(),
                            args: vec![Expression::Identifier(Ident {
                                value: "user_id".to_owned(),
                                quote_style: None,
                            })],
                            distinct: false,
                        })],
                        from: vec![ast::From::Table {
                            name: "commits".to_owned(),
                            alias: None,
                        }],
                        r#where: None,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                    }))),
                ))),
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                group_by: None,
            })),
        );
    }

    #[test]
    fn test_show() {
        assert_stmt_eq("SHOW TABLES;", Statement::ShowTables);
    }

    #[test]
    fn test_copy() {
        assert_stmt_eq(
            "COPY test FROM 'test.csv';",
            Statement::Copy {
                source: CopySource::Table {
                    table_name: vec!["test".to_owned()].into(),
                    columns: vec![],
                },
                to: false,
                target: CopyTarget::File {
                    file: "test.csv".to_owned(),
                },
                options: vec![],
            },
        );

        assert_stmt_eq(
            "COPY lineitem FROM 'lineitem.pq' (FORMAT PARQUET);",
            Statement::Copy {
                source: CopySource::Table {
                    table_name: vec!["lineitem".to_owned()].into(),
                    columns: vec![],
                },
                to: false,
                target: CopyTarget::File {
                    file: "lineitem.pq".to_owned(),
                },
                options: vec![CopyOption::Format(Ident {
                    value: "PARQUET".to_owned(),
                    quote_style: None,
                })],
            },
        );

        assert_stmt_eq(
            "COPY lineitem TO 'lineitem.csv' (FORMAT CSV, DELIMITER '|', HEADER);",
            Statement::Copy {
                source: CopySource::Table {
                    table_name: vec!["lineitem".to_owned()].into(),
                    columns: vec![],
                },
                to: true,
                target: CopyTarget::File {
                    file: "lineitem.csv".to_owned(),
                },
                options: vec![
                    CopyOption::Format(Ident {
                        value: "CSV".to_owned(),
                        quote_style: None,
                    }),
                    CopyOption::Delimiter('|'),
                    CopyOption::Header(true),
                ],
            },
        );
    }

    #[test]
    fn test_skip_useless() {
        assert_stmt_eq(
            "
        -- this is comment should skip this line
        SELECT * FROM person;
        ",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "person".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        )
    }

    #[test]
    fn test_timestamp() {
        assert_stmt_eq(
            "SELECT timestamp '2021-01-01 00:00:00'",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::TypedString {
                    data_type: DataType::Timestamp,
                    value: "2021-01-01 00:00:00".to_owned(),
                })],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_extract_function_args() {
        assert_stmt_eq(
            "SELECT extract(year from date '2021-01-01') as year",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Extract {
                        field: DateTimeField::Year,
                        expr: Box::new(Expression::TypedString {
                            data_type: DataType::Date,
                            value: "2021-01-01".to_owned(),
                        }),
                    },
                    "year".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_interval_function() {
        assert_stmt_eq(
            "SELECT interval '1' day as day",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Interval {
                        expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                        field: IntervalFields::Day,
                    },
                    "day".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT interval '1' month as month",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Interval {
                        expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                        field: IntervalFields::Month,
                    },
                    "month".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT interval '1' year as year",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Interval {
                        expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                        field: IntervalFields::Year,
                    },
                    "year".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT interval '1' hour as hour",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Interval {
                        expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                        field: IntervalFields::Hour,
                    },
                    "hour".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT interval '1' minute as minute",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::ExprWithAlias(
                    Expression::Interval {
                        expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                        field: IntervalFields::Minute,
                    },
                    "minute".to_owned(),
                )],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM orders WHERE o_orderdate < date '1993-07-01' + interval '3' month",
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "orders".to_owned(),
                    alias: None,
                }],
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Lt(
                    Box::new(Expression::Identifier(Ident {
                        value: "o_orderdate".to_owned(),
                        quote_style: None,
                    })),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::TypedString {
                            data_type: DataType::Date,
                            value: "1993-07-01".to_owned(),
                        }),
                        Box::new(Expression::Interval {
                            expr: Box::new(Expression::Literal(ast::Literal::String("3".to_owned()))),
                            field: IntervalFields::Month,
                        }),
                    ))),
                ))),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );
    }

    #[test]
    fn test_parse_date() {
        let mut parser = Parser::new("SELECT '2021-01-01'::date");
        let stmt = parser.parse().unwrap();

        assert_eq!(
            stmt,
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::Cast {
                    expr: Box::new(Expression::Literal(ast::Literal::String("2021-01-01".to_owned()))),
                    data_type: DataType::Date,
                })],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );

        let mut parser = Parser::new("SELECT DATE '2021-01-01'");
        let stmt = parser.parse().unwrap();

        assert_eq!(
            stmt,
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::TypedString {
                    data_type: DataType::Date,
                    value: "2021-01-01".to_owned()
                })],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );
    }

    #[test]
    fn test_postgresql_double_colon() {
        let mut parser = Parser::new("SELECT '1'::int");
        let stmt = parser.parse().unwrap();

        assert_eq!(
            stmt,
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::Cast {
                    expr: Box::new(Expression::Literal(ast::Literal::String("1".to_owned()))),
                    data_type: DataType::Integer,
                })],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );
    }

    #[test]
    fn test_collect_tables() {
        let mut parser = Parser::new(
            "
        WITH 
            t1 AS (SELECT name FROM person),
            t2 AS (SELECT * FROM t1)
        SELECT * FROM t2",
        );
        let _ = parser.parse().unwrap();

        assert_eq!(
            parser.tables,
            vec![TableInfo {
                name: "person".to_owned(),
                alias: None,
                args: vec![]
            },]
        );
        assert_eq!(
            parser.ctes,
            vec![
                TableInfo {
                    name: "t1".to_owned(),
                    alias: None,
                    args: vec![]
                },
                TableInfo {
                    name: "t2".to_owned(),
                    alias: None,
                    args: vec![]
                },
            ]
        );

        let mut parser = Parser::new("WITH cte AS (SELECT name FROM person) SELECT * FROM cte");
        let _ = parser.parse().unwrap();

        assert_eq!(
            parser.tables,
            vec![TableInfo {
                name: "person".to_owned(),
                alias: None,
                args: vec![]
            }]
        );
        assert_eq!(
            parser.ctes,
            vec![TableInfo {
                name: "cte".to_owned(),
                alias: None,
                args: vec![]
            }]
        );

        let mut parser = Parser::new("WITH cte AS (SELECT 1) SELECT * FROM cte");
        let _ = parser.parse().unwrap();

        assert!(parser.tables.is_empty());
        assert_eq!(
            parser.ctes,
            vec![TableInfo {
                name: "cte".to_owned(),
                alias: None,
                args: vec![]
            }]
        );

        let mut parser = Parser::new("SELECT * FROM person");
        let _ = parser.parse().unwrap();

        assert_eq!(
            parser.tables,
            vec![TableInfo {
                name: "person".to_owned(),
                alias: None,
                args: vec![]
            }]
        );

        let mut parser = Parser::new("SELECT * FROM read_csv('./test.csv')");
        let _ = parser.parse().unwrap();

        assert_eq!(
            parser.tables,
            vec![TableInfo {
                name: "read_csv".to_owned(),
                alias: None,
                args: vec![FunctionArgument {
                    id: None,
                    value: Expression::Literal(ast::Literal::String("./test.csv".to_owned()))
                }]
            }]
        );

        let mut parser = Parser::new("SELECT * FROM './tests/test.csv'");
        let _ = parser.parse().unwrap();

        assert_eq!(
            parser.tables,
            vec![TableInfo {
                name: "./tests/test.csv".to_owned(),
                alias: None,
                args: vec![]
            }]
        );
    }

    #[test]
    fn test_parser_error() {
        let stmt = parse_stmt("SELEC").err().unwrap();
        assert_eq!(stmt.to_string(), "error: unexpected token line: 0 column: 4");

        let stmt = parse_stmt("SELECT * FROM").err().unwrap();
        assert_eq!(stmt.to_string(), "error: unexpected EOF line: 0 column: 12");

        let stmt = parse_stmt("SELECT * FROM users WHERE").err().unwrap();
        assert_eq!(stmt.to_string(), "error: unexpected EOF line: 0 column: 24");
    }

    #[test]
    fn test_parse_create_table() {
        assert_stmt_eq(
            "create table t(v1 decimal(10, 2) not null)",
            Statement::CreateTable {
                query: None,
                table: "t".to_owned(),
                columns: vec![ast::Column {
                    name: "v1".to_owned(),
                    datatype: DataType::Decimal(Some(10), Some(2)),
                    nullable: false,
                    unique: false,
                    references: None,
                    primary_key: false,
                    index: false,
                }],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "create table t (a smallint not null);",
            Statement::CreateTable {
                query: None,
                table: "t".to_owned(),
                columns: vec![ast::Column {
                    name: "a".to_owned(),
                    datatype: DataType::Int16,
                    nullable: false,
                    unique: false,
                    references: None,
                    primary_key: false,
                    index: false,
                }],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "create table t (name VARCHAR NOT NULL)",
            Statement::CreateTable {
                query: None,
                table: "t".to_owned(),
                columns: vec![ast::Column {
                    name: "name".to_owned(),
                    datatype: DataType::String,
                    nullable: false,
                    unique: false,
                    references: None,
                    primary_key: false,
                    index: false,
                }],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "create table t(v1 int null)",
            Statement::CreateTable {
                query: None,
                table: "t".to_owned(),
                columns: vec![ast::Column {
                    name: "v1".to_owned(),
                    datatype: DataType::Integer,
                    nullable: true,
                    unique: false,
                    references: None,
                    primary_key: false,
                    index: false,
                }],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1(i INTEGER, j INTEGER);",
            Statement::CreateTable {
                query: None,
                table: "t1".to_owned(),
                columns: vec![
                    ast::Column {
                        name: "i".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                    ast::Column {
                        name: "j".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                ],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE IF NOT EXISTS t1(i INTEGER, j INTEGER);",
            Statement::CreateTable {
                query: None,
                table: "t1".to_owned(),
                columns: vec![
                    ast::Column {
                        name: "i".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                    ast::Column {
                        name: "j".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                ],
                check_exists: true,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1(i INTEGER PRIMARY KEY, j INTEGER);",
            Statement::CreateTable {
                query: None,
                table: "t1".to_owned(),
                columns: vec![
                    ast::Column {
                        name: "i".to_owned(),
                        datatype: DataType::Integer,
                        nullable: false,
                        unique: false,
                        references: None,
                        primary_key: true,
                        index: false,
                    },
                    ast::Column {
                        name: "j".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                ],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1(i INTEGER UNIQUE, j INTEGER);",
            Statement::CreateTable {
                query: None,
                table: "t1".to_owned(),
                columns: vec![
                    ast::Column {
                        name: "i".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: true,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                    ast::Column {
                        name: "j".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                ],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1(i INTEGER NOT NULL, j INTEGER);",
            Statement::CreateTable {
                query: None,
                table: "t1".to_owned(),
                columns: vec![
                    ast::Column {
                        name: "i".to_owned(),
                        datatype: DataType::Integer,
                        nullable: false,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                    ast::Column {
                        name: "j".to_owned(),
                        datatype: DataType::Integer,
                        nullable: true,
                        unique: false,
                        references: None,
                        primary_key: false,
                        index: false,
                    },
                ],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1 AS SELECT * FROM read_csv('path/file.csv');",
            Statement::CreateTable {
                query: Some(Select {
                    with: None,
                    distinct: None,
                    columns: vec![SelectItem::Wildcard],
                    from: vec![ast::From::TableFunction {
                        name: "read_csv".to_owned(),
                        args: vec![FunctionArgument {
                            id: None,
                            value: Expression::Literal(ast::Literal::String("path/file.csv".to_owned())),
                        }],
                        alias: None,
                    }],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                }),
                table: "t1".to_owned(),
                columns: vec![],
                check_exists: false,
            },
        );

        assert_stmt_eq(
            "CREATE TABLE t1 AS FROM read_csv_auto ('path/file.csv');",
            Statement::CreateTable {
                query: Some(Select {
                    with: None,
                    distinct: None,
                    columns: vec![SelectItem::Wildcard],
                    from: vec![ast::From::TableFunction {
                        name: "read_csv_auto".to_owned(),
                        args: vec![FunctionArgument {
                            id: None,
                            value: Expression::Literal(ast::Literal::String("path/file.csv".to_owned())),
                        }],
                        alias: None,
                    }],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                }),
                table: "t1".to_owned(),
                columns: vec![],
                check_exists: false,
            },
        );
    }

    #[test]
    fn leftover_tokens_are_an_error() {
        // A helper that fails to consume its tokens used to make the statement quietly end there,
        // dropping whatever followed. Requiring the stream to be exhausted turns that whole class
        // of bug into a parse error instead of a silently wrong answer.
        for sql in [
            "select a from t group by a nonsense",
            "select a from t where a = 1 nonsense",
            "create table t(a int) total garbage",
            "drop table t extra",
        ] {
            assert!(parse_stmt(sql).is_err(), "expected an error for: {sql}");
        }

        // a trailing semicolon is still fine, with or without surrounding space
        assert!(parse_stmt("select a from t;").is_ok());
        assert!(parse_stmt("select a from t ; ").is_ok());
        assert!(parse_stmt("select a from t").is_ok());
    }

    #[test]
    fn a_column_list_must_be_closed() {
        // Breaking out of the list without consuming the `)` left it in the stream, so everything
        // after the column list was ignored -- `AS SELECT` included, which built an empty table.
        let stmt = parse_stmt("create table t(a int) as select * from src").unwrap();
        let Statement::CreateTable { query, columns, .. } = stmt else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(columns.len(), 1);
        assert!(query.is_some(), "AS SELECT was dropped");

        // a trailing comma is still accepted; TPC-H's create_tables.slt relies on it
        let stmt = parse_stmt("create table t(a int,) as select * from src").unwrap();
        let Statement::CreateTable { query, .. } = stmt else {
            panic!("expected CREATE TABLE");
        };
        assert!(query.is_some());

        assert!(parse_stmt("create table t(a int").is_err(), "unterminated list");
    }

    #[test]
    fn an_explicit_null_marker_is_parsed() {
        let stmt = parse_stmt("create table t(a int null, b int not null)").unwrap();
        let Statement::CreateTable { columns, .. } = stmt else {
            panic!("expected CREATE TABLE");
        };
        assert!(columns[0].nullable);
        assert!(!columns[1].nullable);

        assert!(parse_stmt("create table t(a int primary key null)").is_err());
    }

    #[test]
    fn dropping_something_unsupported_is_an_error() {
        // Anything other than TABLE/SCHEMA used to hit `unimplemented!()` and abort the process.
        for sql in ["DROP INDEX foo;", "DROP VIEW v;", "DROP;"] {
            assert!(parse_stmt(sql).is_err(), "expected an error for: {sql}");
        }

        assert!(parse_stmt("DROP TABLE t;").is_ok());
        assert!(parse_stmt("DROP SCHEMA s;").is_ok());
    }

    #[test]
    fn test_parse_drop_schema() -> Result<()> {
        let stmt = parse_stmt("DROP SCHEMA test;")?;

        assert_eq!(
            stmt,
            Statement::DropSchema {
                schema: "test".to_owned(),
                check_exists: false,
            }
        );

        let stmt = parse_stmt("DROP SCHEMA IF EXISTS test;")?;

        assert_eq!(
            stmt,
            Statement::DropSchema {
                schema: "test".to_owned(),
                check_exists: true,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_create_schema() -> Result<()> {
        let stmt = parse_stmt("CREATE SCHEMA test;")?;

        assert_eq!(
            stmt,
            Statement::CreateSchema {
                schema: "test".to_owned(),
                check_exists: false,
            }
        );

        let stmt = parse_stmt("CREATE SCHEMA IF NOT EXISTS test;")?;

        assert_eq!(
            stmt,
            Statement::CreateSchema {
                schema: "test".to_owned(),
                check_exists: true,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_delete_statement() -> Result<()> {
        let stmt = parse_stmt("DELETE FROM users;")?;

        assert_eq!(
            stmt,
            Statement::Delete {
                table: "users".to_owned(),
                r#where: None,
            }
        );

        let stmt = parse_stmt("DELETE FROM users WHERE id = 1;")?;

        assert_eq!(
            stmt,
            Statement::Delete {
                table: "users".to_owned(),
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(Expression::Identifier("id".into())),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                ))),
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_update_statement() {
        let stmt = parse_stmt("UPDATE users SET name = 'name'").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Update {
                table: "users".to_owned(),
                assignments: vec![Assignment {
                    target: vec!["name"].into(),
                    value: ast::Expression::Literal(ast::Literal::String("name".to_owned()))
                }],
                r#where: None,
            }
        );

        let stmt = parse_stmt("UPDATE users SET name = 'name' WHERE id = 1").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Update {
                table: "users".to_owned(),
                assignments: vec![Assignment {
                    target: vec!["name"].into(),
                    value: ast::Expression::Literal(ast::Literal::String("name".to_owned()))
                }],
                r#where: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(ast::Expression::Identifier("id".into())),
                    Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                ))),
            }
        );

        let stmt = parse_stmt("UPDATE users SET name = 'name', id = 1 WHERE id = 1;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Update {
                table: "users".to_owned(),
                assignments: vec![
                    Assignment {
                        target: vec!["name"].into(),
                        value: ast::Expression::Literal(ast::Literal::String("name".to_owned()))
                    },
                    Assignment {
                        target: vec!["id"].into(),
                        value: ast::Expression::Literal(ast::Literal::Int(1))
                    },
                ],
                r#where: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(ast::Expression::Identifier("id".into())),
                    Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                ))),
            }
        );
    }

    #[test]
    fn test_parse_insert_statement() {
        assert_stmt_eq(
            "insert into t values(null)",
            ast::Statement::Insert {
                query: None,
                table: "t".to_owned(),
                alias: None,
                columns: None,
                values: vec![vec![ast::Expression::Literal(ast::Literal::Null)]],
                on_conflict: None,
                returning: None,
            },
        );

        let stmt = parse_stmt("INSERT INTO users VALUES (1, 'name');").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: None,
                values: vec![vec![
                    ast::Expression::Literal(ast::Literal::Int(1)),
                    ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                ]],
                on_conflict: None,
                returning: None,
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name');").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![vec![
                    ast::Expression::Literal(ast::Literal::Int(1)),
                    ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                ]],
                on_conflict: None,
                returning: None,
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2');").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: None,
                returning: None,
            }
        );

        let stmt =
            parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2') ON CONFLICT (id) DO NOTHING;")
                .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: Some(ast::OnConflict::DoNothing),
                returning: None,
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2') ON CONFLICT (id) DO UPDATE SET name = 'name';")
            .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: Some(ast::OnConflict::DoUpdate {
                    constraints: vec![ast::Ident {
                        value: "id".to_owned(),
                        quote_style: None,
                    }],
                    values: vec![ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::Identifier("name".into())),
                        Box::new(ast::Expression::Literal(ast::Literal::String("name".to_owned()))),
                    ))],
                }),
                returning: None,
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2') ON CONFLICT (id) DO UPDATE SET name = 'name', id = 1;")
            .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: Some(ast::OnConflict::DoUpdate {
                    constraints: vec![ast::Ident {
                        value: "id".to_owned(),
                        quote_style: None,
                    }],
                    values: vec![
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("name".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::String("name".to_owned()))),
                        )),
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("id".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                        )),
                    ],
                }),
                returning: None,
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2') ON CONFLICT (id) DO UPDATE SET name = 'name', id = 1 RETURNING id;")
            .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: Some(ast::OnConflict::DoUpdate {
                    constraints: vec![ast::Ident {
                        value: "id".to_owned(),
                        quote_style: None,
                    }],
                    values: vec![
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("name".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::String("name".to_owned()))),
                        )),
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("id".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                        )),
                    ],
                }),
                returning: Some(vec![
                    (ast::SelectItem::UnNamedExpr(ast::Expression::Identifier("id".into())))
                ]),
            }
        );

        let stmt = parse_stmt("INSERT INTO users (id, name) VALUES (1, 'name'), (2, 'name2') ON CONFLICT (id) DO UPDATE SET name = 'name', id = 1 RETURNING id AS user_id;")
            .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                query: None,
                table: String::from("users"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(1)),
                        ast::Expression::Literal(ast::Literal::String("name".to_owned())),
                    ],
                    vec![
                        ast::Expression::Literal(ast::Literal::Int(2)),
                        ast::Expression::Literal(ast::Literal::String("name2".to_owned())),
                    ],
                ],
                on_conflict: Some(ast::OnConflict::DoUpdate {
                    constraints: vec![ast::Ident {
                        value: "id".to_owned(),
                        quote_style: None,
                    }],
                    values: vec![
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("name".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::String("name".to_owned()))),
                        )),
                        ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::Identifier("id".into())),
                            Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                        )),
                    ],
                }),
                returning: Some(vec![
                    (ast::SelectItem::ExprWithAlias(ast::Expression::Identifier("id".into()), String::from("user_id")))
                ]),
            }
        );

        let stmt = parse_stmt("INSERT INTO tbl SELECT * FROM other_tbl;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                table: String::from("tbl"),
                alias: None,
                columns: None,
                values: vec![],
                on_conflict: None,
                returning: None,
                query: Some(ast::Select {
                    with: None,
                    distinct: None,
                    columns: vec![ast::SelectItem::Wildcard],
                    from: vec![ast::From::Table {
                        name: String::from("other_tbl"),
                        alias: None,
                    }],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                }),
            }
        );

        let stmt = parse_stmt("INSERT INTO tbl FROM other_tbl;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                table: String::from("tbl"),
                alias: None,
                columns: None,
                values: vec![],
                on_conflict: None,
                returning: None,
                query: Some(ast::Select {
                    with: None,
                    distinct: None,
                    columns: vec![ast::SelectItem::Wildcard],
                    from: vec![ast::From::Table {
                        name: String::from("other_tbl"),
                        alias: None,
                    }],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                }),
            }
        );

        let stmt = parse_stmt("INSERT INTO tbl(id,name) SELECT id,name FROM other_tbl;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Insert {
                table: String::from("tbl"),
                alias: None,
                columns: Some(vec![
                    ast::Expression::Identifier("id".into()),
                    ast::Expression::Identifier("name".into()),
                ]),
                values: vec![],
                on_conflict: None,
                returning: None,
                query: Some(ast::Select {
                    with: None,
                    distinct: None,
                    columns: vec![
                        ast::SelectItem::UnNamedExpr(ast::Expression::Identifier("id".into())),
                        ast::SelectItem::UnNamedExpr(ast::Expression::Identifier("name".into())),
                    ],
                    from: vec![ast::From::Table {
                        name: String::from("other_tbl"),
                        alias: None,
                    }],
                    r#where: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                }),
            }
        );
    }

    #[test]
    fn test_parse_select_statement() {
        assert_stmt_eq(
            "SELECT person.id as p_id FROM person",
            ast::Statement::Select(Box::new(ast::Select {
                with: None,
                distinct: None,
                columns: vec![ast::SelectItem::ExprWithAlias(
                    ast::Expression::CompoundIdentifier(vec!["person".into(), "id".into()]),
                    "p_id".to_owned(),
                )],
                from: vec![ast::From::Table {
                    name: "person".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
                having: None,
            })),
        );

        assert_stmt_eq(
            "SELECT 1",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(ast::Expression::Literal(ast::Literal::Int(1)))],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
            })),
        );

        assert_stmt_eq(
            "SELECT -1",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(ast::Expression::UnaryOperator {
                    op: ast::UnaryOperator::Minus,
                    expr: Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                })],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
            })),
        );

        assert_stmt_eq(
            "SELECT id,t.id FROM test as t;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![
                    SelectItem::UnNamedExpr(ast::Expression::Identifier("id".into())),
                    SelectItem::UnNamedExpr(ast::Expression::CompoundIdentifier(vec!["t".into(), "id".into()])),
                ],
                from: vec![ast::From::Table {
                    name: String::from("test"),
                    alias: Some(String::from("t")),
                }],
                r#where: None,
                group_by: None,
                having: None,
            })),
        );

        assert_stmt_eq(
            "SELECT t.* FROM person as t",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![SelectItem::QualifiedWildcard(vec!["t".to_owned()])],
                from: vec![ast::From::Table {
                    name: String::from("person"),
                    alias: Some(String::from("t")),
                }],
                r#where: None,
                group_by: None,
                having: None,
            })),
        );
    }

    #[test]
    fn test_parse_table_function() {
        let stmt = parse_stmt("SELECT * FROM read_csv('./test.csv', delim = '|', header = true, columns = { 'FlightDate': 'DATE' }, force_not_null = ['FlightDate']) as t1;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::TableFunction {
                    name: String::from("read_csv"),
                    args: vec![
                        ast::FunctionArgument {
                            id: None,
                            value: ast::Expression::Literal(ast::Literal::String("./test.csv".to_owned())),
                        },
                        ast::FunctionArgument {
                            id: Some(ast::Ident {
                                value: "delim".to_owned(),
                                quote_style: None,
                            }),
                            value: ast::Expression::Literal(ast::Literal::String("|".to_owned())),
                        },
                        ast::FunctionArgument {
                            id: Some(ast::Ident {
                                value: "header".to_owned(),
                                quote_style: None,
                            }),
                            value: ast::Expression::Literal(ast::Literal::Boolean(true)),
                        },
                        ast::FunctionArgument {
                            id: Some(ast::Ident {
                                value: "columns".to_owned(),
                                quote_style: None,
                            }),
                            value: ast::Expression::Struct(vec![ast::StructField {
                                name: ast::Expression::Literal(ast::Literal::String("FlightDate".to_owned())),
                                value: ast::Expression::Literal(ast::Literal::String("DATE".to_owned())),
                            }]),
                        },
                        ast::FunctionArgument {
                            id: Some(ast::Ident {
                                value: "force_not_null".to_owned(),
                                quote_style: None,
                            }),
                            value: ast::Expression::Array(vec![ast::Expression::Literal(ast::Literal::String(
                                "FlightDate".to_owned()
                            ))]),
                        },
                    ],
                    alias: Some(String::from("t1")),
                }],
                r#where: None,
                group_by: None,
                having: None,
            }))
        );
    }

    #[test]
    fn test_parse_from_item() {
        let stmt = parse_stmt("select * from public.users as u;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("public.users"),
                    alias: Some(String::from("u")),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from catalog.public.users u;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("catalog.public.users"),
                    alias: Some(String::from("u")),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from (select * from users) as u;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::SubQuery {
                    columns: vec![],
                    query: Box::new(ast::Statement::Select(Box::new(Select {
                        with: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                        having: None,
                        distinct: None,
                        columns: vec![SelectItem::Wildcard],
                        from: vec![ast::From::Table {
                            name: String::from("users"),
                            alias: None,
                        }],
                        r#where: None,
                        group_by: None,
                    }))),
                    alias: Some(String::from("u")),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u join users u2 on u.id = u2.id;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Inner,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                    ))),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u left join users u2 on u.id = u2.id;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Left,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                    ))),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u right join users u2 on u.id = u2.id;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distinct: None,
                having: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Right,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                    ))),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u inner join users u2 on u.id = u2.id;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Inner,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                    ))),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u full join users u2 on u.id = u2.id;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Full,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                        Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                    ))),
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u cross join users u2;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Join {
                    join_type: ast::JoinType::Cross,
                    left: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    }),
                    right: Box::new(ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u2")),
                    }),
                    on: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("select * from users u, persons p").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![
                    ast::From::Table {
                        name: String::from("users"),
                        alias: Some(String::from("u")),
                    },
                    ast::From::Table {
                        name: String::from("persons"),
                        alias: Some(String::from("p")),
                    },
                ],
                r#where: None,
                group_by: None,
            }))
        );
    }

    #[test]
    fn test_is_not_null_does_not_swallow_following_clauses() {
        // `parse_keywords(&[Not, Null])` returned true after matching only `NOT`, leaving `NULL`
        // in the stream, which made the enclosing statement drop every clause after the WHERE.
        let stmt = parse_stmt("select v from s where v is not null group by v order by v limit 1;").unwrap();
        let ast::Statement::Select(select) = stmt else {
            panic!("expected a SELECT statement");
        };

        assert_eq!(
            select.r#where,
            Some(ast::Expression::IsNotNull(Box::new(ast::Expression::Identifier(
                "v".into()
            ))))
        );
        assert_eq!(
            select.group_by,
            Some(vec![ast::Expression::Identifier("v".into())]),
            "GROUP BY was dropped"
        );
        assert_eq!(
            select.order_by,
            Some(vec![(ast::Expression::Identifier("v".into()), ast::Order::Asc)]),
            "ORDER BY was dropped"
        );
        assert_eq!(
            select.limit,
            Some(ast::Expression::Literal(ast::Literal::Int(1))),
            "LIMIT was dropped"
        );
    }

    #[test]
    fn test_is_null_requires_the_null_keyword() {
        assert!(parse_stmt("select v from s where v is not 1;").is_err());
        assert!(parse_stmt("select v from s where v is;").is_err());
    }

    #[test]
    fn test_parse_substring() {
        let expr = |args: Vec<Expression>| ast::Expression::Function {
            name: "substring".to_owned(),
            args,
            distinct: false,
        };
        let phone = || ast::Expression::Identifier("c_phone".into());
        let int = |v: i64| ast::Expression::Literal(ast::Literal::Int(v));

        // `FROM`/`FOR` and `,` are interchangeable, and both spellings must produce the same call.
        for sql in [
            "SELECT substring(c_phone from 1 for 2) FROM customer",
            "SELECT substring(c_phone, 1, 2) FROM customer",
        ] {
            let stmt = parse_stmt(sql).unwrap();
            let ast::Statement::Select(select) = stmt else {
                panic!("expected a SELECT statement");
            };
            assert_eq!(
                select.columns,
                vec![SelectItem::UnNamedExpr(expr(vec![phone(), int(1), int(2)]))],
                "unexpected parse for: {sql}"
            );
        }

        // the length is optional in both spellings
        for sql in [
            "SELECT substring(c_phone from 3) FROM customer",
            "SELECT substring(c_phone, 3) FROM customer",
        ] {
            let stmt = parse_stmt(sql).unwrap();
            let ast::Statement::Select(select) = stmt else {
                panic!("expected a SELECT statement");
            };
            assert_eq!(
                select.columns,
                vec![SelectItem::UnNamedExpr(expr(vec![phone(), int(3)]))],
                "unexpected parse for: {sql}"
            );
        }
    }

    #[test]
    fn test_parse_substring_in_predicate_keeps_following_clauses() {
        let stmt = parse_stmt(
            "select substring(a from 1 for 2) as p from t where substring(a from 1 for 2) = 'ab' group by p",
        )
        .unwrap();
        let ast::Statement::Select(select) = stmt else {
            panic!("expected a SELECT statement");
        };

        assert!(select.r#where.is_some(), "WHERE was dropped");
        assert_eq!(
            select.group_by,
            Some(vec![ast::Expression::Identifier("p".into())]),
            "GROUP BY was dropped"
        );
    }

    #[test]
    fn test_in_subquery_does_not_swallow_following_clauses() {
        // parse_in_expr used to leave the subquery's closing paren in the stream, which made the
        // enclosing statement stop early and silently drop everything after it.
        let stmt = parse_stmt("select a from t1 where a in (select x from t2) group by a order by a limit 3;").unwrap();

        let ast::Statement::Select(select) = stmt else {
            panic!("expected a SELECT statement");
        };

        assert!(matches!(select.r#where, Some(ast::Expression::InSubQuery { .. })));
        assert_eq!(
            select.group_by,
            Some(vec![ast::Expression::Identifier("a".into())]),
            "GROUP BY was dropped"
        );
        assert_eq!(
            select.order_by,
            Some(vec![(ast::Expression::Identifier("a".into()), ast::Order::Asc)]),
            "ORDER BY was dropped"
        );
        assert_eq!(
            select.limit,
            Some(ast::Expression::Literal(ast::Literal::Int(3))),
            "LIMIT was dropped"
        );
    }

    #[test]
    fn test_in_subquery_requires_closing_paren() {
        assert!(parse_stmt("select a from t1 where a in (select x from t2").is_err());
    }

    #[test]
    fn test_parse_outer_join() {
        // `OUTER` is optional noise: `LEFT OUTER JOIN` must parse the same as `LEFT JOIN`.
        for (sql, expected) in [
            ("left outer join", ast::JoinType::Left),
            ("right outer join", ast::JoinType::Right),
            ("full outer join", ast::JoinType::Full),
            ("left join", ast::JoinType::Left),
            ("full join", ast::JoinType::Full),
        ] {
            let stmt = parse_stmt(&format!("select * from users u {sql} users u2 on u.id = u2.id;")).unwrap();

            assert_eq!(
                stmt,
                ast::Statement::Select(Box::new(Select {
                    with: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                    distinct: None,
                    having: None,
                    columns: vec![SelectItem::Wildcard],
                    from: vec![ast::From::Join {
                        join_type: expected,
                        left: Box::new(ast::From::Table {
                            name: String::from("users"),
                            alias: Some(String::from("u")),
                        }),
                        right: Box::new(ast::From::Table {
                            name: String::from("users"),
                            alias: Some(String::from("u2")),
                        }),
                        on: Some(ast::Expression::BinaryOperator(ast::BinaryOperator::Eq(
                            Box::new(ast::Expression::CompoundIdentifier(vec!["u".into(), "id".into()])),
                            Box::new(ast::Expression::CompoundIdentifier(vec!["u2".into(), "id".into()])),
                        ))),
                    }],
                    r#where: None,
                    group_by: None,
                }))
            );
        }
    }

    #[test]
    fn test_parse_order_by() {
        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![(ast::Expression::Identifier("id".into()), ast::Order::Asc)]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id ASC;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![(ast::Expression::Identifier("id".into()), ast::Order::Asc)]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id,name,age;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![
                    (ast::Expression::Identifier("id".into()), ast::Order::Asc),
                    (ast::Expression::Identifier("name".into()), ast::Order::Asc),
                    (ast::Expression::Identifier("age".into()), ast::Order::Asc),
                ]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id DESC;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![(ast::Expression::Identifier("id".into()), ast::Order::Desc)]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id DESC, name ASC;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![
                    (ast::Expression::Identifier("id".into()), ast::Order::Desc),
                    (ast::Expression::Identifier("name".into()), ast::Order::Asc),
                ]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users ORDER BY id ASC, name DESC;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: Some(vec![
                    (ast::Expression::Identifier("id".into()), ast::Order::Asc),
                    (ast::Expression::Identifier("name".into()), ast::Order::Desc),
                ]),
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            })),
        );
    }

    #[test]
    fn test_parse_limit_offset() {
        let stmt = parse_stmt("SELECT * FROM users OFFSET 10;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("SELECT * FROM users LIMIT 10;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("SELECT * FROM users LIMIT 10 OFFSET 10;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                offset: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("SELECT * FROM users OFFSET 10 LIMIT 10;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                offset: Some(ast::Expression::Literal(ast::Literal::Int(10))),
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );
    }

    #[test]
    fn test_parse_distinct_select_statement() {
        let stmt = parse_stmt("SELECT DISTINCT * FROM users;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: Some(ast::Distinct::ALL),
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt("SELECT DISTINCT ON(name,age),school FROM users;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: Some(ast::Distinct::DISTINCT(vec![
                    ast::Expression::Identifier("name".into()),
                    ast::Expression::Identifier("age".into()),
                ])),
                columns: vec![SelectItem::UnNamedExpr(ast::Expression::Identifier("school".into()))],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );
    }

    #[test]
    fn test_parse_where() {
        assert_stmt_eq(
            "SELECT * FROM users WHERE id IS NULL",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::IsNull(Box::new(Expression::Identifier("id".into())))),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id IS NOT NULL",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::IsNotNull(Box::new(Expression::Identifier("id".into())))),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id = 1;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(Expression::Identifier("id".into())),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                ))),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id = 1 AND name = 'foo';",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::And(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(Expression::Identifier("id".into())),
                        Box::new(Expression::Literal(ast::Literal::Int(1))),
                    ))),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(Expression::Identifier("name".into())),
                        Box::new(Expression::Literal(ast::Literal::String("foo".to_owned()))),
                    ))),
                ))),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id = 1 OR name = 'foo';",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::BinaryOperator(ast::BinaryOperator::Or(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(Expression::Identifier("id".into())),
                        Box::new(Expression::Literal(ast::Literal::Int(1))),
                    ))),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                        Box::new(Expression::Identifier("name".into())),
                        Box::new(Expression::Literal(ast::Literal::String("foo".to_owned()))),
                    ))),
                ))),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id in (1,2,3)",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::InList {
                    field: Box::new(Expression::Identifier("id".into())),
                    list: vec![
                        Expression::Literal(ast::Literal::Int(1)),
                        Expression::Literal(ast::Literal::Int(2)),
                        Expression::Literal(ast::Literal::Int(3)),
                    ],
                    negated: false,
                }),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id not in (1,2,3)",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::InList {
                    field: Box::new(Expression::Identifier("id".into())),
                    list: vec![
                        Expression::Literal(ast::Literal::Int(1)),
                        Expression::Literal(ast::Literal::Int(2)),
                        Expression::Literal(ast::Literal::Int(3)),
                    ],
                    negated: true,
                }),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id in ('1','2')",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::InList {
                    field: Box::new(Expression::Identifier("id".into())),
                    list: vec![
                        Expression::Literal(ast::Literal::String("1".to_owned())),
                        Expression::Literal(ast::Literal::String("2".to_owned())),
                    ],
                    negated: false,
                }),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id not in ('1','2')",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::InList {
                    field: Box::new(Expression::Identifier("id".into())),
                    list: vec![
                        Expression::Literal(ast::Literal::String("1".to_owned())),
                        Expression::Literal(ast::Literal::String("2".to_owned())),
                    ],
                    negated: true,
                }),
                group_by: None,
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users WHERE id in (select id from users)",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: String::from("users"),
                    alias: None,
                }],
                r#where: Some(Expression::InSubQuery {
                    field: Box::new(Expression::Identifier("id".into())),
                    query: Box::new(ast::Statement::Select(Box::new(Select {
                        with: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                        having: None,
                        distinct: None,
                        columns: vec![SelectItem::UnNamedExpr(Expression::Identifier("id".into()))],
                        from: vec![ast::From::Table {
                            name: String::from("users"),
                            alias: None,
                        }],
                        r#where: None,
                        group_by: None,
                    }))),
                    negated: false,
                }),
                group_by: None,
            })),
        );
    }

    #[test]
    fn test_with() {
        let stmt = parse_stmt("WITH t1 AS (SELECT * FROM users) SELECT * FROM t1;").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: Some(ast::With {
                    recursive: false,
                    cte_tables: vec![ast::Cte {
                        columns: vec![],
                        alias: "t1".to_owned(),
                        query: Box::new(Select {
                            with: None,
                            order_by: None,
                            distinct: None,
                            columns: vec![SelectItem::Wildcard],
                            from: vec![ast::From::Table {
                                name: "users".to_owned(),
                                alias: None,
                            }],
                            r#where: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            offset: None,
                        }),
                    }]
                }),
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "t1".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: None,
            }))
        );

        let stmt = parse_stmt(
            r#"
        WITH t1 AS (
            SELECT * FROM users
        ),
        t2 AS (
            SELECT * FROM pepole
        )
        SELECT * FROM t1,t2;
        "#,
        )
        .unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: Some(ast::With {
                    recursive: false,
                    cte_tables: vec![
                        ast::Cte {
                            columns: vec![],
                            alias: "t1".to_owned(),
                            query: Box::new(Select {
                                with: None,
                                order_by: None,
                                distinct: None,
                                columns: vec![SelectItem::Wildcard],
                                from: vec![ast::From::Table {
                                    name: "users".to_owned(),
                                    alias: None,
                                }],
                                r#where: None,
                                group_by: None,
                                having: None,
                                limit: None,
                                offset: None,
                            }),
                        },
                        ast::Cte {
                            columns: vec![],
                            alias: "t2".to_owned(),
                            query: Box::new(Select {
                                with: None,
                                order_by: None,
                                distinct: None,
                                columns: vec![SelectItem::Wildcard],
                                from: vec![ast::From::Table {
                                    name: "pepole".to_owned(),
                                    alias: None,
                                }],
                                r#where: None,
                                group_by: None,
                                having: None,
                                limit: None,
                                offset: None,
                            }),
                        },
                    ]
                }),
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![
                    ast::From::Table {
                        name: "t1".to_owned(),
                        alias: None,
                    },
                    ast::From::Table {
                        name: "t2".to_owned(),
                        alias: None,
                    },
                ],
                r#where: None,
                group_by: None,
            }))
        );
    }

    #[test]
    fn test_parse_group_by() {
        assert_stmt_eq(
            "SELECT * FROM users GROUP BY id;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: Some(vec![Expression::Identifier("id".into())]),
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users GROUP BY id, name;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: None,
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: Some(vec![
                    Expression::Identifier("id".into()),
                    Expression::Identifier("name".into()),
                ]),
            })),
        );

        assert_stmt_eq(
            "SELECT * FROM users GROUP BY id, name HAVING id = 1;",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: Some(Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(Expression::Identifier("id".into())),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                ))),
                distinct: None,
                columns: vec![SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "users".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: Some(vec![
                    Expression::Identifier("id".into()),
                    Expression::Identifier("name".into()),
                ]),
            })),
        );

        assert_stmt_eq(
            "SELECT id, name FROM user GROUP BY id, name HAVING count(name) > 2",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: Some(Expression::BinaryOperator(ast::BinaryOperator::Gt(
                    Box::new(Expression::Function {
                        name: "count".to_owned(),
                        args: vec![Expression::Identifier("name".into())],
                        distinct: false,
                    }),
                    Box::new(Expression::Literal(ast::Literal::Int(2))),
                ))),
                distinct: None,
                columns: vec![
                    SelectItem::UnNamedExpr(Expression::Identifier("id".into())),
                    SelectItem::UnNamedExpr(Expression::Identifier("name".into())),
                ],
                from: vec![ast::From::Table {
                    name: "user".to_owned(),
                    alias: None,
                }],
                r#where: None,
                group_by: Some(vec![
                    Expression::Identifier("id".into()),
                    Expression::Identifier("name".into()),
                ]),
            })),
        );

        assert_stmt_eq(
            "SELECT 42 HAVING 42 > 108",
            ast::Statement::Select(Box::new(Select {
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                having: Some(Expression::BinaryOperator(ast::BinaryOperator::Gt(
                    Box::new(Expression::Literal(ast::Literal::Int(42))),
                    Box::new(Expression::Literal(ast::Literal::Int(108))),
                ))),
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::Literal(ast::Literal::Int(42)))],
                from: vec![],
                r#where: None,
                group_by: None,
            })),
        );
    }

    #[test]
    fn test_parse_struct() {
        let stmt = parse_expr("{}").unwrap();

        assert_eq!(stmt, Expression::Struct(vec![]));

        let stmt = parse_expr("{ 'FlightDate' : 'Date' }").unwrap();

        assert_eq!(
            stmt,
            Expression::Struct(vec![ast::StructField {
                name: Expression::Literal(ast::Literal::String("FlightDate".to_owned())),
                value: Expression::Literal(ast::Literal::String("Date".to_owned())),
            }])
        );

        let stmt = parse_expr("{ 'FlightDate' : 'Date', 'FlightNumber' : 'String' }").unwrap();

        assert_eq!(
            stmt,
            Expression::Struct(vec![
                ast::StructField {
                    name: Expression::Literal(ast::Literal::String("FlightDate".to_owned())),
                    value: Expression::Literal(ast::Literal::String("Date".to_owned())),
                },
                ast::StructField {
                    name: Expression::Literal(ast::Literal::String("FlightNumber".to_owned())),
                    value: Expression::Literal(ast::Literal::String("String".to_owned())),
                },
            ])
        );
    }

    #[test]
    fn test_parse_arrya() {
        let stmt = parse_expr("[]").unwrap();

        assert_eq!(stmt, Expression::Array(vec![]));

        let stmt = parse_expr("[1,2,3]").unwrap();

        assert_eq!(
            stmt,
            Expression::Array(vec![
                Expression::Literal(ast::Literal::Int(1)),
                Expression::Literal(ast::Literal::Int(2)),
                Expression::Literal(ast::Literal::Int(3)),
            ])
        );

        let stmt = parse_expr("[1,2,3, 'foo']").unwrap();

        assert_eq!(
            stmt,
            Expression::Array(vec![
                Expression::Literal(ast::Literal::Int(1)),
                Expression::Literal(ast::Literal::Int(2)),
                Expression::Literal(ast::Literal::Int(3)),
                Expression::Literal(ast::Literal::String("foo".to_owned())),
            ])
        );
    }

    #[test]
    fn test_parse_ident() {
        let stmt = parse_expr("foobar").unwrap();

        assert_eq!(stmt, Expression::Identifier("foobar".into()));

        let stmt = parse_stmt("SELECT 1").unwrap();

        assert_eq!(
            stmt,
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::Literal(ast::Literal::Int(1)))],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );

        let stmt = parse_stmt("SELECT id").unwrap();

        assert_eq!(
            stmt,
            Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(Expression::Identifier("id".into()))],
                from: vec![],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );
    }

    #[test]
    fn test_parse_float() {
        let stmt = parse_expr("1.0").unwrap();

        assert_eq!(stmt, Expression::Literal(ast::Literal::Float(1.0)));
    }

    #[test]
    fn test_parse_integer() {
        let stmt = parse_expr("123").unwrap();

        assert_eq!(stmt, Expression::Literal(ast::Literal::Int(123)));
    }

    #[test]
    fn test_parse_boolean() {
        let stmt = parse_expr("true").unwrap();

        assert_eq!(stmt, Expression::Literal(ast::Literal::Boolean(true)));

        let stmt = parse_expr("false").unwrap();

        assert_eq!(stmt, Expression::Literal(ast::Literal::Boolean(false)));
    }

    #[test]
    fn test_parse_function_named_after_a_keyword() {
        // `LEFT`/`RIGHT` are join keywords, but they are also standard string functions. Followed by
        // a paren in expression position they must parse as calls.
        // The name keeps the case it was written in; function lookup is case-insensitive.
        for (sql, name) in [("left('abcdef', 2)", "left"), ("LEFT('abcdef', 2)", "LEFT")] {
            assert_eq!(
                parse_expr(sql).unwrap(),
                Expression::Function {
                    name: name.to_owned(),
                    args: vec![
                        Expression::Literal(ast::Literal::String("abcdef".to_owned())),
                        Expression::Literal(ast::Literal::Int(2)),
                    ],
                    distinct: false,
                }
            );
        }

        assert_eq!(
            parse_expr("right(name, 3)").unwrap(),
            Expression::Function {
                name: "right".to_owned(),
                args: vec![
                    Expression::Identifier("name".to_owned().into()),
                    Expression::Literal(ast::Literal::Int(3)),
                ],
                distinct: false,
            }
        );

        // ...while the join keyword still reaches the FROM clause.
        Parser::new("select * from a left join b on a.id = b.id")
            .parse()
            .unwrap();
    }

    #[test]
    fn test_parse_function() {
        let stmt = parse_expr("foo(1, 2, 3)").unwrap();

        assert_eq!(
            stmt,
            Expression::Function {
                name: "foo".to_owned(),
                args: vec![
                    Expression::Literal(ast::Literal::Int(1)),
                    Expression::Literal(ast::Literal::Int(2)),
                    Expression::Literal(ast::Literal::Int(3)),
                ],
                distinct: false,
            }
        );

        let stmt = parse_expr("foo(bar(1, 2, 3))").unwrap();

        assert_eq!(
            stmt,
            Expression::Function {
                name: "foo".to_owned(),
                args: vec![Expression::Function {
                    name: "bar".to_owned(),
                    args: vec![
                        Expression::Literal(ast::Literal::Int(1)),
                        Expression::Literal(ast::Literal::Int(2)),
                        Expression::Literal(ast::Literal::Int(3)),
                    ],
                    distinct: false,
                }],
                distinct: false,
            }
        );
    }

    #[test]
    fn test_parse_prefix_expression() {
        let stmt = parse_expr("-123").unwrap();

        assert_eq!(
            stmt,
            Expression::UnaryOperator {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(Expression::Literal(ast::Literal::Int(123))),
            }
        );
    }

    #[test]
    fn test_parse_infix_expression() {
        let tests = vec![
            (
                "1 + 2 + 3",
                Expression::BinaryOperator(ast::BinaryOperator::Add(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::Literal(ast::Literal::Int(1))),
                        Box::new(Expression::Literal(ast::Literal::Int(2))),
                    ))),
                    Box::new(Expression::Literal(ast::Literal::Int(3))),
                )),
            ),
            (
                "1 + 2",
                Expression::BinaryOperator(ast::BinaryOperator::Add(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(2))),
                )),
            ),
            (
                "1 - 2",
                Expression::BinaryOperator(ast::BinaryOperator::Sub(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(2))),
                )),
            ),
            (
                "1 / 1",
                Expression::BinaryOperator(ast::BinaryOperator::Div(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 * 5",
                Expression::BinaryOperator(ast::BinaryOperator::Mul(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(5))),
                )),
            ),
            (
                "1 = 1",
                Expression::BinaryOperator(ast::BinaryOperator::Eq(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 <> 1",
                Expression::BinaryOperator(ast::BinaryOperator::NotEq(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 != 1",
                Expression::BinaryOperator(ast::BinaryOperator::NotEq(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 > 1",
                Expression::BinaryOperator(ast::BinaryOperator::Gt(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 >= 1",
                Expression::BinaryOperator(ast::BinaryOperator::Gte(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 < 1",
                Expression::BinaryOperator(ast::BinaryOperator::Lt(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 <= 1",
                Expression::BinaryOperator(ast::BinaryOperator::Lte(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 AND 1",
                Expression::BinaryOperator(ast::BinaryOperator::And(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "1 OR 1",
                Expression::BinaryOperator(ast::BinaryOperator::Or(
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                    Box::new(Expression::Literal(ast::Literal::Int(1))),
                )),
            ),
            (
                "-a * b",
                Expression::BinaryOperator(ast::BinaryOperator::Mul(
                    Box::new(Expression::UnaryOperator {
                        op: ast::UnaryOperator::Minus,
                        expr: Box::new(Expression::Identifier("a".into())),
                    }),
                    Box::new(Expression::Identifier("b".into())),
                )),
            ),
            (
                "a + b * c",
                Expression::BinaryOperator(ast::BinaryOperator::Add(
                    Box::new(Expression::Identifier("a".into())),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Mul(
                        Box::new(Expression::Identifier("b".into())),
                        Box::new(Expression::Identifier("c".into())),
                    ))),
                )),
            ),
            (
                "5 > 1 AND 3 < 4",
                Expression::BinaryOperator(ast::BinaryOperator::And(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Gt(
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                        Box::new(Expression::Literal(ast::Literal::Int(1))),
                    ))),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Lt(
                        Box::new(Expression::Literal(ast::Literal::Int(3))),
                        Box::new(Expression::Literal(ast::Literal::Int(4))),
                    ))),
                )),
            ),
            (
                "1 + (2 + 3) + 4",
                Expression::BinaryOperator(ast::BinaryOperator::Add(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::Literal(ast::Literal::Int(1))),
                        Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                            Box::new(Expression::Literal(ast::Literal::Int(2))),
                            Box::new(Expression::Literal(ast::Literal::Int(3))),
                        ))),
                    ))),
                    Box::new(Expression::Literal(ast::Literal::Int(4))),
                )),
            ),
            (
                "(5 + 5) * 2",
                Expression::BinaryOperator(ast::BinaryOperator::Mul(
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                    ))),
                    Box::new(Expression::Literal(ast::Literal::Int(2))),
                )),
            ),
            (
                "2 / (5 + 5)",
                Expression::BinaryOperator(ast::BinaryOperator::Div(
                    Box::new(Expression::Literal(ast::Literal::Int(2))),
                    Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                    ))),
                )),
            ),
            (
                "-(5 + 5)",
                Expression::UnaryOperator {
                    op: ast::UnaryOperator::Minus,
                    expr: (Box::new(Expression::BinaryOperator(ast::BinaryOperator::Add(
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                        Box::new(Expression::Literal(ast::Literal::Int(5))),
                    )))),
                },
            ),
        ];

        for test in tests {
            assert_eq!(parse_expr(test.0).unwrap(), test.1, "test expression: {}", test.0)
        }
    }

    #[test]
    fn test_parse_between_expression() {
        let stmt = parse_stmt("SELECT * FROM tbl WHERE id BETWEEN 1 AND 3").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(ast::Select {
                with: None,
                distinct: None,
                columns: vec![ast::SelectItem::Wildcard],
                from: vec![ast::From::Table {
                    name: "tbl".to_owned(),
                    alias: None
                }],
                r#where: Some(ast::Expression::Between {
                    negated: false,
                    expr: Box::new(ast::Expression::Identifier("id".into())),
                    low: Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                    high: Box::new(ast::Expression::Literal(ast::Literal::Int(3))),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );
    }

    #[test]
    fn test_parse_cast_and_case_expression() {
        let stmt = parse_stmt("SELECT CAST(CASE WHEN a = 1 THEN 2 ELSE 0 END AS decimal(12,2)) FROM t").unwrap();

        assert_eq!(
            stmt,
            ast::Statement::Select(Box::new(Select {
                with: None,
                distinct: None,
                columns: vec![SelectItem::UnNamedExpr(ast::Expression::Cast {
                    expr: Box::new(ast::Expression::Case {
                        operand: None,
                        when_then: vec![(
                            ast::Expression::BinaryOperator(BinaryOperator::Eq(
                                Box::new(ast::Expression::Identifier("a".into())),
                                Box::new(ast::Expression::Literal(ast::Literal::Int(1))),
                            )),
                            ast::Expression::Literal(ast::Literal::Int(2)),
                        )],
                        else_expr: Some(Box::new(ast::Expression::Literal(ast::Literal::Int(0)))),
                    }),
                    data_type: DataType::Decimal(Some(12), Some(2)),
                })],
                from: vec![ast::From::Table {
                    name: "t".to_owned(),
                    alias: None
                }],
                r#where: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            }))
        );
    }

    fn parse_stmt(input: &str) -> Result<Statement> {
        let mut parser = Parser::new(input);
        parser.parse()
    }

    fn parse_expr(input: &str) -> Result<Expression> {
        let mut parser = Parser::new(input);
        parser.parse_expression(0)
    }
}
