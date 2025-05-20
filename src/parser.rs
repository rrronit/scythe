use std::collections::BTreeMap;

use crate::storage::{Column, DataType, Value};
use anyhow::{Result, anyhow};

#[derive(Debug)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Value>,
    },
    Select {
        table: String,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        conditions: Option<Vec<Condition>>,
    },
    Delete {
        table: String,
        conditions: Option<Vec<Condition>>,
    },
    DropTable {
        name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
    },
}

#[derive(Debug)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

#[derive(Debug)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    Equal {
        column: String,
        value: Value,
    },
    NotEqual {
        column: String,
        value: Value,
    },
    GreaterThan {
        column: String,
        value: Value,
    },
    LessThan {
        column: String,
        value: Value,
    },
    GreaterEqual {
        column: String,
        value: Value,
    },
    LessEqual {
        column: String,
        value: Value,
    },
    Like {
        column: String,
        pattern: String,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And {
        left: Box<Condition>,
        right: Box<Condition>,
    },
    Or {
        left: Box<Condition>,
        right: Box<Condition>,
    },
}

impl Condition {
    pub fn evaluate(&self, row: &Vec<Value>, columns: &Vec<Column>) -> bool {
        let get_column_index = |col_name: &str| -> Option<usize> {
            columns.iter().position(|col| col.name == col_name)
        };

        match self {
            Condition::Equal { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] == *value)
            }
            Condition::NotEqual { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] != *value)
            }
            Condition::GreaterThan { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] > *value)
            }
            Condition::LessThan { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] < *value)
            }
            Condition::GreaterEqual { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] >= *value)
            }
            Condition::LessEqual { column, value } => {
                get_column_index(column).map_or(false, |idx| row[idx] <= *value)
            }
            Condition::Like { column, pattern } => {
                get_column_index(column).map_or(false, |idx| row[idx].to_string().contains(pattern))
            }
            Condition::IsNull { column } => {
                get_column_index(column).map_or(false, |idx| matches!(row[idx], Value::Null))
            }
            Condition::IsNotNull { column } => {
                get_column_index(column).map_or(false, |idx| !matches!(row[idx], Value::Null))
            }
            Condition::And { left, right } => {
                left.evaluate(row, columns) && right.evaluate(row, columns)
            }
            Condition::Or { left, right } => {
                left.evaluate(row, columns) || right.evaluate(row, columns)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TokenType {
    Identifier,
    Keyword,
    Operator,
    Punctuation,
    StringLiteral,
    NumericLiteral,
    Comment,
    EOF,
}

#[derive(Debug)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub position: usize,
}

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    pub fn new(input: String) -> Result<Self> {
        let tokens = Parser::tokenize(input)?;
        Ok(Parser { tokens, current: 0 })
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let token = self.peek()?;

        match token.value.to_uppercase().as_str() {
            "CREATE" => self.parse_create(),
            "INSERT" => self.parse_insert(),
            "SELECT" => self.parse_select(),
            "UPDATE" => self.parse_update(),
            "DELETE" => self.parse_delete(),
            "DROP" => self.parse_drop(),
            _ => Err(anyhow!("Unknown statement: {}", token.value)),
        }
    }

    fn tokenize(input: String) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut position = 0;

        let keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "INTO",
            "VALUES",
            "UPDATE",
            "SET",
            "DELETE",
            "CREATE",
            "TABLE",
            "DROP",
            "ALTER",
            "ADD",
            "COLUMN",
            "PRIMARY",
            "KEY",
            "FOREIGN",
            "REFERENCES",
            "INTEGER",
            "TEXT",
            "BOOLEAN",
            "REAL",
            "NULL",
            "NOT",
            "AND",
            "OR",
            "ORDER",
            "BY",
            "ASC",
            "DESC",
            "LIMIT",
            "OFFSET",
            "GROUP",
            "HAVING",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "OUTER",
            "ON",
            "AS",
            "DISTINCT",
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "BETWEEN",
            "IN",
            "LIKE",
            "IS",
            "TRUE",
            "FALSE",
        ];

        let operators = ["=", "<>", ">=", "<=", ">", "<", "+", "-", "*", "/", "%"];
        let punctuation = ["(", ")", ",", ";", "."];

        while position < input.len() {
            let remainder = &input[position..];

            if let Some(whitespace_len) = remainder.find(|c: char| !c.is_whitespace()) {
                if whitespace_len > 0 {
                    position += whitespace_len;
                    continue;
                }
            } else {
                break;
            }

            if remainder.starts_with("--") {
                let end = remainder.find('\n').unwrap_or(remainder.len());
                tokens.push(Token {
                    token_type: TokenType::Comment,
                    value: remainder[..end].to_string(),
                    position,
                });
                position += end;
                continue;
            }

            if remainder.starts_with('\'') || remainder.starts_with('"') {
                let quote = remainder.chars().next().unwrap();
                let mut end = 1;
                let mut escaped = false;

                while end < remainder.len() {
                    let c = remainder.chars().nth(end).unwrap();
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if c == quote {
                        break;
                    }
                    end += 1;
                }

                if end >= remainder.len() {
                    return Err(anyhow!(
                        "Unterminated string literal at position {}",
                        position
                    ));
                }

                tokens.push(Token {
                    token_type: TokenType::StringLiteral,
                    value: remainder[1..end].to_string(),
                    position,
                });
                position += end + 1;
                continue;
            }

            if remainder.chars().next().unwrap().is_digit(10) {
                let mut end = 0;
                let mut has_dot = false;

                while end < remainder.len() {
                    let c = remainder.chars().nth(end).unwrap();
                    if c.is_digit(10) {
                        end += 1;
                    } else if c == '.' && !has_dot {
                        has_dot = true;
                        end += 1;
                    } else {
                        break;
                    }
                }

                tokens.push(Token {
                    token_type: TokenType::NumericLiteral,
                    value: remainder[..end].to_string(),
                    position,
                });
                position += end;
                continue;
            }

            let mut operator_match = false;
            for op in operators.iter() {
                if remainder.starts_with(op) {
                    tokens.push(Token {
                        token_type: TokenType::Operator,
                        value: op.to_string(),
                        position,
                    });
                    position += op.len();
                    operator_match = true;
                    break;
                }
            }
            if operator_match {
                continue;
            }

            let mut punct_match = false;
            for p in punctuation.iter() {
                if remainder.starts_with(p) {
                    tokens.push(Token {
                        token_type: TokenType::Punctuation,
                        value: p.to_string(),
                        position,
                    });
                    position += p.len();
                    punct_match = true;
                    break;
                }
            }
            if punct_match {
                continue;
            }

            if remainder.chars().next().unwrap().is_alphabetic() || remainder.starts_with('_') {
                let mut end = 0;
                while end < remainder.len() {
                    let c = remainder.chars().nth(end).unwrap();
                    if c.is_alphanumeric() || c == '_' {
                        end += 1;
                    } else {
                        break;
                    }
                }

                let identifier = &remainder[..end];
                let token_type = if keywords.contains(&identifier.to_uppercase().as_str()) {
                    TokenType::Keyword
                } else {
                    TokenType::Identifier
                };

                tokens.push(Token {
                    token_type,
                    value: identifier.to_string(),
                    position,
                });
                position += end;
                continue;
            }

            return Err(anyhow!(
                "Unexpected character at position {}: {}",
                position,
                remainder.chars().next().unwrap()
            ));
        }

        tokens.push(Token {
            token_type: TokenType::EOF,
            value: "".to_string(),
            position,
        });

        Ok(tokens)
    }

    fn peek(&self) -> Result<&Token> {
        self.tokens
            .get(self.current)
            .ok_or_else(|| anyhow!("Unexpected end of input"))
    }

    fn advance(&mut self) -> Result<&Token> {
        let token = self
            .tokens
            .get(self.current)
            .ok_or_else(|| anyhow!("Unexpected end of input"))?;
        self.current += 1;
        Ok(token)
    }

    fn consume(&mut self, expected: &str) -> Result<&Token> {
        let token = self
            .tokens
            .get(self.current)
            .ok_or_else(|| anyhow!("Unexpected end of input"))?;
        if token.value.to_uppercase() == expected.to_uppercase() {
            self.current += 1;
            Ok(token)
        } else {
            Err(anyhow!("Expected '{}', got '{}'", expected, token.value))
        }
    }

    fn consume_any(&mut self, expected_types: &[TokenType]) -> Result<&Token> {
        let token = self
            .tokens
            .get(self.current)
            .ok_or_else(|| anyhow!("Unexpected end of input"))?;
        if expected_types.contains(&token.token_type) {
            self.current += 1;
            Ok(token)
        } else {
            Err(anyhow!(
                "Expected token of type {:?}, got {:?}",
                expected_types,
                token.token_type
            ))
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.consume("CREATE")?;
        if self.peek()?.value.to_uppercase() == "INDEX" {
            self.advance()?;
            let name = self.consume_any(&[TokenType::Identifier])?.value.clone();
            self.consume("ON")?;
            let table = self.consume_any(&[TokenType::Identifier])?.value.clone();
            self.consume("(")?;

            let mut columns = Vec::new();
            while self.peek()?.value != ")" {
                let column = self.consume_any(&[TokenType::Identifier])?.value.clone();
                columns.push(column);
                if self.peek()?.value != ")" {
                    self.consume(",")?;
                }
            }

            return Ok(Statement::CreateIndex {
                name,
                table,
                columns,
            });
        }

        self.consume("TABLE")?;
        let name = self.consume_any(&[TokenType::Identifier])?.value.clone();
        self.consume("(")?;

        let mut columns = Vec::new();
        loop {
            let col_name = self.consume_any(&[TokenType::Identifier])?.value.clone();
            let data_type_token = self
                .consume_any(&[TokenType::Keyword, TokenType::Identifier])?
                .value
                .clone();

            let data_type = match data_type_token.to_uppercase().as_str() {
                "INTEGER" | "INT" => DataType::Integer,
                "TEXT" | "VARCHAR" | "CHAR" | "STRING" => DataType::Text,
                "BOOLEAN" | "BOOL" => DataType::Boolean,
                "REAL" | "FLOAT" | "DOUBLE" => DataType::Real,
                _ => return Err(anyhow!("Unknown data type: {}", data_type_token)),
            };

            columns.push(Column {
                name: col_name,
                data_type,
            });

            let token = self.peek()?.value.clone();
            if token == ")" {
                self.advance()?;
                break;
            } else if token == "," {
                self.advance()?;
            } else {
                return Err(anyhow!("Expected ',' or ')', got '{}'", token));
            }
        }

        Ok(Statement::CreateTable { name, columns })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.consume("INSERT")?;
        self.consume("INTO")?;
        let table = self.consume_any(&[TokenType::Identifier])?.value.clone();

        let mut columns = None;
        if self.peek()?.value == "(" {
            self.advance()?;
            let mut col_list = Vec::new();

            loop {
                let col_name = self.consume_any(&[TokenType::Identifier])?.value.clone();
                col_list.push(col_name);

                let token = self.peek()?.value.clone();
                if token == ")" {
                    self.advance()?;
                    break;
                } else if token == "," {
                    self.advance()?;
                } else {
                    return Err(anyhow!("Expected ',' or ')', got '{}'", token));
                }
            }

            columns = Some(col_list);
        }

        self.consume("VALUES")?;

        let mut all_values = Vec::new();

        loop {
            self.consume("(")?;

            loop {
                let value = self.parse_value()?;
                all_values.push(value);

                let token = self.peek()?.value.clone();
                if token == ")" {
                    self.advance()?;
                    break;
                } else if token == "," {
                    self.advance()?;
                } else {
                    return Err(anyhow!("Expected ',' or ')', got '{}'", token));
                }
            }

            if self.current >= self.tokens.len() || self.peek()?.value != "," {
                break;
            } else {
                self.advance()?;
                if self.peek()?.value != "(" {
                    return Err(anyhow!("Expected '(' after ',' in VALUES clause"));
                }
            }
        }

        Ok(Statement::Insert {
            table,
            columns,
            values: all_values,
        })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.consume("SELECT")?;
        let mut columns = Vec::new();

        loop {
            let token = self.peek()?;
            if token.value == "*" {
                self.advance()?;
                columns.push("*".to_string());
                break;
            }

            if token.token_type == TokenType::Identifier {
                columns.push(self.advance()?.value.clone());
            } else {
                return Err(anyhow!(
                    "Expected column name or '*', got '{}'",
                    token.value
                ));
            }

            let next = self.peek()?;
            if next.value.to_uppercase() == "FROM" {
                break;
            } else if next.value == "," {
                self.advance()?;
            } else {
                return Err(anyhow!("Expected ',' or FROM, got '{}'", next.value));
            }
        }

        self.consume("FROM")?;
        let table = self.consume_any(&[TokenType::Identifier])?.value.clone();

        let mut conditions = None;
        if self.current < self.tokens.len() && self.peek()?.value.to_uppercase() == "WHERE" {
            self.advance()?;
            conditions = Some(self.parse_conditions()?);
        }

        let mut order_by = None;
        if self.current < self.tokens.len() && self.peek()?.value.to_uppercase() == "ORDER" {
            self.advance()?;
            self.consume("BY")?;
            let column = self.consume_any(&[TokenType::Identifier])?.value.clone();

            let direction = if self.current < self.tokens.len() {
                match self.peek()?.value.to_uppercase().as_str() {
                    "ASC" => {
                        self.advance()?;
                        OrderDirection::Ascending
                    }
                    "DESC" => {
                        self.advance()?;
                        OrderDirection::Descending
                    }
                    _ => OrderDirection::Ascending,
                }
            } else {
                OrderDirection::Ascending
            };

            order_by = Some(OrderBy { column, direction });
        }

        let mut limit = None;
        if self.current < self.tokens.len() && self.peek()?.value.to_uppercase() == "LIMIT" {
            self.advance()?;
            let limit_token = self
                .consume_any(&[TokenType::NumericLiteral])?
                .value
                .clone();
            limit = Some(
                limit_token
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid LIMIT value"))?,
            );
        }

        Ok(Statement::Select {
            table,
            columns,
            conditions: conditions.unwrap_or(None),
            order_by,
            limit,
        })
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.consume("UPDATE")?;
        let table = self.consume_any(&[TokenType::Identifier])?.value.clone();
        self.consume("SET")?;

        let mut assignments = Vec::new();

        loop {
            let column = self.consume_any(&[TokenType::Identifier])?.value.clone();
            self.consume("=")?;
            let value = self.parse_value()?;

            assignments.push(Assignment { column, value });

            if self.peek()?.value != "," {
                break;
            }
            self.advance()?;
        }

        let mut conditions = None;
        if self.current < self.tokens.len() && self.peek()?.value.to_uppercase() == "WHERE" {
            self.advance()?;
            conditions = Some(self.parse_conditions()?);
        }

        Ok(Statement::Update {
            table,
            assignments,
            conditions: conditions.unwrap_or(None),
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.consume("DELETE")?;
        self.consume("FROM")?;
        let table = self.consume_any(&[TokenType::Identifier])?.value.clone();

        let mut conditions = None;
        if self.current < self.tokens.len() && self.peek()?.value.to_uppercase() == "WHERE" {
            self.advance()?;
            conditions = self.parse_conditions()?;
        }

        Ok(Statement::Delete { table, conditions })
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.consume("DROP")?;
        self.consume("TABLE")?;
        let name = self.consume_any(&[TokenType::Identifier])?.value.clone();

        Ok(Statement::DropTable { name })
    }

    fn parse_conditions(&mut self) -> Result<Option<Vec<Condition>>> {
        let condition = self.parse_condition()?;
        let mut conditions = vec![condition];

        while self.current < self.tokens.len() {
            if self.peek()?.value.to_uppercase() == "AND"
                || self.peek()?.value.to_uppercase() == "OR"
            {
                let operator = self.advance()?.value.to_uppercase();
                let right_condition = self.parse_condition()?;

                let left_condition = conditions.pop().unwrap();
                let combined = if operator == "AND" {
                    Condition::And {
                        left: Box::new(left_condition.unwrap()),
                        right: Box::new(right_condition.unwrap()),
                    }
                } else {
                    Condition::Or {
                        left: Box::new(left_condition.unwrap()),
                        right: Box::new(right_condition.unwrap()),
                    }
                };

                conditions.push(Some(combined));
            } else {
                break;
            }
        }

        Ok(Some(conditions.into_iter().filter_map(|c| c).collect()))
    }

    fn parse_condition(&mut self) -> Result<Option<Condition>> {
        let column = self.consume_any(&[TokenType::Identifier])?.value.clone();

        if self.peek()?.value.to_uppercase() == "IS" {
            self.advance()?;

            if self.peek()?.value.to_uppercase() == "NOT" {
                self.advance()?;
                self.consume("NULL")?;
                return Ok(Some(Condition::IsNotNull { column }));
            } else if self.peek()?.value.to_uppercase() == "NULL" {
                self.advance()?;
                return Ok(Some(Condition::IsNull { column }));
            } else {
                return Err(anyhow!("Expected NULL after IS"));
            }
        }

        let operator = self.advance()?.value.clone();
        let value_token = self.advance()?;

        let value = match value_token.token_type {
            TokenType::NumericLiteral => {
                if value_token.value.contains('.') {
                    Value::Real(value_token.value.parse::<f64>().unwrap())
                } else {
                    Value::Integer(value_token.value.parse::<i64>().unwrap())
                }
            }
            TokenType::StringLiteral => Value::Text(value_token.value.clone()),
            TokenType::Keyword if value_token.value.to_uppercase() == "NULL" => Value::Null,
            TokenType::Keyword if value_token.value.to_uppercase() == "TRUE" => {
                Value::Boolean(true)
            }
            TokenType::Keyword if value_token.value.to_uppercase() == "FALSE" => {
                Value::Boolean(false)
            }
            _ => Value::Text(value_token.value.clone()),
        };

        match operator.as_str() {
            "=" => Ok(Some(Condition::Equal { column, value })),
            "<>" | "!=" => Ok(Some(Condition::NotEqual { column, value })),
            ">" => Ok(Some(Condition::GreaterThan { column, value })),
            "<" => Ok(Some(Condition::LessThan { column, value })),
            ">=" => Ok(Some(Condition::GreaterEqual { column, value })),
            "<=" => Ok(Some(Condition::LessEqual { column, value })),
            "LIKE" => Ok(Some(Condition::Like {
                column,
                pattern: match value {
                    Value::Text(pattern) => pattern,
                    _ => return Err(anyhow!("LIKE pattern must be a string")),
                },
            })),
            _ => Err(anyhow!("Unknown operator: {}", operator)),
        }
    }

    fn parse_value(&mut self) -> Result<Value> {
        let token = self
            .tokens
            .get(self.current)
            .ok_or_else(|| anyhow!("Unexpected end of input"))?;

        let value = match token.token_type {
            TokenType::NumericLiteral => {
                self.current += 1;
                if token.value.contains('.') {
                    Value::Real(token.value.parse::<f64>().unwrap())
                } else {
                    Value::Integer(token.value.parse::<i64>().unwrap())
                }
            }
            TokenType::StringLiteral => {
                self.current += 1;
                Value::Text(token.value.clone())
            }
            TokenType::Keyword => match token.value.to_uppercase().as_str() {
                "NULL" => {
                    self.current += 1;
                    Value::Null
                }
                "TRUE" => {
                    self.current += 1;
                    Value::Boolean(true)
                }
                "FALSE" => {
                    self.current += 1;
                    Value::Boolean(false)
                }
                _ => return Err(anyhow!("Unexpected keyword: {}", token.value)),
            },
            _ => return Err(anyhow!("Unexpected token type: {:?}", token.token_type)),
        };

        Ok(value)
    }
}
