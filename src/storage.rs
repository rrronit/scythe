use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::parser::{Condition, OrderBy};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Database {
    tables: HashMap<String, Table>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Real,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
    Real(f64),
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Real(r) => write!(f, "{}", r),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}

pub struct Storage {
    db: Database,
    db_path: String,
}

impl Storage {
    pub fn new(db_path: &str) -> Result<Self> {
        let mut storage = Storage {
            db: Database {
                tables: HashMap::new(),
            },
            db_path: db_path.to_string(),
        };

        // Load existing database if it exists
        if Path::new(db_path).exists() {
            storage.load_database()?;
        }

        Ok(storage)
    }

    fn load_database(&mut self) -> Result<()> {
        let mut file = File::open(&self.db_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        if contents.is_empty() {
            return Ok(());
        }
        self.db = serde_json::from_str(&contents)?;
        Ok(())
    }

    fn save_database(&self) -> Result<()> {
        let mut file = File::create(&self.db_path)?;
        let json = serde_json::to_string_pretty(&self.db)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<()> {
        if self.db.tables.contains_key(name) {
            return Err(anyhow::anyhow!("Table already exists"));
        }

        let table = Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
        };

        self.db.tables.insert(name.to_string(), table);
        self.save_database()?;

        Ok(())
    }

    pub fn insert_row(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>,
        values: Vec<Value>,
    ) -> Result<()> {
        let table = self
            .db
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        if table.columns.len() != values.len() {
            return Err(anyhow::anyhow!(
                "Number of values does not match number of columns"
            ));
        }

        for (i, value) in values.iter().enumerate() {
            if table.columns[i].data_type == DataType::Integer
                && !matches!(value, Value::Integer(_))
            {
                return Err(anyhow::anyhow!("{:?} is not an integer", value));
            }
            if table.columns[i].data_type == DataType::Text && !matches!(value, Value::Text(_)) {
                return Err(anyhow::anyhow!("{:?} is not a text", value));
            }
            if table.columns[i].data_type == DataType::Boolean
                && !matches!(value, Value::Boolean(_))
            {
                return Err(anyhow::anyhow!("{:?} is not a boolean", value));
            }
        }

        table.rows.push(values);

        self.save_database()?;
        Ok(())
    }

    pub fn get_rows(
        &self,
        table_name: &str,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<Vec<&Vec<Value>>> {
        let table = self
            .db
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let mut rows: Vec<&Vec<Value>> = table.rows.iter().collect();
        println!("conditions: {:?}", conditions);
        if let Some(conditions) = conditions {
            rows = rows
                .into_iter()
                .filter(|row| {
                    conditions
                        .iter()
                        .all(|condition| condition.evaluate(row, &table.columns))
                })
                .collect();
        }

        Ok(rows)
    }
}
