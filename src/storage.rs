use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::parser::{Condition, OrderBy, OrderDirection};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseMetadata {
    tables: HashMap<String, TableMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableMetadata {
    name: String,
    columns: Vec<Column>,
    row_count: usize,
    indexes: Vec<Index>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Index {
    name: String,
    columns: Vec<String>,
    file_path: String,
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
            (Value::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Value::Null) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Less)
    }
}

impl Eq for Value {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RowId {
    pub offset: u64,
}

pub struct Storage {
    metadata: DatabaseMetadata,
    db_dir: PathBuf,
    page_size: usize,
}

impl Storage {
    pub fn new(db_path: &str) -> Result<Self> {
        let db_dir = Path::new(db_path).to_path_buf();

        if !db_dir.exists() {
            fs::create_dir_all(&db_dir)?;
        }

        let metadata_path = db_dir.join("metadata.json");
        let metadata = if metadata_path.exists() {
            let mut file = File::open(&metadata_path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;

            if contents.is_empty() {
                DatabaseMetadata {
                    tables: HashMap::new(),
                }
            } else {
                serde_json::from_str(&contents)?
            }
        } else {
            DatabaseMetadata {
                tables: HashMap::new(),
            }
        };

        Ok(Storage {
            metadata,
            db_dir,
            page_size: 1000,
        })
    }

    fn save_metadata(&self) -> Result<()> {
        let metadata_path = self.db_dir.join("metadata.json");
        let mut file = File::create(metadata_path)?;
        let json = serde_json::to_string_pretty(&self.metadata)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    fn table_path(&self, table_name: &str) -> PathBuf {
        self.db_dir.join(format!("{}.data", table_name))
    }

    fn index_path(&self, table_name: &str, index_name: &str) -> PathBuf {
        self.db_dir
            .join(format!("{}_{}.idx", table_name, index_name))
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<()> {
        if self.metadata.tables.contains_key(name) {
            return Err(anyhow::anyhow!("Table already exists"));
        }

        let table = TableMetadata {
            name: name.to_string(),
            columns,
            row_count: 0,
            indexes: Vec::new(),
        };

        let table_path = self.table_path(name);
        File::create(table_path)?;

        self.metadata.tables.insert(name.to_string(), table);
        self.save_metadata()?;

        Ok(())
    }

    pub fn insert_row(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>,
        values: Vec<Value>,
    ) -> Result<()> {
        let table_path = self.table_path(table_name);

        let table_metadata = self
            .metadata
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let values = if let Some(col_names) = columns {
            if col_names.len() != values.len() {
                return Err(anyhow::anyhow!(
                    "Number of column names does not match number of values"
                ));
            }

            let mut col_map = HashMap::new();
            for (i, col_name) in col_names.iter().enumerate() {
                col_map.insert(col_name.clone(), values[i].clone());
            }

            let mut ordered_values = Vec::with_capacity(table_metadata.columns.len());
            for col in &table_metadata.columns {
                if let Some(value) = col_map.get(&col.name) {
                    ordered_values.push(value.clone());
                } else {
                    ordered_values.push(Value::Null);
                }
            }

            ordered_values
        } else {
            if table_metadata.columns.len() != values.len() {
                return Err(anyhow::anyhow!(
                    "Number of values does not match number of columns"
                ));
            }
            values
        };

        for (i, value) in values.iter().enumerate() {
            match (value, &table_metadata.columns[i].data_type) {
                (Value::Null, _) => {}
                (Value::Integer(_), DataType::Integer) => {}
                (Value::Text(_), DataType::Text) => {}
                (Value::Boolean(_), DataType::Boolean) => {}
                (Value::Real(_), DataType::Real) => {}
                (v, dt) => {
                    return Err(anyhow::anyhow!(
                        "Type mismatch: {:?} is not compatible with {:?}",
                        v,
                        dt
                    ));
                }
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&table_path)?;

        let position = file.seek(SeekFrom::End(0))?;

        let row_json = serde_json::to_string(&values)?;
        writeln!(file, "{}", row_json)?;

        let indexes: Vec<_> = table_metadata
            .indexes
            .iter()
            .map(|idx| (idx.name.clone(), idx.file_path.clone()))
            .collect();
        table_metadata.row_count += 1;
        self.save_metadata()?;

        for (index_name, _) in indexes {
            self.update_index(table_name, &index_name, &values, position)?;
        }

        Ok(())
    }

    fn update_index(
        &self,
        table_name: &str,
        index_name: &str,
        values: &[Value],
        position: u64,
    ) -> Result<()> {
        let table_metadata = self
            .metadata
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let index = table_metadata
            .indexes
            .iter()
            .find(|idx| idx.name == index_name)
            .ok_or_else(|| anyhow::anyhow!("Index not found"))?;

        let mut key_values = Vec::new();
        for col_name in &index.columns {
            let col_idx = table_metadata
                .columns
                .iter()
                .position(|col| &col.name == col_name)
                .ok_or_else(|| anyhow::anyhow!("Column not found in table schema"))?;

            key_values.push(values[col_idx].clone());
        }

        let key = serde_json::to_string(&key_values)?;
        let row_id = RowId { offset: position };

        let index_path = Path::new(&index.file_path);
        let mut index_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_path)?;

        let entry = format!("{}\t{}\n", key, position);
        index_file.write_all(entry.as_bytes())?;

        Ok(())
    }

    fn load_rows_paginated(
        &self,
        table_name: &str,
        start_row: usize,
        max_rows: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let table_path = self.table_path(table_name);
        let file = File::open(table_path)?;
        let reader = BufReader::new(file);

        let mut rows = Vec::new();
        for (i, line) in reader.lines().enumerate() {
            if i < start_row {
                continue;
            }

            if rows.len() >= max_rows {
                break;
            }

            let line = line?;
            let row: Vec<Value> = serde_json::from_str(&line)?;
            rows.push(row);
        }

        Ok(rows)
    }

    pub fn get_rows(
        &self,
        table_name: &str,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<Value>>> {
        let table_metadata = self
            .metadata
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let mut result_rows = Vec::new();

        let use_index = if let Some(ref conditions) = conditions {
            self.find_usable_index(table_name, conditions)
        } else {
            None
        };

        let limit_val = limit.unwrap_or(usize::MAX);

        if let Some(index_name) = use_index {
            result_rows = self.get_rows_using_index(
                table_name,
                &index_name,
                &conditions.unwrap(),
                limit_val,
            )?;
        } else {
            let mut start_row = 0;
            while result_rows.len() < limit_val {
                let rows = self.load_rows_paginated(table_name, start_row, self.page_size)?;
                if rows.is_empty() {
                    break;
                }

                for row in rows {
                    if let Some(ref conditions) = conditions {
                        let match_all = conditions
                            .iter()
                            .all(|condition| condition.evaluate(&row, &table_metadata.columns));

                        if !match_all {
                            continue;
                        }
                    }

                    result_rows.push(row);

                    if result_rows.len() >= limit_val {
                        break;
                    }
                }

                start_row += self.page_size;
            }
        }

        if let Some(order_by) = order_by {
            let column_idx = table_metadata
                .columns
                .iter()
                .position(|col| col.name == order_by.column)
                .ok_or_else(|| anyhow::anyhow!("Order by column not found"))?;

            result_rows.sort_by(|a, b| {
                let cmp = a[column_idx].cmp(&b[column_idx]);
                if order_by.direction == OrderDirection::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }

        if !columns.is_empty() && columns[0] != "*" {
            let mut projected_rows = Vec::new();

            let mut col_indices = HashMap::new();
            for (i, col) in table_metadata.columns.iter().enumerate() {
                col_indices.insert(&col.name, i);
            }

            for row in result_rows {
                let mut projected_row = Vec::new();

                for col_name in &columns {
                    if let Some(&idx) = col_indices.get(col_name) {
                        projected_row.push(row[idx].clone());
                    } else {
                        return Err(anyhow::anyhow!("Column {} not found", col_name));
                    }
                }

                projected_rows.push(projected_row);
            }

            return Ok(projected_rows);
        }

        Ok(result_rows)
    }
    
    fn find_usable_index(&self, table_name: &str, conditions: &[Condition]) -> Option<String> {
        let table_metadata = self.metadata.tables.get(table_name)?;

        let mut condition_columns = Vec::new();
        for condition in conditions {
            if let Condition::Equal { column, .. } = condition {
                condition_columns.push(column.clone());
            }
        }

        for index in &table_metadata.indexes {
            let mut matches = true;
            for index_col in &index.columns {
                if !condition_columns.contains(index_col) {
                    matches = false;
                    break;
                }
            }

            if matches && !index.columns.is_empty() {
                return Some(index.name.clone());
            }
        }

        None
    }

    fn get_rows_using_index(
        &self,
        table_name: &str,
        index_name: &str,
        conditions: &[Condition],
        limit: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let table_metadata = self
            .metadata
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let index = table_metadata
            .indexes
            .iter()
            .find(|idx| idx.name == index_name)
            .ok_or_else(|| anyhow::anyhow!("Index not found"))?;

        let index_path = Path::new(&index.file_path);
        let index_file = File::open(index_path)?;
        let reader = BufReader::new(index_file);

        let data_path = self.table_path(table_name);
        let data_file = File::open(data_path)?;
        let mut data_reader = BufReader::new(data_file);

        let mut key_values = Vec::new();
        for col_name in &index.columns {
            for condition in conditions {
                if let Condition::Equal { column, value } = condition {
                    if column == col_name {
                        key_values.push(value.clone());
                        break;
                    }
                }
            }
        }

        let key_pattern = serde_json::to_string(&key_values)?;

        let mut result_rows = Vec::new();

        for line in reader.lines() {
            if result_rows.len() >= limit {
                break;
            }

            let line = line?;
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0];
            if key == key_pattern {
                let position: u64 = parts[1].parse()?;

                data_reader.seek(SeekFrom::Start(position))?;

                let mut line = String::new();
                data_reader.read_line(&mut line)?;

                let row: Vec<Value> = serde_json::from_str(&line)?;

                let match_all = conditions
                    .iter()
                    .all(|condition| condition.evaluate(&row, &table_metadata.columns));

                if match_all {
                    result_rows.push(row);
                }
            }
        }

        Ok(result_rows)
    }

    pub fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: Vec<String>,
    ) -> Result<()> {
        let index_path = self.index_path(table_name, index_name);
        let index_path_str = index_path.to_string_lossy().to_string();
        let data_path = self.table_path(table_name);

        if !self.metadata.tables.contains_key(table_name) {
            return Err(anyhow::anyhow!("Table does not exist"));
        }

        let table_metadata = self
            .metadata
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        if table_metadata
            .indexes
            .iter()
            .any(|index| index.name == index_name)
        {
            return Err(anyhow::anyhow!("Index already exists"));
        }

        for col_name in &columns {
            if !table_metadata
                .columns
                .iter()
                .any(|col| &col.name == col_name)
            {
                return Err(anyhow::anyhow!("Column {} not found", col_name));
            }
        }

        let mut index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&index_path)?;

        let index = Index {
            name: index_name.to_string(),
            columns: columns.clone(),
            file_path: index_path_str,
        };

        let col_indices = {
            let table_metadata = self.metadata.tables.get_mut(table_name).unwrap();
            table_metadata.indexes.push(index);

            columns
                .iter()
                .map(|col_name| {
                    table_metadata
                        .columns
                        .iter()
                        .position(|col| &col.name == col_name)
                        .ok_or_else(|| anyhow::anyhow!("Column not found"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        self.save_metadata()?;

        let data_file = File::open(data_path)?;
        let reader = BufReader::new(data_file);

        let mut position: u64 = 0;
        for line in reader.lines() {
            let line_position = position;
            let line = line?;
            position += line.len() as u64 + 1;

            let row: Vec<Value> = serde_json::from_str(&line)?;

            let mut key_values = Vec::new();
            for &idx in &col_indices {
                if idx < row.len() {
                    key_values.push(row[idx].clone());
                } else {
                    key_values.push(Value::Null);
                }
            }

            let key = serde_json::to_string(&key_values)?;

            writeln!(index_file, "{}\t{}", key, line_position)?;
        }

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if !self.metadata.tables.contains_key(name) {
            return Err(anyhow::anyhow!("Table does not exist"));
        }

        let table_path = self.table_path(name);

        if table_path.exists() {
            fs::remove_file(table_path)?;
        }

        let index_files = {
            let table = self.metadata.tables.get(name).unwrap();
            table
                .indexes
                .iter()
                .map(|idx| idx.name.clone())
                .collect::<Vec<_>>()
        };

        index_files.iter().for_each(|index_name| {
            self.drop_index(name, index_name).unwrap();
        });

        self.metadata.tables.remove(name);
        self.save_metadata()?;

        Ok(())
    }

    pub fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        let table_metadata = self
            .metadata
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table not found"))?;

        let index_pos = table_metadata
            .indexes
            .iter()
            .position(|idx| idx.name == index_name)
            .ok_or_else(|| anyhow::anyhow!("Index not found"))?;

        let index_path = table_metadata.indexes[index_pos].file_path.clone();

        let path = Path::new(&index_path);
        if path.exists() {
            fs::remove_file(path)?;
        }

        table_metadata.indexes.remove(index_pos);
        self.save_metadata()?;

        Ok(())
    }
}
