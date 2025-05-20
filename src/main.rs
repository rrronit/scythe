mod parser;
mod storage;

use anyhow::Result;
use parser::{Parser, Statement};
use storage::Storage;

fn main() -> Result<()> {
    // Create a new database
    let mut storage = Storage::new("./db")?;

    // Example: Create a users table
    let create_table_sql =
        "CREATE TABLE users ( id INTEGER , name TEXT , active BOOLEAN , age INTEGER )";

    let mut parser = Parser::new(create_table_sql.to_string()).unwrap();
    if let Statement::CreateTable { name, columns } = parser.parse()? {
        storage.create_table(&name, columns)?;
    }

    let insert_sql = "INSERT INTO users VALUES ( 1 , 'John Doe' , true , 20 )".to_string();
    let mut parser = Parser::new(insert_sql).unwrap();
    if let Statement::Insert {
        table,
        columns,
        values,
    } = parser.parse()?
    {
        storage.insert_row(&table, columns, values)?;
    }

    let create_index_sql = "CREATE INDEX idx_name ON users ( age )";
    let mut parser = Parser::new(create_index_sql.to_string()).unwrap();
    if let Statement::CreateIndex {
        name,
        table,
        columns,
    } = parser.parse()?
    {
        storage.create_index(&table, &name, columns)?;
        println!("Created index: {}", name);
    }

    let select_sql = "SELECT * FROM users WHERE  name = 'y7UgDBea9yFo8NyxPylFOFPBncIWjO' ";
    let mut parser = Parser::new(select_sql.to_string()).unwrap();
    if let Statement::Select {
        table,
        columns,
        conditions,
        order_by,
        limit,
    } = parser.parse()?
    {
        let rows = storage.get_rows(&table, columns, conditions, order_by, limit)?;
        for row in rows {
            println!("Row: {:?}", row);
        }
    }

    let drop_table_sql = "DROP TABLE users";
    let mut parser = Parser::new(drop_table_sql.to_string()).unwrap();
    if let Statement::DropTable { name } = parser.parse()? {
        storage.drop_table(&name)?;
        println!("Dropped table: {}", name);
    }

    Ok(())
}
