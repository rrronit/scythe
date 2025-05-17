mod parser;
mod storage;

use anyhow::Result;
use parser::{Parser, Statement};
use storage::Storage;

fn main() -> Result<()> {
    // Create a new database
    let mut storage = Storage::new("scythe.db")?;

    // // Example: Create a users table
    let create_table_sql = "CREATE TABLE useri ( id INTEGER , name TEXT , active BOOLEAN )";

    let mut parser = Parser::new(create_table_sql.to_string()).unwrap();
    if let Statement::CreateTable { name, columns } = parser.parse()? {
        storage.create_table(&name, columns)?;
        println!("Created table: {}", name);
    }

    // Example: Insert a row
    let insert_sql = format!("INSERT INTO users VALUES ( 1 , 'IA1CY3aMTp' , true )",);
    let mut parser = Parser::new(insert_sql).unwrap();
    if let Statement::Insert {
        table,
        columns,
        values,
    } = parser.parse()?
    {
        storage.insert_row(&table, columns, values)?;
    }

    // Example: Select rows
    let select_sql = "SELECT * FROM users WHERE name LIKE 'IA1CY3aMTp'";
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
        println!("Selected rows from {}:", table);
        for row in rows {
            println!("Row: {:?}", row);
        }
    }

    Ok(())
}
