use crate::entities::Metadata;
use rusqlite::{Connection, Result};
use crate::error::Error;
use std::path::Path;

pub struct SQLite {
    connection: Connection,
}

impl SQLite {
    pub fn new(path: &Path) -> Result<SQLite, Error> {
        Ok(SQLite {
            connection: Connection::open(path)?,
        })
    }

    pub fn init_schema(&self) -> Result<(), Error> {
        self.connection.execute(
            "CREATE TABLE metadata (
                id varchar primary key,
                spec_name varchar not null,
                spec_version integer,
                block_height integer not null,
                block_hash char(66) not null,
                hex varchar not null
            );",
            []
        )?;
        Ok(())
    }

    pub fn insert_metadata(&self, metadata: &Metadata) -> Result<(), Error> {
        self.connection.execute(
            "INSERT INTO metadata VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &metadata.id,
                &metadata.spec_name,
                &metadata.spec_version,
                &metadata.block_height,
                &metadata.block_hash,
                &metadata.hex
            )
        )?;
        Ok(())
    }
}
