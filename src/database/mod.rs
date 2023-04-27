mod entity;
pub mod mysql;

use sea_orm::DatabaseConnection;

#[derive(Clone)]
pub struct DataSource {
    pub sea_orm: DatabaseConnection,
}
