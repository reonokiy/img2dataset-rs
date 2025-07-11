use std::path::Path;

use anyhow::Result;
use sqlx::{
    migrate,
    sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool},
};

pub struct State {
    pool: SqlitePool,
}
struct DownloadSuccess {
    success: i64,
}

impl State {
    pub async fn new(base_path: &Path) -> Result<Self> {
        let db_path = base_path.join("state.db");
        let pool = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&db_path)
                .journal_mode(SqliteJournalMode::Wal)
                .auto_vacuum(SqliteAutoVacuum::Full)
                .foreign_keys(true)
                .create_if_missing(true),
        )
        .await?;
        migrate!("./migrations").run(&pool).await?;
        Ok(State { pool })
    }

    pub async fn get_download_state(&self, url: &str) -> Result<Option<bool>> {
        let success = sqlx::query_as!(
            DownloadSuccess,
            "SELECT success FROM download_info WHERE url = ?",
            url
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(success.map_or(None, |s| Some(s.success == 1)))
    }

    pub async fn set_download_state(&self, url: &str, success: bool) -> Result<()> {
        let success_val = if success { 1 } else { 0 };
        sqlx::query!(
            "INSERT OR REPLACE INTO download_info (url, success) VALUES (?, ?)",
            url,
            success_val
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
