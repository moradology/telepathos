//! Read-only FTS search over the Hermes gateway's session store.
//! Couples to Hermes's internal schema deliberately; isolated behind
//! set_search_backend so it can swap to an official API later.

use rusqlite::{params, types::Type, Connection, Row};
use telepathy_lanes::{MAX_ENRICHED_LANE_TITLE_CODEPOINTS, MAX_ENRICHED_LANE_TITLE_UTF8_BYTES};
use telepathy_proto::{MAX_OPAQUE_ID_BYTES, MAX_OPAQUE_ID_LENGTH};

const MAX_SEARCH_RESULT_UTF8_BYTES: usize = 8 * 1024;
const MAX_SEARCH_RESULT_CODEPOINTS: usize = 4 * 1024;
const SEARCH_UNAVAILABLE_MESSAGE: &str = "Search unavailable.";

/// SQLite's `substr` counts Unicode characters for TEXT values. Keep the SQL
/// limits separate from the byte limits below: SQL prevents an untrusted
/// value from being materialized in full, while Rust preserves UTF-8 when a
/// multibyte value reaches the shared byte cap.
const MAX_CHAT_ID_SQL_CODEPOINTS: usize = MAX_OPAQUE_ID_LENGTH;
const MAX_TITLE_SQL_CODEPOINTS: usize = MAX_ENRICHED_LANE_TITLE_CODEPOINTS;

/// Bound an untrusted database title before it can enter `/api/state`. Slice
/// only at Rust character boundaries so the resulting UTF-8 remains valid.
/// An empty input remains absent rather than becoming a meaningless title.
pub fn bounded_title(value: &str) -> Option<String> {
    bounded_text(
        value,
        MAX_ENRICHED_LANE_TITLE_UTF8_BYTES,
        MAX_ENRICHED_LANE_TITLE_CODEPOINTS,
    )
}

fn bounded_chat_id(value: &str) -> Option<String> {
    bounded_text(value, MAX_OPAQUE_ID_BYTES, MAX_OPAQUE_ID_LENGTH)
}

fn bounded_text(value: &str, max_bytes: usize, max_codepoints: usize) -> Option<String> {
    if value.is_empty() {
        return None;
    }
    let mut end = 0;
    let mut codepoints = 0;
    for (offset, character) in value.char_indices() {
        let next = offset + character.len_utf8();
        if next > max_bytes || codepoints >= max_codepoints {
            break;
        }
        end = next;
        codepoints += 1;
    }
    (end > 0).then(|| value[..end].to_owned())
}

fn bounded_search_result(value: String) -> String {
    bounded_text(
        &value,
        MAX_SEARCH_RESULT_UTF8_BYTES,
        MAX_SEARCH_RESULT_CODEPOINTS,
    )
    .unwrap_or_default()
}

fn bounded_sql_text(row: &Row<'_>, column: usize) -> rusqlite::Result<String> {
    // The SQL expression returns a BLOB deliberately. That lets malformed
    // TEXT bytes be validated here without asking rusqlite to allocate an
    // unbounded String from the database value.
    let bytes: Vec<u8> = row.get(column)?;
    String::from_utf8(bytes).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Blob, Box::new(error))
    })
}

fn search_failure(operation: &str, error: &dyn std::fmt::Display) -> String {
    eprintln!("hermes search {operation} failed: {error}");
    SEARCH_UNAVAILABLE_MESSAGE.to_owned()
}

fn latest_titles_failure(operation: &str, error: &dyn std::fmt::Display) {
    eprintln!("hermes latest titles {operation} failed: {error}");
}

/// Returns a spoken-form summary of lanes/sessions matching the query.
pub fn search_sessions(db_path: &str, query: &str, _lane_ids: &[String]) -> String {
    let con = match Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    {
        Ok(c) => c,
        Err(error) => return search_failure("opening database", &error),
    };
    // phrase-quote the query so FTS doesn't choke on identifiers
    let safe = query.replace('"', "");
    let sql = "
        SELECT
            CASE WHEN typeof(s.chat_id) = 'text'
                THEN CAST(substr(s.chat_id, 1, ?2) AS BLOB)
                ELSE X''
            END AS chat_id,
            CASE WHEN typeof(s.title) = 'text'
                THEN CAST(substr(s.title, 1, ?3) AS BLOB)
                ELSE X''
            END AS title,
            COUNT(*) as hits,
            MAX(m.timestamp) as latest
        FROM messages_fts f
        JOIN messages m ON m.rowid = f.rowid
        JOIN sessions s ON s.id = m.session_id
        WHERE typeof(s.chat_id) = 'text'
            AND messages_fts MATCH ?1
        GROUP BY s.id
        ORDER BY hits DESC, latest DESC
        LIMIT 5";
    let mut stmt = match con.prepare(sql) {
        Ok(s) => s,
        Err(error) => return search_failure("preparing query", &error),
    };
    let rows = stmt.query_map(
        params![
            safe,
            MAX_CHAT_ID_SQL_CODEPOINTS as i64,
            MAX_TITLE_SQL_CODEPOINTS as i64
        ],
        |r| {
            Ok((
                bounded_sql_text(r, 0)?,
                bounded_sql_text(r, 1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, f64>(3)?,
            ))
        },
    );
    match rows {
        Err(error) => search_failure("running query", &error),
        Ok(rows) => {
            let mut hits = Vec::with_capacity(5);
            for row in rows {
                let (chat_id, title, _hits, latest) = match row {
                    Ok(row) => row,
                    Err(error) => return search_failure("reading query result", &error),
                };
                let Some(chat_id) = bounded_chat_id(&chat_id) else {
                    continue;
                };
                let title = bounded_title(&title).unwrap_or_default();
                let latest = latest.max(0.0) as u64;
                let age_days = now_secs().saturating_sub(latest) / 86400;
                let age = if age_days == 0 {
                    "today".to_string()
                } else {
                    format!("{age_days}d ago")
                };
                hits.push(format!(
                    "{chat_id}: {} ({age})",
                    if title.is_empty() { "untitled" } else { &title }
                ));
            }
            if hits.is_empty() {
                "No conversations matched.".into()
            } else {
                bounded_search_result(hits.join("; "))
            }
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Latest non-empty session title per chat_id — lane-name enrichment.
pub fn latest_titles(db_path: &str) -> Vec<(String, String)> {
    let con = match Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    {
        Ok(c) => c,
        Err(error) => {
            latest_titles_failure("opening database", &error);
            return vec![];
        }
    };
    let sql = "
        SELECT
            CAST(substr(chat_id, 1, ?1) AS BLOB) AS chat_id,
            CAST(substr(title, 1, ?2) AS BLOB) AS title
        FROM (
            SELECT chat_id, title, MAX(last_activity_at) AS lat
            FROM sessions
            WHERE typeof(chat_id) = 'text'
                AND typeof(title) = 'text'
                AND title != ''
            GROUP BY chat_id
        ) ORDER BY lat DESC LIMIT 50";
    let mut stmt = match con.prepare(sql) {
        Ok(s) => s,
        Err(error) => {
            latest_titles_failure("preparing query", &error);
            return vec![];
        }
    };
    let rows = match stmt.query_map(
        params![
            MAX_CHAT_ID_SQL_CODEPOINTS as i64,
            MAX_TITLE_SQL_CODEPOINTS as i64
        ],
        |r| Ok((bounded_sql_text(r, 0)?, bounded_sql_text(r, 1)?)),
    ) {
        Ok(rows) => rows,
        Err(error) => {
            latest_titles_failure("running query", &error);
            return vec![];
        }
    };
    rows.filter_map(|row| match row {
        Ok((chat_id, title)) => {
            let chat_id = bounded_chat_id(&chat_id)?;
            let title = bounded_title(&title)?;
            Some((chat_id, title))
        }
        Err(error) => {
            latest_titles_failure("reading query result", &error);
            None
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        path: PathBuf,
    }

    impl TestDatabase {
        fn new() -> Self {
            let id = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "telepathy-hermes-search-{}-{id}.sqlite",
                std::process::id()
            ));
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "
                    CREATE TABLE sessions (
                        id INTEGER PRIMARY KEY,
                        chat_id,
                        title,
                        last_activity_at REAL
                    );
                    CREATE TABLE messages (
                        session_id INTEGER NOT NULL,
                        timestamp REAL NOT NULL,
                        content TEXT NOT NULL
                    );
                    CREATE VIRTUAL TABLE messages_fts USING fts5(content);
                    ",
                )
                .unwrap();
            drop(connection);
            Self { path }
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn enriched_titles_truncate_on_utf8_boundaries_and_codepoint_caps() {
        let exact = "é".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS);
        assert_eq!(exact.len(), MAX_ENRICHED_LANE_TITLE_UTF8_BYTES);
        assert_eq!(bounded_title(&exact).as_deref(), Some(exact.as_str()));

        let over = format!("{exact}é");
        assert_eq!(bounded_title(&over).as_deref(), Some(exact.as_str()));

        let emoji = "😀".repeat(65);
        let bounded = bounded_title(&emoji).unwrap();
        assert_eq!(bounded, "😀".repeat(64));
        assert_eq!(bounded.len(), 256);
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
        assert_eq!(bounded_title(""), None);
    }

    #[test]
    fn sqlite_values_are_bounded_before_search_and_title_materialization() {
        let database = TestDatabase::new();
        let connection = Connection::open(&database.path).unwrap();
        let chat_id = "é".repeat(MAX_OPAQUE_ID_BYTES);
        let title = "😀".repeat(MAX_ENRICHED_LANE_TITLE_CODEPOINTS * 4);
        connection
            .execute(
                "INSERT INTO sessions (id, chat_id, title, last_activity_at) VALUES (1, ?1, ?2, 0)",
                (&chat_id, &title),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages (rowid, session_id, timestamp, content) VALUES (1, 1, 0, 'needle')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO messages_fts (rowid, content) VALUES (1, 'needle')",
                [],
            )
            .unwrap();
        drop(connection);

        let expected_chat_id = "é".repeat(MAX_OPAQUE_ID_BYTES / "é".len());
        let expected_title = "😀".repeat(MAX_ENRICHED_LANE_TITLE_UTF8_BYTES / "😀".len());
        let titles = latest_titles(database.path.to_str().unwrap());
        assert_eq!(
            titles,
            vec![(expected_chat_id.clone(), expected_title.clone())]
        );

        let result = search_sessions(database.path.to_str().unwrap(), "needle", &[]);
        assert!(result.contains(&expected_chat_id));
        assert!(result.contains(&expected_title));
        assert!(result.len() <= MAX_SEARCH_RESULT_UTF8_BYTES);
        assert!(result.chars().count() <= MAX_SEARCH_RESULT_CODEPOINTS);
        assert!(std::str::from_utf8(result.as_bytes()).is_ok());
    }

    #[test]
    fn malformed_sqlite_values_are_ignored_or_sanitized() {
        let database = TestDatabase::new();
        let connection = Connection::open(&database.path).unwrap();
        connection
            .execute_batch(
                "
                INSERT INTO sessions (id, chat_id, title, last_activity_at)
                VALUES (1, CAST(X'80FF' AS TEXT), CAST(X'80FF' AS TEXT), 0);
                INSERT INTO messages (rowid, session_id, timestamp, content)
                VALUES (1, 1, 0, 'needle');
                INSERT INTO messages_fts (rowid, content) VALUES (1, 'needle');
                INSERT INTO sessions (id, chat_id, title, last_activity_at)
                VALUES (2, 'good-chat', X'80FF', 0);
                ",
            )
            .unwrap();
        drop(connection);

        let result = search_sessions(database.path.to_str().unwrap(), "needle", &[]);
        assert_eq!(result, SEARCH_UNAVAILABLE_MESSAGE);
        assert!(!result.contains("UTF-8"));
        assert!(latest_titles(database.path.to_str().unwrap()).is_empty());
    }

    #[test]
    fn sqlite_failures_return_one_fixed_message() {
        let database = TestDatabase::new();
        let connection = Connection::open(&database.path).unwrap();
        connection
            .execute_batch(
                "
                DROP TABLE messages_fts;
                DROP TABLE messages;
                DROP TABLE sessions;
                ",
            )
            .unwrap();
        drop(connection);
        let result = search_sessions(database.path.to_str().unwrap(), "needle", &[]);
        assert_eq!(result, SEARCH_UNAVAILABLE_MESSAGE);
        assert!(!result.contains("no such table"));
        assert!(!result.contains(database.path.to_str().unwrap()));
    }

    #[test]
    fn final_search_result_bound_preserves_utf8() {
        let result = bounded_search_result("😀".repeat(MAX_SEARCH_RESULT_CODEPOINTS + 1));
        assert_eq!(
            result,
            "😀".repeat(MAX_SEARCH_RESULT_UTF8_BYTES / "😀".len())
        );
        assert!(result.len() <= MAX_SEARCH_RESULT_UTF8_BYTES);
        assert!(result.chars().count() <= MAX_SEARCH_RESULT_CODEPOINTS);
        assert!(std::str::from_utf8(result.as_bytes()).is_ok());
    }
}
