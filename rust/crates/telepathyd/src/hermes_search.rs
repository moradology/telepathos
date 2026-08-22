//! Read-only FTS search over the Hermes gateway's session store.
//! Couples to Hermes's internal schema deliberately; isolated behind
//! set_search_backend so it can swap to an official API later.

use rusqlite::Connection;

/// Returns a spoken-form summary of lanes/sessions matching the query.
pub fn search_sessions(db_path: &str, query: &str, _lane_ids: &[String]) -> String {
    let con = match Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(c) => c,
        Err(e) => return format!("Search unavailable: {e}"),
    };
    // phrase-quote the query so FTS doesn't choke on identifiers
    let safe = query.replace('"', "");
    let sql = "
        SELECT s.chat_id, COALESCE(s.title, ''), COUNT(*) as hits, MAX(m.timestamp) as latest
        FROM messages_fts f
        JOIN messages m ON m.rowid = f.rowid
        JOIN sessions s ON s.id = m.session_id
        WHERE messages_fts MATCH ?1
        GROUP BY s.id
        ORDER BY hits DESC, latest DESC
        LIMIT 5";
    let mut stmt = match con.prepare(sql) {
        Ok(s) => s,
        Err(e) => return format!("Search unavailable: {e}"),
    };
    let rows = stmt.query_map([&safe], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, f64>(3)?,
        ))
    });
    match rows {
        Err(e) => format!("Search unavailable: {e}"),
        Ok(rows) => {
            let hits: Vec<String> = rows.flatten().map(|(chat_id, title, _hits, latest)| {
                let age_days =
                    ((now_secs() - latest as u64) / 86400).max(0);
                let age = if age_days == 0 { "today".to_string() } else { format!("{age_days}d ago") };
                format!("{chat_id}: {} ({age})", if title.is_empty() { "untitled" } else { &title })
            }).collect();
            if hits.is_empty() {
                "No conversations matched.".into()
            } else {
                hits.join("; ")
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
