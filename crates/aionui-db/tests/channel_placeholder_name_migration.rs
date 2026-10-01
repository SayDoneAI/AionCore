use sqlx::Row;

async fn insert_conversation(pool: &sqlx::SqlitePool, id: &str, name: &str, source: &str) {
    sqlx::query(
        "INSERT INTO conversations
         (id, user_id, name, type, extra, source, name_source, created_at, updated_at)
         VALUES (?, 'system_default_user', ?, 'acp', '{}', ?, 'user', 1, 1)",
    )
    .bind(id)
    .bind(name)
    .bind(source)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn migration_reopens_legacy_channel_placeholders_for_auto_naming() {
    let db = aionui_db::init_database_memory().await.unwrap();
    let pool = db.pool();

    insert_conversation(pool, "legacy-codex", "Codex", "weixin").await;
    insert_conversation(pool, "legacy-dated", "0929|其他|WeChat", "lark").await;
    insert_conversation(pool, "custom-channel", "Codex notes", "weixin").await;
    insert_conversation(pool, "desktop-codex", "Codex", "aionui").await;

    sqlx::query(include_str!("../migrations/045_recover_channel_placeholder_names.sql"))
        .execute(pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT id, name_source FROM conversations ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap();
    let source_by_id = rows
        .into_iter()
        .map(|row| (row.get::<String, _>("id"), row.get::<Option<String>, _>("name_source")))
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(source_by_id.get("legacy-codex"), Some(&None));
    assert_eq!(source_by_id.get("legacy-dated"), Some(&None));
    assert_eq!(source_by_id.get("custom-channel"), Some(&Some("user".to_owned())));
    assert_eq!(source_by_id.get("desktop-codex"), Some(&Some("user".to_owned())));
}
