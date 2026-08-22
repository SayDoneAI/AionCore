use aionui_db::{
    DatabaseInitOptions, init_database, init_database_memory, init_database_staged_with_options,
    init_database_with_options,
};
use sqlx::Row;

// -- T1.1 Initialization --

#[tokio::test]
async fn init_creates_users_table() {
    let db = init_database_memory().await.unwrap();

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert!(
        count.0 >= 1,
        "users table should exist and have at least the system user"
    );
}

// -- T1.2 Pragma configuration --

#[tokio::test]
async fn pragma_foreign_keys_enabled() {
    let db = init_database_memory().await.unwrap();

    let row: (i64,) = sqlx::query_as("PRAGMA foreign_keys")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(row.0, 1, "foreign_keys should be ON");
}

#[tokio::test]
async fn pragma_busy_timeout() {
    let db = init_database_memory().await.unwrap();

    let row: (i64,) = sqlx::query_as("PRAGMA busy_timeout")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(row.0, 5000, "busy_timeout should be 5000ms");
}

#[tokio::test]
async fn pragma_journal_mode_wal_on_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_database(&dir.path().join("test.db")).await.unwrap();

    let row: (String,) = sqlx::query_as("PRAGMA journal_mode")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(
        row.0.to_lowercase(),
        "wal",
        "journal_mode should be WAL for file-backed DB"
    );
    db.close().await;
}

// -- T1.3 Idempotent re-initialization --

#[tokio::test]
async fn idempotent_reinit_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // First init + insert test data
    let db = init_database(&path).await.unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES ('u1', 'alice', 'hash123', 1000, 1000)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    db.close().await;

    // Second init — data should persist
    let db = init_database(&path).await.unwrap();
    let row = sqlx::query("SELECT username FROM users WHERE id = 'u1'")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(row.get::<String, _>("username"), "alice");
    db.close().await;
}

// -- T1.4 Migrations --

#[tokio::test]
async fn migrations_applied() {
    let db = init_database_memory().await.unwrap();

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = 1")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert!(count.0 >= 1, "at least one migration should be applied");
}

// -- T1.5 System default user --

#[tokio::test]
async fn system_default_user_exists() {
    let db = init_database_memory().await.unwrap();

    let row = sqlx::query("SELECT id, username, password_hash FROM users WHERE id = 'system_default_user'")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(row.get::<String, _>("id"), "system_default_user");
    assert_eq!(row.get::<String, _>("username"), "admin");
    assert_eq!(
        row.get::<String, _>("password_hash"),
        "",
        "system user should have empty password hash"
    );
}

#[tokio::test]
async fn system_user_has_valid_timestamps() {
    let before = aionui_common::now_ms();
    let db = init_database_memory().await.unwrap();
    let after = aionui_common::now_ms();

    let row = sqlx::query("SELECT created_at, updated_at FROM users WHERE id = 'system_default_user'")
        .fetch_one(db.pool())
        .await
        .unwrap();

    let created = row.get::<i64, _>("created_at");
    let updated = row.get::<i64, _>("updated_at");
    assert!(
        created >= before && created <= after,
        "created_at should be within test window"
    );
    assert!(
        updated >= before && updated <= after,
        "updated_at should be within test window"
    );
}

// -- Schema validation --

#[tokio::test]
async fn users_table_accepts_all_columns() {
    let db = init_database_memory().await.unwrap();

    sqlx::query(
        "INSERT INTO users \
         (id, username, email, password_hash, avatar_path, jwt_secret, created_at, updated_at, last_login) \
         VALUES ('u1', 'testuser', 'test@example.com', 'hash', '/avatar.png', 'secret', 1000, 2000, 3000)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let row = sqlx::query("SELECT * FROM users WHERE id = 'u1'")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(row.get::<String, _>("email"), "test@example.com");
    assert_eq!(
        row.get::<Option<String>, _>("avatar_path"),
        Some("/avatar.png".to_string())
    );
    assert_eq!(row.get::<Option<String>, _>("jwt_secret"), Some("secret".to_string()));
    assert_eq!(row.get::<Option<i64>, _>("last_login"), Some(3000));
}

#[tokio::test]
async fn username_unique_constraint() {
    let db = init_database_memory().await.unwrap();

    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES ('u1', 'duplicate', 'h', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let result = sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES ('u2', 'duplicate', 'h', 1, 1)",
    )
    .execute(db.pool())
    .await;

    assert!(result.is_err(), "duplicate username should violate unique constraint");
}

#[tokio::test]
async fn email_unique_constraint() {
    let db = init_database_memory().await.unwrap();

    sqlx::query(
        "INSERT INTO users (id, username, email, password_hash, created_at, updated_at) \
         VALUES ('u1', 'user1', 'same@example.com', 'h', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let result = sqlx::query(
        "INSERT INTO users (id, username, email, password_hash, created_at, updated_at) \
         VALUES ('u2', 'user2', 'same@example.com', 'h', 1, 1)",
    )
    .execute(db.pool())
    .await;

    assert!(result.is_err(), "duplicate email should violate unique constraint");
}

// -- Corruption recovery --

#[tokio::test]
async fn corruption_recovery_creates_backup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Write invalid content to simulate corruption
    std::fs::write(&path, b"not a valid sqlite database").unwrap();

    let db = init_database_with_options(
        &path,
        DatabaseInitOptions {
            recover_corrupted_database: true,
        },
    )
    .await
    .unwrap();

    // Recovered database should work
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(count.0 >= 1, "recovered DB should have system user");

    // Backup file should exist
    let has_backup = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains("backup"));
    assert!(has_backup, "backup of corrupted file should exist");

    db.close().await;
}

#[tokio::test]
async fn corruption_without_recovery_authorization_preserves_original_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let original = b"not a valid sqlite database";
    std::fs::write(&path, original).unwrap();

    let result = init_database(&path).await;

    assert!(result.is_err(), "unconfirmed startup must not rebuild corrupted DB");
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let has_backup = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains("backup"));
    assert!(!has_backup, "unconfirmed startup must not create a backup file");
}

#[tokio::test]
async fn unauthorized_open_stage_corruption_upgrades_to_recoverable_stage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    // Corruption-like on-disk content: "file is not a database".
    std::fs::write(&path, b"not a valid sqlite database").unwrap();

    // Unauthorized startup (recover_corrupted_database = false).
    let err = init_database_staged_with_options(
        &path,
        DatabaseInitOptions {
            recover_corrupted_database: false,
        },
    )
    .await
    .expect_err("unauthorized corruption must not rebuild, must surface a staged error");

    // The fix: corruption-like failure must upgrade to the recoverable stage,
    // NOT the generic database.open stage (which AionUi maps to restart-only).
    assert_eq!(err.stage(), "database.recoverable_corruption");

    // Original corrupt file untouched, no backup created (no rebuild without auth).
    assert_eq!(std::fs::read(&path).unwrap(), b"not a valid sqlite database");
    let has_backup = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains("backup"));
    assert!(!has_backup, "unauthorized startup must not create a backup file");
}

// -- Directory creation --

#[tokio::test]
async fn creates_parent_directories() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sub").join("nested").join("test.db");

    let db = init_database(&path).await.unwrap();
    assert!(path.exists(), "database file should be created in nested directory");
    db.close().await;
}

#[tokio::test]
async fn initialization_ignores_old_aionui_database_filenames() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path();
    let old_backend = data_dir.join("aionui-backend.db");
    let old_legacy = data_dir.join("aionui.db");
    let path = data_dir.join("saydone.db");
    let old_backend_contents = b"old backend marker";
    let old_legacy_contents = b"old legacy marker";
    std::fs::write(&old_backend, old_backend_contents).unwrap();
    std::fs::write(&old_legacy, old_legacy_contents).unwrap();

    let db = init_database(&path).await.unwrap();

    assert!(path.exists(), "startup must create the SayDone database");
    assert_eq!(
        std::fs::read(&old_backend).unwrap(),
        old_backend_contents,
        "startup must not copy or modify the old backend database"
    );
    assert_eq!(
        std::fs::read(&old_legacy).unwrap(),
        old_legacy_contents,
        "startup must not copy or modify the old legacy database"
    );
    db.close().await;
}

// -- Concurrent migrator regression (ELECTRON-1KK) --
//
// Repro for the Sentry secondary symptom: two processes opening the same
// SQLite DB on first start (e.g. Electron auto-update spawning the new
// version while the old one is still finalising shutdown, or
// `aioncore doctor` racing the server) both decide to apply the same
// migration version. sqlx-sqlite's lock()/unlock() are no-ops, so without
// the advisory file lock and retry-on-UNIQUE the slower process used to
// blow up with `UNIQUE constraint failed: _sqlx_migrations.version`.
//
// We use OS threads (not tokio::spawn) so each migrator runs on its own
// runtime — this matches the real "two processes" topology more closely
// than cooperative tasks would, and avoids the `&SqlitePool: Send` lifetime
// gymnastics that block tokio::spawn on this future.
#[test]
fn concurrent_init_database_does_not_panic_on_unique_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");

    let mut handles = Vec::new();
    for _ in 0..8 {
        let p = path.clone();
        handles.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move { init_database(&p).await })
        }));
    }

    // Every thread must succeed — none should bubble up the UNIQUE-constraint
    // error from `_sqlx_migrations`.
    let mut errors = Vec::new();
    for h in handles {
        match h.join().expect("thread panicked") {
            Ok(_db) => {}
            Err(e) => errors.push(e.to_string()),
        }
    }
    assert!(
        errors.is_empty(),
        "all parallel migrators should succeed, got errors: {errors:?}"
    );

    // All migrators converged on the same baseline schema with no duplicate
    // `_sqlx_migrations` rows.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let db = init_database(&path).await.unwrap();
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert!(count.0 >= 1, "at least one migration should be recorded");

        let dup: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM (SELECT version FROM _sqlx_migrations GROUP BY version HAVING COUNT(*) > 1)",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(dup.0, 0, "no duplicate versions should ever exist in _sqlx_migrations");
        db.close().await;
    });

    // Lock file is created next to the DB and is harmless to leave behind.
    let lock = path.with_file_name("saydone.db.migrate.lock");
    assert!(lock.exists(), "advisory lock file should be present after migrate");
}
