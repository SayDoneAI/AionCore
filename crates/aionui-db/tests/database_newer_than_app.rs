//! Downgrade detection: opening a database written by a NEWER app version.
//!
//! When `_sqlx_migrations` contains a version this binary's embedded migrator
//! does not know, sqlx fails with `MigrateError::VersionMissing`. Startup must
//! surface the dedicated `database.newer_than_app` stage (Sentry ELECTRON-31Z)
//! instead of the generic `database.migration` one, and must NOT treat the
//! intact-but-newer database as corruption to back up and rebuild.

use aionui_db::{
    DATABASE_NEWER_THAN_APP_STAGE, DatabaseInitOptions, DbError, init_database_staged,
    init_database_staged_with_options, latest_known_migration_version,
};

/// A migration version far above anything this binary will ever ship.
const FUTURE_MIGRATION_VERSION: i64 = 999_999;
const LEGACY_INITIAL_SCHEMA_CHECKSUM: [u8; 48] = [
    0xe1, 0x8a, 0x43, 0x94, 0x62, 0x74, 0x89, 0xc0, 0x83, 0x77, 0x81, 0x38, 0xa6, 0x95, 0xaa, 0xf9, 0x4d, 0xf3, 0xa8,
    0x75, 0x4f, 0x34, 0xca, 0x0a, 0xab, 0x07, 0x9b, 0x4e, 0xc9, 0x27, 0x79, 0xdd, 0x70, 0xa8, 0x1f, 0xcf, 0x67, 0x47,
    0x2c, 0x53, 0xe1, 0x8f, 0xc8, 0xbe, 0xc2, 0x51, 0x2c, 0x54,
];
const LEGACY_NORMALIZE_SCHEMA_CHECKSUM: [u8; 48] = [
    0x52, 0x0e, 0x39, 0x8d, 0x27, 0xbc, 0xcd, 0xdd, 0x27, 0xbc, 0x98, 0xb2, 0xe9, 0x0b, 0x1f, 0xbf, 0x90, 0x34, 0x75,
    0x7a, 0x41, 0xb1, 0x28, 0x93, 0xac, 0xf5, 0x1e, 0xc4, 0x00, 0xfd, 0xef, 0xaa, 0xd8, 0x8e, 0x76, 0x9a, 0x08, 0xd1,
    0x15, 0xf8, 0x35, 0x8f, 0xdf, 0xc3, 0x31, 0xa0, 0xc4, 0x44,
];

async fn seed_future_migration_row(path: &std::path::Path) {
    let db = init_database_staged(path).await.unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time)
         VALUES (?, 'from a newer app version', CURRENT_TIMESTAMP, TRUE, x'00', 0)",
    )
    .bind(FUTURE_MIGRATION_VERSION)
    .execute(db.pool())
    .await
    .unwrap();
    db.close().await;
}

#[tokio::test]
async fn newer_database_fails_with_dedicated_stage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");
    seed_future_migration_row(&path).await;

    let err = init_database_staged(&path).await.expect_err("downgrade must fail");
    assert_eq!(err.stage(), DATABASE_NEWER_THAN_APP_STAGE);

    let source = err.into_source();
    assert_eq!(source.missing_migration_version(), Some(FUTURE_MIGRATION_VERSION));
    assert!(
        matches!(
            &source,
            DbError::Migration(sqlx::migrate::MigrateError::VersionMissing(v)) if *v == FUTURE_MIGRATION_VERSION
        ),
        "unexpected source error: {source}"
    );
}

#[tokio::test]
async fn newer_database_is_not_recovered_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");
    seed_future_migration_row(&path).await;

    // Even with the rebuild flag authorized, an intact newer database must not
    // be backed up and replaced — that would destroy the user's data when the
    // actual fix is upgrading the app.
    let err = init_database_staged_with_options(
        &path,
        DatabaseInitOptions {
            recover_corrupted_database: true,
        },
    )
    .await
    .expect_err("downgrade must fail even with recovery authorized");
    assert_eq!(err.stage(), DATABASE_NEWER_THAN_APP_STAGE);
    assert!(path.exists(), "database file must be left in place");

    let backups: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().contains(".backup."))
        .collect();
    assert!(backups.is_empty(), "no backup/rebuild for a newer database");
}

#[tokio::test]
async fn genuine_migration_failures_keep_generic_stage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");

    // Tamper with an applied migration's checksum: sqlx reports
    // VersionMismatch, which is not a downgrade and must keep the generic
    // migration stage.
    let db = init_database_staged(&path).await.unwrap();
    sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    db.close().await;

    let err = init_database_staged(&path)
        .await
        .expect_err("checksum mismatch must fail");
    assert_eq!(err.stage(), "database.migration");
    assert_eq!(err.into_source().missing_migration_version(), None);
}

#[tokio::test]
async fn legacy_initial_schema_checksum_is_aligned_without_losing_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");

    let db = init_database_staged(&path).await.unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES ('legacy-user', 'legacy-user', 'hash', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 2")
        .execute(db.pool())
        .await
        .unwrap();
    db.close().await;

    let db = init_database_staged(&path)
        .await
        .expect("known legacy initial checksum must be aligned");
    let username: String = sqlx::query_scalar("SELECT username FROM users WHERE id = 'legacy-user'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(username, "legacy-user");

    let checksum: Vec<u8> = sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 2")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_ne!(checksum, LEGACY_NORMALIZE_SCHEMA_CHECKSUM);
    db.close().await;
}

#[tokio::test]
async fn partially_aligned_legacy_database_resumes_from_migration_two() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");

    let db = init_database_staged(&path).await.unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 2")
        .bind(LEGACY_NORMALIZE_SCHEMA_CHECKSUM.to_vec())
        .execute(db.pool())
        .await
        .unwrap();
    db.close().await;

    let db = init_database_staged(&path)
        .await
        .expect("partially aligned legacy database must resume at migration 2");
    let checksum: Vec<u8> = sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 2")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_ne!(checksum, LEGACY_NORMALIZE_SCHEMA_CHECKSUM);
    db.close().await;
}

#[tokio::test]
async fn unknown_initial_schema_checksum_remains_a_migration_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saydone.db");

    let db = init_database_staged(&path).await.unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 1")
        .execute(db.pool())
        .await
        .unwrap();
    db.close().await;

    let err = init_database_staged(&path)
        .await
        .expect_err("unknown migration 001 checksum must fail");
    assert_eq!(err.stage(), "database.migration");
}

#[test]
fn latest_known_migration_version_is_present_and_sane() {
    let version = latest_known_migration_version().expect("embedded migrations must not be empty");
    // 037 is the newest migration at the time this test was written; the
    // constant only moves forward.
    assert!(version >= 37, "unexpectedly low migration version: {version}");
    assert!(version < FUTURE_MIGRATION_VERSION);
}
