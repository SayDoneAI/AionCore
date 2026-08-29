use std::fs;

use aion_types::message::ContentBlock;
use aionui_common::constants::AIONUI_FILES_MARKER;
use tempfile::NamedTempFile;

use super::build_content_blocks;

#[test]
fn converts_supported_image_to_native_block() {
    let image_file = NamedTempFile::with_suffix(".png").unwrap();
    fs::write(image_file.path(), b"\x89PNG\r\n\x1a\n").unwrap();
    let image_path = image_file.path().to_string_lossy().into_owned();
    let content = format!("look at this\n\n{AIONUI_FILES_MARKER}\n{image_path}");

    let blocks = build_content_blocks(&content, std::slice::from_ref(&image_path));

    assert_eq!(blocks.len(), 2);
    assert!(matches!(
        &blocks[0],
        ContentBlock::Text { text }
            if text == &format!("look at this\n\n[Attached files]\n{image_path}")
    ));
    assert!(matches!(
        &blocks[1],
        ContentBlock::Image { image_url }
            if image_url.url == "data:image/png;base64,iVBORw0KGgo="
    ));
}

#[test]
fn keeps_extension_only_image_as_path() {
    let image_file = NamedTempFile::with_suffix(".png").unwrap();
    fs::write(image_file.path(), b"not an image").unwrap();
    let image_path = image_file.path().to_string_lossy().into_owned();

    let blocks = build_content_blocks("inspect", std::slice::from_ref(&image_path));

    assert_eq!(blocks.len(), 1);
    assert!(matches!(
        &blocks[0],
        ContentBlock::Text { text }
            if text == &format!("inspect\n\n[Attached files]\n{image_path}")
    ));
}

#[test]
fn keeps_missing_and_non_image_files_as_paths() {
    let files = vec!["/tmp/missing-image.png".to_owned(), "/tmp/notes.txt".to_owned()];

    let blocks = build_content_blocks("see attachments", &files);

    assert!(matches!(
        &blocks[0],
        ContentBlock::Text { text }
            if text == "see attachments\n\n[Attached files]\n/tmp/missing-image.png\n/tmp/notes.txt"
    ));
    assert_eq!(blocks.len(), 1);
}

#[test]
fn preserves_literal_marker_when_suffix_does_not_match_files() {
    let literal = format!("discuss {AIONUI_FILES_MARKER}\nnot-the-attached-path");

    let blocks = build_content_blocks(&literal, &["/tmp/image.png".to_owned()]);

    assert!(matches!(
        &blocks[0],
        ContentBlock::Text { text }
            if text.starts_with(&literal) && text.ends_with("[Attached files]\n/tmp/image.png")
    ));
}

#[test]
fn appends_all_authoritative_attachment_paths() {
    let files = vec!["/tmp/notes.txt".to_owned(), "/tmp/missing-image.png".to_owned()];

    let blocks = build_content_blocks("see attachments", &files);

    assert!(matches!(
        &blocks[0],
        ContentBlock::Text { text }
            if text == "see attachments\n\n[Attached files]\n/tmp/notes.txt\n/tmp/missing-image.png"
    ));
}
