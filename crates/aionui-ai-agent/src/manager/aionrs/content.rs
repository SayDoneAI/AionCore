use std::path::Path;

use aion_types::message::{ContentBlock, ImageUrl, extension_to_image_media_type};
use aionui_common::constants::AIONUI_FILES_MARKER;
use base64::Engine;
use tracing::warn;

const ATTACHED_FILES_HEADER: &str = "[Attached files]";
const MAX_INLINE_IMAGE_BYTES: u64 = 5 * 1024 * 1024;

/// Build provider-independent user input from the message and its attachments.
///
/// Image attachments are represented as native data URI blocks so vision-capable
/// providers receive the actual image bytes. Non-image and degraded attachments
/// remain local paths in the text block for workspace tools such as `ViewImage`.
pub(super) fn build_content_blocks(content: &str, files: &[String]) -> Vec<ContentBlock> {
    let mut text = strip_attachment_metadata(content, files).trim().to_owned();
    let mut image_blocks = Vec::new();
    for file_path in files {
        if let Some(block) = load_image_block(file_path) {
            image_blocks.push(block);
        }
    }

    if !files.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(ATTACHED_FILES_HEADER);
        for file_path in files {
            text.push('\n');
            text.push_str(file_path);
        }
    }

    let mut blocks = Vec::with_capacity((if text.is_empty() { 0 } else { 1 }) + image_blocks.len());
    if !text.is_empty() {
        blocks.push(ContentBlock::Text { text });
    }
    blocks.extend(image_blocks);
    blocks
}

fn load_image_block(file_path: &str) -> Option<ContentBlock> {
    let media_type = extension_to_image_media_type(Path::new(file_path).extension()?.to_str()?)?;
    let metadata = std::fs::metadata(file_path).ok()?;
    if !metadata.is_file() {
        return None;
    }
    if metadata.len() > MAX_INLINE_IMAGE_BYTES {
        warn!(path = %file_path, bytes = metadata.len(), "image attachment exceeds inline size limit; keeping path");
        return None;
    }
    let bytes = match std::fs::read(file_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(path = %file_path, error = %error, "image attachment read failed; keeping path");
            return None;
        }
    };
    if bytes.len() as u64 > MAX_INLINE_IMAGE_BYTES {
        warn!(path = %file_path, bytes = bytes.len(), "image attachment grew past inline size limit; keeping path");
        return None;
    }
    let detected_media_type = detect_image_media_type(&bytes)?;
    if detected_media_type != media_type {
        warn!(
            path = %file_path,
            extension_media_type = media_type,
            detected_media_type,
            "image attachment type does not match extension; keeping path"
        );
        return None;
    }

    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    Some(ContentBlock::Image {
        image_url: ImageUrl {
            url: format!("data:{detected_media_type};base64,{encoded}"),
        },
    })
}

fn detect_image_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn strip_attachment_metadata<'a>(content: &'a str, files: &[String]) -> &'a str {
    if files.is_empty() {
        return content;
    }
    let Some((user_text, metadata)) = content.rsplit_once(AIONUI_FILES_MARKER) else {
        return content;
    };
    let metadata_files = metadata.lines().map(str::trim).filter(|line| !line.is_empty());
    if metadata_files.eq(files.iter().map(String::as_str)) {
        user_text
    } else {
        content
    }
}

#[cfg(test)]
#[path = "content_test.rs"]
mod content_test;
