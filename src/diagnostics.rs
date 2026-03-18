use serde_json::Value;

use crate::adapters::HttpMethod;

const MAX_PREVIEW_CHARS: usize = 1_024;

pub(crate) fn preview_text(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = compact.chars();
    let preview: String = chars.by_ref().take(MAX_PREVIEW_CHARS).collect();
    if chars.next().is_some() {
        format!("{preview}...<truncated>")
    } else {
        preview
    }
}

pub(crate) fn preview_json(value: &Value) -> String {
    preview_text(&value.to_string())
}

pub(crate) fn preview_optional_json(value: Option<&Value>) -> String {
    value
        .map(preview_json)
        .unwrap_or_else(|| "null".to_string())
}

pub(crate) fn http_method_name(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
    }
}

#[cfg(test)]
mod tests {
    use super::preview_text;

    #[test]
    fn preview_text_compacts_whitespace_and_truncates() {
        let raw = format!("  one \n two\tthree {} ", "x".repeat(1_100));
        let preview = preview_text(&raw);

        assert!(preview.starts_with("one two three"));
        assert!(preview.ends_with("...<truncated>"));
        assert!(preview.len() < raw.len());
    }
}
