/// Truncate to at most `max_chars` Unicode scalar values (not bytes).
pub fn truncate_str(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        s.to_string()
    } else {
        let take = max_chars.saturating_sub(1);
        format!("{}…", s.chars().take(take).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_respects_char_boundaries() {
        let s = "WMATIC →[Quick V2] TOKENX →[Uni V3] USDC";
        let out = truncate_str(s, 20);
        assert!(out.chars().count() <= 20);
        assert!(out.ends_with('…') || out.len() <= s.len());
    }
}
