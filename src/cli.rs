use std::io::{self, Write};

pub fn help_requested<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    args.into_iter()
        .skip(1)
        .any(|arg| matches!(arg.as_ref(), "-h" | "--help"))
}

pub fn print_help(bin_name: &str) -> io::Result<()> {
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let usage = format!(
        "{bin_name} - Polygon arbitrage runtime\n\nUsage: {bin_name}\n\nOptions:\n  -h, --help    Show this help and exit\n"
    );
    stdout.write_all(usage.as_bytes())?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::{help_requested, print_help};

    #[test]
    fn help_flag_is_detected_without_starting_runtime() {
        assert!(help_requested(["rpbot", "--help"]));
        assert!(help_requested(["rpbot", "-h"]));
        assert!(!help_requested(["rpbot"]));
        assert!(help_requested(["tui", "--help"]));
    }

    #[test]
    fn print_help_flushes_usage_line() {
        print_help("rpbot").expect("help text should write");
    }
}
