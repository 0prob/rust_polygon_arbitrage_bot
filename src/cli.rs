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

pub fn print_help() -> io::Result<()> {
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    stdout.write_all(
        b"rpbot - Polygon arbitrage runtime\n\nUsage: rpbot\n\nOptions:\n  -h, --help    Show this help and exit\n",
    )
}

#[cfg(test)]
mod tests {
    use super::help_requested;

    #[test]
    fn help_flag_is_detected_without_starting_runtime() {
        assert!(help_requested(["rpbot", "--help"]));
        assert!(help_requested(["rpbot", "-h"]));
        assert!(!help_requested(["rpbot"]));
    }
}
