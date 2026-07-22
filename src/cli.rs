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
    // Keep help self-contained: no dotenv / log / network side effects.
    let usage = format!(
        "\
{bin_name} - Polygon mainnet MEV arbitrage runtime

Usage:
  {bin_name}
  {bin_name} -h | --help

Options:
  -h, --help    Show this help and exit

Config:
  Loads `.env` (or DOTENV_PATH) then process env. See .env.example.
  Concurrent rpbot/tui processes are replaced on startup unless
  RPBOT_ALLOW_MULTIPLE is set. Logs: RPBOT_LOG / RPBOT_LOG_DIR.
"
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
