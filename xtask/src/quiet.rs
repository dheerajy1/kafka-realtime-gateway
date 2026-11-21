use std::io::{self, BufRead, BufReader, Write};
use std::process::Command;

// Which lines of the cargo output are shown besides the one-line progress
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Show {
    // Everything except the Compiling and Checking lines, so warnings and errors stay
    All,

    // Only the summary lines of a test run: Finished, Running, test result, FAILED, panicked, Doc-tests and errors
    TestSummary,
}

// Words of cargo that start a line saying which crate is being worked on
const PROGRESS_WORDS: [&str; 4] = ["Compiling", "Checking", "Documenting", "Building"];

// Words that keep a line in the test summary
const SUMMARY_WORDS: [&str; 6] = [
    "Finished",
    "Running",
    "test result",
    "FAILED",
    "panicked",
    "Doc-tests",
];

// Removes colour codes from a line and writes the plain text into the given buffer
fn strip_ansi_into(line: &str, plain: &mut String) {
    plain.clear();

    let mut chars = line.chars();

    while let Some(character) = chars.next() {
        if character == '\u{1b}' {
            // A colour code ends at its first letter
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(character);
        }
    }
}

// Removes the variables `cargo run` sets for xtask, so a nested cargo
// command sees the same environment as a direct cargo command
pub fn clean_cargo_env(command: &mut Command) {
    for (key, _) in std::env::vars_os() {
        let name = key.as_encoded_bytes();

        if name.starts_with(b"CARGO_PKG_")
            || name.starts_with(b"CARGO_MANIFEST_")
            || matches!(
                name,
                b"CARGO_CRATE_NAME" | b"CARGO_BIN_NAME" | b"CARGO_PRIMARY_PACKAGE"
            )
        {
            command.env_remove(&key);
        }
    }
}

// Returns the verb and the crate name, such as ("Compiling", "ring"), when the line is a progress line
fn progress_text(plain: &str) -> Option<(&str, &str)> {
    let mut words = plain.split_whitespace();
    let first = words.next()?;
    let name = words.next()?;

    PROGRESS_WORDS.contains(&first).then_some((first, name))
}

// True when a plain line is shown in the given mode
fn keep_line(plain: &str, show: Show) -> bool {
    match show {
        Show::All => true,
        Show::TestSummary => {
            !plain.contains("SIGTERM")
                && (SUMMARY_WORDS.iter().any(|word| plain.contains(word))
                    || plain.starts_with("error"))
        }
    }
}

// Runs a cargo command and shows one overwritten progress line instead of a line per crate
// Returns whether the command succeeded
pub fn run(mut command: Command, show: Show) -> io::Result<bool> {
    // Both cargo streams go into one pipe in order, colours stay on
    let (pipe_reader, pipe_writer) = io::pipe()?;

    command
        .env("CARGO_TERM_COLOR", "always")
        .stdout(pipe_writer.try_clone()?)
        .stderr(pipe_writer);

    clean_cargo_env(&mut command);

    let mut child = command.spawn()?;

    // The command still holds the write end of the pipe, so drop it here
    // or the read loop below never sees the end of the output
    drop(command);

    let mut out = io::stdout().lock();
    let mut progress_shown = false;

    let mut reader = BufReader::new(pipe_reader);
    let mut raw = Vec::new();
    let mut plain = String::new();

    loop {
        raw.clear();

        if reader.read_until(b'\n', &mut raw)? == 0 {
            break;
        }

        if raw.last() == Some(&b'\n') {
            raw.pop();
        }

        let line = String::from_utf8_lossy(&raw);
        strip_ansi_into(&line, &mut plain);

        if let Some((verb, name)) = progress_text(&plain) {
            // Clears the line and writes the new text without a newline
            write!(out, "\r\x1b[2K   {verb} {name}")?;
            out.flush()?;
            progress_shown = true;
        } else if keep_line(&plain, show) {
            if progress_shown {
                write!(out, "\r\x1b[2K")?;
                progress_shown = false;
            }

            writeln!(out, "{line}")?;
        }
    }

    if progress_shown {
        write!(out, "\r\x1b[2K")?;
    }

    out.flush()?;

    Ok(child.wait()?.success())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Colour codes are removed and the text stays
    #[test]
    fn strips_colour_codes() {
        let mut plain = String::new();

        strip_ansi_into(
            "\u{1b}[1m\u{1b}[92m   Compiling\u{1b}[0m ring v0.17.14",
            &mut plain,
        );

        assert_eq!(plain, "   Compiling ring v0.17.14");
    }

    // The buffer is cleared first, so a second line does not keep the text of the first
    #[test]
    fn strip_reuses_the_buffer() {
        let mut plain = String::new();

        strip_ansi_into("first line", &mut plain);
        strip_ansi_into("\u{1b}[0msecond", &mut plain);

        assert_eq!(plain, "second");
    }

    // A Compiling line gives the progress text with only the crate name
    #[test]
    fn reads_a_compiling_line() {
        assert_eq!(
            progress_text("   Compiling ring v0.17.14 (/path)"),
            Some(("Compiling", "ring"))
        );
        assert_eq!(
            progress_text("    Checking kafka-api-gateway v0.1.0"),
            Some(("Checking", "kafka-api-gateway"))
        );
    }

    // Other lines are not progress lines
    #[test]
    fn other_lines_are_not_progress() {
        assert_eq!(progress_text("    Finished `dev` profile in 1s"), None);
        assert_eq!(progress_text("warning: unused variable"), None);
        assert_eq!(progress_text(""), None);
    }

    // Show::All keeps every line that is not progress
    #[test]
    fn all_keeps_everything() {
        assert!(keep_line("warning: unused variable", Show::All));
        assert!(keep_line("    Finished `dev` profile", Show::All));
    }

    // The test summary keeps the summary lines and errors only
    #[test]
    fn test_summary_keeps_summary_lines() {
        assert!(keep_line("test result: ok. 3 passed", Show::TestSummary));
        assert!(keep_line(
            "     Running unittests src/lib.rs",
            Show::TestSummary
        ));
        assert!(keep_line(
            "thread 'a' panicked at src/lib.rs:1",
            Show::TestSummary
        ));
        assert!(keep_line(
            "error[E0382]: borrow of moved value",
            Show::TestSummary
        ));
        assert!(!keep_line("test auth::works ... ok", Show::TestSummary));
        assert!(!keep_line("warning: unused variable", Show::TestSummary));
    }

    // A line with SIGTERM is hidden in the test summary
    #[test]
    fn test_summary_hides_sigterm_lines() {
        assert!(!keep_line("Running stop test SIGTERM", Show::TestSummary));
    }
}
