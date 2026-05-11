//! `pe-operator-audit` — retrospective audit of operator clustering vs sweep fills.
//!
//! Thin shim around `pe_bootstrap::operator_audit::run`. The implementation
//! lives in the library module so integration tests under
//! `crates/bootstrap/tests/` can exercise the full pipeline without
//! spawning a subprocess. See `pe_bootstrap::operator_audit` for the
//! retrospective-vs-walk-forward framing and the anti-gaming caveat.

use std::io::{self, Write};
use std::process::ExitCode;

use pe_bootstrap::operator_audit::{HELP_TEXT, parse_args, run};

fn main() -> ExitCode {
    let args_in: Vec<String> = std::env::args().collect();
    let parsed = match parse_args(args_in) {
        Ok(Some(args)) => args,
        Ok(None) => {
            // --help branch: stdout + exit 0.
            print!("{HELP_TEXT}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            let _ = writeln!(io::stderr(), "error: {e:#}\n\n{HELP_TEXT}");
            return ExitCode::from(2);
        }
    };

    match run(parsed) {
        Ok(output) => {
            let _ = io::stdout().write_all(output.markdown.as_bytes());
            ExitCode::SUCCESS
        }
        Err(e) => {
            let _ = writeln!(io::stderr(), "audit failed: {e:#}");
            ExitCode::FAILURE
        }
    }
}
