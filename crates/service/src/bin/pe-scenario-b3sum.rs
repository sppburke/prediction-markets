//! Scenario-only stand-in for `b3sum` on standard input, the only form the docs/29 audit calls.
use std::io::{self, Read, Write};

fn main() -> io::Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 65536];
    let mut input = io::stdin().lock();
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    writeln!(io::stdout().lock(), "{}  -", hasher.finalize().to_hex())
}
