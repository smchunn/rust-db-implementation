// src/main.rs

use std::io;
use rsql::{Table, run_repl};

fn main() {
    let mut table = Table::new();
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout();
    run_repl(&mut table, &mut stdin, &mut stdout);
}
