#![warn(clippy::unwrap_used)]
#![doc = include_str!("../README.md")]

mod parser;

use std::fs;
use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use clap::Parser;
use color_eyre::eyre::Context;
use color_eyre::Result;
use parser::IteratorExt;
use rust_decimal::Decimal;
use serde::Serialize;

/// Converts a Sparkassen Danmark PDF export file to CSV
#[derive(Debug, Parser)]
struct Args {
    /// A Sparkassen Danmark PDF export file
    input: PathBuf,
    /// CSV destination file
    output: PathBuf,
    /// Prints the raw the lines found in the PDF
    #[arg(long)]
    print_lines: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Posting {
    pub date: NaiveDate,
    pub description: String,
    pub amount: Decimal,
    pub balance: Decimal,
    pub currency: String,
}

fn write_to_csv(postings: impl Iterator<Item = Result<Posting>>, file: &Path) -> Result<()> {
    let mut writer = csv::Writer::from_path(file)?;
    for posting in postings {
        let posting = posting?;
        writer.serialize(&posting)?;
    }
    writer.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let Args {
        input,
        output,
        print_lines,
    } = Args::parse();

    let text = pdf_extract::extract_text_from_mem(
        &fs::read(&input)
            .with_context(|| format!("Could not read input file {input:?}"))
            .with_context(|| format!("Could not extract PDF content from file {input:?}"))?,
    )?;
    let postings = text
        .lines()
        .inspect(|line| {
            if print_lines {
                println!("{line}");
            }
        })
        .postings();
    write_to_csv(postings, output.as_path())
}
