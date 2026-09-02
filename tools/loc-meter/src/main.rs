//! `loc-meter`: the canonical LOC meter (M2-K18). See `tools/loc-meter/README.md`.

use std::path::PathBuf;
use std::process::ExitCode;

use loc_meter::{Category, MeterError, Options, Report};

const USAGE: &str = "usage: loc-meter [--base <ref>] [--head <ref>] [--files]\n\
  --base   integration branch, measured from merge-base(base, head) (default: main)\n\
  --head   commit measured (default: HEAD)\n\
  --files  list every billed row\n\
exit 0 measured, 2 refused (dirty tree, unresolved module), 1 failed";

fn main() -> ExitCode {
    let mut base = "main".to_string();
    let mut head = "HEAD".to_string();
    let mut files = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--base" => base = args.next().unwrap_or_default(),
            "--head" => head = args.next().unwrap_or_default(),
            "--files" => files = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("loc-meter: unknown argument `{other}`\n{USAGE}");
                return ExitCode::FAILURE;
            }
        }
    }
    let repo = match loc_meter::toplevel(
        &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    ) {
        Ok(root) => PathBuf::from(root),
        Err(e) => {
            eprintln!("loc-meter: {e}");
            return ExitCode::FAILURE;
        }
    };
    match loc_meter::measure(&Options {
        repo,
        base: base.clone(),
        head: head.clone(),
    }) {
        Ok(report) => {
            print_report(&report, &base, &head, files);
            ExitCode::SUCCESS
        }
        Err(e @ (MeterError::Dirty(_) | MeterError::Unresolved { .. })) => {
            eprint!("loc-meter: {e}");
            ExitCode::from(2)
        }
        Err(e) => {
            eprintln!("loc-meter: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print_report(report: &Report, base: &str, head: &str, files: bool) {
    println!(
        "loc-meter: {} (merge-base of {base}) .. {} ({head})",
        &report.base[..12],
        &report.head[..12]
    );
    for category in Category::BUDGET {
        println!("{}", line(report, category, ""));
    }
    println!("outside every budget line:");
    for category in Category::EXCLUDED {
        println!("{}", line(report, category, "  "));
    }
    if files {
        println!("files:");
        for row in &report.files {
            println!(
                "  {:<11} +{:<5} -{:<5} {}",
                row.category.label(),
                row.delta.added,
                row.delta.deleted,
                row.display_path()
            );
        }
    }
}

fn line(report: &Report, category: Category, indent: &str) -> String {
    let total = report.total(category);
    format!(
        "{indent}{:<11} +{:<5} -{:<5} net {:<6} {}",
        category.label(),
        total.added,
        total.deleted,
        total.net(),
        category.describe()
    )
}
