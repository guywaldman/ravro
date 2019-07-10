///! # rargo
///!
///! A CLI for manipulating [AVRO](https://avro.apache.org/) files.
///!
///! This crate currently expects each line to be a [Record](https://avro.apache.org/docs/1.8.1/spec.html#schema_record).
use avro_cli::AvroCli;
use failure::Error;
use prettytable::{color, Attr, Cell, Row, Table};
use regex::Regex;
use structopt::StructOpt;

mod avro_cli;

#[derive(StructOpt, Debug)]
#[structopt(name = "ravro")]
enum RavroArgs {
    #[structopt(name = "get")]
    /// Get fields from an Avro file
    Get {
        /// Names of the fields to get to get
        #[structopt(short = "f", long = "fields")]
        fields_to_get: Vec<String>,

        /// Files to process
        #[structopt(short = "p", long = "path")]
        path: String,

        /// Codec to uncompress with.
        /// Can be omitted or "deflate"
        #[structopt(short = "c", long = "codec")]
        codec: Option<String>,

        /// Regex to search. Only a row with a matching field will appear in the outputted table
        #[structopt(short = "s", long = "search")]
        search: Option<String>,
    },
}

fn main() -> Result<(), Error> {
    match RavroArgs::from_args() {
        RavroArgs::Get {
            fields_to_get,
            path,
            search,
            codec,
        } => {
            let avro = AvroCli::from(path, codec);
            let fields_to_get = if fields_to_get.is_empty() {
                avro.get_all_field_names()
            } else {
                fields_to_get
            };

            let mut table = Table::new();

            let header_cells: Vec<Cell> = fields_to_get
                .iter()
                .map(|f| {
                    Cell::new(f)
                        .with_style(Attr::Bold)
                        .with_style(Attr::ForegroundColor(color::BLUE))
                        .with_style(Attr::Underline(true))
                })
                .collect();
            table.add_row(Row::new(header_cells));

            let rows = avro.get_fields(fields_to_get);
            let filtered_rows: Vec<Vec<String>> = rows
                .into_iter()
                .filter(|r| {
                    r.iter()
                        .find(|v| match &search {
                            None => true,
                            Some(search) => {
                                let search =
                                    Regex::new(&search).expect("Regular expression is invalid");
                                search.is_match(v)
                            }
                        })
                        .is_some()
                })
                .collect();
            for fields_for_row in filtered_rows {
                let row_cells: Vec<Cell> = fields_for_row
                    .iter()
                    .filter_map(|v| {
                        let mut cell = Cell::new(v);
                        if let Some(search) = &search {
                            let search =
                                Regex::new(&search).expect("Regular expression is invalid");
                            if search.is_match(v) {
                                cell.style(Attr::Bold);
                                cell.style(Attr::ForegroundColor(color::GREEN));
                            }
                        }

                        if v == avro_cli::NULL {
                            cell.style(Attr::ForegroundColor(color::RED));
                        } else if v == avro_cli::NA {
                            cell.style(Attr::ForegroundColor(color::BRIGHT_RED));
                        }

                        Some(cell)
                    })
                    .collect();
                table.add_row(Row::new(row_cells));
            }

            table.printstd();
        }
    }

    Ok(())
}
