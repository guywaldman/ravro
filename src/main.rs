///! # rargo
///!
///! A CLI for manipulating [AVRO](https://avro.apache.org/) files.
///!
///! This crate currently expects each line to be a [Record](https://avro.apache.org/docs/1.8.1/spec.html#schema_record).
use failure::Error;
use prettytable::{color, Attr, Cell, Row, Table};
use regex::Regex;
use structopt::StructOpt;
use cli::{CliService, AvroColumnarValue, AvroData};
use avro_value::AvroValue;

mod avro_value;
mod cli;

#[derive(StructOpt, Debug)]
#[structopt(name = "ravro")]
enum RavroArgs {
    #[structopt(name = "get")]
    /// Get fields from an Avro file
    Get {
        /// Files to process
        path: String,

        /// Names of the fields to get to get
        #[structopt(short = "f", long = "fields")]
        fields_to_get: Vec<String>,

        /// Codec to uncompress with.
        /// Can be omitted or "deflate"
        #[structopt(short = "c", long = "codec")]
        codec: Option<String>,

        /// Regex to search. Only a row with a matching field will appear in the outputted table
        #[structopt(short = "s", long = "search")]
        search: Option<String>,

        /// Maximum number of records to show
        #[structopt(short = "t", long = "take")]
        take: Option<u32>,

        /// Output format.
        /// Omit for pretty table output, or specify: "csv"
        #[structopt(short = "p", long = "format")]
        output_format: Option<String>
    },
}

fn main() -> Result<(), Error> {
    match RavroArgs::from_args() {
        RavroArgs::Get {
            fields_to_get,
            path,
            search,
            codec,
            take,
            output_format
        } => {
            let avro = CliService::from(path, codec);
            let fields_to_get = if fields_to_get.is_empty() {
                avro.get_all_field_names()
            } else {
                fields_to_get
            };

            let data = avro.get_fields(&fields_to_get, take);

            match output_format {
                None => print_as_table(&fields_to_get, data, search),
                Some(format_option) => match format_option.as_ref() {
                    "csv" => print_as_csv(&fields_to_get, data).expect("Could not print Avro as CSV"),
                    _ => panic!("Output format not recognized")
                }
            }
        }
    }

    Ok(())
}

fn print_as_table(field_names: &[String], data: AvroData, search: Option<String>) {
    let mut table = Table::new();

    let header_cells: Vec<Cell> = field_names
        .iter()
        .map(|f| {
            Cell::new(f)
                .with_style(Attr::Bold)
                .with_style(Attr::ForegroundColor(color::BLUE))
                .with_style(Attr::Underline(true))
        })
        .collect();
    table.add_row(Row::new(header_cells));

    let filtered_data: AvroData = data
        .into_iter()
        .filter(|r| {
            r.iter()
                .find(|v| match &search {
                    None => true,
                    Some(search) => {
                        let search =
                            Regex::new(&search).expect("Regular expression is invalid");
                        search.is_match(&v.value().to_string())
                    }
                })
                .is_some()
        })
        .collect();

    for fields_for_row in filtered_data {
        let row_cells: Vec<Cell> = fields_for_row
            .iter()
            .filter_map(|v: &AvroColumnarValue| {
                let value_str = v.value().to_string();
                let mut cell = Cell::new(&value_str);
                if let Some(search) = &search {
                    let search =
                        Regex::new(&search).expect("Regular expression is invalid");
                    if search.is_match(&value_str) {
                        cell.style(Attr::Bold);
                        cell.style(Attr::ForegroundColor(color::GREEN));
                    }
                }

                match v.value() {
                    AvroValue::Na => cell.style(Attr::ForegroundColor(color::RED)),
                    _ => {}
                }

                Some(cell)
            })
            .collect();
        table.add_row(Row::new(row_cells));
    }

    table.printstd();
}

fn print_as_csv(field_names: &[String], data: AvroData) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_writer = csv::Writer::from_writer(std::io::stdout());

    // Headers
    csv_writer.write_record(field_names)?;

    for row in data {
        csv_writer.write_record(row.iter().map(|val: &AvroColumnarValue| val.value().to_string()).collect::<Vec<String>>())?;
    }

    csv_writer.flush()?;
    Ok(())
}