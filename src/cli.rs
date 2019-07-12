use avro_rs::types::Value;
use avro_rs::{Codec, Reader};
use glob::glob;
use std::fs;
use std::path::PathBuf;
use crate::avro_value::AvroValue;
pub(crate) const CODEC_DEFLATE: &'static str = "deflate";

#[derive(Debug)]
pub(crate) struct AvroFile {
    data: Vec<u8>,
    path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct CliService {
    files: Vec<AvroFile>,
}

#[derive(Debug, Clone)]
pub(crate) struct AvroColumnarValue {
    name: String,
    value: AvroValue
}

impl AvroColumnarValue {
    pub fn from(name: String, value: AvroValue) -> Self {
        AvroColumnarValue { name, value }
    }

    pub fn value(&self) -> &AvroValue {
        &self.value
    }
}

impl CliService {
    /// Creates an `Avro` as a union of all avros in the received paths
    ///
    /// # Arguments
    ///
    /// * `path` - A glob to match against Avro files to load
    /// * `codec` - A codec for decompression
    pub fn from(path: String, codec: Option<String>) -> Self {
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in glob(&path).expect("Failed to read glob pattern") {
            match entry {
                Ok(p) => paths.push(p),
                Err(e) => panic!("{:?}", e),
            }
        }

        if paths.len() == 0 {
            panic!("No files found");
        }

        // TODO: Add `Codec::Snappy`
        let mut codec_for_decompressing: Codec = Codec::Null;
        if let Some(c) = codec {
            if c == CODEC_DEFLATE {
                codec_for_decompressing = Codec::Deflate;
            }
        }

        let mut files: Vec<AvroFile> = Vec::new();
        for path in paths {
            let mut data =
                fs::read(&path).expect(&format!("Could not read from path {0}", path.display()));
            codec_for_decompressing.decompress(&mut data).expect("Could not successfully decompress Avro file. Make sure that the codec you specified is correct");
            files.push(AvroFile { data, path });
        }

        CliService { files }
    }

    /// Get all the names of the columns.
    /// Relies on the first record
    pub fn get_all_field_names(&self) -> Vec<String> {
        let first_file = &self.files[0];
        let mut reader = Reader::new(&first_file.data[..]).expect(&format!(
            "Could not read Avro file {}",
            first_file.path.display()
        ));
        if let Ok(Value::Record(fields)) = reader
            .next()
            .expect("Avro must have at least one record row to infer schema")
        {
            fields
                .iter()
                .map(|(f, _)| f.to_owned())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        }
    }

    /// Get all columns and values
    /// 
    /// # Arguments
    /// * `fields_to_get` - Names of the columns to retrieve
    /// * `take` - Number of rows to take
    pub fn get_fields(&self, fields_to_get: Vec<String>, take: Option<u32>) -> Vec<Vec<AvroColumnarValue>> {
        let mut extracted_fields = Vec::new();
        for file in &self.files {
            let reader = Reader::new(&file.data[..])
                .expect(&format!("Could not read Avro file {}", file.path.display()));

            for (i, row) in reader.enumerate() {
                if extracted_fields.len() as u32 >= take.unwrap_or(u32::max_value()) {
                    break;
                }

                let row = row.expect(&format!("Could not parse row {} from the Avro", i));
                if let Value::Record(fields) = row {
                    let mut extracted_fields_for_row = Vec::new();
                    for field_name in &fields_to_get {
                        let field_value_to_insert =
                            match fields.iter().find(|(n, _)| n == field_name) {
                                Some((field_name, field_value)) => {
                                    let v = field_value.clone();
                                    AvroColumnarValue::from(field_name.to_owned(), AvroValue::from(v))
                                },
                                None => AvroColumnarValue::from(field_name.to_owned(), AvroValue::na())
                            };
                        extracted_fields_for_row.push(field_value_to_insert);
                    }
                    extracted_fields.push(extracted_fields_for_row);
                }
            }
        }
        extracted_fields
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_get_all_field_names() {
        println!("asdas");
        let path_to_test_avro = Path::new("./test_assets/bttf.avro").to_str().unwrap().to_owned();
        let cli = CliService::from(path_to_test_avro, None);
        let field_names = cli.get_all_field_names();
        assert_eq!(field_names, vec!["firstName", "lastName", "age"]);
    }

    #[test]
    fn test_get_fields() {
        println!("asdas");
        let path_to_test_avro = Path::new("./test_assets/bttf.avro").to_str().unwrap().to_owned();
        let _cli = CliService::from(path_to_test_avro, None);
        // let field_names = cli.get_fields(vec!["firstName", "age"], None);
        // assert_eq!(field_names, vec!["firstName", "lastName", "age"]);
    }
}
