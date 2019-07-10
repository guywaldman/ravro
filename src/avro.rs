use avro_rs::types::Value;
use avro_rs::{Codec, Reader};
use std::fs;
use glob::glob;
use std::path::PathBuf;

pub(crate) const NULL: &'static str = "null";
pub(crate) const NA: &'static str = "N/A";
pub(crate) const CODEC_DEFLATE: &'static str = "deflate";

#[derive(Debug)]
pub(crate) struct AvroFile {
    data: Vec<u8>,
    path: PathBuf
}

#[derive(Debug)]
pub(crate) struct Avro {
    files: Vec<AvroFile>
}

impl Avro {
    /// Creates an `Avro` as a union of all avros in the received paths
    /// 
    /// # Arguments
    ///
    /// * `path` - A glob to match against Avro files to load
    pub fn from(path: String, codec: Option<String>) -> Self {
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in glob(&path).expect("Failed to read glob pattern") {
            match entry {
                Ok(p) => paths.push(p),
                Err(e) => panic!("{:?}", e),
            }
        }

        if paths.len() == 0 {
            panic!("No files found")
        }

        let mut codec_for_decompressing: Codec = Codec::Null;
        // TODO: Add `Codec::Snappy`
        if let Some(c) = codec {
            if c == CODEC_DEFLATE {
                codec_for_decompressing = Codec::Deflate;
            }
        }
        
        let mut files: Vec<AvroFile> = Vec::new();
        for path in paths {
            let mut data = fs::read(&path).expect(&format!(
                    "Could not read from path {0}", path.display())
                );
            codec_for_decompressing.decompress(&mut data).expect("Could not successfully decompress Avro file. Make sure that the codec you specified is correct");
            files.push(AvroFile { data, path });
        }

        Avro { files }
    }

    pub fn get_all_field_names(&self) -> Vec<String> {
        let first_file = &self.files[0];
        let mut reader = Reader::new(&first_file.data[..]).expect(&format!("Could not read Avro file {}", first_file.path.display()));
        if let Ok(Value::Record(fields)) = reader.next().expect("Avro must have at least one record row to infer schema") {
            fields.iter().map(|(f, _)| f.to_owned()).collect::<Vec<String>>()
        } else {
            Vec::new()
        }
    }

    pub fn get_fields(&self, fields_to_get: Vec<String>) -> Vec<Vec<String>> {
        let mut extracted_fields: Vec<Vec<String>> = Vec::new();
        for file in &self.files {
            let reader = Reader::new(&file.data[..]).expect(&format!("Could not read Avro file {}", file.path.display()));

            for (i, row) in reader.enumerate() {
                let row = row.expect(&format!("Could not parse row {} from the Avro", i));
                if let Value::Record(fields) = row {
                    let mut extracted_fields_for_row: Vec<String> = Vec::new();
                    for field_name in &fields_to_get {
                        let field_value_to_insert = match fields.iter().find(|(n, _)| n == field_name) {
                            Some((_, val)) => format_avro_value(&val),
                            None => NA.to_owned()
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

pub(crate) fn format_avro_value(value: &Value) -> String {
    match value {
        Value::Array(a) => format!(
            "{}",
            a.iter()
                .map(|v| format_avro_value(v))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Bytes(b) => format!(
            "{}",
            b.iter()
                .map(|n| format!("{}", n))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Boolean(b) => format!("{}", b),
        Value::Double(d) => format!("{}", d),
        Value::Enum(id, desc) => format!("{} ({})", id, desc),
        Value::Fixed(_, f) => format!(
            "{}",
            f.iter()
                .map(|n| format!("{}", n))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Float(f) => format!("{}", f),
        Value::Int(i) => format!("{}", i),
        Value::Long(l) => format!("{}", l),
        Value::Map(m) => format!(
            "{}",
            m.iter()
                .map(|(k, v)| format!("{}: {}", k, format_avro_value(v)))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::Null => NULL.to_owned(),
        Value::Record(m) => format!(
            "{}",
            m.iter()
                .map(|(k, v)| format!("{}: {}", k, format_avro_value(v)))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        Value::String(s) => s.clone(),
        Value::Union(u) => format_avro_value(&*u),
    }
}
