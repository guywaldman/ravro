use avro_rs::types::Value;
use avro_rs::Reader;
use std::fs;
use std::path::PathBuf;

pub(crate) const NULL: &'static str = "null";
pub(crate) const NA: &'static str = "N/A";

#[derive(Debug)]
pub(crate) struct Avro {
    buf: Vec<u8>,
}

impl Avro {
    /// Creates an `Avro` as a union of all avros in the received paths
    pub fn from(paths: Vec<PathBuf>) -> Self {
        let mut buf: Vec<u8> = Vec::new();
        for path in &paths {
            buf.append(&mut fs::read(path).expect(&format!(
                "Could not read from path {0}",
                &path.to_str().unwrap()
            )));
        }
        Avro { buf }
    }

    pub fn get_all_field_names(&self) -> Vec<String> {
        let mut reader = Reader::new(&self.buf[..]).expect("Could not read joined Avro file");
        if let Ok(Value::Record(fields)) = reader.next().expect("Avro must have at least one record row to infer schema") {
            fields.iter().map(|(f, _)| f.to_owned()).collect::<Vec<String>>()
        } else {
            Vec::new()
        }
    }

    pub fn get_fields(&self, fields_to_get: Vec<String>) -> Vec<Vec<String>> {
        let reader = Reader::new(&self.buf[..]).expect("Could not read joined Avro file");

        let mut extracted_fields: Vec<Vec<String>> = Vec::new();
        for (i, row) in reader.enumerate() {
            let row = row.expect(&format!("Could not parse row {} from Avro", i));
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
