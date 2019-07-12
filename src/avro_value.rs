use avro_rs::types::Value;
use std::fmt;

pub(crate) const NULL: &'static str = "null";
pub(crate) const NA: &'static str = "N/A";

#[derive(Debug, Clone)]
pub(crate) enum AvroValue {
    Value(Value),
    Na
}


impl<'a> AvroValue {
    pub fn from(value: Value) -> Self {
        AvroValue::Value(value)
    }

    pub fn na() -> Self {
        AvroValue::Na
    }

    pub fn to_string(&self) -> String {
        format!("{}", self)
    }
}

impl<'a> fmt::Display for AvroValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AvroValue::Value(v) => write!(f, "{}", format_avro_value(v)),
            AvroValue::Na => write!(f, "{}", NA)
        }
    }
}

fn format_avro_value(value: &Value) -> String {
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