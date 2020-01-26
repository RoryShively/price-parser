use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;

use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename(deserialize = "Date"))]
    #[serde(with = "parse_csv_date")]
    timestamp: DateTime<Utc>,

    #[serde(rename(deserialize = "Symbol"))]
    symbol: String,

    #[serde(rename(deserialize = "Open"))]
    open: f64,

    #[serde(rename(deserialize = "High"))]
    high: f64,

    #[serde(rename(deserialize = "Low"))]
    low: f64,

    #[serde(rename(deserialize = "Close"))]
    close: f64,

    #[serde(rename(deserialize = "Volume BTC"))]
    volume_btc: f64,

    #[serde(rename(deserialize = "Volume USD"))]
    volume_usd: f64,

    // Enhanced data
    #[serde(default)]
    avg_50_day: f64,

    #[serde(default)]
    avg_100_day: f64,
}

mod parse_csv_date {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %I:%M:%S-%p";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        // insert minutes
        s.insert_str(13, ":00:00");

        let naive_dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom);
        let utc_dt = Utc.from_utc_datetime(&resp?);
        return Ok(utc_dt);
    }
}

struct Pipeline<'a> {
    data: VecDeque<&'a mut Record>,
}

impl<'a> Pipeline<'a> {
    pub fn new() -> Pipeline<'a> {
        Pipeline {
            data: VecDeque::new(),
        }
    }

    fn add_record(mut self, r: &'a mut Record) {
        self.data.push_back(r);
    }
}

fn main() -> std::io::Result<()> {
    println!("Starting price parser");

    let mut pipeline = Pipeline::new();

    let price_data_filename = "/home/kobe/Dev/data/Coinbase_BTCUSD_1h.csv";
    let file = File::open(price_data_filename)?;
    let buf_reader = BufReader::new(file);

    let mut csv_reader = csv::Reader::from_reader(buf_reader);
    for result in csv_reader.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
        pipeline.add_record(&mut record);
    }

    Ok(())
}
