//! Writes user_daily/ and campaign_daily/ as uncompressed Parquet, several files in parallel.

use crate::parse::S;
use crate::URow;
use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

pub struct CRow {
    pub id: S,
    pub channel: S,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

const USER_SCHEMA: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMP_SCHEMA: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(usize::MAX)
            .build(),
    )
}

fn write_file(path: &Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let file = BufWriter::with_capacity(1 << 20, File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display())));
    let mut w = SerializedFileWriter::new(file, schema, props()).expect("parquet writer");
    let mut rg = w.next_row_group().expect("row group");
    let mut it = cols.into_iter();
    while let Some(mut cw) = rg.next_column().expect("column") {
        match it.next().expect("column count") {
            Col::Bytes(v, def) => {
                cw.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).expect("write bytes");
            }
            Col::I32(v) => {
                cw.typed::<Int32Type>().write_batch(&v, None, None).expect("write i32");
            }
            Col::I64(v) => {
                cw.typed::<Int64Type>().write_batch(&v, None, None).expect("write i64");
            }
        }
        cw.close().expect("close column");
    }
    rg.close().expect("close row group");
    w.close().expect("close file");
}

#[inline]
fn ba(s: S) -> ByteArray {
    ByteArray::from(Bytes::from_static(s))
}

fn user_cols(rows: &[&URow], cats: &[S]) -> Vec<Col> {
    let mut tops = Vec::new();
    let mut def = Vec::with_capacity(rows.len());
    for r in rows {
        if r.top == u32::MAX {
            def.push(0);
        } else {
            def.push(1);
            tops.push(ba(cats[r.top as usize]));
        }
    }
    vec![
        Col::Bytes(rows.iter().map(|r| ba(r.name)).collect(), None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.events).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::Bytes(tops, Some(def)),
    ]
}

fn camp_cols(rows: &[CRow]) -> Vec<Col> {
    vec![
        Col::Bytes(rows.iter().map(|r| ba(r.id)).collect(), None),
        Col::Bytes(rows.iter().map(|r| ba(r.channel)).collect(), None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.purchasing).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::I64(rows.iter().map(|r| r.assisted).collect()),
    ]
}

pub fn write_all(out: &Path, users: Vec<Vec<URow>>, camps: Vec<CRow>, cats: &[S], threads: usize) {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let all: Vec<&URow> = users.iter().flat_map(|v| v.iter()).collect();
    let nfiles = threads.max(1).min(all.len().max(1).div_ceil(50_000)).max(1);
    let per = all.len().div_ceil(nfiles).max(1);
    let pieces: Vec<&[&URow]> = if all.is_empty() { vec![&all[..]] } else { all.chunks(per).collect() };
    rayon::join(
        || {
            pieces.par_iter().enumerate().for_each(|(i, rows)| {
                write_file(&ud.join(format!("part-{i:05}.parquet")), USER_SCHEMA, user_cols(rows, cats));
            })
        },
        || write_file(&cd.join("part-00000.parquet"), CAMP_SCHEMA, camp_cols(&camps)),
    );
}
