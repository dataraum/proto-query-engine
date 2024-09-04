mod opfs_store;
pub mod utils;

use datafusion::arrow::array::RecordBatchWriter;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use js_sys::Uint8Array;
use once_cell::sync::Lazy;
use opfs_store::OpfsFileSystem;
use std::sync::Arc;
use std::sync::OnceLock;
use url::Url;
use wasm_bindgen::prelude::*;

fn _opfs_url() -> &'static Box<Url> {
    static OPFS_PREFIX: OnceLock<Box<Url>> = OnceLock::new();
    OPFS_PREFIX.get_or_init(|| Box::new(Url::parse("opfs://").unwrap()))
}

static CTX: Lazy<SessionContext> = Lazy::new(|| {
    let ctx = SessionContext::new();
    let opfs_store: OpfsFileSystem = OpfsFileSystem::new();
    ctx.register_object_store(_opfs_url().as_ref(), Arc::new(opfs_store));
    ctx
});

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub async fn unegister_table(table_name: String) -> Result<(), JsError> {
    let table_ref = TableReference::from(table_name);
    CTX.deregister_table(table_ref).unwrap();
    Ok(())
}

#[wasm_bindgen]
pub async fn register_csv(file_name: String, table_name: String) -> Result<(), JsError> {
    // TODO: convert to ipc during import https://stackoverflow.com/questions/76566489/convert-csv-to-apache-arrow-in-rust
    let ctx = &CTX;
    let table_ref = TableReference::from(table_name.clone());
    if !ctx.table_exist(table_ref).unwrap() {
        let mut register_path = "opfs:///".to_owned();
        register_path.push_str(&file_name.as_str());
        // register CSV as table
        ctx.register_csv(
            &table_name.as_str(),
            register_path.as_str(),
            CsvReadOptions::new(),
        )
        .await?;
    }
    Ok(())
}

#[wasm_bindgen]
pub async fn run_sql(sql_query: String) -> Result<JsValue, JsError> {
    // create a plan to run a SQL query
    let df = CTX.sql(&sql_query.as_str()).await?;
    let schema = Schema::from(df.schema());
    // execute the plan and collect the results as Vec<RecordBatch>
    let results: Vec<RecordBatch> = df.collect().await?;

    // serialize to in memory vector
    let mut output: Vec<u8> = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema).unwrap();
    for batch in results {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let js_arr = Uint8Array::from(&output[..]);
    Ok(JsValue::from(&js_arr))
}
