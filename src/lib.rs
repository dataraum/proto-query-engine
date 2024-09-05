mod opfs_store;
pub mod web_fs_utils;

use datafusion::arrow::array::RecordBatchWriter;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use js_sys::Uint8Array;
use once_cell::sync::Lazy;
use opfs_store::OpfsFileSystem;
use web_fs_utils::cp_csv_to_arrow;
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
pub async fn load_csv_bytes(file_uint8: Uint8Array, file_digest: String, ) -> Result<(), JsError> {
    cp_csv_to_arrow(file_uint8, file_digest).await.unwrap();
    Ok(())
}

#[wasm_bindgen]
pub async fn register_table(file_digest: String, table_name: String) -> Result<(), JsError> { 
    let ctx = &CTX;
    let table_ref = TableReference::from(table_name.clone());
    if !ctx.table_exist(table_ref).unwrap() {
        let register_path = format!("opfs:///{file_digest}.arrow");
        // register as table
        ctx.register_arrow(
            &table_name.as_str(),
            register_path.as_str(),
            ArrowReadOptions::default(),
        )
        .await?;
    }
    Ok(())
}

#[wasm_bindgen]
pub async fn register_csv(file_digest: String, table_name: String) -> Result<(), JsError> {
    let ctx = &CTX;
    let table_ref = TableReference::from(table_name.clone());
    if !ctx.table_exist(table_ref).unwrap() {
        let register_path = format!("opfs:///{file_digest}.csv");
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

    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?.with_preserve_dict_id(false);
    let mut writer = StreamWriter::try_new_with_options(&mut output, &schema, options).unwrap();
    for batch in results {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let js_arr = Uint8Array::from(&output[..]);
    Ok(JsValue::from(&js_arr))
}
