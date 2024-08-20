pub mod utils;
mod opfs;

use datafusion::arrow::array::RecordBatchWriter;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use js_sys::Uint8Array;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use opfs::OpfsFileSystem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use url::Url;
use wasm_bindgen::prelude::*;

fn _mem_url() -> &'static Box<Url> {
    static MEM_LOCATION: OnceLock<Box<Url>> = OnceLock::new();
    MEM_LOCATION.get_or_init(|| Box::new(Url::parse("opfs://").unwrap()))
}

static CTX: Lazy<Mutex<SessionContext>> = Lazy::new(|| {
    let ctx = SessionContext::new();
    let mem_store = InMemory::new();
    let opfs_store: OpfsFileSystem = OpfsFileSystem::new();
    ctx.register_object_store(_mem_url().as_ref(), Arc::new(opfs_store));
    Mutex::new(ctx)
});

fn _get_path(mem_name: &mut String) -> Path {
    mem_name.push_str(".csv");
    return Path::from(mem_name.clone().as_str());
}

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub async fn delete_table(file_name: String, table_name: String) -> Result<(), JsValue> {
    let ctx = CTX.lock().unwrap();
    let mem_store = ctx.runtime_env().object_store(_mem_url()).unwrap();
    let mut mem_name = file_name.to_owned();
    let path = _get_path(&mut mem_name);
    mem_store.delete(&path).await.unwrap();
    let table_ref = TableReference::from(table_name);
    let _ = ctx.deregister_table(table_ref);
    Ok(())
}

#[wasm_bindgen]
pub async fn has_table(table_name: String) -> Result<JsValue, JsValue> {
    let ctx = CTX.lock().unwrap();
    let table_ref = TableReference::from(table_name);
    let has_tbl = ctx.table_exist(table_ref).unwrap();
    Ok(JsValue::from_bool(has_tbl))
}

#[wasm_bindgen]
pub async fn load_csv(file_name: String, table_name: String) -> Result<(), JsValue> {
    //https://stackoverflow.com/questions/76566489/convert-csv-to-apache-arrow-in-rust
    let mut register_path = "opfs:///".to_owned();
    register_path.push_str(&file_name.as_str());
    {
        let ctx = CTX.lock().unwrap();
        let table_ref = TableReference::from(table_name.clone());
        if !ctx.table_exist(table_ref).unwrap() {
            // register the temporary CSV table
            ctx.register_csv(
                &table_name.as_str(),
                register_path.as_str(),
                CsvReadOptions::new(),
            ).await.unwrap();
        }
    }
    Ok(())
}

#[wasm_bindgen]
pub async fn run_sql(sql_query: String) -> Result<JsValue, JsValue> {
    // create a plan to run a SQL query
    let df_opt: Option<DataFrame>;
    {
        df_opt = Some(CTX.lock().unwrap().sql(&sql_query.as_str()).await.unwrap());
    }
    let df = df_opt.unwrap();
    let schema = Schema::from(df.schema());
    // execute the plan and collect the results as Vec<RecordBatch>
    let results: Vec<RecordBatch> = df.collect().await.unwrap();

    // serialize to in memory vector
    let mut output: Vec<u8> = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema).unwrap();
    for batch in results {
        writer.write(&batch).unwrap();
    }
    let _ = writer.close();
    let js_arr = Uint8Array::from(&output[..]);
    Ok(JsValue::from(&js_arr))
}
