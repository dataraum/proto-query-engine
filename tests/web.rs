//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use std::assert_eq;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use js_sys::Uint8Array;
use proto_query_engine::utils::{get_file_folder, get_from_promise};
use proto_query_engine::{load_csv, run_sql};
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::{console_log, *};
use web_sys::{FileSystemFileHandle, FileSystemGetFileOptions, FileSystemWritableFileStream};

wasm_bindgen_test_configure!(run_in_browser);

async fn set_up() -> Result<(), JsError> {
    let import_handle = get_file_folder().await?;
    let import_file = get_from_promise::<FileSystemFileHandle>(
        import_handle
            .get_file_handle_with_options("12test2", FileSystemGetFileOptions::new().create(true)),
    )
    .await?;
    let writable =
        get_from_promise::<FileSystemWritableFileStream>(import_file.create_writable()).await?;
    let _ = writable.write_with_str("a,b,c\n1,2,3").unwrap();
    let _ = writable.close();
    Ok(())
}

#[wasm_bindgen_test]
async fn pass() {
    let _set_up = set_up().await;
    let _add_result = load_csv("12test2".to_string(), "test".to_string()).await;
    let result =
        run_sql("SELECT a, MIN(b) FROM test WHERE a <= b GROUP BY a LIMIT 100".to_string()).await;

    let js_value = JsValue::from(result.clone().err());
    let ok_value = JsValue::from(result.ok().unwrap());
    let arr = Uint8Array::new(&ok_value);
    let arr_vec: Vec<u8> = arr.to_vec();
    let reader = StreamReader::try_new(&arr_vec[..], None).unwrap();
    let mut results: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        results.push(batch.unwrap());
    }
    console_log!("{js_value:?}");
    console_log!("{arr:?}");
    console_log!("{results:?}");
    assert_eq!(JsValue::undefined(), js_value);

    datafusion::assert_batches_eq!(
        vec![
            "+---+-------------+",
            "| a | MIN(test.b) |",
            "+---+-------------+",
            "| 1 | 2           |",
            "+---+-------------+",
        ],
        &results
    );
}
