use std::{io::Cursor, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatchWriter;
use datafusion::arrow::{
    csv::{reader::Format, ReaderBuilder},
    datatypes::{ArrowNativeType, Schema},
    error::ArrowError,
    ipc::{
        writer::{FileWriter, IpcWriteOptions},
        MetadataVersion,
    },
};
use futures::{channel::oneshot::Sender, FutureExt};
use js_sys::{try_iter, Promise, Uint8Array};
use object_store::{path::Path, ObjectMeta};
use regex::Regex;
use serde::Deserialize;
use std::io::Seek;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    window, File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream, Window,
};

#[derive(Debug)]
pub struct FileResponse {
    pub bytes: Option<Bytes>,
    pub name: String,
    pub last_modified: DateTime<Utc>,
    pub size: usize,
}

#[derive(Deserialize)]
pub struct CsvConfig {
    pub delimiter: String,
    pub quote: String,
    pub comment: String,
    pub escape: String,
    pub null_regex: String,
    pub truncated: bool,
}

pub async fn get_file_folder(window: &Window) -> FileSystemDirectoryHandle {
    let navigator = window.navigator();
    let storage = navigator.storage();
    let root = get_from_promise::<FileSystemDirectoryHandle>(storage.get_directory()).await;
    let options = &FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    return get_from_promise::<FileSystemDirectoryHandle>(
        root.get_directory_handle_with_options("data", options),
    )
    .await;
}

pub async fn get_from_promise<T: JsCast>(promise: Promise) -> T {
    return JsFuture::from(promise)
        .map(|result| match result {
            Ok(value) => {
                assert!(value.has_type::<T>());
                Ok(value.unchecked_into::<T>())
            }
            Err(e) => Err(e),
        })
        .await
        .unwrap();
}

pub async fn cp_csv_to_arrow(
    u8_arr: Uint8Array,
    name: String,
    csv_config: JsValue,
) -> Result<Schema, ArrowError> {
    // moving Window as ref from the static async context to prevent loss of context
    let mut bytes_cursor = Cursor::new(u8_arr.to_vec());
    let cfg: CsvConfig = serde_wasm_bindgen::from_value(csv_config).unwrap();

    let delimiter = if cfg.delimiter.len() == 1 {
        cfg.delimiter.as_bytes()[0]
    } else {
        b','
    };

    let mut csv_format = Format::default()
        .with_header(true)
        .with_delimiter(delimiter)
        .with_truncated_rows(cfg.truncated);

    if cfg.quote.len() == 1 {
        csv_format = csv_format.with_quote(cfg.quote.as_bytes()[0]);
    }
    if cfg.comment. len() == 1 {
        csv_format = csv_format.with_comment(cfg.comment.as_bytes()[0]);
    }
    if cfg.escape. len() == 1 {
        csv_format = csv_format.with_escape(cfg.escape.as_bytes()[0]);
    }
    if cfg.null_regex.len() > 0 && cfg.null_regex.len() <= 32 {
        csv_format = csv_format.with_null_regex(Regex::new(&cfg.null_regex).unwrap());
    }
    
    let (schema, _) = csv_format
        .infer_schema(&mut bytes_cursor, Some(100))
        .unwrap();
    bytes_cursor.rewind().unwrap();

    let csv_reader = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_format(csv_format)
        .build(bytes_cursor)
        .unwrap();

    let mut output: Vec<u8> = Vec::new();

    let options =
        IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?.with_preserve_dict_id(false);
    let mut writer =
        FileWriter::try_new_with_options(&mut output, &schema.clone(), options).unwrap();

    for batch in csv_reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error),
        }
    }
    writer.close().unwrap();

    let option_arrow = &FileSystemGetFileOptions::default();
    option_arrow.set_create(true);

    let arrow_name = format!("{name}.arrow");
    let window: Window = window().unwrap();
    let import_handle = get_file_folder(&window).await;
    let arrow_file_handle = get_from_promise::<FileSystemFileHandle>(
        import_handle.get_file_handle_with_options(&arrow_name.as_str(), &option_arrow),
    )
    .await;

    let write_file_stream =
        get_from_promise::<FileSystemWritableFileStream>(arrow_file_handle.create_writable()).await;

    JsFuture::from(write_file_stream.write_with_u8_array(&output).unwrap())
        .await
        .unwrap();
    JsFuture::from(write_file_stream.close()).await.unwrap();

    Ok(schema)
}

pub fn get_file_data(tx: Sender<FileResponse>, name: String, head: bool) {
    wasm_bindgen_futures::spawn_local({
        let f_name = name;
        async move {
            // moving Window as ref from the static async context to prevent loss of context
            let window: Window = window().unwrap();
            let import_handle = get_file_folder(&window).await;
            let file_handle = get_from_promise::<FileSystemFileHandle>(
                import_handle.get_file_handle(f_name.as_str()),
            )
            .await;
            let csv_file = get_from_promise::<File>(file_handle.get_file()).await;
            let csv_bytes = if head {
                None
            } else {
                let bytes = JsFuture::from(csv_file.array_buffer())
                    .map(|value| match value {
                        Ok(value) => {
                            let u8_arr = Uint8Array::new(&value);
                            Ok(Bytes::from(u8_arr.to_vec()))
                        }
                        Err(e) => Err(e),
                    })
                    .await
                    .unwrap();
                Some(bytes)
            };
            let milliseconds_since: i64 = csv_file.last_modified() as i64;
            let time = DateTime::from_timestamp_millis(milliseconds_since).unwrap();
            let resp = FileResponse {
                bytes: csv_bytes,
                name: csv_file.name(),
                last_modified: time,
                size: csv_file.size().as_usize(),
            };
            tx.send(resp).unwrap();
        }
    });
}

pub fn get_files(tx: std::sync::mpsc::Sender<ObjectMeta>) {
    wasm_bindgen_futures::spawn_local({
        async move {
            // moving Window as ref from the static async context to prevent loss of context
            let window: Window = web_sys::window().unwrap();
            let import_handle = get_file_folder(&window).await;

            let iterator = try_iter(&import_handle.values())
                .unwrap()
                .ok_or_else(|| "need to pass iterable JS values!")
                .unwrap();

            for value in iterator {
                let value = value.unwrap();
                assert!(value.has_type::<FileSystemFileHandle>());
                let file_handle = value.unchecked_into::<FileSystemFileHandle>();
                let file = get_from_promise::<File>(file_handle.get_file()).await;
                let mut path_str = "opfs://data/".to_owned();
                path_str.push_str(file.name().as_str());
                let milliseconds_since: i64 = file.last_modified() as i64;
                let time = DateTime::from_timestamp_millis(milliseconds_since);
                let meta = ObjectMeta {
                    location: Path::parse(path_str).unwrap(),
                    last_modified: time.unwrap(),
                    size: file.size().as_usize(),
                    e_tag: None,
                    version: None,
                };
                tx.send(meta).unwrap();
            }
        }
    });
}
