use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::ArrowNativeType;
use futures::FutureExt;
use js_sys::{Promise, Uint8Array};
use tokio::sync::oneshot::Sender;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions, Window,
};

#[derive(Debug)]
pub struct FileResponse {
    pub bytes: Option<Bytes>,
    pub name: String,
    pub last_modified: DateTime<Utc>,
    pub size: usize,
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
        .map(|result| async {
            match result {
                Ok(value) => {
                    assert!(value.has_type::<T>());
                    Ok(value.unchecked_into::<T>())
                }
                Err(e) => Err(e),
            }
        })
        .await
        .await
        .unwrap();
}

pub fn get_file_data(tx: Sender<Box<FileResponse>>, name: String, head: bool) {
    wasm_bindgen_futures::spawn_local({
        let f_name = name;
        async move {
            // moving Window as ref from the static async context to prevent loss of context
            let window: Window = web_sys::window().unwrap();
            let import_handle = get_file_folder(&window).await;
            let file_handle = get_from_promise::<FileSystemFileHandle>(
                import_handle.get_file_handle(f_name.as_str()),
            )
            .await;
            let csv_file = get_from_promise::<File>(file_handle.get_file()).await;
            let csv_bytes: Option<Bytes> = if head {
                None
            } else {
                let bytes = JsFuture::from(csv_file.array_buffer()).map(|value| {
                    match value {
                        Ok(value) => {
                            let u8_arr = Uint8Array::new(&value);
                            Ok(Bytes::from(u8_arr.to_vec()))
                        }
                        Err(e) => Err(e)
                    }
                }).await.unwrap();
                Some(bytes)
            };
            let milliseconds_since: i64 = csv_file.last_modified() as i64;
            let time = DateTime::from_timestamp_millis(milliseconds_since).unwrap();
            let resp = Box::new(FileResponse {
                bytes: csv_bytes,
                name: csv_file.name(),
                last_modified: time,
                size: csv_file.size().as_usize(),
            });
            tx.send(resp).unwrap();
        }
    });
}
