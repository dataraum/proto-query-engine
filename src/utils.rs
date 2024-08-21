use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::ArrowNativeType;
use futures::FutureExt;
use js_sys::{JsString, Promise};
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

// fn print_type_of<T>(_: &T) {
//     let typy = std::any::type_name::<T>();
//     console::log_1(&typy.into());
// }

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
            let window: Window = web_sys::window().unwrap();
            let import_handle = get_file_folder(&window).await;
            let file_handle = get_from_promise::<FileSystemFileHandle>(
                import_handle.get_file_handle(f_name.as_str()),
            )
            .await;
            let csv_file = get_from_promise::<File>(file_handle.get_file()).await;
            let csv_text: Option<String> = if head {
                None
            } else {
                let js_string = get_from_promise::<JsString>(csv_file.text()).await;
                Some(js_string.into())
            };
            let milliseconds_since: i64 = csv_file.last_modified() as i64;
            let time = DateTime::from_timestamp_millis(milliseconds_since).unwrap();
            let resp = Box::new(FileResponse {
                bytes: if head {
                    None
                } else {
                    Some(Bytes::from(csv_text.unwrap()))
                },
                name: csv_file.name(),
                last_modified: time,
                size: csv_file.size().as_usize(),
            });
            tx.send(resp).unwrap();
        }
    });
}
