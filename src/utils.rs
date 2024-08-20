use futures::FutureExt;
use js_sys::Promise;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemGetDirectoryOptions};

pub async fn get_file_folder() -> Result<FileSystemDirectoryHandle, JsValue> {
    let window = web_sys::window().unwrap();
    let navigator = window.navigator();
    let storage = navigator.storage();
    let root = get_from_promise::<FileSystemDirectoryHandle>(storage.get_directory()).await?;
    let options = &FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    let import_handle = get_from_promise::<FileSystemDirectoryHandle>(
        root.get_directory_handle_with_options("data", options),
    )
    .await?;
    return Ok(import_handle);
}

pub async fn get_from_promise<T: JsCast>(f: Promise) -> Result<T, JsValue> {
    let promise = js_sys::Promise::resolve(&f.into());
    return JsFuture::from(promise)
        .map(|result| match result {
            Ok(value) => {
                assert!(value.has_type::<T>());
                Ok(value.unchecked_into::<T>())
            }
            Err(e) => Err(JsValue::from_str(e.as_string().unwrap().as_str())),
        })
        .await;
}
