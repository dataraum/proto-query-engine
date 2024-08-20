use js_sys::Promise;
use wasm_bindgen::{JsCast, JsError, JsValue};
use web_sys::{FileSystemDirectoryHandle, FileSystemGetDirectoryOptions};

pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

pub async fn get_file_folder() -> Result<FileSystemDirectoryHandle, JsValue> {
    let window = web_sys::window().expect("Missing Window");
    web_sys::console::log_1(&window.is_secure_context().to_string().into());
    let navigator = window.navigator();
    let storage = navigator.storage();
    let root = get_from_promise::<FileSystemDirectoryHandle>(storage.get_directory()).await?;
    let options = &FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    let import_handle =
        get_from_promise::<FileSystemDirectoryHandle>(root.get_directory_handle_with_options(
            "data",
            options,
        ))
        .await?;
    return Ok(import_handle);
}

pub async fn get_from_promise<T: wasm_bindgen::JsCast>(f: Promise) -> Result<T, JsValue> {
    let promise = js_sys::Promise::resolve(&f.into());
    let to_check = wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .unwrap_or(JsValue::from_bool(false));
    if to_check.has_type::<T>() {
        let result = to_check.unchecked_into::<T>();
        return Ok(result);
    }
    return Err(JsValue::from_str("Not the right type"));
}
