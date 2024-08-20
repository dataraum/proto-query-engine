use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::ArrowNativeType;
use futures::stream::{BoxStream, StreamExt};
use js_sys::JsString;
use object_store::{
    path::Path, Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result,
};
use std::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use wasm_bindgen::JsCast;
use wasm_bindgen::{prelude::Closure, JsValue};
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions
};

use crate::utils::{get_file_folder, get_from_promise};

impl std::fmt::Display for OpfsFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpfsFileSystem()")
    }
}

#[derive(Debug, Default)]
pub struct OpfsFileSystem {}

#[async_trait]
impl ObjectStore for OpfsFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        return Err(Error::Generic {
            store: "put_opts",
            source: Box::new(Error::NotImplemented),
        });
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        return Err(Error::Generic {
            store: "put_multipart_opts",
            source: Box::new(Error::NotImplemented),
        });
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        web_sys::console::log_1(&"Hello, world!".into());
        let loc_str = location.to_string();
        #[derive(Debug)]
        struct FileResponse {
            size: usize,
            bytes: Bytes,
            last_modified: DateTime<Utc>,
        }
        
        let (tx, rx) = oneshot::channel::<FileResponse>();

        wasm_bindgen_futures::spawn_local(async move {
            let f_name: &str = &loc_str.as_str();
            let import_handle = get_file_folder().await.unwrap();
            let file_handle =
                get_from_promise::<FileSystemFileHandle>(import_handle.get_file_handle(f_name))
                    .await.unwrap();
            let csv_file = get_from_promise::<File>(file_handle.get_file()).await.unwrap();
            let csv_text: String = get_from_promise::<JsString>(csv_file.text()).await.unwrap().into();
            let milliseconds_since: i64 = csv_file.last_modified() as i64;
            let time = DateTime::from_timestamp_millis(milliseconds_since).unwrap();
            let resp = FileResponse{
                size: csv_file.size().as_usize(),
                bytes: Bytes::from(csv_text),
                last_modified: time,
            };
            tx.send(resp).unwrap();
        });
        
        let response = rx.await.unwrap();

        let meta: ObjectMeta = ObjectMeta {
            location: location.to_owned(),
            last_modified: response.last_modified, 
            size: response.size,
            e_tag: Some(response.size.to_string()),
            version: None,
        };
        web_sys::console::log_1(&JsValue::from_str(&meta.size.to_string()));
        let log_str = String::from_utf8(response.bytes.clone().to_vec());
        web_sys::console::log_1(&JsValue::from(&log_str.unwrap()));

        // let (range , data) = match options.range {
        //     Some(range) => {
        //         let r = range.as_range(response.bytes.len()).context(RangeSnafu)?;
        //         (r.clone(), response.bytes.slice(r))
        //     }
        //     None => (0..response.bytes.len(), response.bytes),
        // };
        let range = std::ops::Range { start: 0, end: response.bytes.len() };

        let stream = futures::stream::once(futures::future::ready(Ok(response.bytes)));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            attributes: Attributes::default(),
            meta,
            range,
        })
    }
    async fn delete(&self, location: &Path) -> Result<()> {
        return Err(Error::Generic {
            store: "delete",
            source: Box::new(Error::NotImplemented),
        });
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let (tx, rx): (Sender<ObjectMeta>, Receiver<ObjectMeta>) = mpsc::channel();

        let closure_c: Closure<dyn FnMut(JsValue) + 'static> =
            Closure::once(move |val: JsValue| {
                if val.has_type::<FileSystemDirectoryHandle>() {
                    let file_sync_handle = val.unchecked_into::<FileSystemDirectoryHandle>();
                    let mut nxt_file = file_sync_handle.values().next();
                    //while nxt_file.is_ok() {
                    let closure_b: Closure<dyn FnMut(JsValue) + 'static> =
                        Closure::once(move |val: JsValue| {
                            if val.has_type::<FileSystemFileHandle>() {
                                let import_file_handle =
                                    val.unchecked_into::<FileSystemFileHandle>();
                                let file_handle = import_file_handle.get_file();

                                let closure_c: Closure<dyn FnMut(JsValue) + 'static> =
                                    Closure::once(move |val: JsValue| {
                                        if val.has_type::<File>() {
                                            let file = val.unchecked_into::<File>();
                                            let mut path_str = "opfs://data/".to_owned();
                                            path_str.push_str(file.name().as_str());
                                            let milliseconds_since: i64 =
                                                file.last_modified() as i64;
                                            let time =
                                                DateTime::from_timestamp_millis(milliseconds_since);
                                            let meta: ObjectMeta = ObjectMeta {
                                                location: Path::parse(path_str).unwrap(),
                                                last_modified: time.unwrap(),
                                                size: file.size().as_usize(),
                                                e_tag: None,
                                                version: None,
                                            };
                                            let _ = tx.send(meta);
                                        }
                                    });
                                let _ = file_handle.then(&closure_c);
                            }
                        });
                    let _ = nxt_file.unwrap().then(&closure_b);
                    //nxt_file = file_sync_handle.values().next();
                    //}
                }
            });

        let _ = Self::root_handler(closure_c);

        let s: Vec<_> = rx.into_iter().map(|meta| Ok(meta)).collect();
        return futures::stream::iter(s).boxed();
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        return Err(Error::Generic {
            store: "list_with_delimiter",
            source: Box::new(Error::NotImplemented),
        });
    }
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        return Err(Error::Generic {
            store: "copy",
            source: Box::new(Error::NotImplemented),
        });
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        return Err(Error::Generic {
            store: "copy_if_not_exists",
            source: Box::new(Error::NotImplemented),
        });
    }
}

impl OpfsFileSystem {
    /// Create new filesystem storage with no prefix
    pub fn new() -> Self {
        Self::default()
    }

    fn root_handler(closure_b: Closure<dyn FnMut(JsValue) + 'static>) {
        web_sys::console::log_1(&"helllo 1331".into());
        let window = web_sys::window().unwrap();
        let navigator = window.navigator();
        let storage = navigator.storage();
        let root = storage.get_directory();
        web_sys::console::log_1(&"helllo 123121".into());

        let closure_a: Closure<dyn FnMut(JsValue) + 'static> =
            Closure::once(move |val: JsValue| {
                web_sys::console::log_1(&"helllo 123121".into());
                if val.has_type::<FileSystemDirectoryHandle>() {
                    let import_handle = val.unchecked_into::<FileSystemDirectoryHandle>();
                    let fs_options = &FileSystemGetFileOptions::new();
                    fs_options.set_create(true);
                    let import_file_handle =
                        import_handle.get_file_handle_with_options("data", fs_options);
                    let _ = import_file_handle.then(&closure_b);
                }
            });
        let _ = root.then(&closure_a);
    }
}
