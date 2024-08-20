use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::ArrowNativeType;
use futures::{
    executor,
    stream::{BoxStream, StreamExt},
};
use js_sys::{ArrayBuffer, Uint8Array};
use object_store::{
    path::Path, Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result,
};
use wasm_bindgen_futures::JsFuture;
use std::{
    any::Any,
    ops::Range,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio::sync::{mpsc as t_mpsc, oneshot};
//use futures::channel::oneshot;
use wasm_bindgen::JsCast;
use wasm_bindgen::{prelude::Closure, JsValue};
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions,
    FileSystemSyncAccessHandle,
};

use crate::utils::{get_file_folder, get_from_promise};
//use snafu::Snafu;

// /// A specialized `Error` for filesystem object store-related errors
// #[derive(Debug, Snafu)]
// #[allow(missing_docs)]
// pub(crate) enum Error {
//     #[snafu(display("Unable to open file {}: {}", path.display(), source))]
//     UnableToOpenFile {
//         source: io::Error,
//         path: PathBuf,
//     },
// }

// impl From<Error> for object_store::Error {
//     fn from(source: Error) -> Self {
//         match source {
//             _ => Self::Generic {
//                 store: "OpfsFileSystem",
//                 source: Box::new(source),
//             },
//         }
//     }
// }

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
        //let mut file_opt: Option<File> = None;
        //let mut file_sync_opt: Option<FileSystemSyncAccessHandle> = None;
        //let jsb = JsBridgeImpl {};
        //let import_handle = jsb.get_file_folder().await.unwrap();
        let (tx, rx) = oneshot::channel::<Vec<u8>>();

        web_sys::console::log_1(&"loc_str".into());
        //Self::get_file_bytes(tx, loc_str);
        Self::get_files(tx, loc_str);
        let buffer = rx.await;
        if buffer.is_err() {
            let err = buffer.clone().err().unwrap();
            web_sys::console::error_1(&JsValue::from_str(&err.to_string()));
        }
        let buff_bytes = Bytes::from(buffer.unwrap());
        /////SEND!!!!
        let meta = ObjectMeta {
            location: location.to_owned(),
            last_modified: Utc::now(), //time_modified.unwrap(),
            size: buff_bytes.len(),
            e_tag: None,
            version: None,
        };
        let range = Range {
            start: 0,
            end: meta.size,
        };

        let stream = futures::stream::once(futures::future::ready(Ok(buff_bytes)));
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
    fn get_files(tx: tokio::sync::oneshot::Sender<Vec<u8>>, loc_str: String) {
        wasm_bindgen_futures::spawn_local( async move {
            let f_name: &str = &loc_str.as_str();
            let import_handle = get_file_folder().await.unwrap();
            web_sys::console::log_1(&import_handle.name().into());
            web_sys::console::log_1(&import_handle.to_string().into());
            // while key.is_ok() {
            //     let value = JsFuture::from(key.unwrap()).await.unwrap();
            //     web_sys::console::log_1(&value.as_string().into());
            //     key = key.next();
            // }

            web_sys::console::log_1(&f_name.into());
            let file_handle =
                get_from_promise::<FileSystemFileHandle>(import_handle.get_file_handle(f_name))
                    .await
                    .unwrap();
            web_sys::console::log_1(&file_handle.name().into());
            web_sys::console::log_1(&"helllo 22".into());
            let handle = file_handle.create_sync_access_handle();
            let file = get_from_promise::<File>(
                file_handle.get_file(),
            )
            .await
            .unwrap();
        
            web_sys::console::log_1(&"helllo 33".into());
            web_sys::console::log_1(&file.size().into());
            web_sys::console::log_1(&file.array_buffer().to_string().into());
            if file.size() > 0.0 {
                let arr_buff_pr = file.array_buffer();
                web_sys::console::log_1(&"helllo 55".into());
                let arr_buff = JsFuture::from(arr_buff_pr).await.unwrap();
                web_sys::console::log_1(&"helllo 66".into());
                assert!(arr_buff.is_instance_of::<ArrayBuffer>());
                web_sys::console::log_1(&"helllo 44".into());
                let typebuf: js_sys::Uint8Array = Uint8Array::new(&arr_buff);
                let mut buffer = vec![0; typebuf.length() as usize];
                typebuf.copy_to(&mut buffer[..]);

                web_sys::console::log_1(&"helllo 99".into());
                let _ = tx.send(buffer);
            }

            //let arr_buff = get_from_promise::<ArrayBuffer>(file.array_buffer()).await.unwrap();
            
        });
    }
    fn get_file_bytes(tx: tokio::sync::oneshot::Sender<Vec<u8>>, loc_str: String) {
        web_sys::console::log_1(&"helllo again".into());
        let closure_a: Closure<dyn FnMut(JsValue) + 'static> =
            Closure::once(move |val: JsValue| {
                web_sys::console::log_1(&"helllo 22".into());
                if val.has_type::<FileSystemDirectoryHandle>() {
                    let import_handle = val.unchecked_into::<FileSystemDirectoryHandle>();
                    let fs_options = &FileSystemGetFileOptions::new();
                    fs_options.set_create(false);
                    let import_file_handle =
                        import_handle.get_file_handle_with_options(&loc_str, fs_options);
                    let closure_b: Closure<dyn FnMut(JsValue) + 'static> =
                        Closure::once(move |val: JsValue| {
                            web_sys::console::log_1(&"helllo 444".into());
                            if val.has_type::<FileSystemFileHandle>() {
                                let import_file_handle =
                                    val.unchecked_into::<FileSystemFileHandle>();
                                let file_sync_handle =
                                    import_file_handle.create_sync_access_handle();
                                let closure_c: Closure<dyn FnMut(JsValue) + 'static> =
                                    Closure::once(move |val: JsValue| {
                                        web_sys::console::log_1(&"helllo 454".into());
                                        if val.has_type::<FileSystemSyncAccessHandle>() {
                                            let file_sync_handle =
                                                val.unchecked_into::<FileSystemSyncAccessHandle>();
                                            let mut buffer: Vec<u8> =
                                                vec![
                                                    0;
                                                    file_sync_handle.get_size().unwrap().as_usize()
                                                ];
                                            let _ =
                                                file_sync_handle.read_with_u8_array(&mut buffer);

                                            web_sys::console::log_1(&"helllo 99".into());
                                            let _ = tx.send(buffer);
                                        }
                                    });

                                let _ = file_sync_handle.then(&closure_c);
                            }
                        });
                    let _ = import_file_handle.then(&closure_b);
                }
            });
        web_sys::console::log_1(&"helllo 11".into());
        let _ = Self::root_handler(closure_a);
    }
}
