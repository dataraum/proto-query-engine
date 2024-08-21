use async_trait::async_trait;
use chrono::DateTime;
use datafusion::arrow::datatypes::ArrowNativeType;
use futures::stream::{BoxStream, StreamExt};
use object_store::GetRange;
use object_store::{
    path::Path, Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result,
};
use snafu::{ResultExt, Snafu};
use std::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use wasm_bindgen::JsCast;
use wasm_bindgen::{prelude::Closure, JsValue};
use web_sys::{
    console, File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions,
};

use crate::utils::{get_file_data, FileResponse};

#[derive(Debug, Snafu)]
pub(crate) enum InvalidGetRange {
    #[snafu(display(
        "Wanted range starting at {requested}, but object was only {length} bytes long"
    ))]
    StartTooLarge { requested: usize, length: usize },
}

#[derive(Debug, Snafu)]
enum OpfsError {
    #[snafu(display("Invalid range: {source}"))]
    Range { source: InvalidGetRange },
}

impl From<OpfsError> for object_store::Error {
    fn from(source: OpfsError) -> Self {
        match source {
            _ => Error::Generic {
                store: "InMemory",
                source: Box::new(source),
            },
        }
    }
}

impl std::fmt::Display for OpfsFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpfsFileSystem()")
    }
}
#[derive(Debug, Default)]
pub struct OpfsFileSystem {
    //window: &'static Window
}

#[async_trait]
impl ObjectStore for OpfsFileSystem {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::Generic {
            store: "put_multipart_opts",
            source: Box::new(Error::NotImplemented),
        })
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> { 
        console::log_1(&"head".into());
        let loc_string = location.to_string();
        let (tx, rx) = oneshot::channel::<FileResponse>();
        get_file_data(tx, loc_string.to_owned());
        let response = rx.await.unwrap();
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: response.last_modified,
            size: response.bytes.len(),
            e_tag: Some(response.name),
            version: None,
        })
    }

    // async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
    //     console::log_1(&"ranges".into());
    //     let loc_string = location.to_string();
    //     let (tx, rx) = oneshot::channel::<FileResponse>();
    //     get_file_data(tx, loc_string);
    //     let response = rx.await.unwrap();
    //     let range = Range{start: 0, end: response.bytes.len()};
    //     let bbb = [response.bytes.slice(range)];
    //     Ok(bbb.to_vec())
    // }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        console::log_1(&"get".into());
        let loc_string = location.to_string();

        let (tx, rx) = oneshot::channel::<FileResponse>();

        get_file_data(tx, loc_string);

        let response = rx.await.unwrap();

        let meta: ObjectMeta = ObjectMeta {
            location: location.clone(),
            last_modified: response.last_modified,
            size: response.bytes.len(),
            e_tag: Some(response.name),
            version: None,
        };

        //console::log_1(&JsValue::from_str(String::from_utf8(Vec::<u8>::from(response.bytes.clone())).unwrap().as_str()));

        let (range, data) = match options.range {
            Some(range) => {
                let len = response.bytes.len();
                let r = (match range {
                    GetRange::Bounded(r) => {
                        if r.start >= len {
                            Err(InvalidGetRange::StartTooLarge {
                                requested: r.start,
                                length: len,
                            })
                        } else if r.end > len {
                            Ok(r.start..len)
                        } else {
                            Ok(r.clone())
                        }
                    }
                    GetRange::Offset(o) => {
                        if o >= len {
                            Err(InvalidGetRange::StartTooLarge {
                                requested: o,
                                length: len,
                            })
                        } else {
                            Ok(o..len)
                        }
                    }
                    GetRange::Suffix(n) => Ok(len.saturating_sub(n)..len),
                })
                .context(RangeSnafu)?;
                (r.clone(), response.bytes.slice(r))
            }
            None => (0..response.bytes.len(), response.bytes),
        };
        let stream = futures::stream::once(futures::future::ready(Ok(data)));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            attributes: Attributes::default(),
            meta,
            range,
        })
    }
    async fn delete(&self, _: &Path) -> Result<()> {
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
                    let nxt_file = file_sync_handle.values().next();
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

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        return Err(Error::Generic {
            store: "list_with_delimiter",
            source: Box::new(Error::NotImplemented),
        });
    }
    async fn copy(&self, _: &Path, _: &Path) -> Result<()> {
        return Err(Error::Generic {
            store: "copy",
            source: Box::new(Error::NotImplemented),
        });
    }
    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> Result<()> {
        return Err(Error::Generic {
            store: "copy_if_not_exists",
            source: Box::new(Error::NotImplemented),
        });
    }
}

impl OpfsFileSystem {
    /// Create new filesystem storage with no prefix
    pub fn new() -> OpfsFileSystem {
        //window = window
        Self::default()
    }

    fn root_handler(closure_b: Closure<dyn FnMut(JsValue) + 'static>) {
        let window = web_sys::window().unwrap();
        let navigator = window.navigator();
        let storage = navigator.storage();
        let root = storage.get_directory();

        let closure_a: Closure<dyn FnMut(JsValue) + 'static> =
            Closure::once(move |val: JsValue| {
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
