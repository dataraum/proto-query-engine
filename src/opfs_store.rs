use async_trait::async_trait;
use futures::channel::oneshot;
use futures::stream::{BoxStream, StreamExt};
use object_store::GetRange;
use object_store::{
    path::Path, Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result,
};
use snafu::{ResultExt, Snafu};
use std::sync::mpsc;

use crate::web_fs_utils::{get_file_data, get_files, FileResponse};

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
                store: "OpfsFileSystem",
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
pub struct OpfsFileSystem {}

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
        let loc_string = location.to_string();
        let (tx, rx) = oneshot::channel::<FileResponse>();
        get_file_data(tx, loc_string.to_owned(), true);
        let response = rx.await.unwrap();
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: response.last_modified,
            size: response.size,
            e_tag: Some(response.name),
            version: None,
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let loc_string = location.to_string();
        let (tx, rx) = oneshot::channel::<FileResponse>();
        get_file_data(tx, loc_string, false);
        let response = rx.await.unwrap();

        let meta: ObjectMeta = ObjectMeta {
            location: location.clone(),
            last_modified: response.last_modified,
            size: response.size,
            e_tag: Some(response.name),
            version: None,
        };

        let bytes: bytes::Bytes = response.bytes.unwrap();
        // Copied from GetRange
        let (range, data) = match options.range {
            Some(range) => {
                let len = bytes.len() as u64;
                let r = (match range {
                    GetRange::Bounded(r) => {
                        if r.start >= len {
                            Err(InvalidGetRange::StartTooLarge {
                                requested: r.start as usize,
                                length: len as usize,
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
                                requested: o as usize,
                                length: len as usize,
                            })
                        } else {
                            Ok(o..len)
                        }
                    }
                    GetRange::Suffix(n) => Ok(len.saturating_sub(n)..len),
                })
                .context(RangeSnafu)?;
                (r.clone(), bytes.slice(r.start as usize..r.end as usize))
            }
            None => (0..bytes.len() as u64, bytes),
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
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let (tx, rx) = mpsc::channel::<ObjectMeta>();

        get_files(tx);

        let s: Vec<_> = rx.into_iter().map(|meta| Ok(meta)).collect();
        futures::stream::iter(s).boxed()
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
        Self::default()
    }
}
