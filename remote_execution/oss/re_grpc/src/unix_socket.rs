/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A connector that dials the machine-local CAS daemon over a Unix socket, whatever URI tonic
//! asks it for.

pub use imp::UnixConnector;

#[cfg(unix)]
mod imp {
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;

    use hyper_util::rt::TokioIo;
    use tonic::transport::Uri;
    use tower::Service;

    /// Dials one Unix socket, whatever URI it is asked for.
    #[derive(Clone)]
    pub struct UnixConnector {
        path: Arc<PathBuf>,
    }

    impl UnixConnector {
        pub fn new(path: Arc<PathBuf>) -> Self {
            Self { path }
        }
    }

    impl Service<Uri> for UnixConnector {
        type Response = TokioIo<tokio::net::UnixStream>;
        type Error = std::io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            let path = Arc::clone(&self.path);
            Box::pin(
                async move { Ok(TokioIo::new(tokio::net::UnixStream::connect(&*path).await?)) },
            )
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;

    use hyper_util::rt::TokioIo;
    use tonic::transport::Uri;
    use tower::Service;

    /// Never dialed off Unix: `GrpcChannelConnector::connect` rejects `unix://` addresses there.
    #[derive(Clone)]
    pub struct UnixConnector;

    impl UnixConnector {
        pub fn new(_path: Arc<PathBuf>) -> Self {
            Self
        }
    }

    impl Service<Uri> for UnixConnector {
        type Response = TokioIo<tokio::net::TcpStream>;
        type Error = std::io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            Box::pin(async move {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Unix sockets are not supported on this platform",
                ))
            })
        }
    }
}
