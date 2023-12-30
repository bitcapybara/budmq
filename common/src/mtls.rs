use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Certs {
    pub ca_cert: PathBuf,
    pub endpoint_cert: EndpointCerts,
}

#[derive(Clone)]
pub enum EndpointCerts {
    Client(Certificate),
    Server(Certificate),
}

impl EndpointCerts {
    pub fn ca(&self) -> &Path {
        match self {
            EndpointCerts::Client(c) => c.certificate.as_path(),
            EndpointCerts::Server(s) => s.certificate.as_path(),
        }
    }

    pub fn key(&self) -> &Path {
        match self {
            EndpointCerts::Client(c) => c.private_key.as_path(),
            EndpointCerts::Server(s) => s.private_key.as_path(),
        }
    }
}

#[derive(Clone)]
pub struct Certificate {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}
