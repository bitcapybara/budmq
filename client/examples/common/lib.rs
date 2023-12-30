use bud_common::mtls;
use std::path::PathBuf;

pub fn client_certs(certs: PathBuf) -> mtls::Certs {
    mtls::Certs {
        ca_cert: certs.join("ca-cert.pem"),
        endpoint_cert: mtls::EndpointCerts::Client(mtls::Certificate {
            certificate: certs.join("client-cert.pem"),
            private_key: certs.join("client-key.pem"),
        }),
    }
}
