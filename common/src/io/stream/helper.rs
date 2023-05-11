use futures::StreamExt;
use log::error;
use s2n_quic::stream::ReceiveStream;
use tokio_util::codec::FramedRead;

use crate::{
    io::Error,
    protocol::{Packet, PacketCodec, Response, ReturnCode},
};

use super::ResMap;

pub async fn start_recv(res_map: ResMap, mut framed: FramedRead<ReceiveStream, PacketCodec>) {
    while let Some(packet_res) = framed.next().await {
        let (id, resp) = match packet_res {
            Ok(Packet::Response(Response { request_id, code })) => {
                if matches!(code, ReturnCode::Success) {
                    (request_id, Ok(()))
                } else {
                    (request_id, Err(Error::FromPeer(code)))
                }
            }
            Ok(_) => {
                error!("io::writer received unexpected packet, execpted RESPONSE");
                continue;
            }
            Err(e) => {
                error!("io::writer decode protocol error: {e}");
                continue;
            }
        };
        if let Some(res_tx) = res_map.remove_res_tx(id).await {
            res_tx.send(resp).ok();
        }
    }
}
