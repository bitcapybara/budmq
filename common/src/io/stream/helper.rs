use futures::StreamExt;
use s2n_quic::stream::ReceiveStream;
use tokio_util::codec::FramedRead;

use crate::{
    io::Error,
    protocol::{Packet, PacketCodec, ReturnCode},
};

use super::ResMap;

pub async fn start_recv(res_map: ResMap, mut framed: FramedRead<ReceiveStream, PacketCodec>) {
    while let Some(packet_res) = framed.next().await {
        // TODO get id from packet
        let id = 0;
        let Some(res_tx) = res_map.remove_res_tx(id).await else {
                continue;
            };
        let resp = match packet_res {
            Ok(Packet::Response(ReturnCode::Success)) => Ok(()),
            Ok(Packet::Response(code)) => Err(Error::FromServer(code)),
            Ok(_) => Err(Error::ReceivedUnexpectedPacket),
            Err(e) => Err(Error::Protocol(e)),
        };
        res_tx.send(resp).ok();
    }
}
