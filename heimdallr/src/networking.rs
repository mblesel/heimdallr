use std::net::{SocketAddr, TcpStream};
use std::io::{Write};
use serde::{Serialize, Deserialize};

use super::HeimdallrClient;

#[derive(Serialize, Deserialize, Debug)]
pub enum DaemonPktType
{
    ClientInfoPkt,
    MutexCreationPkt,
    MutexLockReqPkt,
    MutexWriteAndReleasePkt,
    BarrierPkt,
    FinalizePkt,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonPkt
{
    pub job: String,
    pub pkt_type: DaemonPktType,
    pub pkt: Vec<u8>,
}

impl DaemonPkt
{
    // TODO make generic constructor
    pub fn new()
    {
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ClientInfoPkt
{
    pub job: String,
    pub size: u32,
    pub listener_addr: SocketAddr,
}

impl ClientInfoPkt
{
    pub fn new(job: String, size: u32, listener_addr: SocketAddr) -> ClientInfoPkt
    {
        ClientInfoPkt{job, size, listener_addr}
    }

    pub fn send(&self, mut stream: &TcpStream)
    {
        let msg = bincode::serialize(self).unwrap();
        let pkt = DaemonPkt{job: self.job.clone(), pkt_type: DaemonPktType::ClientInfoPkt,
            pkt: msg};
        let data = bincode::serialize(&pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonReplyPkt
{
    pub id: u32,
    pub client_listeners: Vec<SocketAddr>,
}

impl DaemonReplyPkt
{
    pub fn new(id: u32, client_listeners: &Vec<SocketAddr>) -> DaemonReplyPkt
    {
        DaemonReplyPkt {id, client_listeners: client_listeners.to_vec()}
    }

    pub fn send(&self, mut stream: &TcpStream)
    {
        let msg = bincode::serialize(self).unwrap();
        stream.write(msg.as_slice()).unwrap();
        stream.flush().unwrap();
    }

    pub fn receive(stream: &TcpStream) -> DaemonReplyPkt
    {
        let daemon_reply: DaemonReplyPkt = bincode::deserialize_from(stream).unwrap();

        daemon_reply
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexCreationPkt
{
    pub name: String,
    pub client_id: u32,
    pub start_data: Vec<u8>,
}

impl MutexCreationPkt
{
    pub fn new(name: String, id: u32, serialized_data: Vec<u8>) -> MutexCreationPkt
    {
        MutexCreationPkt{name, client_id: id, start_data: serialized_data}
    }

    pub fn send(&self, client: &HeimdallrClient, mut stream: &TcpStream)
    {
        let pkt = bincode::serialize(self).unwrap();
        let daemon_pkt = DaemonPkt{ job: client.job.clone(),
            pkt_type: DaemonPktType::MutexCreationPkt, pkt};

        let data = bincode::serialize(&daemon_pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MutexCreationReplyPkt
{
    pub name: String,
}

impl MutexCreationReplyPkt
{
    pub fn new(name: &str) -> MutexCreationReplyPkt
    {
        MutexCreationReplyPkt{name: name.to_string()}
    }

    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(&self).unwrap();
        stream.write(pkt.as_slice()).unwrap();
        stream.flush().unwrap();
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> MutexCreationReplyPkt
    {
        bincode::deserialize_from(stream).unwrap()
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexLockReqPkt
{
    pub name: String,
    pub listener_addr: SocketAddr,
}

impl MutexLockReqPkt
{
    pub fn new(name: &str,listener_addr: SocketAddr) -> MutexLockReqPkt
    {
        MutexLockReqPkt{name: name.to_string(), listener_addr}
    }

    pub fn send(&self, job: &str,  stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(self).unwrap();
        let daemon_pkt = DaemonPkt{ job: job.to_string(),
            pkt_type: DaemonPktType::MutexLockReqPkt, pkt};

        let data = bincode::serialize(&daemon_pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();

        Ok(())
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexWriteAndReleasePkt
{
    pub mutex_name: String,
    pub data: Vec<u8>,
}

impl MutexWriteAndReleasePkt
{
    pub fn new(mutex_name: &str, data: Vec<u8>) -> MutexWriteAndReleasePkt
    {
        MutexWriteAndReleasePkt{mutex_name: mutex_name.to_string(), data}
    }

    pub fn send(&self, job: &str, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(self).unwrap();
        let daemon_pkt = DaemonPkt{job: job.to_string(),
            pkt_type: DaemonPktType::MutexWriteAndReleasePkt, pkt};
        let data = bincode::serialize(&daemon_pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BarrierPkt
{
    pub id: u32,
    pub size: u32,
}

impl BarrierPkt
{
    pub fn new(id: u32, size: u32) -> BarrierPkt
    {
        BarrierPkt {id, size}
    }

    pub fn send(&self, job: &str, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(self).unwrap();
        let daemon_pkt = DaemonPkt{job: job.to_string(),
            pkt_type: DaemonPktType::BarrierPkt, pkt};
        let data = bincode::serialize(&daemon_pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();

        Ok(())
    }

}

#[derive(Serialize, Deserialize, Debug)]
pub struct BarrierReplyPkt
{
    pub id: u32,
}

impl BarrierReplyPkt
{
    pub fn new(id: u32) -> BarrierReplyPkt
    {
        BarrierReplyPkt {id}
    }

    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(&self).unwrap();
        stream.write(pkt.as_slice()).unwrap();
        stream.flush().unwrap();
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> BarrierReplyPkt
    {
        bincode::deserialize_from(stream).unwrap()
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct FinalizePkt
{
    pub id: u32,
    pub size: u32
}

impl FinalizePkt
{
    pub fn new(id: u32, size: u32) -> FinalizePkt
    {
        FinalizePkt {id, size}
    }

    pub fn send(&self, job: &str, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(self).unwrap();
        let daemon_pkt = DaemonPkt{job: job.to_string(),
            pkt_type: DaemonPktType::FinalizePkt, pkt};
        let data = bincode::serialize(&daemon_pkt).unwrap();
        stream.write(data.as_slice()).unwrap();
        stream.flush().unwrap();

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinalizeReplyPkt
{
    pub id: u32,
}

impl FinalizeReplyPkt
{
    pub fn new(id: u32) -> FinalizeReplyPkt
    {
        FinalizeReplyPkt {id}
    }

    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = bincode::serialize(&self).unwrap();
        stream.write(pkt.as_slice()).unwrap();
        stream.flush().unwrap();
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> FinalizeReplyPkt
    {
        bincode::deserialize_from(stream).unwrap()
    }
}



#[derive(Serialize, Deserialize, Debug)]
pub struct ClientOperationPkt
{
    pub client_id: u32,
    pub op_id: u32,
    pub addr: SocketAddr,
}
