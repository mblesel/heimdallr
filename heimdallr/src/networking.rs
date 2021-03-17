use std::net::{SocketAddr, TcpStream, TcpListener, ToSocketAddrs};
use std::io::{Write, BufReader};
use serde::{Serialize, Deserialize};


//
// Client to Daemon packets
//

#[derive(Serialize, Deserialize, Debug)]
pub enum DaemonPktType
{
    ClientRegistration(ClientRegistrationPkt),
    MutexCreation(MutexCreationPkt),
    MutexLockReq(MutexLockReqPkt),
    MutexWriteAndRelease(MutexWriteAndReleasePkt),
    Barrier(BarrierPkt),
    Finalize(FinalizePkt),
}

impl DaemonPkt
{
    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let msg = bincode::serialize(&self).expect("Could not serialize DaemonPkt");
        stream.write(msg.as_slice())?;
        stream.flush()?;
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> DaemonPkt
    {
        // TODO see if Bufreader can be used here without loosing data when client
        // sends two packages successively with the daemon not already being at this
        // receive call
        // let reader = BufReader::new(stream);
        bincode::deserialize_from(stream).expect("Could not deserialize DaemonPkt")
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonPkt
{
    pub job: String,
    pub pkt: DaemonPktType,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ClientRegistrationPkt
{
    pub job: String,
    pub size: u32,
    pub listener_addr: SocketAddr,
}
impl ClientRegistrationPkt
{
    pub fn new(job: &str, size: u32, listener_addr: SocketAddr) -> DaemonPkt
    {
        let pkt = DaemonPktType::ClientRegistration(ClientRegistrationPkt{job: job.to_string(), size, listener_addr});

        DaemonPkt {job: job.to_string(), pkt}
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
    pub fn new(name: &str, id: u32, serialized_data: Vec<u8>, job: &str) -> DaemonPkt
    {
        let pkt = DaemonPktType::MutexCreation(MutexCreationPkt{name: name.to_string(), client_id: id, start_data: serialized_data});
        DaemonPkt{job: job.to_string(), pkt}
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexLockReqPkt
{
    pub name: String,
    pub id: u32,
}

impl MutexLockReqPkt
{
    pub fn new(name: &str,client_id: u32, job: &str) -> DaemonPkt
    {
        let pkt = DaemonPktType::MutexLockReq(MutexLockReqPkt{name: name.to_string(), id: client_id});
        DaemonPkt{job: job.to_string(), pkt}
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
    pub fn new(mutex_name: &str, data: Vec<u8>, job: &str) -> DaemonPkt
    {
        let pkt = DaemonPktType::MutexWriteAndRelease(MutexWriteAndReleasePkt{mutex_name: mutex_name.to_string(), data});
        DaemonPkt{job: job.to_string(), pkt}
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
    pub fn new(id: u32, size: u32, job: &str) -> DaemonPkt
    {
        let pkt = DaemonPktType::Barrier(BarrierPkt {id, size});
        DaemonPkt{job: job.to_string(), pkt}
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
    pub fn new(id: u32, size: u32, job: &str) -> DaemonPkt
    {
        let pkt = DaemonPktType::Finalize(FinalizePkt {id, size});
        DaemonPkt {job: job.to_string(), pkt}
    }
}


//
// Daemon to Client packets
//

#[derive(Serialize, Deserialize, Debug)]
pub enum DaemonReplyPkt
{
    ClientRegistrationReply(ClientRegistrationReplyPkt),
    MutexCreationReply(MutexCreationReplyPkt),
    BarrierReply(BarrierReplyPkt),
    FinalizeReply(FinalizeReplyPkt),
}

impl DaemonReplyPkt
{
    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let msg = bincode::serialize(&self).expect("Could not serialize DaemonReplyPkt");
        stream.write(msg.as_slice())?;
        stream.flush()?;
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> Self
    {
        let reader = BufReader::new(stream);
        bincode::deserialize_from(reader).expect("Could not deserialize DaemonReplyPkt")
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ClientRegistrationReplyPkt
{
    pub id: u32,
    pub client_listeners: Vec<SocketAddr>,
}

impl ClientRegistrationReplyPkt
{
    pub fn new(id: u32, client_listeners: &Vec<SocketAddr>) -> DaemonReplyPkt
    {
        DaemonReplyPkt::ClientRegistrationReply(ClientRegistrationReplyPkt {id, client_listeners: client_listeners.to_vec()})
    }

    pub fn receive(stream: &TcpStream) -> Option<ClientRegistrationReplyPkt>
    {
        let de = DaemonReplyPkt::receive(stream);
        match de
        {
            DaemonReplyPkt::ClientRegistrationReply(r) => Some(r),
            _ => None,
        }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexCreationReplyPkt
{
    pub name: String,
}

impl MutexCreationReplyPkt
{
    pub fn new(name: &str) -> DaemonReplyPkt
    {
        DaemonReplyPkt::MutexCreationReply(MutexCreationReplyPkt{name: name.to_string()})
    }

    pub fn receive(stream: &TcpStream) -> Option<MutexCreationReplyPkt>
    {
        let de = DaemonReplyPkt::receive(stream);
        match de
        {
            DaemonReplyPkt::MutexCreationReply(r) => Some(r),
            _ => None,
        }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct BarrierReplyPkt
{
    pub id: u32,
}

impl BarrierReplyPkt
{
    pub fn new(id: u32) -> DaemonReplyPkt
    {
        DaemonReplyPkt::BarrierReply(BarrierReplyPkt{id})
    }

    pub fn receive(stream: &TcpStream) -> Option<BarrierReplyPkt>
    {
        let de = DaemonReplyPkt::receive(stream);
        match de
        {
            DaemonReplyPkt::BarrierReply(r) => Some(r),
            _ => None,
        }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct FinalizeReplyPkt
{
    pub id: u32,
}

impl FinalizeReplyPkt
{
    pub fn new(id: u32) -> DaemonReplyPkt
    {
        DaemonReplyPkt::FinalizeReply(FinalizeReplyPkt{id})
    }

    pub fn receive(stream: &TcpStream) -> Option<FinalizeReplyPkt>
    {
        let de = DaemonReplyPkt::receive(stream);
        match de 
        {
            DaemonReplyPkt::FinalizeReply(r) => Some(r),
            _ => None,
        }
    }
}


//
// Client to Client packets
//

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientOperationPkt
{
    pub client_id: u32,
    pub op_id: u32,
    pub addr: SocketAddr,
}

impl ClientOperationPkt
{
    pub fn new(client_id: u32, op_id: u32, addr: SocketAddr) -> Self
    {
        ClientOperationPkt {client_id, op_id, addr}
    }

    pub fn send(self, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let msg = bincode::serialize(&self).expect("Could not serialize ClientOperationPkt");
        stream.write(msg.as_slice())?;
        stream.flush()?;
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> Self
    {
        let reader = BufReader::new(stream);
        bincode::deserialize_from(reader).expect("Could not deserialize ClientOperationPkt")
    }
}


//
// General networking functions
//

pub fn connect(addr: &SocketAddr) -> std::io::Result<TcpStream>
{
    TcpStream::connect(addr)
}


pub fn bind_listener<A: ToSocketAddrs>(ip: &A) -> std::io::Result<TcpListener>
{
    TcpListener::bind(ip)
}
