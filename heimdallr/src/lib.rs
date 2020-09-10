use std::process;
use std::net::{TcpStream, TcpListener};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::io::{Write, Read};
use serde::{Serialize, Deserialize};

pub struct HeimdallrClient
{
    pub job: String,
    pub size: u32,
    pub id: u32,
}

impl HeimdallrClient
{
    pub fn new(job: String, size: u32, daemon_addr: SocketAddrV4)
        -> Result<HeimdallrClient, &'static str>
    {
        let mut stream = connect(daemon_addr);

        let client_info = ClientInfoPkt::new(job.clone(), size);
        client_info.send(&stream);

        let mut reply: String = String::new();

        // TODO make nicer
        let mut de = serde_json::Deserializer::from_reader(stream);
        let reply = DaemonReplyPkt::deserialize(&mut de).unwrap();

        let id: u32 = reply.id;
        

        Ok(HeimdallrClient {job, size, id})
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientInfoPkt
{
    pub job: String,
    pub size: u32,
}

impl ClientInfoPkt
{
    pub fn new(job: String, size: u32) -> ClientInfoPkt
    {
        ClientInfoPkt{job, size}
    }

    pub fn send(&self, mut stream: &TcpStream)
    {
        let msg = serde_json::to_string(&self).unwrap();
        stream.write(msg.as_bytes());
        println!("SENT");
    }

    pub fn receive(mut stream: &TcpStream) -> ClientInfoPkt
    {
        let mut de = serde_json::Deserializer::from_reader(stream);
        let client_info = ClientInfoPkt::deserialize(&mut de).unwrap();

        client_info
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonReplyPkt
{
    pub id: u32,
}

fn connect(addr: SocketAddrV4) -> TcpStream
{
    let stream = TcpStream::connect(addr).unwrap_or_else(|err|
        {
            eprintln!("Error in connecting to daemon at: {}", err);
            process::exit(1);
        });

    println!("Connected to server at: {}", addr);

    stream
}
