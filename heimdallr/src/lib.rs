use std::process;
use std::net::{TcpStream, TcpListener};
use std::net::{SocketAddr,SocketAddrV4};
use std::io::{Write, Read};
use std::fmt;
use serde::{Serialize, Deserialize};

pub struct HeimdallrClient
{
    pub job: String,
    pub size: u32,
    pub id: u32,
    pub client_sockets: Vec<SocketAddr>,
    pub listener: TcpListener,
    pub client_listeners: Vec<SocketAddr>,
}

impl HeimdallrClient
{
    pub fn init(job: String, size: u32, daemon_addr: SocketAddrV4)
        -> Result<HeimdallrClient, &'static str>
    {
        let stream = connect(daemon_addr);
        //TODO
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let client_info = ClientInfoPkt::new(job.clone(), size, listener.local_addr().unwrap());
        client_info.send(&stream);

        let daemon_reply = DaemonReplyPkt::receive(&stream);

        Ok(HeimdallrClient {job, size, id:daemon_reply.id,
            client_sockets:daemon_reply.client_sockets, listener,
            client_listeners: daemon_reply.client_listeners})
    }

    pub fn send<T>(&self, data: &T, dest: u32) -> Result<(), &'static str>
    {
        let mut stream = client_connect(self.client_listeners.get(dest as usize).unwrap()).unwrap();

        stream.write(b"Hello from other client");

        Ok(())
    }

    pub fn receive<T>(&self, source: u32) -> Result<(), &'static str>
    {
        let mut stream = client_listen(&self.listener).unwrap();

        let mut buf = String::new();
        stream.read_to_string(&mut buf);

        println!("Received: {}", buf);

        Ok(())
    }
}

impl fmt::Display for HeimdallrClient
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        write!(f, "HeimdallClient:\n  Job: {}\n  Size: {}\n  Client id: {}",
            self.job, self.size, self.id)
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
        let msg = serde_json::to_string(&self).unwrap();
        stream.write(msg.as_bytes()).unwrap();
    }

    pub fn receive(stream: &TcpStream) -> ClientInfoPkt
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
    pub client_sockets: Vec<SocketAddr>,
    pub client_listeners: Vec<SocketAddr>,
}

impl DaemonReplyPkt
{
    pub fn new(id: u32, clients: &Vec<TcpStream>, client_listeners: &Vec<SocketAddr>) -> DaemonReplyPkt
    {
        let mut client_sockets = Vec::<SocketAddr>::new();

        for stream in clients.iter()
        {
            client_sockets.push(stream.peer_addr().unwrap());
        }

        DaemonReplyPkt {id, client_sockets,client_listeners: client_listeners.to_vec()}
    }

    pub fn send(&self, mut stream: &TcpStream)
    {
        let msg = serde_json::to_string(&self).unwrap();
        stream.write(msg.as_bytes()).unwrap();
    }

    pub fn receive(stream: &TcpStream) -> DaemonReplyPkt
    {
        let mut de = serde_json::Deserializer::from_reader(stream);
        let daemon_reply = DaemonReplyPkt::deserialize(&mut de).unwrap();

        daemon_reply
    }
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

fn client_connect(addr: &SocketAddr) -> std::io::Result<TcpStream>
{
    let stream = TcpStream::connect(addr)?;
    Ok(stream)
}

fn client_listen(listener: &TcpListener) -> std::io::Result<TcpStream>
{
    let (stream, _) = listener.accept()?;
    Ok(stream)
}
