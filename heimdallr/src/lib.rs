use std::process;
use std::net::{TcpStream, TcpListener};
use std::net::{SocketAddr, IpAddr};
use std::io::{Write, BufReader};
use std::fmt;
use std::env;
use std::fs::File;
use std::str::FromStr;

use serde::{Serialize, Deserialize};
use local_ipaddress;

pub struct HeimdallrClient
{
    pub job: String,
    pub size: u32,
    pub id: u32,
    pub listener: TcpListener,
    pub client_listeners: Vec<SocketAddr>,
}

impl HeimdallrClient
{
    pub fn init(mut args: std::env::Args)
        -> Result<HeimdallrClient, &'static str>
    {

        let mut job = match args.next()
        {
            Some(arg) => arg,
            None => "".to_string(),
        };

        let mut partition = "".to_string();
        let mut size: u32 = 0;
        let mut node = "".to_string();

        while let Some(arg) = args.next()
        {
            match arg.as_str()
            {
                "-p" | "--partition" => 
                {
                    partition = match args.next()
                    {
                        Some(p) => p,
                        None => return Err("Error in partition argument."),
                    };
                },
                "-j" | "--jobs" => 
                {
                    size = match args.next()
                    {
                        Some(s) => s.parse().unwrap(),
                        None => return Err("Error in setting job count."),
                    };
                },
                "-n" | "--node" => 
                {
                    node = match args.next()
                    {
                        Some(n) => n,
                        None => return Err("Error in setting node."),
                    };
                },
                "--job-name" =>
                {
                    job = match args.next()
                    {
                        Some(jn) => jn,
                        None => return Err("Error in setting job-name."),
                    };
                },
                _ => (),
            };
        }

        if partition.is_empty() | node.is_empty() | (size == 0)
        {
            eprintln!("Error: client did not provide all necessary arguments.\n  partition: {}\n  node: {}\n  jobs: {}\nShutting down.", &partition, &node, size);
            process::exit(1);
        }

        // Find daemon address from daemon config file
        let home = env::var("HOME").unwrap();
        let path = format!("{}/.config/heimdallr/{}/{}",home, &partition, &node);
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        let daemon_config: DaemonConfig = serde_json::from_reader(reader).unwrap();
        // let daemon_config = DaemonConfig::deserialize(&content).unwrap();


        let stream = connect(daemon_config.client_addr);

        // Get IP of this node
        let ip = match local_ipaddress::get()
        {
            Some(i) => IpAddr::from_str(&i).unwrap(),
            None => IpAddr::from_str("0.0.0.0").unwrap(),
        };
        let listener = TcpListener::bind(format!("{}:0", ip)).unwrap();
        
        let client_info = ClientInfoPkt::new(job.clone(), size, listener.local_addr().unwrap());
        client_info.send(&stream);

        let daemon_reply = DaemonReplyPkt::receive(&stream);

        
        Ok(HeimdallrClient {job, size, id:daemon_reply.id,
            listener, client_listeners: daemon_reply.client_listeners})
    }

    pub fn send<T>(&self, data: &T, dest: u32) -> Result<(), &'static str>
        where T: Serialize,
    {
        let mut stream = client_connect(self.client_listeners.get(dest as usize).unwrap()).unwrap();

        let msg = serde_json::to_string(data).unwrap();
        stream.write(msg.as_bytes()).unwrap();

        Ok(())
    }

    pub fn receive<'a, T>(&self, source: u32) -> Result<T, &'static str>
        where T: Deserialize<'a>,
    {
        let stream = client_listen(&self.listener).unwrap();

        let mut de = serde_json::Deserializer::from_reader(stream);
        let data = T::deserialize(&mut de).unwrap();

        Ok(data)
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
pub struct DaemonConfig
{
    pub name: String,
    pub partition: String,
    pub client_addr: SocketAddr,
    pub daemon_addr: SocketAddr,
}

impl DaemonConfig
{
    pub fn new(name: String, partition: String, client_addr: SocketAddr, daemon_addr: SocketAddr)
        -> DaemonConfig
    {
        DaemonConfig{name, partition, client_addr, daemon_addr}
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

fn connect(addr: SocketAddr) -> TcpStream
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
