use std::process;
use std::collections::HashMap;
use std::net::{TcpStream, TcpListener};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::io::{Write, Read};

use heimdallr::ClientInfoPkt;
use heimdallr::DaemonReplyPkt;


struct Daemon
{
    name: String,
    addr: SocketAddrV4,
    listener: TcpListener,
    jobs: HashMap<String, Job>,
}

impl Daemon
{
    fn new(name: String, addr: SocketAddrV4) -> std::io::Result<Daemon>
    {
        let listener = TcpListener::bind(addr)?;
        let jobs = HashMap::<String, Job>::new();
        
        Ok(Daemon {name, addr, listener, jobs})
    }

    fn new_connection(&mut self, stream: TcpStream) -> std::io::Result<()>
    {
        println!("New client connected from: {}", stream.peer_addr()?);

        let client_info = ClientInfoPkt::receive(&stream);

        let job = self.jobs.entry(client_info.job.clone()).or_insert(Job::new(client_info.job, client_info.size).unwrap());

        job.clients.push(stream);

        if job.clients.len() as u32 == (job.size)
        {
            println!("All {} clients for job: {} have been found.", job.size, job.name);
            for (idx, mut stream) in job.clients.iter().enumerate()
            {
                // TODO make nicer
                println!("{} : {}", idx, stream.peer_addr().unwrap());
                let reply = DaemonReplyPkt{id:idx as u32};
                let msg = serde_json::to_string(&reply).unwrap();
                stream.write(msg.as_bytes());
            }
        }

        Ok(())
    }
    
}

struct Job
{
    name: String,
    size: u32,
    clients: Vec<TcpStream>,
}

impl Job
{
    fn new(name: String, size: u32) -> Result<Job, &'static str>
    {
        let clients = Vec::<TcpStream>::new();
        Ok(Job {name, size, clients})
    }
}


fn run(mut daemon: Daemon) -> Result<(), &'static str>
{
    println!("Listening for new client connections.");
    for stream in daemon.listener.try_clone().unwrap().incoming()
    {
        match stream
        {
            Ok(stream) =>
            {
                daemon.new_connection(stream).unwrap();
            },
            Err(e) =>
            {
                eprintln!("Error in daemon listening to incoming connections: {}", e);
            }
        }
    }
    Ok(())
}


fn main() 
{
    // TODO: Make port/name read from config or take them as args
    let name = String::from("heimdallrd");
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4664);
    let daemon = Daemon::new(name, addr).unwrap_or_else(|err|
        {
            eprintln!("Could not start daemon correctly: {} \n Shutting down.", err);
            process::exit(1);
        });

    println!("Daemon running under name: {} and address: {}", daemon.name, daemon.addr);

    run(daemon).unwrap_or_else(|err|
        {
            eprintln!("Error in running daemon: {}", err);
        });


    println!("Daemon shutting down.");
}
