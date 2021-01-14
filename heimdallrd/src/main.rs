use std::process;
use std::collections::HashMap;
use std::net::{TcpStream, TcpListener};
use std::net::{SocketAddr,IpAddr};
use std::io::Write;
use std::path::Path;
use std::env;
use std::fs;
use std::str::FromStr;
use std::collections::VecDeque;

use serde::de::Deserialize;

use local_ipaddress;

use heimdallr::DaemonPkt;
use heimdallr::DaemonPktType;
use heimdallr::ClientInfoPkt;
use heimdallr::MutexCreationPkt;
use heimdallr::MutexCreationReplyPkt;
use heimdallr::MutexLockReqPkt;
use heimdallr::MutexWriteAndReleasePkt;
use heimdallr::BarrierPkt;
use heimdallr::BarrierReplyPkt;
use heimdallr::DaemonReplyPkt;
use heimdallr::DaemonConfig;


struct Daemon
{
    name: String,
    partition: String, 
    client_addr: SocketAddr,
    daemon_addr: SocketAddr,
    client_listener: TcpListener,
    _daemon_listener: TcpListener,
    jobs: HashMap<String, Job>,
    connection_count: u64,
}


impl Daemon
{
    fn new(name: String, partition: String,) -> std::io::Result<Daemon>
    {
        // Get IP of this node
        let ip = match local_ipaddress::get()
        {
            Some(i) => IpAddr::from_str(&i).unwrap(),
            None => IpAddr::from_str("0.0.0.0").unwrap(),
        };
        let client_addr = SocketAddr::new(ip, 4664);
        let client_listener = TcpListener::bind(client_addr)?;

        let daemon_addr = SocketAddr::new(ip, 4665);
        let _daemon_listener = TcpListener::bind(daemon_addr)?;

        let jobs = HashMap::<String, Job>::new();

        let connection_count: u64 = 0;

        let daemon = Daemon{name, partition, client_addr, daemon_addr,
                 client_listener, _daemon_listener, jobs, connection_count};

        daemon.create_partition_file()?;

        Ok(daemon)
    }

    fn create_partition_file(&self) -> std::io::Result<()>
    {
        let config_home = match env::var("XDG_CONFIG_HOME")
        {
            Ok(path) => path,
            Err(_) => 
            {
                eprintln!("XDG_CONFIG_HOME is not set. Falling back to default path: ~/.config");
                let home = env::var("HOME").unwrap();
                format!("{}/.config", home)
            },
        };

        let path = format!("{}/heimdallr/{}", config_home, &self.partition);
        if Path::new(&path).exists() == false
        {
            fs::create_dir_all(&path)?;
        }

        let daemon_config = DaemonConfig::new(self.name.clone(), self.partition.clone(),
                 self.client_addr.clone(), self.daemon_addr.clone());

        let file_path = format!("{}/{}", path, self.name);
        let serialzed = serde_json::to_string(&daemon_config).unwrap();
        fs::write(&file_path, serialzed)?;
        println!("Writing heimdallr daemon config to: {}", file_path);

        Ok(())
    }

    fn handle_client_connection(&mut self, stream: TcpStream) -> std::io::Result<()>
    {
        println!("New client connected from: {}", stream.peer_addr()?);
        self.connection_count += 1;
        println!("CONNECTION COUNT: {}", self.connection_count);

        let mut de = serde_json::Deserializer::from_reader(&stream);
        let pkt = DaemonPkt::deserialize(&mut de).unwrap();

        println!("  Pkt type: {:?}", pkt.pkt_type);

        match pkt.pkt_type
        {
            DaemonPktType::ClientInfoPkt =>
            {
                let client_info = serde_json::from_str::<ClientInfoPkt>(&pkt.pkt).unwrap();

                let job = self.jobs.entry(client_info.job.clone())
                    .or_insert(Job::new(client_info.job, client_info.size).unwrap());

                job.clients.push(stream);
                job.client_listeners.push(client_info.listener_addr);

                if job.clients.len() as u32 == (job.size)
                {
                    println!("  All {} clients for job: {} have been found.", job.size, job.name);
                    for (idx, stream) in job.clients.iter().enumerate()
                    {
                        println!("    {} : {}", idx, stream.peer_addr().unwrap());
                        let reply = DaemonReplyPkt::new(idx as u32, &job.client_listeners);
                        reply.send(&stream);
                    }
                }
            },
            DaemonPktType::MutexCreationPkt =>
            {
                let mutex_pkt = serde_json::from_str::<MutexCreationPkt>(&pkt.pkt).unwrap();

                let job = self.jobs.get_mut(&pkt.job).unwrap();
                let mutex = job.mutexes.entry(mutex_pkt.name.clone())
                    .or_insert(HeimdallrDaemonMutex::new(mutex_pkt.name.clone(), job.size, mutex_pkt.start_data.clone()));
                
                mutex.register_client(stream);
            },
            DaemonPktType::MutexLockReqPkt =>
            {
                let lock_req_pkt = serde_json::from_str::<MutexLockReqPkt>(&pkt.pkt).unwrap();

                let job = self.jobs.get_mut(&pkt.job).unwrap();
                let mutex = job.mutexes.get_mut(&lock_req_pkt.name);

                match mutex
                {
                    Some(m) => m.access_request(lock_req_pkt.listener_addr),
                    None => eprintln!("Error: the requested mutex was not found."),
                }
            },
            DaemonPktType::MutexWriteAndReleasePkt =>
            {
                let write_pkt = serde_json::from_str::<MutexWriteAndReleasePkt>(&pkt.pkt).unwrap();

                let job = self.jobs.get_mut(&pkt.job).unwrap();
                let mutex = job.mutexes.get_mut(&write_pkt.mutex_name);

                match mutex
                {
                    Some(m) =>
                    {
                        m.data = write_pkt.data;
                        m.release_request();
                    },
                    None => eprintln!("Error: the requested mutex was not found"),
                }
            },
            DaemonPktType::BarrierPkt =>
            {
                let barrier_pkt = serde_json::from_str::<BarrierPkt>(&pkt.pkt).unwrap();
                println!("received barrier pkt from client {}", barrier_pkt.id);

                let job = self.jobs.get_mut(&pkt.job).unwrap();
                let barrier = &mut job.barrier;

                if barrier.size == 0
                {
                    barrier.start(barrier_pkt.size);
                }

                barrier.register_client(stream);
            },
        }

        Ok(())
    }
    
}

struct Job
{
    name: String,
    size: u32,
    clients: Vec<TcpStream>,
    client_listeners: Vec<SocketAddr>,
    mutexes: HashMap::<String, HeimdallrDaemonMutex>,
    barrier: DaemonBarrier,
}

impl Job
{
    fn new(name: String, size: u32) -> Result<Job, &'static str>
    {
        let clients = Vec::<TcpStream>::new();
        let client_listeners = Vec::<SocketAddr>::new();
        let mutexes = HashMap::<String, HeimdallrDaemonMutex>::new();
        let barrier = DaemonBarrier::new();
        Ok(Job {name, size, clients, client_listeners, mutexes, barrier})
    }
}


struct HeimdallrDaemonMutex
{
    name: String,
    size: u32,
    constructed: bool,
    clients: Vec::<TcpStream>,
    data: String,
    access_queue: VecDeque::<SocketAddr>,
    locked: bool,
    current_owner: Option::<SocketAddr>,
}

//TODO destroy function
impl HeimdallrDaemonMutex
{
    pub fn new(name: String, size: u32, start_data: String) -> HeimdallrDaemonMutex
    {
        let clients = Vec::<TcpStream>::new();
        let data = start_data;
        let access_queue = VecDeque::<SocketAddr>::new();
        
        HeimdallrDaemonMutex{name, size, constructed: false, clients, data, access_queue,
            locked:false, current_owner: None}
    }

    // TODO check for duplicate clients?
    pub fn register_client(&mut self, stream: TcpStream)
    {
        println!("register_client function entry");
        self.clients.push(stream);

        if self.clients.len() as u32 == self.size
        {
            for stream in &mut self.clients
            {
                let reply_pkt = MutexCreationReplyPkt::new(&self.name);
                reply_pkt.send(stream).unwrap();
            }
            self.constructed = true;
            println!("Mutex: {} has been created", self.name);
        }
        println!("register_client function exit");
    }

    pub fn access_request(&mut self, addr: SocketAddr)
    {
        println!("access_request function entry");
        self.access_queue.push_back(addr);
        self.grant_next_lock();
        println!("access_request function exit");
    }

    pub fn release_request(&mut self)
    {
        println!("release_request function entry");
        if self.locked
        {
            self.locked = false;
            self.current_owner = None;
            self.grant_next_lock();
        }
        else
        {
            eprintln!("Error: Release request on Mutex that was not locked");
        }
        println!("release_request function exit");
    }

    fn send_data(&mut self) -> Result<(), &'static str>
    {
        println!("send_data function entry");
        match self.current_owner
        {
            Some(addr) =>
            {
                println!("  Sending mutex data: {}", self.data);
                let mut stream = TcpStream::connect(addr).unwrap();
                stream.write(self.data.as_bytes()).unwrap();
                stream.flush().unwrap();
                stream.shutdown(std::net::Shutdown::Both).unwrap();
                println!("send_data function exit");
                Ok(())
            },
            None => Err("Error: Mutex has no current valid owner to send data to"),
        }
    }

    fn grant_next_lock(&mut self)
    {
        println!("grant_next_lock function entry");
        if (!self.locked) & (!self.access_queue.is_empty())
        {
            self.current_owner = self.access_queue.pop_front();
            self.locked = true;
            println!("Ownership of mutex given");
            self.send_data().unwrap();
        }
        println!("grant_next_lock function exit");
    }
}


struct DaemonBarrier
{
    size: u32,
    clients: Vec::<TcpStream>,
}

impl DaemonBarrier
{
    pub fn new() -> DaemonBarrier
    {
        let size = 0;
        let clients = Vec::<TcpStream>::new();
        
        DaemonBarrier {size, clients}
    }

    pub fn start(&mut self, size: u32)
    {
        self.size = size;
    }

    pub fn register_client(&mut self, stream: TcpStream)
    {
        self.clients.push(stream);

        if self.clients.len() as u32 == self.size
        {
            for stream in &mut self.clients
            {
                let reply_pkt = BarrierReplyPkt::new(self.size);
                reply_pkt.send(stream).unwrap();
            }
            self.size = 0;
            self.clients = Vec::<TcpStream>::new();
            println!("Barrier has completed");
        }
    }
}




fn run(mut daemon: Daemon) -> Result<(), &'static str>
{
    println!("Listening for new client connections.");
    for stream in daemon.client_listener.try_clone().unwrap().incoming()
    {
        match stream
        {
            Ok(stream) =>
            {
                daemon.handle_client_connection(stream).unwrap();
            },
            Err(e) =>
            {
                eprintln!("Error in daemon listening to incoming connections: {}", e);
            }
        }
    }
    Ok(())
}

fn parse_args(mut args: std::env::Args) -> Result<(String, String), &'static str>
{
    args.next();

    let mut partition = String::new();
    let mut name = String::new();

    while let Some(arg) = args.next()
    {
        match arg.as_str()
        {
            "-p" | "--partition" => 
            {
                partition = match args.next()
                {
                    Some(p) => p.to_string(),
                    None => return Err("No valid partition name given."),
                };
            },
            "-n" | "--name" => 
            {
                name = match args.next()
                {
                    Some(n) => n.to_string(),
                    None => return Err("No valid daemon name given."),
                };
            },
            _ => return Err("Unknown argument error."),
        };
    }

    Ok((name, partition))
}


fn main() 
{
    let (name, partition) = parse_args(env::args()).unwrap_or_else(|err|
        {
            eprintln!("Error: Problem parsing arguments: {}", err);
            process::exit(1);
        });
            
    let daemon = Daemon::new(name, partition).unwrap_or_else(|err|
        {
            eprintln!("Error: Could not start daemon correctly: {} \n Shutting down.", err);
            process::exit(1);
        });

    println!("Daemon running under name: {} and address: {}", daemon.name, daemon.client_addr);

    run(daemon).unwrap_or_else(|err|
        {
            eprintln!("Error in running daemon: {}", err);
        });


    println!("Daemon shutting down.");
}
