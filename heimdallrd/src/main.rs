use std::process;
use std::collections::HashMap;
use std::net::{TcpStream, TcpListener};
use std::net::{SocketAddr,IpAddr};
use std::path::Path;
use std::env;
use std::fs;
use std::str::FromStr;

use local_ipaddress;

use heimdallr::ClientInfoPkt;
use heimdallr::DaemonReplyPkt;
use heimdallr::DaemonConfig;


struct Daemon
{
    name: String,
    partition: String, 
    client_addr: SocketAddr,
    daemon_addr: SocketAddr,
    client_listener: TcpListener,
    daemon_listener: TcpListener,
    jobs: HashMap<String, Job>,
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
        let daemon_listener = TcpListener::bind(daemon_addr)?;

        let jobs = HashMap::<String, Job>::new();

        let daemon = Daemon{name, partition, client_addr, daemon_addr,
                 client_listener, daemon_listener, jobs};

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

    fn new_connection(&mut self, stream: TcpStream) -> std::io::Result<()>
    {
        println!("New client connected from: {}", stream.peer_addr()?);

        let client_info = ClientInfoPkt::receive(&stream);

        let job = self.jobs.entry(client_info.job.clone()).or_insert(Job::new(client_info.job, client_info.size).unwrap());

        job.clients.push(stream);
        job.client_listeners.push(client_info.listener_addr);

        if job.clients.len() as u32 == (job.size)
        {
            println!("All {} clients for job: {} have been found.", job.size, job.name);
            for (idx, stream) in job.clients.iter().enumerate()
            {
                println!("{} : {}", idx, stream.peer_addr().unwrap());
                let reply = DaemonReplyPkt::new(idx as u32, &job.client_listeners);
                reply.send(&stream);
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
    client_listeners: Vec<SocketAddr>,
}

impl Job
{
    fn new(name: String, size: u32) -> Result<Job, &'static str>
    {
        let clients = Vec::<TcpStream>::new();
        let client_listeners = Vec::<SocketAddr>::new();
        Ok(Job {name, size, clients, client_listeners})
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
