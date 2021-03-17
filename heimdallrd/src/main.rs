use std::process;
use std::collections::HashMap;
use std::net::{TcpStream, TcpListener, SocketAddr, IpAddr};
use std::io::Write;
use std::path::Path;
use std::{env, fs, thread};
use std::str::FromStr;
use std::collections::VecDeque;
use std::sync::{Mutex, Arc, Barrier};

use local_ipaddress;
use pnet::datalink;

use heimdallr::DaemonConfig;
use heimdallr::networking::*;


struct Daemon
{
    name: String,
    partition: String,
    client_listener_addr: SocketAddr,
    client_listener: TcpListener,
}

impl Daemon
{
    fn new(name: &str, partition: &str, interface: &str) -> std::io::Result<Daemon>
    {
        // Get IP of this node
        let mut ip = match local_ipaddress::get()
        {
            Some(i) => IpAddr::from_str(&i).unwrap(),
            None => IpAddr::from_str("0.0.0.0").unwrap(),
        };

        // Use the manually specified network interface
        if !interface.is_empty()
        {
            let interfaces = datalink::interfaces();
            for i in interfaces
            {
                if i.name == interface
                {
                    println!("Using specified network interface {} with ip {}",
                        i.name, i.ips[0]);
                    ip = i.ips[0].ip();

                }
            }
        }

        let client_listener_addr = SocketAddr::new(ip, 4664);

        let client_listener = heimdallr::networking::bind_listener(&client_listener_addr)?;

        let daemon = Daemon{name: name.to_string(), partition: partition.to_string(),
            client_listener_addr, client_listener};

        daemon.create_partition_file().unwrap();
        
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
                let home = env::var("HOME").expect("HOME environment variable is not set");
                format!("{}/.config", home)
            },
        };

        let path = format!("{}/heimdallr/{}", config_home, &self.partition);
        if Path::new(&path).exists() == false
        {
            fs::create_dir_all(&path)?;
        }

        let daemon_config = DaemonConfig::new(&self.name, &self.partition,
                 self.client_listener_addr.clone(), self.client_listener_addr.clone());

        let file_path = format!("{}/{}", path, self.name);
        let serialized = serde_json::to_string(&daemon_config)
            .expect("Could not serialize DaemonConfig");
        fs::write(&file_path, serialized)?;
        println!("Writing heimdallr daemon config to: {}", file_path);

        Ok(())
    }
}


struct Job
{
    size: u32,
    barrier: Mutex<DaemonBarrier>,
    finalize: Mutex<JobFinalization>,
    mutexes: Mutex<HashMap<String, HeimdallrDaemonMutex>>
}

impl Job
{
    fn new(size: u32) -> std::io::Result<Job>
    {
        // let clients = Vec::<TcpStream>::new();
        // let client_listeners = Vec::<SocketAddr>::new();
        let mutexes = Mutex::new(HashMap::<String, HeimdallrDaemonMutex>::new());
        let barrier = Mutex::new(DaemonBarrier::new(size));
        let finalize = Mutex::new(JobFinalization::new(size));
        // Ok(Job {name: name.to_string(), size, clients, client_listeners,
        //     mutexes, barrier, finalize})
        Ok(Job{size, barrier, finalize, mutexes})
    }
}


struct HeimdallrDaemonMutex
{
    name: String,
    streams: Vec<Option<TcpStream>>,
    constructed: bool,
    data: Vec<u8>,
    access_queue: VecDeque<u32>,
    locked: bool,
    current_owner: Option<u32>,
}

impl HeimdallrDaemonMutex
{
    fn new(name: &str, size: u32, start_data: Vec<u8>) -> Self
    {
        let mut streams = Vec::<Option<TcpStream>>::new();
        streams.resize_with(size as usize, || None);
        let access_queue = VecDeque::<u32>::new();

        Self {name: name.to_string(), streams, constructed: false, 
            data: start_data, access_queue, locked: false, current_owner: None}
    }

    fn register_client(&mut self, id: u32, stream: TcpStream)
    {
        self.streams[id as usize] = Some(stream);
        self.constructed = !self.streams.iter().any(|x| x.is_none());
    }

    fn access_request(&mut self, client_id: u32)
    {
        self.access_queue.push_back(client_id);
        self.grant_next_lock();
    }

    fn release_request(&mut self)
    {
        if self.locked
        {
            self.locked = false;
            self.current_owner = None;
            self.grant_next_lock();
        }
        else
        {
            eprintln!("Error: Release Request on Mutex that was not locked");
        }
    }

    fn grant_next_lock(&mut self)
    {
        if (!self.locked) & (!self.access_queue.is_empty())
        {
            self.current_owner = self.access_queue.pop_front();
            self.locked = true;
            self.send_data();
        }
    }

    fn send_data(&mut self)
    {
        match self.current_owner
        {
            Some(id) =>
            {
                let stream = self.streams.get_mut(id as usize).unwrap();
                match stream
                {
                    Some(s) =>
                    {
                        s.write(self.data.as_slice()).unwrap();
                        s.flush().unwrap();
                    },
                    None => eprintln!("Error: No valid TcpStream found for client"),
                }
            },
            None => eprintln!("Error: Mutex has no current owner to send data"),
        }
    }
}


struct DaemonBarrier
{
    size: u32,
    streams: Vec<Option<TcpStream>>,
    finished: bool,
}

impl DaemonBarrier
{
    fn new(size: u32) -> Self
    {
        let mut streams = Vec::<Option<TcpStream>>::new();
        streams.resize_with(size as usize, || None);

        Self {size, streams, finished: false}
    }

    fn register_client(&mut self, id: u32, stream: TcpStream)
    {
        self.streams[id as usize] = Some(stream);
        self.finished = !self.streams.iter().any(|x| x.is_none());
    }

    fn reset(&mut self)
    {
        self.streams = Vec::<Option<TcpStream>>::new();
        self.streams.resize_with(self.size as usize, || None);
        self.finished = false;
    }
}


struct JobFinalization
{
    streams: Vec<Option<TcpStream>>,
    finished: bool,
}

impl JobFinalization 
{
    fn new(size: u32) -> Self
    {
        let mut streams = Vec::<Option<TcpStream>>::new();
        streams.resize_with(size as usize, || None);

        Self {streams, finished: false}
    }

    fn register_client(&mut self, id: u32, stream: TcpStream)
    {
        self.streams[id as usize] = Some(stream);
        self.finished = !self.streams.iter().any(|x| x.is_none());
    }
}


fn handle_client(mut stream: TcpStream, job: Arc<Job>, thread_barrier: Arc<Barrier>)
{
    // println!("thread spawned for job: {}", job.name);

    loop
    {
        let pkt = DaemonPkt::receive(&stream);
        // println!("Received DaemonPkt: {:?}", pkt);

        match pkt.pkt
        {
            DaemonPktType::MutexCreation(mutex_pkt) =>
            {
                let mut mutexes = job.mutexes.lock().unwrap();
                let mutex = mutexes.entry(mutex_pkt.name.clone())
                    .or_insert(HeimdallrDaemonMutex::new(&mutex_pkt.name, job.size,
                            mutex_pkt.start_data));

                mutex.register_client(mutex_pkt.client_id, stream.try_clone().unwrap());
                drop(mutexes);

                thread_barrier.wait();
                let mut mutexes = job.mutexes.lock().unwrap();
                let mutex = mutexes.get_mut(&mutex_pkt.name).unwrap();
                if mutex.constructed
                {
                    let reply = MutexCreationReplyPkt::new(&mutex.name);
                    reply.send(&mut stream).expect("Could not send MutexCreationReplyPkt");
                }
                else
                {
                    eprintln!("Expected Mutex to be constructed at this point");
                }
            },
            DaemonPktType::MutexLockReq(mutex_pkt) =>
            {
                let mut mutexes = job.mutexes.lock().unwrap();
                let mutex = mutexes.get_mut(&mutex_pkt.name)
                    .expect("Mutex for MutexLockReq does not exist");
                mutex.access_request(mutex_pkt.id);
            
            },
            DaemonPktType::MutexWriteAndRelease(mutex_pkt) =>
            {
                // TODO check for correct client id?
                let mut mutexes = job.mutexes.lock().unwrap();
                let mutex = mutexes.get_mut(&mutex_pkt.mutex_name)
                    .expect("Mutex for MutexLockReq does not exist");
                mutex.data = mutex_pkt.data;
                mutex.release_request();
            },
            DaemonPktType::Barrier(barrier_pkt) =>
            {
                let mut barrier = job.barrier.lock().unwrap();
                barrier.register_client(barrier_pkt.id, stream.try_clone().unwrap());
                drop(barrier);

                thread_barrier.wait();
                let barrier = job.barrier.lock().unwrap();
                if barrier.finished
                {
                    let reply = BarrierReplyPkt::new(job.size);
                    reply.send(&mut stream).expect("Could not send BarrierReplyPkt");
                }
                else
                {
                    eprintln!("Expected all client to have participated in barrier already")
                }
                drop(barrier);

                let b_res = thread_barrier.wait();
                if b_res.is_leader()
                {
                    let mut barrier = job.barrier.lock().unwrap();
                    barrier.reset();
                }
                thread_barrier.wait();
            },
            //TODO Maybe use RwLock instead of mutex
            DaemonPktType::Finalize(finalize_pkt) =>
            {
                // TODO Cleanup
                let mut fini = job.finalize.lock().unwrap();
                fini.register_client(finalize_pkt.id, stream.try_clone().unwrap());
                drop(fini);
                thread_barrier.wait();
                let fini = job.finalize.lock().unwrap();
                if fini.finished
                {
                    let reply = FinalizeReplyPkt::new(job.size);
                    reply.send(&mut stream).expect("Could not send FinalizeReplyPkt");
                }
                else
                {
                    eprintln!("Expected to have already received all FinalizePkts")
                }
                drop(fini);
                thread_barrier.wait();
                return ()
            },
            _ => (),
        }
    }
}


fn run(daemon: Daemon) -> std::io::Result<()>
{   
    let mut job_name = "".to_string();
    let mut job_size = 0;
    let mut clients = Vec::<TcpStream>::new();
    let mut client_listeners = Vec::<SocketAddr>::new();

    for stream in daemon.client_listener.incoming()
    {
        match stream
        {
            Ok(stream) =>
            {
                let pkt = DaemonPkt::receive(&stream);

                match pkt.pkt
                {
                    DaemonPktType::ClientRegistration(client_reg) =>
                    {
                        // println!("Received ClientRegistrationPkt: {:?}", client_reg);
                        
                        if job_name.is_empty()
                        {
                            job_name = client_reg.job.clone();
                            job_size = client_reg.size;
                        }
                        
                        clients.push(stream);
                        client_listeners.push(client_reg.listener_addr);
                    }
                    _ => eprintln!("Unknown Packet type"),
                }
            },
            Err(e) =>
            {
                eprintln!("Error in daemon listening to incoming connections: {}", e);
            },
        }

        if clients.len() as u32 == job_size
        {
            break;
        }
    }

    println!("All clients for job have connected");
    let mut job_threads = Vec::<thread::JoinHandle<()>>::new();
    let job_arc = Arc::new(Job::new(job_size).unwrap());
    let thread_barrier = Arc::new(Barrier::new(job_size as usize));
    
    for id in 0..clients.len()
    {
        let mut stream = clients.remove(0);
        let reply = ClientRegistrationReplyPkt::new(id as u32, &client_listeners);
        reply.send(&mut stream)?;

        let job = Arc::clone(&job_arc);
        let barrier = Arc::clone(&thread_barrier);

        let t = thread::spawn(move||
        {
            handle_client(stream, job, barrier);
        });

        job_threads.push(t);
    }

    for t in job_threads
    {
        t.join().unwrap();
        println!("All job threads joined");
        process::exit(0);
    }
    Ok(())
}


fn parse_args(mut args: std::env::Args) -> Result<(String, String, String), &'static str>
{
    args.next();

    let mut partition = String::new();
    let mut name = String::new();
    let mut interface = String::new();

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
            "--interface" =>
            {
                interface = match args.next()
                {
                    Some(i) => i.to_string(),
                    None => return Err("No valid network interface name given."),
                }
            },
            _ => return Err("Unknown argument error."),
        };
    }
    Ok((name, partition, interface))
}


fn main() 
{
    let (name, partition, interface) = parse_args(env::args()).unwrap_or_else(|err|
    {
        eprintln!("Error: Problem parsing arguments: {}", err);
        process::exit(1);
    });
            
    let daemon = Daemon::new(&name, &partition, &interface).unwrap_or_else(|err|
    {
        eprintln!("Error: Could not start daemon correctly: {} \n Shutting down.", err);
        process::exit(1);
    });

    println!("Daemon running under name: {} and address: {}", daemon.name, daemon.client_listener_addr);

    run(daemon).unwrap_or_else(|err|
    {
        eprintln!("Error in running daemon: {}", err);
    });


    println!("Daemon shutting down.");
}








