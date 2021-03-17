pub mod networking;

use std::process;
use std::net::{SocketAddr, IpAddr,TcpListener, TcpStream};
use std::io::{Write, BufReader};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::{fmt, env, thread};
use std::fs::File;
use std::str::FromStr;

use serde::{Serialize, Deserialize};
use local_ipaddress;
use pnet::datalink;

use crate::networking::*;


pub struct HeimdallrClient
{
    pub job: String,
    pub size: u32,
    pub id: u32,
    pub listener: TcpListener,
    pub client_listeners: Vec<SocketAddr>,
    readers: Arc<Mutex<HashMap<(u32,u32),SocketAddr>>>,
    pub cmd_args: Vec<String>,
    daemon_stream: TcpStream,
}

impl HeimdallrClient
{
    pub fn init(mut args: std::env::Args) -> Result<HeimdallrClient, &'static str>
    {

        let mut job = match args.next()
        {
            Some(arg) => arg,
            None => "".to_string(),
        };

        let mut partition = "".to_string();
        let mut size: u32 = 0;
        let mut node = "".to_string();
        let mut cmd_args = Vec::<String>::new();
        let mut interface = "".to_string();

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
                "--interface" =>
                {
                    interface = match args.next()
                    {
                        Some(i) => i,
                        None => return Err("No valid network interface name given."),
                    }
                },
                "--args" =>
                {
                    while let Some(a) = args.next()
                    {
                        cmd_args.push(a);
                    }
                    break;
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
        let home = env::var("HOME").expect("HOME environment variable is not set");
        let path = format!("{}/.config/heimdallr/{}/{}",home, &partition, &node);
        let file = File::open(&path)
            .expect(&format!("Could not open node file for this job at {}", path));
        let reader = BufReader::new(file);
        let daemon_config: DaemonConfig = serde_json::from_reader(reader)
            .expect("Could not parse DaemonConfig file");

        let mut stream = networking::connect(&daemon_config.client_addr)
            .expect(&format!("Could not connect to daemon at: {}", daemon_config.client_addr));

        // Get IP of this node
        let mut ip = match local_ipaddress::get()
        {
            Some(i) => IpAddr::from_str(&i).expect("Error in setting local ip address"),
            None => IpAddr::from_str("0.0.0.0").expect("Error in setting local ip address"),
        };

        // If specified use the given network interface
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

        let listener = networking::bind_listener(&format!("{}:0", ip))
            .expect("Could not create listener for this client");
        
        let client_reg = ClientRegistrationPkt::new(&job, size, listener.local_addr().unwrap());
        client_reg.send(&mut stream).expect("Could not send ClientRegistrationPkt");

        let reply = ClientRegistrationReplyPkt::receive(&stream)
            .expect("Error in receiving daemon reply");

        let readers = Arc::new(Mutex::new(HashMap::<(u32,u32),SocketAddr>::new()));
        
        let client = HeimdallrClient {job, size, id:reply.id,
            listener, client_listeners: reply.client_listeners,
            readers, cmd_args, daemon_stream: stream};

        // Start listener handler thread that handles incoming connections from other clients
        client.listener_handler();

        Ok(client)
    }

    pub fn listener_handler(&self)
    {
        let listener = self.listener.try_clone().unwrap();
        let readers = Arc::clone(&self.readers);

        thread::spawn(move || 
        {
            for stream in listener.incoming()
            {
                match stream
                {
                    Ok(stream) =>
                    {
                        let op_pkt = ClientOperationPkt::receive(&stream);
                        let mut r = readers.lock().expect("Error in locking 'readers' Mutex");
                        // TODO check that no such entry already exists and handle
                        // that case
                        r.insert((op_pkt.client_id, op_pkt.op_id), op_pkt.addr);
                    },
                    Err(e) =>
                    {
                        eprintln!("Error in daemon listening to incoming connections: {}", e);
                    }
                }
            }
        });
    }

    pub fn send<T>(&self, data: &T, dest: u32, id: u32) -> std::io::Result<()>
        where T: Serialize,
    {
        let mut stream = networking::connect(self.client_listeners.get(dest as usize).unwrap())?;

        let ip = self.listener.local_addr()?.ip();
        let op_listener = networking::bind_listener(&format!("{}:0", ip))
            .expect("Could not create listener for this send operation");

        let op_pkt = ClientOperationPkt::new(self.id, id, op_listener.local_addr()?);   
        op_pkt.send(&mut stream)?;

        let (mut stream2, _) = op_listener.accept()?;
        let msg = bincode::serialize(data).expect("Error in serializing data");
        stream2.write(msg.as_slice())?;
        stream2.flush()?;
        
        Ok(())
    }

    pub fn send_slice<T>(&self, data: &[T], dest: u32, id: u32) -> std::io::Result<()>
        where T: Serialize,
    {
        let mut stream = networking::connect(self.client_listeners.get(dest as usize).unwrap())?;

        let ip = self.listener.local_addr()?.ip();
        let op_listener = networking::bind_listener(&format!("{}:0", ip))?;
        let op_pkt = ClientOperationPkt::new(self.id, id, op_listener.local_addr()?);   
        op_pkt.send(&mut stream)?;

        let (mut stream2, _) = op_listener.accept()?;
        let msg = bincode::serialize(data).expect("Could not serialize send_slice data");
        stream2.write(msg.as_slice())?;
        stream2.flush()?;
        
        Ok(())
    }

    pub fn receive<T>(&self, source: u32, id: u32) -> std::io::Result<T>
        where T: serde::de::DeserializeOwned,
    {
        loop
        {
            let mut r = self.readers.lock().expect("Could not lock 'readers' Mutex");
            let addr = r.remove(&(source,id));
            match addr
            {
                Some(a) =>
                {
                    let stream = networking::connect(&a)?;
                    let reader = BufReader::new(&stream);
                    let data: T = bincode::deserialize_from(reader)
                        .expect("Could not deserialize received data");
                    return Ok(data);
                },
                None => continue,
            }
        }
    }

    pub fn receive_any_source<T>(&self, id: u32) -> std::io::Result<T>
        where T: serde::de::DeserializeOwned,
    {
        loop
        {
            let mut r = self.readers.lock().expect("Could not lock 'readers' Mutex");
            let mut key: Option<(u32,u32)> = None;
            for k in r.keys()
            {
                if k.1 == id 
                {
                    key = Some(k.clone());
                    break;
                }
            }

            match key
            {
                Some(k) =>
                {
                    let addr = r.remove(&k);
                    match addr
                    {
                        Some(a) =>
                        {
                            let stream = networking::connect(&a)?;
                            let reader = BufReader::new(&stream);
                            let data: T = bincode::deserialize_from(reader)
                                .expect("Could not deserialize data in receive_any_source");
                            return Ok(data);
                        },
                        None => continue,
                    }
                },
                None => (),
            }
        }
    }


    pub fn send_nb<T>(&self, data: T, dest: u32, id: u32) 
        -> std::io::Result<NbDataHandle<std::io::Result<T>>>
        where T: Serialize + std::marker::Send + 'static
    {
        let dest_addr = self.client_listeners.get(dest as usize).unwrap().clone();
        let ip = self.listener.local_addr()?.ip();
        let self_id = self.id;
        let t = thread::spawn(move || 
            {
                let mut stream = networking::connect(&dest_addr)?;
                let op_listener = networking::bind_listener(&format!("{}:0", ip))?;
                let op_pkt = ClientOperationPkt::new(self_id, id,
                    op_listener.local_addr()?);   
                op_pkt.send(&mut stream)?;

                let (mut stream2, _) = op_listener.accept()?;
                let msg = bincode::serialize(&data)
                    .expect("Could not serialize data in send_nb");
                stream2.write(msg.as_slice())?;
                stream2.flush()?;

                Ok(data)
            });
        
        Ok(NbDataHandle::<std::io::Result<T>>::new(t))
    }


    pub fn receive_nb<T>(&self, source: u32, id: u32) 
        -> std::io::Result<NbDataHandle<std::io::Result<T>>>
        where T: serde::de::DeserializeOwned + std::marker::Send + 'static,
    {
        let readers = Arc::clone(&self.readers);

        let t = thread::spawn(move ||
            {
                loop
                {
                    let mut r = readers.lock().expect("Could not lock 'readers' Mutex");
                    let addr = r.remove(&(source,id));
                    match addr
                    {
                        Some(a) =>
                        {
                            let stream = networking::connect(&a)?;
                            let reader = BufReader::new(&stream);
                            let data: T = bincode::deserialize_from(reader)
                                .expect("Could not deserialize received data in receive_nb");
                            return Ok(data);
                        },
                        None => continue,
                    }
                }
            });

        Ok(NbDataHandle::<std::io::Result<T>>::new(t))
    }


    pub fn create_mutex<T>(&mut self, name: &str, start_data: T) 
        -> std::io::Result<HeimdallrMutex<T>>
        where T: Serialize
    {
        Ok(HeimdallrMutex::<T>::new(self, name, start_data)?)
    }


    pub fn barrier(&mut self) -> std::io::Result<()>
    {
        let pkt = BarrierPkt::new(self.id, self.size, &self.job);
        pkt.send(&mut self.daemon_stream)?;
        BarrierReplyPkt::receive(&self.daemon_stream).expect("Could not receive BarrierReplyPkt");
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

impl Drop for HeimdallrClient
{
    fn drop(&mut self)
    {
        // let mut stream = networking::connect(&self.daemon_addr)
        //     .expect("Could not connect to daemin in finalization procedure of HeimdallrClient");

        let finalize_pkt = FinalizePkt::new(self.id, self.size, &self.job);
        finalize_pkt.send(&mut self.daemon_stream).expect("Could not send FinalizePkt");
        self.daemon_stream.flush().expect("Error in flushing stream");
        FinalizeReplyPkt::receive(&self.daemon_stream).expect("Could not receive FinalizeReplyPkt");
    }
}


#[derive(Debug)]
pub struct NbDataHandle<T>
{
    t: thread::JoinHandle<T>
}

impl<T> NbDataHandle<T>
{
    pub fn new(t: thread::JoinHandle<T>) -> NbDataHandle<T>
    {
        NbDataHandle::<T>{t}
    }

    pub fn data(self) -> T
    {
        let data = self.t.join().expect("Error in joining thread of NbDataHandle");
        data
    }
}


pub struct HeimdallrMutex<T>
{
    name: String,
    job: String,
    daemon_stream: TcpStream,
    client_id: u32,
    data: T,
}

impl<'a, T> HeimdallrMutex<T>
    where T: Serialize,
{
    pub fn new(client: &mut HeimdallrClient, name: &str,  start_value: T) 
        -> std::io::Result<HeimdallrMutex<T>>
    {
        let ser_data = bincode::serialize(&start_value)
            .expect("Could not serialize Mutex's start value");
        let pkt = MutexCreationPkt::new(name, client.id, ser_data, &client.job);
        // let mut stream = networking::connect(&client.daemon_addr)?;
        pkt.send(&mut client.daemon_stream)?;

        let reply = MutexCreationReplyPkt::receive(&client.daemon_stream)
            .expect("Could not receive MutexCreationReplyPkt");

        if reply.name != name
        {
            panic!("Error: miscommunication in mutex creation. Name mismatch")
        }

        Ok(HeimdallrMutex::<T>{name: name.to_string(), job: client.job.clone(),
            daemon_stream: client.daemon_stream.try_clone().unwrap(), 
            client_id: client.id,
            data: start_value})
    }

    pub fn lock(&'a mut self) -> std::io::Result<HeimdallrMutexDataHandle::<'a,T>>
        where T: serde::de::DeserializeOwned,
    {
        // TODO remove return socketaddr from packet
        // let mut stream = networking::connect(&self.daemon_addr)?;
        // let ip = self.client_addr.ip();
        // let op_listener = networking::bind_listener(&format!("{}:0", ip))?;

        let lock_req_pkt = MutexLockReqPkt::new(&self.name, self.client_id,&self.job);
        lock_req_pkt.send(&mut self.daemon_stream)?;


        // let (stream2, _) = op_listener.accept()?;
        let reader = BufReader::new(&self.daemon_stream);
        self.data = bincode::deserialize_from(reader)
            .expect("Could not deserialize mutex data");

        Ok(HeimdallrMutexDataHandle::<T>::new(self))
    }

    fn push_data(&mut self) -> std::io::Result<()> 
    {
        // let mut stream = networking::connect(&self.daemon_addr)?;
        let ser_data = bincode::serialize(&self.data)
            .expect("Could not serialize Mutex data");
        let write_pkt = MutexWriteAndReleasePkt::new(&self.name, ser_data, &self.job);
        write_pkt.send(&mut self.daemon_stream)?;
        self.daemon_stream.flush()?;
        Ok(())
    }
}


pub struct HeimdallrMutexDataHandle<'a,T>
    where T: Serialize+ Deserialize<'a>,
{
    mutex: &'a mut HeimdallrMutex<T>,
}

impl<'a,T> HeimdallrMutexDataHandle<'a,T>
    where T: Serialize + Deserialize<'a>,
{
    pub fn new(mutex: &'a mut HeimdallrMutex<T>) 
        -> HeimdallrMutexDataHandle<'a,T>
    {
        HeimdallrMutexDataHandle::<'a,T>{mutex}
    }

    pub fn get(&self) -> &T
    {
        &self.mutex.data
    }

    pub fn set(&mut self, value: T)
    {
        self.mutex.data = value;
    }
}

impl<'a,T> Drop for HeimdallrMutexDataHandle<'a,T>
    where T: Serialize + Deserialize<'a>,
{
    fn drop(&mut self)
    {
        self.mutex.push_data().expect("Error in pushing Mutex data to daemon");
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
    pub fn new(name: &str, partition: &str, client_addr: SocketAddr, daemon_addr: SocketAddr)
        -> DaemonConfig
    {
        DaemonConfig{name: name.to_string(), partition: partition.to_string(),
            client_addr, daemon_addr}
    }
}
