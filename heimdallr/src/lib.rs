use std::process;
use std::net::{TcpStream, TcpListener};
use std::net::{SocketAddr, IpAddr};
use std::io::{Write, BufReader};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::fmt;
use std::env;
use std::thread;
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
    daemon_addr: SocketAddr,
    readers: Arc<Mutex<HashMap<(u32,u32),SocketAddr>>>,
    pub cmd_args: Vec<String>


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
        let mut cmd_args = Vec::<String>::new();

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


        let readers = Arc::new(Mutex::new(HashMap::<(u32,u32),SocketAddr>::new()));
        
        let client = HeimdallrClient {job, size, id:daemon_reply.id,
            listener, client_listeners: daemon_reply.client_listeners,
            daemon_addr: daemon_config.client_addr, readers, cmd_args};

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
                            let reader = BufReader::new(stream);
                            let mut de = serde_json::Deserializer::from_reader(reader);
                            let op_pkt = ClientOperationPkt::deserialize(&mut de).unwrap();
                            // println!("METADATA RECEIVED");
                            // println!("  PENDING:{}, {}", op_pkt.client_id, op_pkt.op_id);
                            let mut r = readers.lock().unwrap();
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

    pub fn send<T>(&self, data: &T, dest: u32, id: u32) -> Result<(), &'static str>
        where T: Serialize,
    {
        // println!("send function entry");
        let mut stream = client_connect(self.client_listeners.get(dest as usize).unwrap()).unwrap();

        let ip = self.listener.local_addr().unwrap().ip();
        let op_listener = TcpListener::bind(format!("{}:0", ip)).unwrap();
        let op_pkt = ClientOperationPkt{client_id: self.id, op_id: id, addr: op_listener.local_addr().unwrap()};   
        let op_msg = serde_json::to_string(&op_pkt).unwrap();
        stream.write(op_msg.as_bytes()).unwrap();
        stream.flush().unwrap();
        // println!("  metadata sent");

        let (mut stream2, _) = op_listener.accept().unwrap();
        let msg = serde_json::to_string(data).unwrap();
        stream2.write(msg.as_bytes()).unwrap();
        stream.flush().unwrap();
        
        // println!("send function exit");
        Ok(())
    }

    pub fn receive<'a, T>(&self, source: u32, id: u32) -> Result<T, &'static str>
        where T: Deserialize<'a>,
    {
        // println!("receive function entry");
        // println!(" LOOKING FOR: {}, {}", source, id);
        loop
        {
            let mut r = self.readers.lock().unwrap();
            let addr = r.remove(&(source,id));
            match addr
            {
                Some(a) =>
                {
                    // println!("  found metadata");
                    let stream = TcpStream::connect(a).unwrap();
                    let reader = BufReader::new(&stream);
                    let mut de = serde_json::Deserializer::from_reader(reader);
                    let data = T::deserialize(&mut de).unwrap();
                    // println!("receive function exit");
                    return Ok(data);
                },
                None => continue,
            }
        }
    }

    pub fn receive_any_source<'a, T>(&self, id: u32) -> Result<T, &'static str>
        where T: Deserialize<'a>,
    {
        loop
        {
            let mut r = self.readers.lock().unwrap();
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
                            let stream = TcpStream::connect(a).unwrap();
                            let reader = BufReader::new(stream);
                            let mut de = serde_json::Deserializer::from_reader(reader);
                            let data = T::deserialize(&mut de).unwrap();
                            return Ok(data);
                        },
                        None => continue,
                    }
                },
                None => (),
            }
        }
    }


    pub fn send_nb<T>(&self, data: T, dest: u32, id: u32) -> Result<NbDataHandle<T>, &'static str>
        where T: Serialize + std::marker::Send + 'static,
    {
        let dest_addr = self.client_listeners.get(dest as usize).unwrap().clone();
        let ip = self.listener.local_addr().unwrap().ip();
        let self_id = self.id;
        let t = thread::spawn(move || 
            {
                let mut stream = client_connect(&dest_addr).unwrap();
                let op_listener = TcpListener::bind(format!("{}:0", ip)).unwrap();
                let op_pkt = ClientOperationPkt{client_id: self_id, op_id: id,
                    addr: op_listener.local_addr().unwrap()};   
                let op_msg = serde_json::to_string(&op_pkt).unwrap();
                stream.write(op_msg.as_bytes()).unwrap();
                stream.flush().unwrap();

                let (mut stream2, _) = op_listener.accept().unwrap();
                let msg = serde_json::to_string(&data).unwrap();
                stream2.write(msg.as_bytes()).unwrap();
                stream2.flush().unwrap();

                data
            });
        
        Ok(NbDataHandle::<T>::new(t))
    }

    pub fn receive_nb<'a, T>(&self, source: u32, id: u32) -> Result<NbDataHandle<T>, &'static str>
        where T: Deserialize<'a> + std::marker::Send + 'static,
    {
        let readers = Arc::clone(&self.readers);

        let t = thread::spawn(move ||
            {
                loop
                {
                    let mut r = readers.lock().unwrap();
                    let addr = r.remove(&(source,id));
                    match addr
                    {
                        Some(a) =>
                        {
                            let stream = TcpStream::connect(a).unwrap();
                            let reader = BufReader::new(stream);
                            let mut de = serde_json::Deserializer::from_reader(reader);
                            let data = T::deserialize(&mut de).unwrap();
                            return data;
                        },
                        None => continue,
                    }
                }
            });

        Ok(NbDataHandle::<T>::new(t))
    }


    pub fn create_mutex<'a, T>(&self, name: String, start_data: T) -> HeimdallrMutex<T>
        where T: Serialize + Deserialize<'a>,
    {
        HeimdallrMutex::<T>::new(&self, name, start_data)
    }


    pub fn barrier(&self) -> Result<(), &'static str>
    {
        // println!("barrier function entry");
        let pkt = BarrierPkt::new(self.id, self.size);
        let mut stream = TcpStream::connect(&self.daemon_addr).unwrap();
        pkt.send(&self.job, &mut stream).unwrap();
        BarrierReplyPkt::receive(&stream);
        // println!("barrier function exit");
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
        let data = self.t.join().unwrap();
        data
    }
}


pub struct NbSend<T>
{
    t: thread::JoinHandle<T>
}

impl<T> NbSend<T>
{
    pub fn new(t: thread::JoinHandle<T>) -> NbSend<T>
    {
        NbSend::<T>{t}
    }

    pub fn data(self) -> T
    {
        let data = self.t.join().unwrap();
        data
    }
}


pub struct NbReceive<T>
{
    t: thread::JoinHandle<T>,
}

impl<T> NbReceive<T>
{
    pub fn new(t: thread::JoinHandle<T>) -> NbReceive<T>
    {
        NbReceive::<T>{t}
    }

    pub fn data(self) -> T
    {
        let data = self.t.join().unwrap();
        data
    }
}


pub struct HeimdallrMutex<T>
{
    name: String,
    job: String,
    daemon_addr: SocketAddr,
    client_addr: SocketAddr,
    data: T,
}

impl<'a, T> HeimdallrMutex<T>
    where T: Serialize+ Deserialize<'a>,
{
    pub fn new(client: &HeimdallrClient, name: String,  start_value: T) -> HeimdallrMutex<T>
    {
        // println!("mutex::new function entry");
        let ser_data = serde_json::to_string(&start_value).unwrap();
        let pkt = MutexCreationPkt::new(name.clone(), client.id, ser_data);
        let stream = TcpStream::connect(client.daemon_addr).unwrap();
        pkt.send(&client, &stream);

        let reply = MutexCreationReplyPkt::receive(&stream);

        //TODO
        if reply.name != name
        {
            eprintln!("Error: miscommunication in mutex creation. Name mismatch");
        }

        // println!("mutex::new function exit");
        HeimdallrMutex::<T>{name, job: client.job.clone(), daemon_addr: client.daemon_addr,
            client_addr: client.listener.local_addr().unwrap(),data: start_value}
    }

    // pub fn lock(&'a mut self) -> std::io::Result<HeimdallrMutexDataHandle::<'a,T>>
    // {
    //     println!("lock function entry");
    //     let mut stream = TcpStream::connect(self.daemon_addr)?;
    //     let lock_req_pkt = MutexLockReqPkt::new(&self.name);
    //     lock_req_pkt.send(&self.job, &mut stream)?;
    //
    //     let mut de = serde_json::Deserializer::from_reader(stream);
    //     self.data = T::deserialize(&mut de).unwrap();
    //
    //     println!("lock function exit");
    //     Ok(HeimdallrMutexDataHandle::<T>::new(self))
    // }

    pub fn lock(&'a mut self) -> std::io::Result<HeimdallrMutexDataHandle::<'a,T>>
    {
        // println!("lock function entry");
        let mut stream = TcpStream::connect(self.daemon_addr)?;
        let ip = self.client_addr.ip();
        let op_listener = TcpListener::bind(format!("{}:0", ip)).unwrap();

        let lock_req_pkt = MutexLockReqPkt::new(&self.name, op_listener.local_addr().unwrap());
        lock_req_pkt.send(&self.job, &mut stream)?;


        let (stream2, _) = op_listener.accept().unwrap();
        let mut de = serde_json::Deserializer::from_reader(stream2);
        self.data = T::deserialize(&mut de).unwrap();

        // println!("lock function exit");
        Ok(HeimdallrMutexDataHandle::<T>::new(self))

        // let ip = self.listener.local_addr().unwrap().ip();
        // let self_id = self.id;
        // let t = thread::spawn(move || 
        //     {
        //         let mut stream = client_connect(&dest_addr).unwrap();
        //         let op_listener = TcpListener::bind(format!("{}:0", ip)).unwrap();
        //         let op_pkt = ClientOperationPkt{client_id: self_id, op_id: id,
        //             addr: op_listener.local_addr().unwrap()};   
        //         let op_msg = serde_json::to_string(&op_pkt).unwrap();
        //         stream.write(op_msg.as_bytes()).unwrap();
        //         stream.flush().unwrap();
        //
        //         let (mut stream2, _) = op_listener.accept().unwrap();
        //         let msg = serde_json::to_string(&data).unwrap();
        //         stream2.write(msg.as_bytes()).unwrap();
        //         stream2.flush().unwrap();
    }

    fn push_data(&self) 
    {
        // println!("push_data function entry");
        let mut stream = TcpStream::connect(self.daemon_addr).unwrap();

        let ser_data = serde_json::to_string(&self.data).unwrap();
        let write_pkt = MutexWriteAndReleasePkt::new(&self.name, ser_data);
        write_pkt.send(&self.job, &mut stream).unwrap();
        stream.flush().unwrap();
        // println!("push_data function exit");
    }
}


pub struct HeimdallrMutexDataHandle<'a,T>
    where T: Serialize+ Deserialize<'a>,
{
    mutex: &'a mut HeimdallrMutex<T>,
}

impl<'a,T> HeimdallrMutexDataHandle<'a,T>
    where T: Serialize+ Deserialize<'a>,
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
        self.mutex.push_data();
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
pub enum DaemonPktType
{
    ClientInfoPkt,
    MutexCreationPkt,
    MutexLockReqPkt,
    MutexWriteAndReleasePkt,
    BarrierPkt,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonPkt
{
    pub job: String,
    pub pkt_type: DaemonPktType,
    pub pkt: String,
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
        let msg = serde_json::to_string(&self).unwrap();
        let pkt = DaemonPkt{job: self.job.clone(), pkt_type: DaemonPktType::ClientInfoPkt,
            pkt: msg};
        let data = serde_json::to_string(&pkt).unwrap();
        stream.write(data.as_bytes()).unwrap();
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
        let msg = serde_json::to_string(&self).unwrap();
        stream.write(msg.as_bytes()).unwrap();
        stream.flush().unwrap();
    }

    pub fn receive(stream: &TcpStream) -> DaemonReplyPkt
    {
        let mut de = serde_json::Deserializer::from_reader(stream);
        let daemon_reply = DaemonReplyPkt::deserialize(&mut de).unwrap();

        daemon_reply
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexCreationPkt
{
    pub name: String,
    pub client_id: u32,
    pub start_data: String,
}

impl MutexCreationPkt
{
    pub fn new(name: String, id: u32, serialized_data: String) -> MutexCreationPkt
    {
        MutexCreationPkt{name, client_id: id, start_data: serialized_data}
    }

    pub fn send(&self, client: &HeimdallrClient, mut stream: &TcpStream)
    {
        let pkt = serde_json::to_string(self).unwrap();
        let daemon_pkt = DaemonPkt{ job: client.job.clone(),
            pkt_type: DaemonPktType::MutexCreationPkt, pkt};

        let data = serde_json::to_string(&daemon_pkt).unwrap();
        stream.write(data.as_bytes()).unwrap();
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
        let pkt = serde_json::to_string(&self).unwrap();
        stream.write(pkt.as_bytes())?;
        stream.flush().unwrap();
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> MutexCreationReplyPkt
    {
        let mut de = serde_json::Deserializer::from_reader(stream);
        MutexCreationReplyPkt::deserialize(&mut de).unwrap()
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
        let pkt = serde_json::to_string(&self).unwrap();
        let daemon_pkt = DaemonPkt{ job: job.to_string(),
            pkt_type: DaemonPktType::MutexLockReqPkt, pkt};

        let data = serde_json::to_string(&daemon_pkt).unwrap();
        stream.write(data.as_bytes())?;
        stream.flush().unwrap();

        Ok(())
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MutexWriteAndReleasePkt
{
    pub mutex_name: String,
    pub data: String,
}

impl MutexWriteAndReleasePkt
{
    pub fn new(mutex_name: &str, data: String) -> MutexWriteAndReleasePkt
    {
        MutexWriteAndReleasePkt{mutex_name: mutex_name.to_string(), data}
    }

    pub fn send(&self, job: &str, stream: &mut TcpStream) -> std::io::Result<()>
    {
        let pkt = serde_json::to_string(&self).unwrap();
        let daemon_pkt = DaemonPkt{job: job.to_string(),
            pkt_type: DaemonPktType::MutexWriteAndReleasePkt, pkt};
        let data = serde_json::to_string(&daemon_pkt).unwrap();
        stream.write(data.as_bytes())?;
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
        let pkt = serde_json::to_string(&self).unwrap();
        let daemon_pkt = DaemonPkt{job: job.to_string(),
            pkt_type: DaemonPktType::BarrierPkt, pkt};
        let data = serde_json::to_string(&daemon_pkt).unwrap();
        stream.write(data.as_bytes())?;
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
        let pkt = serde_json::to_string(&self).unwrap();
        stream.write(pkt.as_bytes())?;
        stream.flush().unwrap();
        Ok(())
    }

    pub fn receive(stream: &TcpStream) -> BarrierReplyPkt
    {
        let mut de = serde_json::Deserializer::from_reader(stream);
        BarrierReplyPkt::deserialize(&mut de).unwrap()
    }
}



#[derive(Serialize, Deserialize, Debug)]
pub struct ClientOperationPkt
{
    pub client_id: u32,
    pub op_id: u32,
    pub addr: SocketAddr,
}


fn connect(addr: SocketAddr) -> TcpStream
{
    let stream = TcpStream::connect(addr).unwrap_or_else(|err|
        {
            eprintln!("Error in connecting to daemon at: {}", err);
            process::exit(1);
        });

    // println!("Connected to server at: {}", addr);

    stream
}




fn client_connect(addr: &SocketAddr) -> std::io::Result<TcpStream>
{
    let stream = TcpStream::connect(addr)?;
    Ok(stream)
}
