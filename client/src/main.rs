// use std::net::{Ipv4Addr, SocketAddrV4};
use std::env;

use heimdallr::HeimdallrClient;

use gethostname::gethostname;

fn _wait(secs: u64)
{
    std::thread::sleep(std::time::Duration::from_secs(secs));
}

fn _test_send_rec(client: &HeimdallrClient, from: u32, to: u32) 
{
    let buf = format!("TEST Message from client {}", client.id);
    match client.id
    {
        f if f == from => client.send(&buf, to, 0).unwrap(),
        t if t == to =>
        {
            let rec: String = client.receive(from,0).unwrap();
            println!("Received: {}", rec);
        },
        _ => (),
    }
}

fn _client_test_1() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    println!("Client created successfuly.\n{}", client);
    println!("Client listener addrs:");
    for (id, addr) in client.client_listeners.iter().enumerate()
    {
        println!("  id: {}, listener_addr: {}", id, addr);
    }

    _test_send_rec(&client, 0, 1);
    _test_send_rec(&client, 1, 2);
    _test_send_rec(&client, 2, 0);

    Ok(()) }

fn _big_vec_send_rec() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    match client.id
    {
        0 =>
        {
            let mut buf = Vec::<i64>::new();
            for i in 0..40000000 as i64
            // for i in 0..100000 as i64
            {
                buf.push(i);
            }
            println!("Client 0: done creating vec");
            println!("Client 0: starting send");
            client.send(&mut buf, 1, 0).unwrap();
            println!("Client 0: done sending");
        },
        1 => 
        {
            let buf: Vec::<i64>;
            println!("Client 1: start receiving");
            buf = client.receive(0,0).unwrap();
            println!("Client 1: done receiving");
            println!("{:?}", buf[42]);
        },
        _ => (),
    }

    Ok(())
}

fn _nb_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    match client.id
    {
        0 =>
        {
            let mut buf = Vec::<i64>::new();
            for i in 0..20000000 as i64
            // for i in 0..100000 as i64
            {
                buf.push(i);
            }
            println!("send_nb call");
            let a_send = client.send_nb(buf, 1, 0).unwrap();
            println!("send_nb call done");
            let s = String::from("test");
            client.send(&s, 1, 1).unwrap();
            buf = a_send.data();
            println!("got data buffer ownership back");
            println!("{}", buf[4664]);
        },
        1 => 
        {
            let buf: Vec::<i64>;
            println!("receive_nb call");
            let a_recv = client.receive_nb::<Vec::<i64>>(0,0).unwrap();
            println!("receive_nb call done");
            let s: String = client.receive(0,1).unwrap();
            println!("{}",s);
            buf = a_recv.data();
            println!("got buf data ownership");
            println!("{}", buf[4664]);
        },
        _ => (),
    }

    Ok(())
}

fn _gather_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    match client.id
    {
        0 =>
        {
            let (mut r1, mut r2, mut r3): (u64, u64, u64);

            r1 = client.receive(1,0).unwrap();
            r2 = client.receive(2,0).unwrap();
            r3 = client.receive(3,0).unwrap();
            println!("{}, {}, {}", r1, r2, r3);
            r3 = client.receive(3,0).unwrap();
            r2 = client.receive(2,0).unwrap();
            r1 = client.receive(1,0).unwrap();
            println!("{}, {}, {}", r3, r2, r1);
            println!("END");

        },
        1 => 
        {
            let s: u64 = 1;
            client.send(&s,0,0).unwrap();
            client.send(&s,0,0).unwrap();
        },
        2 => 
        {
            let s: u64 = 2;
            client.send(&s,0,0).unwrap();
            client.send(&s,0,0).unwrap();
        },
        3 => 
        {
            let s: u64 = 3;
            client.send(&s,0,0).unwrap();
            client.send(&s,0,0).unwrap();
        },
        _ => (),
    }
    Ok(())
}


fn _mutex_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    let mut mutex = client.create_mutex("testmutex".to_string(), 0 as u64);

    {
        let mut m = mutex.lock().unwrap();
        println!("before: {}", m.get());
        m.set(m.get()+42);
        println!("after: {}", m.get());
    }

    let mut mutex2 = client.create_mutex("testmutex2".to_string(), "".to_string());

    {
        let mut m = mutex2.lock().unwrap();
        println!("before: {}", m.get());
        let s = format!("Client {} was here", client.id);
        m.set(s);
        println!("after: {}", m.get());
    }

    Ok(())
}

fn _mutex_test2() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();
    let mut mutex = client.create_mutex("testmutex".to_string(), 0 as u64);

    for _ in 0..25000
    {
        let mut m = mutex.lock().unwrap();
        m.set(m.get()+1);
    }

    let m = mutex.lock().unwrap();
    println!("MUtex: {}", m.get());

    Ok(())
}

fn _receive_any_source_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    let mut buf: String;

    match client.id
    {
        0 => 
        {
            for i in 1..client.size
            {
                buf = client.receive_any_source(i).unwrap();
                println!("{}",buf);
            }
        },
        _ =>
        {
            buf = format!("Message from process {}", client.id);
            client.send(&buf, 0, client.id).unwrap();
        },
    }


    Ok(())
}


fn _barrier_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    match client.id
    {
        0 => 
        {
            _wait(1);
            client.barrier().unwrap();
            println!("BARRIER DONE");
        },
        1 =>
        {
            _wait(2);
            client.barrier().unwrap();
            println!("BARRIER DONE");
        },
        2 =>
        {
            _wait(4);
            client.barrier().unwrap();
            println!("BARRIER DONE");
        },
        _ =>
        {
            client.barrier().unwrap();
            println!("BARRIER DONE");
        },
    }

    Ok(())
}


fn _cluster_test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    let hostname = gethostname();

    println!("Client id: {} on host: {:?}\n", client.id, hostname);

    _test_send_rec(&client, 0, 1);
    _test_send_rec(&client, 1, 2);
    _test_send_rec(&client, 2, 3);
    _test_send_rec(&client, 3, 0);

    Ok(())
}

fn _send_slice() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();

    let mut sendbuf = vec![0.0;20];
    for i in 0..sendbuf.len()
    {
        sendbuf[i] = i as f64;
    }
    let mut recvbuf = vec![0.0;20];

    match client.id
    {
        0 =>
        {
            client.send_slice(&sendbuf[10..20], 1, 0).unwrap();
            println!("SEND BUF: {:?}", sendbuf);
        },
        1 => 
        {
            recvbuf.splice(0..10, client.receive::<Vec<f64>>(0, 0).unwrap());
            println!("RECV BUF: {:?}", recvbuf);
        },
        _ => (),
    }

    Ok(())
}



fn main() -> std::io::Result<()>
{
    // _mutex_test2()?;   
    // _barrier_test()?;
    _send_slice()?;
    Ok(())
}


