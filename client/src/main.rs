// use std::net::{Ipv4Addr, SocketAddrV4};
use std::env;

use heimdallr::HeimdallrClient;

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
            let rec = client.receive::<String>(from,0).unwrap();
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
    
    Ok(())
}

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
            buf = client.receive::<Vec::<i64>>(0,0).unwrap();
            println!("Client 1: done receiving");
            println!("{:?}", buf[42]);
        },
        _ => (),
    }

    Ok(())
}

fn _async_test() -> std::io::Result<()>
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
            println!("send_async call");
            let a_send = client.send_async(buf, 1, 0).unwrap();
            println!("send_async call done");
            let s = String::from("test");
            client.send(&s, 1, 1).unwrap();
            buf = a_send.data();
            println!("got data buffer ownership back");
            println!("{}", buf[4664]);
        },
        1 => 
        {
            let buf: Vec::<i64>;
            println!("receive_async call");
            let a_recv = client.receive_async::<Vec::<i64>>(0,0).unwrap();
            println!("receive_async call done");
            let s = client.receive::<String>(0,1).unwrap();
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

            r1 = client.receive::<u64>(1,0).unwrap();
            r2 = client.receive::<u64>(2,0).unwrap();
            r3 = client.receive::<u64>(3,0).unwrap();
            println!("{}, {}, {}", r1, r2, r3);
            r3 = client.receive::<u64>(3,0).unwrap();
            r2 = client.receive::<u64>(2,0).unwrap();
            r1 = client.receive::<u64>(1,0).unwrap();
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



fn main() -> std::io::Result<()>
{
    _gather_test()?;   
    Ok(())
}


