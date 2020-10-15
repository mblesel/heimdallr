// use std::net::{Ipv4Addr, SocketAddrV4};
use std::env;

use heimdallr::HeimdallrClient;

fn _test_send_rec(client: &HeimdallrClient, from: u32, to: u32) 
{
    let buf = format!("TEST Message from client {}", client.id);
    match client.id
    {
        f if f == from => client.send(&buf, to).unwrap(),
        t if t == to =>
        {
            let rec = client.receive::<String>().unwrap();
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
            // client.nb_send(&mut buf, 1).unwrap();
            client.send(&mut buf, 1).unwrap();
            println!("Client 0: done sending");
        },
        1 => 
        {
            let buf: Vec::<i64>;
            println!("Client 1: start receiving");
            // buf = client.nb_receive::<Vec::<i64>>(0).unwrap();
            buf = client.receive::<Vec::<i64>>().unwrap();
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
            let a_send = client.send_async(buf, 1).unwrap();
            println!("send_async call done");
            buf = a_send.data();
            println!("got data buffer ownership back");
            println!("{}", buf[4664]);
        },
        1 => 
        {
            let buf: Vec::<i64>;
            println!("receive_async call");
            let a_recv = client.receive_async::<Vec::<i64>>().unwrap();
            println!("receive_async call done");
            buf = a_recv.data();
            println!("got buf data ownership");
            println!("{}", buf[4664]);
        },
        _ => (),
    }

    Ok(())
}



fn main() -> std::io::Result<()>
{
    _async_test()?;
    Ok(())
}


