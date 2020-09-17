use std::net::{Ipv4Addr, SocketAddrV4};

use heimdallr::HeimdallrClient;

fn test_send_rec(client: &HeimdallrClient, from: u32, to: u32) 
{
    let buf = format!("TEST Message from client {}", client.id);
    match client.id
    {
        f if f == from => client.send(&buf, to).unwrap(),
        t if t == to =>
        {
            let rec = client.receive::<String>(from).unwrap();
            println!("Received: {}", rec);
        },
        _ => (),
    }
}


fn main() -> std::io::Result<()>
{
    let job = String::from("test_job");
    let size: u32 = 3;
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4664);
    
    let client = HeimdallrClient::init(job, size, addr).unwrap();

    println!("Client created successfuly.\n{}", client);
    println!("Client listener addrs:");
    for (id, addr) in client.client_listeners.iter().enumerate()
    {
        println!("  id: {}, listener_addr: {}", id, addr);
    }
    
    test_send_rec(&client, 0, 1);
    test_send_rec(&client, 1, 2);
    test_send_rec(&client, 2, 0);
    
    Ok(())
}


