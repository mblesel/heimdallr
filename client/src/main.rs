use std::net::{TcpStream, TcpListener};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::process;
use std::io::Write;

use heimdallr::HeimdallrClient;


fn main() -> std::io::Result<()>
{
    let job = String::from("test_job");
    let size: u32 = 2;
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4664);
    
    let client = HeimdallrClient::new(job, size, addr).unwrap();

    println!("Client created successfuly.\nname: {}, size: {}, id: {}", client.job,
        client.size, client.id);

    Ok(())
}


