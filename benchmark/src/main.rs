use heimdallr::*;
use std::env;
use std::time::Instant;


fn main()
{
    let mut client = HeimdallrClient::init(env::args()).unwrap();

    let buff_size: u32 = client.cmd_args[0].parse().unwrap();
    let iterations: u32 = client.cmd_args[1].parse().unwrap();

    let mut buf: Vec<char> = vec!['A'; buff_size as usize];

    println!("Running with {} bytes and {} iterations", buff_size, iterations);

    client.barrier().unwrap();
    let now = Instant::now();

    for _ in 0..iterations
    {
        match client.id
        {
            0 =>
            {
                client.send(&buf, 1,0).unwrap();
                buf = client.receive(1,1).unwrap();
            },
            1 =>
            {
                buf = client.receive(0,0).unwrap();
                client.send(&buf, 0,1).unwrap();
            },
            _ => (),
        }
    }

    client.barrier().unwrap();
    let duration = now.elapsed();

    println!("Total runtime: {:.6}", duration.as_secs_f64());
    
}
