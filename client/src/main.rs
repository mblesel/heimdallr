// use std::net::{Ipv4Addr, SocketAddrV4};
use std::env;
use std::ops::{Index, IndexMut};

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

// Simple data structure for a 2D matrix
// Has an efficient continuous 1D memory layout
#[derive(Debug)]
struct PartdiffMatrix
{
    rows: usize,
    cols: usize,
    first1: Vec<f64>,
    first2: Vec<f64>,
    matrix: Vec<f64>,
    last1: Vec<f64>,
    last2: Vec<f64>,
}

impl PartdiffMatrix
{
    fn new(rows: usize, cols: usize) -> Result<PartdiffMatrix, &'static str>
    {
        if (rows == 0) & (cols == 0)
        {
            let first1 = Vec::<f64>::new(); 
            let first2 = Vec::<f64>::new();
            let last1 = Vec::<f64>::new();
            let last2 = Vec::<f64>::new();
            let matrix = Vec::<f64>::new();

            return Ok(PartdiffMatrix{rows, cols, first1, first2, matrix, last1, last2})
        }
        else if rows < 5
        {
            return Err("Not enough rows");
        }

        let first1 = vec![0.0; cols];
        let first2 = vec![0.0; cols];
        let last1 = vec![0.0; cols];
        let last2 = vec![0.0; cols];
        let matrix = vec![0.0; (rows-4) * cols];

        Ok(PartdiffMatrix{rows, cols, first1, first2, matrix, last1, last2})
    }
}

// Implementation of Index and IndexMut traits for the matrix
// 2d-array-indexing allows access to matrix elements with following syntax:
//   matrix[[x,y]]
//
// This version is used if the crate is build with: --features "2d-array-indexing"
// 
// Also supports switching between indexing with or without bounds checking
// This can be set by building the crate with or without: --features "unsafe-indexing"
impl Index<[usize; 2]> for PartdiffMatrix
{
    type Output = f64;

    fn index(&self, idx: [usize; 2]) -> &Self::Output
    {       
        unsafe
        {
            match idx[0]
            {
                0 => &self.first1.get_unchecked(idx[1]),
                1 => &self.first2.get_unchecked(idx[1]),
                x if x == self.rows-2 => &self.last1.get_unchecked(idx[1]),
                x if x == self.rows-1 => &self.last2.get_unchecked(idx[1]),
                _ => &self.matrix.get_unchecked((idx[0]-2) * self.cols + idx[1]),
            }
        }
    }
}

impl IndexMut<[usize; 2]> for PartdiffMatrix
{
    fn index_mut(&mut self, idx: [usize; 2]) -> &mut Self::Output
    {
        unsafe
        {
            match idx[0]
            {
                0 => self.first1.get_unchecked_mut(idx[1]),
                1 => self.first2.get_unchecked_mut(idx[1]),
                x if x == self.rows-2 => self.last1.get_unchecked_mut(idx[1]),
                x if x == self.rows-1 => self.last2.get_unchecked_mut(idx[1]),
                _ => self.matrix.get_unchecked_mut((idx[0]-2) * self.cols + idx[1]),
            }
        }
    }
}

#[derive(Debug)]
struct PartdiffMatrixCollection
{
    m1: PartdiffMatrix,
    m2: PartdiffMatrix,
}

impl PartdiffMatrixCollection
{
    pub fn new(rows: usize, cols: usize, num_matrices: u32) -> PartdiffMatrixCollection
    {
        let m1 = PartdiffMatrix::new(rows, cols).unwrap();

        let m2 = match num_matrices
        {
            2 => PartdiffMatrix::new(rows,cols).unwrap(),
            _ => PartdiffMatrix::new(0,0).unwrap(),
        };
        
        PartdiffMatrixCollection {m1, m2}
    }
}

impl Index<usize> for PartdiffMatrixCollection
{
    type Output = PartdiffMatrix;

    fn index(&self, idx: usize) -> &Self::Output
    {       
        match idx
        {
            0 => &self.m1,
            1 => &self.m2,
            _ => &self.m1,
        }
    }
}

impl IndexMut<usize> for PartdiffMatrixCollection
{
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output
    {
        match idx
        {
            0 => &mut self.m1,
            1 => &mut self.m2,
            _ => &mut self.m1,
        }
    }
}






fn _test() -> std::io::Result<()>
{
    let client = HeimdallrClient::init(env::args()).unwrap();
    
    let mut matrices = PartdiffMatrixCollection::new(10,10,2);

    let mut m_in = matrices.m1;

    let nb;

    match client.id
    {
        0 => 
        {
            for i in 0..10
            {
                for j in 0..10
                {
                    m_in[[i,j]] = (i*10 + j) as f64;
                }
            }
            nb = client.send_nb(m_in.first1, 1, 0).unwrap();
            m_in.first1 = nb.data();
        },
        1 =>
        {
            let nb = client.receive_nb(0,0).unwrap();
            m_in.first1 = nb.data();
        },
        _ => (),
    }

    


    for i in 0..10
    {
        for j in 0..10
        {
            print!("{:.4} ", m_in[[i,j]]);
        }
        print!("\n");
    }

    matrices.m1 = m_in;

    println!("{}", matrices.m1[[0,5]]);

    Ok(())
}



fn main() -> std::io::Result<()>
{
    // _mutex_test2()?;   
    // _barrier_test()?;
    _test()?;
    Ok(())
}


