use std::time::{Instant, Duration};
use std::ops::{Index,IndexMut};
use std::process;
use std::env;
use std::vec;

use heimdallr::HeimdallrClient;
// The supported calculation Algorithms Gauss Seidel working on the same matrix
// Jacobi using in and out matrices
#[derive(Debug, PartialEq)]
enum CalculationMethod
{
    MethGaussSeidel,
    MethJacobi,
}

// For parsing command line arguments
impl std::str::FromStr for CalculationMethod
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err>
    {
        match s
        {
            "MethGaussSeidel" | "1" => Ok(CalculationMethod::MethGaussSeidel),
            "MethJacobi" | "2" => Ok(CalculationMethod::MethJacobi),
            _ => Err(format!("'{}' is not a valid value for CalculationMethod", s)),
        }
    }
}

// The supported inference functions used during calculation
// F0:     f(x,y) = 0
// FPiSin: f(x,y) = 2pi^2*sin(pi*x)sin(pi*y)
#[derive(Debug, PartialEq)]
enum InferenceFunction
{
    FuncF0,
    FuncFPiSin,
}

// For parsing command line arguments
impl std::str::FromStr for InferenceFunction
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err>
    {
        match s
        {
            "FuncF0" | "1" => Ok(InferenceFunction::FuncF0),
            "FuncFPiSin" | "2" => Ok(InferenceFunction::FuncFPiSin),
            _ => Err(format!("'{}' is not a valid value for InferenceFunction", s)),
        }
    }
}


// The supported termination conditions
// TermPrec: terminate after set precision is reached
// TermIter: terminate after set amount of iterations
#[derive(Debug, PartialEq)]
enum TerminationCondition
{
    TermPrec,
    TermIter,
}

// For parsing command line arguments
impl std::str::FromStr for TerminationCondition
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err>
    {
        match s
        {
            "TermPrec" | "1" => Ok(TerminationCondition::TermPrec),
            "TermIter" | "2" => Ok(TerminationCondition::TermIter),
            _ => Err(format!("'{}' is not a valid value for TerminationCondition", s)),
        }
    }
}


// Data structure for storing the given parameters for the calculation
#[derive(Debug)]
struct CalculationOptions
{
    number: u64,                        // number of threads
    method: CalculationMethod,          // Gauss Seidel or Jacobi method of iteration
    interlines: usize,                  // matrix size = interline*8+9
    inf_func: InferenceFunction,        // inference function
    termination: TerminationCondition,  // termination condition
    term_iteration: u64,                // terminate if iteration number reached
    term_precision: f64,                // terminate if precision reached
}

impl CalculationOptions
{
    fn new(number: u64, method: CalculationMethod, interlines: usize, inf_func: InferenceFunction,
        termination: TerminationCondition, term_iteration: u64, term_precision: f64)
        -> CalculationOptions
    {
        CalculationOptions{number, method, interlines, inf_func, termination, term_iteration, term_precision}
    }
}


// Data structure for storing the the data needed during calculation
#[derive(Debug)]
struct CalculationArguments
{
    n: usize,                       // Number of spaces between lines (lines=n+1)
    num_matrices: usize,            // number of matrices
    h: f64,                         // length of a space between two lines
    m1: PartdiffMatrix,
    m2: PartdiffMatrix,
}

impl CalculationArguments
{
    fn new(n: usize, rows: usize, cols: usize, num_matrices: usize, h: f64) -> CalculationArguments
    {
        let m1 = PartdiffMatrix::new(rows,cols);
        let m2 = match num_matrices
        {
            2 => PartdiffMatrix::new(rows,cols),
            _ => PartdiffMatrix::new(0,0),
        };

        CalculationArguments{n, num_matrices, h, m1, m2}
    }
}


// Data structure for storing result data of the calculation
#[derive(Debug)]
struct CalculationResults
{
    m: usize,             // Index of matrix that holds the final state
    stat_iteration: u64,  // number of current iteration
    stat_precision: f64,  // actual precision of all slaces in iteration
}

impl CalculationResults
{
    fn new(m: usize, stat_iteration: u64, stat_precision: f64) -> CalculationResults
    {
        CalculationResults{m, stat_iteration, stat_precision}
    }
}


// TODO
#[derive(Debug)]
struct ProcessData
{
    chunk_size: u64,
    from: u64,
    to: u64,
}

impl ProcessData
{
    fn new(chunk_size: u64, from: u64, to: u64) -> ProcessData
    {
        ProcessData {chunk_size, from, to}
    }
}



// Simple data structure for a 2D matrix
// Has an efficient continuous 1D memory layout
#[derive(Debug)]
struct PartdiffMatrix
{
    rows: usize,
    cols: usize,
    matrix: Vec<f64>,
}

impl PartdiffMatrix
{
    fn new(rows: usize, cols: usize) -> PartdiffMatrix
    {
        let matrix = vec![0.0; ((rows)*(cols)) as usize];
        PartdiffMatrix{rows, cols, matrix}
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
            &self.matrix.get_unchecked(idx[0] * self.cols + idx[1])
        }
    }
}

impl IndexMut<[usize; 2]> for PartdiffMatrix
{
    fn index_mut(&mut self, idx: [usize; 2]) -> &mut Self::Output
    {
        unsafe
        {
            self.matrix.get_unchecked_mut(idx[0] * self.cols + idx[1])
        }
    }
}


// Display help message to show the required command line arguments to run the binary
fn usage()
{
    println!("Usage: ./rust_partdiff [number] [method] [interlines] [func] [termination] [prec/iter]\n");
    println!("  -number:      number of threads (1 .. n)");
    println!("  -method:      calculation method (MethGaussSeidel/MethJacobi OR 1/2)");
    println!("  -interlines:  number of interlines (1 .. n)");
    println!("                  matrixsize = (interlines * 8) + 9");
    println!("  -func:        inference function (FuncF0/FuncFPiSin OR 1/2)");
    println!("  -termination: termination condition (TermPrec/TermIter OR 1/2)");
    println!("                  TermPrec: sufficient precision");
    println!("                  TermIter: number of iterations");
    println!("  -prec/iter:   depending on termination:");
    println!("                  precision: 1e-4 .. 1e-20");
    println!("                  iterations: 1 .. n");
}


// Helper function to parse command line arguments
fn parse_arg<U>(arg: Option<&String>) -> U
where U: std::str::FromStr,
      <U as std::str::FromStr>::Err: std::fmt::Display
{
    let ret: U = match arg
    {
        Some(a) =>
        {
            a.parse().unwrap_or_else(|error|
                {
                    eprintln!("Error: {}", error);
                    usage();
                    process::exit(1);
                })
        },
        None =>
        {
            eprintln!("Error: incomplete arguments.");
            usage();
            process::exit(1);
        },
    };
    ret
}

// Parsing of command line arguments
fn ask_params(cmd_args: &Vec::<String>) -> CalculationOptions
{
    // TODO keep authors of original c version?
    // println!("============================================================");
    // println!("Program for calculation of partial differential equations.  ");
    // println!("============================================================");
    // println!("(c) Dr. Thomas Ludwig, TU München.");
    // println!("    Thomas A. Zochler, TU München.");
    // println!("    Andreas C. Schmidt, TU München.");
    // println!("============================================================");

    // TODO interactive arguments   
       
    let mut args = cmd_args.iter();
    
    let number: u64 = parse_arg(args.next());
    if number < 1
    {
        eprintln!("Error number argument must be a positive integer");
        usage();
        process::exit(1);
    }

    let method: CalculationMethod = parse_arg(args.next());

    let interlines: usize = parse_arg(args.next());

    let inf_func: InferenceFunction = parse_arg(args.next());

    let termination: TerminationCondition = parse_arg(args.next());

    // Check for the meaning of the last argument
    match termination
    {
        TerminationCondition::TermPrec =>
        {
            let prec: f64 = parse_arg(args.next());
            if (prec < 1e-20) | (prec > 1e-4)
            {
                eprintln!("Error: termination precision must be between 1e-20 and 1e-4");
                usage();
                process::exit(1);
            }
            return CalculationOptions::new(number, method, interlines, inf_func, termination, std::u64::MAX, prec);
        },
        TerminationCondition::TermIter =>
        {
            let iterations = parse_arg(args.next());
            if iterations < 1
            {
                eprintln!("Error: termination iterations must be > 0");
                usage();
                process::exit(1);
            }
            return CalculationOptions::new(number, method, interlines, inf_func, termination, iterations, 0.0);
        },
    }
}


// Determine calculation arguments and initialize calculation results
fn init_variables(client: &HeimdallrClient, options: &CalculationOptions) -> (CalculationArguments, CalculationResults, ProcessData)
{
    let n: usize = (options.interlines * 8) + 9 - 1;
    let num_matrices: usize = match options.method
    {
        CalculationMethod::MethGaussSeidel => 1,
        CalculationMethod::MethJacobi => 2,
    };
    let h: f64 = 1.0 as f64 / n as f64;


    // Calculate data distribution
    let div = (n as u64 -1 ) / client.size as u64;
    let rest = (n as u64 -1 ) % client.size as u64;

    let (chunk_size, from, to): (u64, u64, u64);

    if client.id < rest as u32
    {
        chunk_size = div + 3;
        from = client.id as u64 * (div+1) + 1;
        to = from + div;
    }
    else
    {
        chunk_size = div+2;
        from = client.id as u64 * (div) + rest + 1;
        to = from + div - 1;
    }

    let arguments = CalculationArguments::new(n, chunk_size as usize, n+1, num_matrices, h);
    let results = CalculationResults::new(0,0,0.0);
    let process_data = ProcessData::new(chunk_size,from,to);

    (arguments, results, process_data)
}


// Initialize the matrix borders according to the used inference function
fn init_matrices(arguments: &mut CalculationArguments, options: &CalculationOptions)
{
    if options.inf_func == InferenceFunction::FuncF0
    {
        let n = arguments.n;
        let h = arguments.h;

        for g in 0 .. arguments.num_matrices as usize
        {
            let m = match g
            {
                1 => &mut arguments.m2,
                _ => &mut arguments.m1,
            };
            for i in 0..(n+1)
            {
                    m[[i,0]] = 1.0 - (h * i as f64);   
                    m[[i,n]] = h * i as f64;
                    m[[0,i]] = 1.0 - (h * i as f64);
                    m[[n,i]] = h * i as f64;
            }
        }
    }
}

// Initialize the matrix borders according to the used inference function
fn init_matrices_heimdallr(client: &HeimdallrClient, arguments: &mut CalculationArguments,
    options: &CalculationOptions, process_data: &ProcessData)
{
    if options.inf_func == InferenceFunction::FuncF0
    {
        let n = arguments.n;
        let h = arguments.h;

        for g in 0 .. arguments.num_matrices as usize
        {
            let m = match g
            {
                1 => &mut arguments.m2,
                _ => &mut arguments.m1,
            };

            for i in 0..process_data.chunk_size as usize
            {
                    m[[i,0]] = 1.0 - (h * (process_data.from + i as u64 -1) as f64);
                    m[[i,n]] = h * (process_data.from + i as u64 -1) as f64;
            }

            for i in 0..(n+1)
            {
                if client.id == 0
                {
                    m[[0,i]] = 1.0 - (h * i as f64);
                }
                else if client.id == client.size-1
                {
                    m[[process_data.chunk_size as usize -1,i]] = h * i as f64;
                }
            }

            if client.id == 0
            {
                m[[0,n]] = 0.0;
            }
            else if client.id == client.size-1
            {
                m[[process_data.chunk_size as usize -1,0]] = 0.0;
            }
        }
    }
}


// Main calculation
fn calculate(arguments: &mut CalculationArguments, results: &mut CalculationResults, options: &CalculationOptions)
{
    const PI: f64 = 3.141592653589793;
    const TWO_PI_SQUARE: f64 = 2.0 * PI * PI;

    let n = arguments.n;
    let h = arguments.h;

    let mut star: f64;
    let mut residuum: f64;
    let mut maxresiduum: f64;

    let mut pih: f64 = 0.0;
    let mut fpisin: f64 = 0.0;

    let mut term_iteration = options.term_iteration;

    // for distinguishing between old and new state of the matrix if two matrices are used
    let mut in_matrix: usize = 0;

    if options.method == CalculationMethod::MethJacobi   
    {
        in_matrix = 1;
    }

    if options.inf_func == InferenceFunction::FuncFPiSin
    {
        pih = PI * h;
        fpisin = 0.25 * TWO_PI_SQUARE * h * h;
    }

    while term_iteration > 0
    {
        let (m_in, m_out) = match in_matrix
        {
            1 => (&mut arguments.m2, &mut arguments.m1),
            _ => (&mut arguments.m1, &mut arguments.m2),
        };

        maxresiduum = 0.0;

        for i in 1..n
        {
            let mut fpisin_i = 0.0;

            if options.inf_func == InferenceFunction::FuncFPiSin
            {
                fpisin_i = fpisin * (pih * i as f64).sin();
            }

            for j in 1..n
            {
                star = 0.25 * (m_in[[i-1,j]] + m_in[[i+1,j]] +
                        m_in[[i,j-1]] + m_in[[i,j+1]]);

                if options.inf_func == InferenceFunction::FuncFPiSin
                {
                    star += fpisin_i * (pih * j as f64).sin();
                }

                if (options.termination == TerminationCondition::TermPrec) | (term_iteration == 1)
                {
                    residuum = (m_in[[i,j]] - star).abs();

                    maxresiduum = match residuum
                    {
                        r if r < maxresiduum => maxresiduum,
                        _ => residuum,
                    };
                }

                m_out[[i,j]] = star;
            }
        }

        results.stat_iteration += 1;
        results.stat_precision = maxresiduum;

        if in_matrix == 1
        {
            in_matrix = 0;
        }
        else
        {
            in_matrix = 1;
        }

        match options.termination
        {
            TerminationCondition::TermPrec =>
            {
                if maxresiduum < options.term_precision
                {
                    term_iteration = 0;
                }
            },
            TerminationCondition::TermIter => term_iteration -= 1,
        }
    }

    results.m = in_matrix;
}


// Main calculation
fn calculate_jacobi_heimdallr(client: &mut HeimdallrClient, mut arguments: CalculationArguments,
    results: &mut CalculationResults, options: &CalculationOptions,
    process_data: &ProcessData) 
    -> CalculationArguments
{
    const PI: f64 = 3.141592653589793;
    const TWO_PI_SQUARE: f64 = 2.0 * PI * PI;

    let n = arguments.n;
    let h = arguments.h;
let mut star: f64;
    let mut residuum: f64;
    let mut maxresiduum: f64;

    let mut pih: f64 = 0.0;
    let mut fpisin: f64 = 0.0;

    let mut term_iteration = options.term_iteration;

    // for distinguishing between old and new state of the matrix if two matrices are used
    let mut in_matrix: usize = 0;

    if options.method == CalculationMethod::MethJacobi   
    {
        in_matrix = 1;
    }

    if options.inf_func == InferenceFunction::FuncFPiSin
    {
        pih = PI * h;
        fpisin = 0.25 * TWO_PI_SQUARE * h * h;
    }

    let rank = client.id;
    let size = client.size;
    
    let proc_next = rank as i32 +1;

    let proc_before = rank as i32 -1;

    let from = process_data.from;
    let chunk_size = process_data.chunk_size;

    let mut global_maxresiduum = client.create_mutex::<f64>("maxresiduum", 0.0)
        .unwrap();


    while term_iteration > 0
    {
        println!("Iteration: {}", results.stat_iteration);
        maxresiduum = 0.0;

        if options.termination == TerminationCondition::TermPrec
        {
            client.barrier().unwrap();
            if client.id == 0
            {
                let mut mr = global_maxresiduum.lock().unwrap();
                mr.set(0.0);
            }
        }


        let (mut m_in, mut m_out) = match in_matrix
        {
            1 => (arguments.m2, arguments.m1),
            _ => (arguments.m1, arguments.m2),
        };
        
        if rank < size-1
        {
            client.send_slice(
                &m_in.matrix[((m_in.rows-2)*m_in.cols)..((m_in.rows-1)*m_in.cols)],
                proc_next as u32, 2).unwrap();
            m_in.matrix.splice(((m_in.rows-1)*m_in.cols)..((m_in.rows)*m_in.cols),
                client.receive::<Vec<f64>>(proc_next as u32, 1).unwrap());
        }
        if rank > 0
        {
            m_in.matrix.splice(0..(m_in.cols),
                client.receive::<Vec<f64>>(proc_before as u32, 2).unwrap());
            client.send_slice(&m_in.matrix[m_in.cols..(2*m_in.cols)], proc_before as u32, 1).unwrap();
        }


        for i in 1..chunk_size as usize -1 
        {
            let mut fpisin_i = 0.0;

            if options.inf_func == InferenceFunction::FuncFPiSin
            {
                fpisin_i = fpisin * (pih * (i + from as usize - 1)as f64).sin();
            }

            for j in 1..n as usize 
            {
                star = 0.25 * (m_in[[i-1,j]] + m_in[[i+1,j]] + m_in[[i,j-1]] + m_in[[i,j+1]]);

                if options.inf_func == InferenceFunction::FuncFPiSin
                {
                    star += fpisin_i * (pih * j as f64).sin();
                }

                if (options.termination == TerminationCondition::TermPrec) | (term_iteration == 1)
                {
                    residuum = (m_in[[i,j]] - star).abs();

                    maxresiduum = match residuum
                    {
                        r if r < maxresiduum => maxresiduum,
                        _ => residuum,
                    };
                }

                m_out[[i,j]] = star;
            }
        }

        results.stat_iteration += 1;

        if (options.termination == TerminationCondition::TermPrec) | (term_iteration == 1)
        {
            {
                let mut mr = global_maxresiduum.lock().unwrap();
                match *mr.get()
                {
                    r if r < maxresiduum => mr.set(maxresiduum),
                    _ => (),
                }
            }
            client.barrier().unwrap();
        }
        
        if in_matrix == 1
        {
            arguments.m1 = m_out;
            arguments.m2 = m_in;
            in_matrix = 0;
        }
        else
        {
            arguments.m1 = m_in;
            arguments.m2 = m_out;
            in_matrix = 1;
        }


        match options.termination
        {
            TerminationCondition::TermPrec =>
            {
                {
                    let mr = global_maxresiduum.lock().unwrap();
                    if *mr.get() < options.term_precision
                    {
                        term_iteration = 0;
                    }
                }
            },
            TerminationCondition::TermIter => term_iteration -= 1,
        }
        
    }

    let mr = global_maxresiduum.lock().unwrap();
    results.stat_precision = *mr.get();
    results.m = in_matrix;
    arguments
}


// Display important information about the calculation
fn display_statistics(arguments: &CalculationArguments, results: &CalculationResults, options: &CalculationOptions, duration: Duration)
{
    let n = arguments.n;
    
    println!("Berechnungszeit:    {:.6}", duration.as_secs_f64());
    println!("Speicherbedarf:     {:.4} MiB", ((n+1)*(n+1)*std::mem::size_of::<f64>()*arguments.num_matrices) as f64 / 1024.0 / 1024.0);
    println!("Berechnungsmethode: {:?}", options.method);
    println!("Interlines:         {}", options.interlines);
    print!("Stoerfunktion:      ");
    match options.inf_func
    {
        InferenceFunction::FuncF0 => print!("f(x,y) = 0\n"),
        InferenceFunction::FuncFPiSin => print!("f(x,y) = 2pi^2*sin(pi*x)sin(pi*y)\n"),
    }
    print!("Terminierung:       ");
    match options.termination
    {
        TerminationCondition::TermPrec => print!("Hinreichende Genauigkeit\n"),
        TerminationCondition::TermIter => print!("Anzahl der Iterationen\n"),
    }
    println!("Anzahl Iterationen: {}", results.stat_iteration);
    println!("Norm des Fehlers:   {:.6e}", results.stat_precision);
}


// Beschreibung der Funktion displayMatrix:                              
//                                                                       
// Die Funktion displayMatrix gibt eine Matrix                           
// in einer "ubersichtlichen Art und Weise auf die Standardausgabe aus.  
//                                                                       
// Die "Ubersichtlichkeit wird erreicht, indem nur ein Teil der Matrix   
// ausgegeben wird. Aus der Matrix werden die Randzeilen/-spalten sowie  
// sieben Zwischenzeilen ausgegeben.                                     
fn display_matrix(arguments: &mut CalculationArguments, results: &CalculationResults, options: &CalculationOptions)
{
    let matrix = match results.m
    {
        1 => &mut arguments.m2,
        _ => &mut arguments.m1,
    };
    let interlines = options.interlines;

    println!("Matrix:");
    for y in 0..9 as usize
    {
        for x in 0..9 as usize
        {
            print!(" {:.4}", matrix[[y * (interlines+1),x * (interlines+1)]]);
        }
        print!("\n");
    }
}



fn display_matrix_heimdallr(client: &HeimdallrClient, arguments: &mut CalculationArguments, results: &CalculationResults, options: &CalculationOptions, process_data: &ProcessData)
{
    let matrix = match results.m
    {
        1 => &mut arguments.m2,
        _ => &mut arguments.m1,
    };

    let from = match client.id
    {
        0 => process_data.from - 1,
        _ => process_data.from,
    };

    let to = match client.id
    {
        x if x == client.size-1 => process_data.to+1,
        _ => process_data.to,
    };

    if client.id == 0 {
        println!("Matrix:");
    }

    for y in 0..9
    {
        let line = y * (options.interlines+1);
        let mut recv = Vec::<f64>::new();

        match client.id
        {
            0 =>
            {
                if (line < from as usize) | (line > to as usize)
                {
                    recv = client.receive_any_source(42+y as u32).unwrap();
                }
            },
            _ =>
            {
                if (line >= from as usize) & (line <= to as usize)
                {
                    let mut send = Vec::<f64>::new();
                    for x in 0..9 as usize
                    {
                        send.push(matrix[[line - from as usize +1, x * (options.interlines+1)]]);
                    }
                    print!("\n");
                    client.send(&send, 0,42+y as u32).unwrap();
                }
            },
        }

        if client.id == 0
        {
            if (line >= from as usize) & (line <= to as usize)
            {
                for x in 0..9
                {
                    let col = x * (options.interlines+1);
                    print!(" {:.4}", matrix[[line, col]]);
                }   
                print!("\n");
            }
            else
            {
                for x in 0..9
                {
                    print!(" {:.4}", recv[x]);
                }
                print!("\n");
            }
        }
    }
}



fn main()
{
    let mut client = HeimdallrClient::init(env::args()).unwrap();

    let options = ask_params(&client.cmd_args);
    let (mut arguments, mut results, process_data) = init_variables(&client, &options);

    if (client.size == 1) | (client.size >= arguments.n as u32 -1) | 
        (options.method == CalculationMethod::MethGaussSeidel)
    {
        println!("Executing with only 1 process.");
        if client.id == 0
        {
            init_matrices(&mut arguments, &options);
            let now = Instant::now();
            calculate(&mut arguments, &mut results, &options);
            let duration = now.elapsed();
            display_statistics(&arguments, &results, &options, duration);
            display_matrix(&mut arguments, &results, &options);
        }
    }
    else
    {
        println!("Executing with {} clients", client.size);
        init_matrices_heimdallr(&client, &mut arguments, &options, &process_data);
        let now = Instant::now();
        arguments = calculate_jacobi_heimdallr(&mut client, arguments, &mut results, &options,
            &process_data);
        let duration = now.elapsed();

        if client.id == 0
        {
            display_statistics(&arguments, &results, &options, duration);
        }

        display_matrix_heimdallr(&client, &mut arguments, &results, &options, &process_data);
    }



}
