use crate::{tcp_network::bind,
            json_framer::JsonFramer,
            Batch,
            tcp_network::TcpNetwork,
            transact::{TransactionManager, eval, forward}};

use tokio::{io::AsyncWriteExt,
            io::AsyncReadExt,
            runtime::Runtime,
            spawn};
use tokio_fd::AsyncFd;

use std::io::{Error,ErrorKind};
use std::convert::TryFrom;

type Fd = std::os::unix::io::RawFd;
use nix::unistd::*;
use std::sync::Arc;
//use std::sync::Mutex as SyncMutex;
use async_std::sync::Mutex;

pub async fn output_json(k:&Batch)  -> Result<(), std::io::Error>  {
//    println!("{}", serde_json::to_string(&k)?);
    let js = match serde_json::to_string(&k){
        Ok(x)=>x,
        Err(_x) => return Err(Error::new(ErrorKind::Other, "oh no!"))
    };
    let mut pin = AsyncFd::try_from(1)?;
    // write_all??
    pin.write(js.as_bytes()).await?;
    Ok(())
}

// this is the self-framed json input from stdout of one of my children
// would rather hide h
// there is an event barrier here, but its pretty lacking. the first
// batch to arrive is assumed to contain everything needed.  so once
// we get that from everyone, we kick off evaluation

async fn read_output(t: Arc::<Mutex::<TransactionManager>>, f:Box::<Fd>) -> Result<(), std::io::Error> {
    let mut jf = JsonFramer::new();    
    let mut pin = AsyncFd::try_from(*f)?;
    let mut buffer = [0; 64];
    loop {
        let res = pin.read(&mut buffer).await?;
        for i in jf.append(&buffer[0..res])? {
            let v: Batch = serde_json::from_str(&i)?;
            println!("eval{}", v);
            let tc = t.clone();
            match eval(tc, v).await {
                Ok(b) => {
                    // println!("eval completez {}", b);
                    // shouldn't exit on eval error
                    forward(t.clone(), b).await?; // not really dude
                }
                
                Err(x) =>  {
                    println!("erraru rivalu!");
                    return Err(Error::new(ErrorKind::Other, x))
                }
            };
        }
    }
}
    
pub fn start_node(f:Vec::<Fd>) {
    let t = Arc::new(Mutex::new(TransactionManager::new()));
    let n = TcpNetwork::new();
    let rt = Runtime::new().unwrap();
    let _eg = rt.enter();
    
    rt.block_on(async move {
        for i in f {
            let tc= t.clone();
            spawn(async move {match read_output(tc, Box::new(i)).await {
                Ok(_)=>(),
                Err(x)=> {println!("err {}", x); ()} // process exit
            }});
        }
        match bind(&n, t).await {
            Ok(_) => (),
            Err(x) => {print!("bind failure {}", x);
                       return}
        };
    });
}

pub fn make_child() -> Result<(Fd, Fd), nix::Error> {
    let (in_r, in_w) = pipe().unwrap();
    let (out_r, out_w) = pipe().unwrap();

    match unsafe{fork()} {
        // child was here before .. we'll want that for kills from here, i guess we
        // could close stdin
        Ok(ForkResult::Parent{..}) => {
            Ok((in_w, out_r))            
        }

        // maybe it makes sense to run the management json over different
        // file descriptors so we can use stdout for ad-hoc debugging
        // without confusing the json parser
        Ok(ForkResult::Child) => {
            dup2(out_w, 2)?;                
            dup2(out_w, 1)?;
            dup2(in_r, 0)?;
            start_node(vec![0]);
            Ok((in_w, out_r))
        }
        Err(x) => Err(x),
    }
}

// parameterize network..with i guess a factory!
// i would kind of prefer to kick off init from inside ddlog, but
// odaat

pub fn start_children (n:usize, _init:Batch) {
    let mut children_in = Vec::<Fd>::new();
    let mut children_out = Vec::<Fd>::new();    
    println!("start children {}",n);
    // 0 is us
    for _i in 1..n {
        match make_child() {
            Ok((to, from)) => {
                children_in.push(from);
                children_out.push(to);               
                ()
            }
            Err(x) => {
                println!("fork failed {}", x);
                return
            }
        }
    }
    // wire up nid 0s address..no one is listening to my stdin!
    start_node(children_in);
}

