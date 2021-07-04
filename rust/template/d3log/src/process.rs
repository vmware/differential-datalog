// functions for managing forked children.
// - start_node(), which is the general d3log runtime start, and probably doesn't belong here
// - implementation of Port over pipes
//
// this needs to get broken apart or shifted a little since the children in the future
// will be other ddlog executables

// this is all quite rendundant with the various existing wrappers, especially tokio::process
// however, we need to be inside the child fork before exec to add the management
// descriptors, otherwise most of this could go

use crate::{
    async_error,
    error::Error,
    fact,
    json_framer::JsonFramer,
    record_batch::{record_deserialize_batch, record_serialize_batch, RecordBatch},
    Batch, Evaluator, Port, Transport,
};
use differential_datalog::record::*;
use nix::unistd::*;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryFrom, ffi::CString};
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, spawn};
use tokio_fd::AsyncFd;

type Fd = std::os::unix::io::RawFd;

// these could be allocated dynamically and passed in env
pub const MANAGEMENT_INPUT_FD: Fd = 3;
pub const MANAGEMENT_OUTPUT_FD: Fd = 4;

// xxx - this should follow trait Network
// really we probably want to have a forwarding table
// xxx child read input

#[derive(Clone)]
pub struct FileDescriptorPort {
    pub fd: Fd,
    //    e: Evaluator,
    pub management: Port,
}

impl Transport for FileDescriptorPort {
    fn send(&self, b: Batch) {
        let js = async_error!(self.management, record_serialize_batch(b));
        let mut pin = AsyncFd::try_from(self.fd).expect("asynch");
        spawn(async move { pin.write_all(&js).await });
    }
}

async fn read_output<F>(f: Fd, mut callback: F) -> Result<(), Error>
where
    F: FnMut(&[u8]),
{
    let mut pin = AsyncFd::try_from(f)?;
    let mut buffer = [0; 1024];
    loop {
        let res = pin.read(&mut buffer).await?;
        callback(&buffer[0..res]);
    }
}

#[derive(Clone)]
pub struct ProcessManager {
    e: Evaluator,
    processes: Arc<Mutex<HashMap<Pid, Arc<Mutex<Child>>>>>,
    management: Port,
    //    tm: ArcTransactionManager, // maybe we just need a port here
}

impl Transport for ProcessManager {
    fn send(self: &Self, b: Batch) {
        // we think the dispatcher has given only facts from our relation
        // this should be from, is that not so?
        println!("process batch {}", b);
        for (_, p, w) in &RecordBatch::from(self.e.clone(), b) {
            // what about other values of w?
            if w == -1 {
                // kill if we can find the uuid..i guess and if the total weight is 1
            }
            println!("Forking ..");
            if w == 1 {
                self.make_child(self.e.clone(), p, self.management.clone())
                    .expect("fork failure");
                //                self.processes.lock().expect("lock").insert(v.id, pid);
            }
        }
    }
}

struct Child {
    uuid: u128,
    eval: Evaluator,
    //pid: Pid,
    management: Port,
    //    management_to_child: Port, // xxx - hook up to broadcast
}

impl Child {
    pub fn report_status(&self) {
        self.management.send(fact!(
            d3_application::ProcessStatus,
            id => self.uuid.into_record(),
            memory_bytes => 0.into_record(),
            threadds => 0.into_record(),
            time => self.eval.now().into_record()));
    }
}

impl ProcessManager {
    pub fn new(e: Evaluator, management: Port) -> ProcessManager {
        // allocate wait thread
        let p = ProcessManager {
            e,
            processes: Arc::new(Mutex::new(HashMap::new())),
            management,
        };
        p
    }

    // arrange to listen to management channels if they exist
    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    // in the earlier model, we deliberately refrained from
    // starting the multithreaded tokio runtime until after
    // we'd forked all the children. lets see if we can
    // can fork this executable w/o running exec if tokio has been
    // started.

    // since this is really an async error maybe deliver it here
    pub fn make_child(&self, e: Evaluator, process: Record, management: Port) -> Result<(), Error> {
        // ideally we wouldn't allocate the management pair
        // unless we were actually going to use it..

        let (management_in_r, _management_in_w) = pipe().unwrap();
        let (management_out_r, management_out_w) = pipe().unwrap();

        let (standard_in_r, _standard_in_w) = pipe().unwrap();
        let (standard_out_r, standard_out_w) = pipe().unwrap();
        let (standard_err_r, standard_err_w) = pipe().unwrap();

        let id = process.get_struct_field("id").unwrap();

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => {
                println!("-> parent child pid {}", child);
                // move above so we dont have to try to undo the fork on error
                let c = Arc::new(Mutex::new(Child {
                    eval: e,
                    uuid: u128::from_record(id)?,
                    //pid: child,
                    management: management.clone(),
                    // management_to_child: Arc::new(FileDescriptorPort {
                    //                        management: management.clone(),
                    //                        fd: management_in_w,
                    //                    }),
                }));

                if let Some(_) = process.get_struct_field("management") {
                    let c2 = c.clone();
                    spawn(async move {
                        let mut jf = JsonFramer::new();
                        read_output(management_out_r, move |b: &[u8]| {
                            (|| -> Result<(), Error> {
                                for i in jf.append(b)? {
                                    let v = record_deserialize_batch(i)?;
                                    // let v: RecordBatch = serde_json::from_str(&i)?;
                                    c2.clone().lock().expect("lock").management.send(v);
                                    // we shouldn't be doing this on every input - demo hack
                                    // i guess we just limit it to one
                                    c2.clone().lock().expect("lock").report_status();
                                }
                                Ok(())
                            })()
                            .expect("mangement forward");
                        })
                        .await
                        .expect("json read");
                    });
                }

                spawn(async move {
                    read_output(standard_out_r, |b: &[u8]| {
                        // utf8 framing issues?
                        print!("child {} {}", child, std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });

                spawn(async move {
                    read_output(standard_err_r, |b: &[u8]| {
                        println!("child error {}", std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });
                self.processes.lock().expect("lock").insert(child, c);
                Ok(())
            }

            Ok(ForkResult::Child) => {
                // plumb stdin and stdout regardless

                println!("about to dup2");
                if !process.get_struct_field("executable").is_none() {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                //unsafe {
                if let Some(e) = process.get_struct_field("executable") {
                    // FIXME: Temporary fix. this should be fixed ddlog-wide
                    let e = e.to_string().replace("\"", "");
                    if let Some(id) = process.get_struct_field("id") {
                        println!("executable name {}", e);
                        let path =
                            CString::new(e.clone().to_string()).expect("CString::new failed");
                        let arg0 =
                            CString::new(e.clone().to_string()).expect("CString::new failed");
                        // assign the child uuid here and pass in environment, just to avoid
                        // having to deal with getting it from the child asynchronously
                        // take a real map and figure out how to get a &[Cstring]
                        let u = format!("uuid={}", id);
                        let env1 = CString::new(u).expect("CString::new failed");

                        // ideally error would be wired up. execve returns Infallible
                        execve(&path, &[arg0], &[env1])?;
                    }
                }
                Ok(())
                // misformed process record?
            }
            Err(e) => {
                panic!("Fork failed!");
            }
        }
    }
}
