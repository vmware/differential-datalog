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
    broadcast::{Adder, Broadcast},
    error::Error,
    fact,
    json_framer::JsonFramer,
    record_batch::{deserialize_record_batch, record_serialize_batch, RecordBatch},
    Batch, Evaluator, Port, Transport,
};
use differential_datalog::record::*;
use nix::unistd::*;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryFrom, ffi::CString};
use tokio::runtime::Runtime;
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
    pub eval: Evaluator,
    pub management: Port,
}

impl Transport for FileDescriptorPort {
    fn send(&self, b: Batch) {
        let js = async_error!(
            self.management,
            record_serialize_batch(RecordBatch::from(self.eval.clone(), b))
        );
        // keep this around, would you?
        let mut pin = async_error!(self.management, AsyncFd::try_from(self.fd));
        spawn(async move { pin.write_all(&js).await });
    }
}

async fn read_output<F>(fd: Fd, mut callback: F) -> Result<(), Error>
where
    F: FnMut(&[u8]),
{
    let mut pin = AsyncFd::try_from(fd)?;
    let mut buffer = [0; 1024];
    loop {
        let res = pin.read(&mut buffer).await?;
        callback(&buffer[0..res]);
    }
}

#[derive(Clone)]
pub struct ProcessManager {
    eval: Evaluator,
    rt: Arc<tokio::runtime::Runtime>,
    processes: Arc<Mutex<HashMap<Pid, Arc<Mutex<Child>>>>>,
    management: Port,
    b: Arc<Broadcast>,
    //    tm: ArcTransactionManager, // maybe we just need a port here
}

impl Transport for ProcessManager {
    fn send(&self, b: Batch) {
        // we think the dispatcher has given only facts from our relation
        // this should be from, is that not so?
        for (_, p, weight) in &RecordBatch::from(self.eval.clone(), b) {
            // what about other values of weight?
            if weight == -1 {
                // kill if we can find the uuid..i guess and if the total weight is 1
            }
            if weight == 1 {
                self.make_child(self.eval.clone(), p).expect("fork failure");
                //                self.processes.lock().expect("lock").insert(v.id, pid);
            }
        }
    }
}

struct Child {
    uuid: u128,
    eval: Evaluator,
    pid: Pid,
    management: Port,
    management_to_child: Port,
}

impl Child {
    pub fn report_status(&self) {
        self.management.send(fact!(
            d3_application::ProcessStatus,
            id => self.uuid.into_record(),
            memory_bytes => 0.into_record(),
            threads => 0.into_record(),
            time => self.eval.now().into_record()));
    }
}

impl ProcessManager {
    pub fn new(
        eval: Evaluator,
        rt: Arc<tokio::runtime::Runtime>,
        b: Arc<Broadcast>,
    ) -> ProcessManager {
        // allocate wait thread
        ProcessManager {
            eval,
            rt,
            processes: Arc::new(Mutex::new(HashMap::new())),
            management: b.clone(),
            b: b.clone(),
        }
    }

    // arrange to listen to management channels if they exist
    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    // since this is really an async error maybe deliver it here
    pub fn make_child(&self, eval: Evaluator, process: Record) -> Result<(), Error> {
        // ideally we wouldn't allocate the management pair
        // unless we were actually going to use it..

        let (management_in_r, management_in_w) = pipe().unwrap();
        let (management_out_r, management_out_w) = pipe().unwrap();

        let (standard_in_r, _standard_in_w) = pipe().unwrap();
        let (standard_out_r, standard_out_w) = pipe().unwrap();
        let (standard_err_r, standard_err_w) = pipe().unwrap();

        let id = process.get_struct_field("id").unwrap();

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => {
                // move above so we dont have to try to undo the fork on error
                let child_obj = Arc::new(Mutex::new(Child {
                    eval: eval.clone(),
                    uuid: u128::from_record(id)?,
                    pid: child,
                    management: self.b.clone(),
                    management_to_child: Arc::new(FileDescriptorPort {
                        eval: eval.clone(),
                        management: self.b.clone(),
                        fd: management_in_w,
                    }),
                }));

                if let Some(_) = process.get_struct_field("management") {
                    let c2 = child_obj.clone();
                    let management_clone = self.b.clone();
                    let rt = self.rt.clone();
                    rt.spawn(async move {
                        let mut jf = JsonFramer::new();
                        let mut first = true;
                        let management_clone = management_clone.clone();

                        let sh_management =
                            management_clone.clone().add(Arc::new(FileDescriptorPort {
                                management: management_clone.clone(),
                                eval: eval.clone(),
                                fd: management_out_r,
                            }));

                        let a = management_clone.clone();
                        async_error!(
                            a,
                            read_output(management_out_r, move |b: &[u8]| {
                                let management_clone = management_clone.clone();
                                for i in async_error!(management_clone.clone(), jf.append(b)) {
                                    let v = async_error!(
                                        management_clone.clone(),
                                        deserialize_record_batch(i)
                                    );

                                    sh_management.clone().send(v);

                                    // assume the child needs to announce something before we recognize it
                                    // as started. assume its tcp address information so we dont need to
                                    // implement the join
                                    if first {
                                        c2.clone().lock().expect("lock").report_status();
                                        first = false;
                                    }
                                }
                            })
                            .await
                        );
                    });
                }

                self.rt.spawn(async move {
                    read_output(standard_out_r, |b: &[u8]| {
                        // utf8 framing issues?
                        print!("child {} {}", child, std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });

                self.rt.spawn(async move {
                    read_output(standard_err_r, |b: &[u8]| {
                        println!("child error {}", std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });
                self.processes
                    .lock()
                    .expect("lock")
                    .insert(child, child_obj);
                Ok(())
            }

            Ok(ForkResult::Child) => {
                // plumb stdin and stdout regardless

                if process.get_struct_field("executable").is_some() {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                //unsafe {
                if let Some(exec) = process.get_struct_field("executable") {
                    // FIXME: Temporary fix. this should be fixed ddlog-wide
                    let exec = exec.to_string().replace("\"", "");
                    if let Some(id) = process.get_struct_field("id") {
                        let path = CString::new(exec.clone()).expect("CString::new failed");
                        let arg0 = CString::new(exec).expect("CString::new failed");
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
            Err(_) => {
                panic!("Fork failed!");
            }
        }
    }
}
