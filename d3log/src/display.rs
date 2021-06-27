use crate::{async_error, fact, Batch, Evaluator, Forwarder, Port, RecordBatch, Transport};
use differential_datalog::record::{IntoRecord, Record};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use hyper_websocket_lite::{server_upgrade, AsyncClient};
use rand::prelude::*;
use std::borrow::Cow;
use std::convert::Infallible;
use std::str::from_utf8;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use websocket_codec::{Message, Opcode}; //MessageCodec

#[derive(Clone)]
pub struct Display {
    management: Port,
    data: Port,
    f: Forwarder,
    eval: Evaluator,
}

#[derive(Clone)]
pub struct Browser {
    eval: Evaluator,
    uuid: u128,
    s: tokio::sync::mpsc::Sender<Message>,
}

impl Transport for Browser {
    fn send(self: &Self, b: Batch) {
        let a = self.clone();
        let rb = RecordBatch::from(self.eval.clone(), b);
        // async error
        let encoded = serde_json::to_string(&rb).expect("send json encoding error");
        tokio::spawn(async move { a.s.send(Message::text(encoded)).await });
    }
}

const JS_BATCH_HANDLER: &[u8] = include_bytes!("display.js");

async fn on_client(d: Display, stream_mut: AsyncClient) {
    let (tx, mut rx) = channel(100);

    let b = Browser {
        eval: d.eval.clone(),
        uuid: random::<u128>(),
        s: tx.clone(),
    };
    d.f.register(b.uuid, Arc::new(b.clone()));

    d.management.send(fact!(
        display::Browser,
        t =>d.eval.clone().now().into_record(),
        uuid => b.uuid.into_record()));

    let (mut stx, srx) = stream_mut.split();

    let m2 = d.management.clone();
    tokio::spawn(async move {
        loop {
            if let Some(msg) = rx.recv().await {
                async_error!(m2.clone(), stx.send(msg).await);
            }
        }
    });

    let mut crx = srx;
    let m2 = d.management.clone();
    loop {
        let (msg, next) = crx.into_future().await;

        match msg {
            Some(Ok(msg)) => match msg.opcode() {
                Opcode::Text | Opcode::Binary => {
                    // async error
                    let v = &msg.data().to_vec();
                    let s = from_utf8(v).expect("display json utf8 error");
                    println!("browser data {}", s.clone());
                    let v: RecordBatch =
                        serde_json::from_str(s.clone()).expect("display json parse error");
                    d.data.clone().send(Batch::Record(v));
                }
                Opcode::Ping => {
                    async_error!(m2.clone(), tx.send(Message::pong(msg.into_data())).await);
                }
                Opcode::Close => {}
                Opcode::Pong => {}
            },
            Some(Err(_err)) => {
                async_error!(m2.clone(), tx.send(Message::close(None)).await);
            }
            None => {}
        }
        crx = next;
    }
}

async fn hello(d: Display, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.headers().contains_key("upgrade") {
        return server_upgrade(req, move |x| on_client(d.clone(), x))
            .await
            .map_err(|_| panic!("upgrade"));
    }
    Ok(Response::new(Body::from(JS_BATCH_HANDLER)))
}

impl Display {
    pub async fn new(port: u16, eval: Evaluator, management: Port, f: Forwarder, data: Port) {
        let d = Display {
            eval,
            management: management.clone(),
            data: data.clone(),
            f: f.clone(),
        };
        let addr = ([0, 0, 0, 0], port).into();

        let m2 = management.clone();
        if let Ok(server) = Server::try_bind(&addr) {
            let server = server.serve(make_service_fn(move |_conn| {
                let d = d.clone();
                async move { Ok::<_, Infallible>(service_fn(move |req| hello(d.clone(), req))) }
            }));

            async_error!(m2.clone(), server.await);
        }
    }
}
