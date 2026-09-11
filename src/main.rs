use std::{
    cell::Cell,
    net::{SocketAddr, ToSocketAddrs as _},
    ops::ControlFlow,
    path::PathBuf,
    pin::pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, anyhow};
use clap::{ArgAction, Parser};
use crossbeam::atomic::AtomicCell;
use futures::FutureExt as _;
use hyper::{Request, body::Incoming, client::conn::http1::SendRequest};
use hyper_util::rt::TokioIo;
use rustc_hash::FxHashMap;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{ReceiverStream, UnboundedReceiverStream},
};
use tracing::{Instrument, level_filters::LevelFilter};

use crate::script::{
    ArgTy, ArgsMap, DataPoint, LuaDt, MetricData, NamedSource, Response, RuntimeMsg, RuntimeState,
    VuState, load_config, trace_args,
};

mod script;

#[derive(clap::Parser)]
struct Cli {
    /// Lua script that defines workload schedule and behavior
    script: PathBuf,

    /// Address to connect to
    #[clap(long, short = 'H')]
    host: String,

    /// Number of VUs (effectively maximum concurrency)
    #[clap(long, short = 'u')]
    vus: usize,

    /// Output file for metrics
    #[clap(long, short = 'o')]
    output: Option<PathBuf>,

    /// Args that will be passed to the script
    #[clap(trailing_var_arg = true, allow_hyphen_values = true)]
    trailing: Vec<String>,
}

fn setup_tracing() -> anyhow::Result<()> {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt};

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(
                    tracing_subscriber::fmt::format::FmtSpan::NEW
                        | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                )
                .with_thread_names(false)
                .with_writer(std::io::stderr)
                .with_filter(
                    tracing_subscriber::EnvFilter::builder()
                        .with_default_directive(LevelFilter::INFO.into())
                        .from_env()?,
                ),
        )
        .init();
    Ok(())
}

fn parse_script_args(
    script_path: &str,
    script: &str,
    args: &Vec<String>,
) -> anyhow::Result<FxHashMap<String, String>> {
    let param_schema = trace_args(script)?;

    let mut cmd = clap::Command::new(script_path.to_string())
        .no_binary_name(true)
        .ignore_errors(true)
        .allow_external_subcommands(true);
    for param in &param_schema {
        if matches!(param.ty, ArgTy::Bool) {
            cmd = cmd.arg(
                clap::Arg::new(&param.name)
                    .long(&param.name)
                    .action(ArgAction::SetTrue),
            );
            let negated_name = param
                .name
                .strip_prefix("no-")
                .map(str::to_string)
                .unwrap_or_else(|| format!("no-{}", param.name));
            cmd = cmd.arg(
                clap::Arg::new(negated_name)
                    .long(&param.name)
                    .action(ArgAction::SetFalse),
            );
        } else {
            cmd = cmd.arg(clap::Arg::new(&param.name).long(&param.name));
        }
    }

    let matches = cmd.get_matches_from(args);

    let mut args = FxHashMap::default();

    for param in param_schema {
        if matches!(param.ty, ArgTy::Bool) {
            args.insert(
                param.name.clone(),
                matches.get_flag(&param.name).to_string(),
            );
        } else {
            if let Some(value) = matches.get_one::<String>(&param.name) {
                args.insert(param.name.clone(), value.clone());
            }
        }
    }

    Ok(args)
}

fn resolve_connectable_address(addr: &str) -> Option<SocketAddr> {
    addr.to_socket_addrs()
        .unwrap()
        .find(|&addr| std::net::TcpStream::connect(addr).is_ok())
}

#[derive(Debug)]
enum FatalError {
    VuDisconnected,
    Connect(std::io::ErrorKind),
    Handshake(hyper::Error),
    RequestFormat(http::Error),
}

enum SendError {
    Disconnected,
    Timeout,
}

type HttpRequest = http::Request<String>;

type HttpSender = SendRequest<String>;

fn script_request_to_http(request: script::Request) -> Result<HttpRequest, FatalError> {
    Ok(Request::builder()
        .uri(request.path)
        .body(request.body.unwrap_or_default())
        .map_err(|e| FatalError::RequestFormat(e))?)
}

struct ResponseInfo {
    status: u16,
}

enum ResponseError {
    Disconnected,
    TimedOut,
}

async fn handle_request_message(
    request: HttpRequest,
    timeout: Option<Duration>,
    sender: &mut HttpSender,
) -> Result<ResponseInfo, ResponseError> {
    let result_fut = sender.send_request(request).map(|res| {
        res.map(|res| ResponseInfo {
            status: res.status().as_u16(),
        })
        .map_err(|_| ResponseError::Disconnected)
    });

    if let Some(timeout) = timeout {
        let timeout_fut = tokio::time::sleep(timeout);
        // the http request is cancel-safe, but results in the connection being
        // closed. this means that both explicit disconnection and timing out
        // are to be treated the same by the outer handler loop
        tokio::select! {
            _ = timeout_fut => Err(ResponseError::TimedOut),
            result = result_fut => result,
        }
    } else {
        result_fut.await
    }
}

#[tracing::instrument(level = "trace", skip_all, ret)]
async fn handle_runtime_messages(
    addr: SocketAddr,
    messages: &mut (impl Stream<Item = RuntimeMsg> + Unpin),
) -> Result<ControlFlow<()>, FatalError> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| FatalError::Connect(e.kind()))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(FatalError::Handshake)?;

    tokio::spawn(
        async move {
            let _ = conn.await.inspect_err(|err| {
                tracing::trace!(%err);
            });
        }
        .instrument(tracing::trace_span!("conn")),
    );

    while let Some(message) = messages.next().await {
        match message {
            script::RuntimeMsg::Request { request, response } => {
                let timeout = request.timeout.map(Into::into);
                let request = script_request_to_http(request)?;
                match handle_request_message(request, timeout, &mut sender).await {
                    Ok(resp) => {
                        response
                            .send(script::Response {
                                status: resp.status,
                                error: None,
                            })
                            .map_err(|_| FatalError::VuDisconnected)?;
                    }
                    Err(e) => {
                        response
                            .send(script::Response {
                                status: 0,
                                error: Some(match e {
                                    ResponseError::Disconnected => {
                                        script::ResponseError::Disconnected
                                    }
                                    ResponseError::TimedOut => script::ResponseError::TimedOut,
                                }),
                            })
                            .map_err(|_| FatalError::VuDisconnected)?;
                        return Ok(ControlFlow::Continue(()));
                    }
                }
            }
            script::RuntimeMsg::Sleep { duration, response } => {
                tokio::time::sleep(duration).await;
                response.send(()).map_err(|_| FatalError::VuDisconnected)?;
            }
        }
    }

    Ok(ControlFlow::Break(()))
}

#[tracing::instrument(level = "trace", skip_all, fields(%addr), ret)]
async fn reactor_loop(
    addr: SocketAddr,
    messages: &mut (impl Stream<Item = RuntimeMsg> + Unpin),
) -> Result<(), FatalError> {
    loop {
        match handle_runtime_messages(addr, messages).await? {
            ControlFlow::Continue(_) => {}
            ControlFlow::Break(_) => break Ok(()),
        }
    }
}

#[derive(serde::Serialize)]
struct DataPointSer {
    metric: String,
    time: LuaDt,
    data: FxHashMap<String, MetricData>,
}

async fn collect_data_points_json_file(
    stream: &mut (impl Stream<Item = DataPoint> + Unpin),
    file: &mut (impl AsyncWrite + Unpin),
) -> anyhow::Result<()> {
    let mut writer = BufWriter::new(file);

    let mut buf = Vec::new();

    while let Some(item) = stream.next().await {
        serde_json::to_writer(
            &mut buf,
            &DataPointSer {
                metric: item.metric,
                time: item.timestamp,
                data: item.values,
            },
        )
        .expect("serialization should succeed");
        writer.write_all(&buf).await?;
        writer.write_all(b"\n").await?;
        buf.clear();
    }

    writer.flush().await?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
#[error("opening file {0}")]
struct OpenFile(PathBuf);

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    setup_tracing()?;

    let script =
        std::fs::read_to_string(&cli.script).with_context(|| OpenFile(cli.script.clone()))?;

    let script_name = cli.script.to_string_lossy().to_string();

    let args = parse_script_args(&script_name, &script, &cli.trailing)?;

    println!("wrk3");
    println!();

    for (key, value) in &args {
        println!("{key} => {value}")
    }
    println!();

    let config = load_config(
        Arc::new(args.clone()),
        NamedSource::new(&script_name, &script),
    )?;
    for stage in &config.stages {
        println!("* {} {}req/s", stage.duration, stage.rate);
    }

    let addr = resolve_connectable_address(&cli.host)
        .ok_or_else(|| anyhow!("failed to resolve address {}", cli.host))?;

    println!("addr {addr}");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(|| {
            static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
            let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
            format!("worker-{id}")
        })
        .build()?;

    runtime.block_on(async {
        let (runtime_tx, runtime_rx) = mpsc::channel(1);
        let (metrics_tx, metrics_rx) = mpsc::unbounded_channel::<DataPoint>();

        let metrics_jh = if let Some(output_path) = &cli.output {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output_path)
                .await
                .with_context(|| OpenFile(output_path.clone()))?;
            tokio::spawn(async move {
                collect_data_points_json_file(
                    &mut UnboundedReceiverStream::new(metrics_rx),
                    &mut file,
                )
                .await
            })
        } else {
            tokio::spawn(
                async move {
                    let mut stream = UnboundedReceiverStream::new(metrics_rx);
                    while let Some(point) = stream.next().await {
                        tracing::trace!(
                            time = %point.timestamp,
                            metric = point.metric,
                            data = ?point.values,
                        );
                    }
                    Ok(())
                }
                .instrument(tracing::trace_span!("metrics")),
            )
        };

        tokio::spawn({
            let args = FxHashMap::clone(&args);
            let script = script.clone();
            let script_name = script_name.clone();
            async move {
                let start_time = Arc::new(AtomicCell::new(Instant::now()));
                let state = VuState::new(
                    Arc::new(args),
                    NamedSource::new(&script_name, &script),
                    RuntimeState {
                        sender: runtime_tx,
                        metrics: metrics_tx,
                        start_time,
                    },
                )
                .unwrap();
                state.run_main().await.unwrap();
                state.run_main().await.unwrap();
                state.run_main().await.unwrap();
            }
        });

        tokio::spawn(async move {
            reactor_loop(addr, &mut ReceiverStream::new(runtime_rx))
                .await
                .unwrap();
        })
        .await
        .unwrap();

        metrics_jh.await.unwrap()?;

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}
