use std::{
    collections::VecDeque,
    net::{SocketAddr, ToSocketAddrs as _},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, anyhow, bail};
use clap::{ArgAction, Parser};
use crossbeam::atomic::AtomicCell;
use futures::{FutureExt as _, StreamExt as _};
use http_body_util::BodyExt;
use hyper::{Request, client::conn::http1::SendRequest};
use hyper_util::rt::TokioIo;
use indicatif::ProgressStyle;
use rustc_hash::FxHashMap;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, UnboundedReceiverStream},
};
use tracing::{Instrument, Span, level_filters::LevelFilter};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::script::{
    ArgTy, ArgsMap, DataPoint, LuaDt, MainCtx, MetricData, NamedSource, RuntimeMsg, RuntimeState,
    VuState, data_point, load_config, trace_args,
};

mod script;

shadow_rs::shadow!(build);

#[derive(clap::Parser)]
#[clap(version = version_string_static())]
struct Cli {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Run workload specified by a script
    Run(RunCmd),
    /// Execute a single request using the script
    Request(RequestCmd),
}

#[derive(clap::Args)]
struct RunCmd {
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

    /// Number of times to attempt dispatching each iteration to the pool of VUs
    #[clap(long, default_value_t = 8)]
    dispatch_attempts: usize,

    /// Args that will be passed to the script
    #[clap(trailing_var_arg = true, allow_hyphen_values = true)]
    trailing: Vec<String>,
}

#[derive(clap::Args)]
struct RequestCmd {
    /// Lua script that defines request behavior
    script: PathBuf,

    /// Address to connect to
    #[clap(long, short = 'H')]
    host: String,

    /// Args that will be passed to the script
    #[clap(trailing_var_arg = true, allow_hyphen_values = true)]
    trailing: Vec<String>,

    /// Seed for the Lua PRNG
    #[clap(long, short = 's', default_value_t = 0)]
    seed: u64,
}

fn banner() -> &'static str {
    concat!(
        r"   _     _     __ ____   __ __ _____ ",
        "\n",
        r"  | |   / |   / // __ \ / // //____ |",
        "\n",
        r"  | |  /  |  / // /_/ // //_/    _/_/",
        "\n",
        r"  | | / / | / // __ _// /\ \     \ \ ",
        "\n",
        r"  | |/ /| |/ // / \ \/ /  \ \____/ / ",
        "\n",
        r"  |___/ |___//_/   \_\/    \_\____/  ",
        "\n",
    )
}

fn version_string() -> String {
    if build::GIT_CLEAN {
        format!("{} ({})", build::PKG_VERSION, build::SHORT_COMMIT)
    } else {
        format!("{} ({}*)", build::PKG_VERSION, build::SHORT_COMMIT)
    }
}

fn version_string_static() -> &'static str {
    Box::leak(version_string().into_boxed_str())
}

fn setup_tracing() -> anyhow::Result<()> {
    use tracing_indicatif::{
        IndicatifLayer,
        filter::{IndicatifFilter, hide_indicatif_span_fields},
    };
    use tracing_subscriber::{
        Layer as _, fmt::format::DefaultFields, layer::SubscriberExt as _, util::SubscriberInitExt,
    };

    let indicatif_layer = IndicatifLayer::new();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(
                    tracing_subscriber::fmt::format::FmtSpan::NEW
                        | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                )
                .with_thread_names(false)
                .with_writer(indicatif_layer.get_stderr_writer())
                .with_filter(
                    tracing_subscriber::EnvFilter::builder()
                        .with_default_directive(LevelFilter::INFO.into())
                        .from_env()?,
                ),
        )
        .with(
            indicatif_layer
                .with_span_field_formatter(hide_indicatif_span_fields(DefaultFields::new()))
                .with_filter(IndicatifFilter::new(false)),
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
            let mut arg = clap::Arg::new(&param.name).long(&param.name);
            if let Some(default) = &param.default_value {
                arg = arg.default_value(default.to_string());
            }
            cmd = cmd.arg(arg);
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

#[derive(Debug, thiserror::Error)]
enum FatalError {
    #[error("vu worker stopped")]
    VuDisconnected,
    #[error("metrics collector stopped")]
    MetricsDisconnected,
    #[error("connection to server: {0}")]
    Connect(std::io::ErrorKind),
    #[error("http handshake")]
    Handshake(#[source] hyper::Error),
    #[error("request formatting")]
    RequestFormat(#[source] http::Error),
}

type HttpRequest = http::Request<String>;

type HttpSender = SendRequest<String>;

fn script_request_to_http(host: &str, request: script::Request) -> Result<HttpRequest, FatalError> {
    let mut b = Request::builder()
        .uri(request.path)
        .method(match request.method {
            script::Method::Get => "GET",
            script::Method::Post => "POST",
            script::Method::Put => "PUT",
            script::Method::Delete => "DELETE",
            script::Method::Patch => "PATCH",
        })
        .header("host", host);

    if let Some(headers) = request.headers {
        for (key, value) in headers {
            b = b.header(key, value);
        }
    }

    b.body(request.body.unwrap_or_default())
        .map_err(FatalError::RequestFormat)
}

struct ResponseInfo {
    status: u16,
    body: hyper::body::Incoming,
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
            body: res.into_body(),
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

async fn discard_body(mut body: hyper::body::Incoming) -> Result<(), anyhow::Error> {
    while let Some(frame) = body.frame().await {
        let _ = frame?;
    }
    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, ret)]
async fn handle_runtime_messages(
    addr: SocketAddr,
    messages: &mut (impl Stream<Item = RuntimeMsg> + Unpin),
    host: &str,
    start_time: Arc<AtomicCell<Instant>>,
    metrics: mpsc::UnboundedSender<DataPoint>,
) -> Result<ControlFlow<()>, FatalError> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| FatalError::Connect(e.kind()))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(FatalError::Handshake)?;

    metrics
        .send(data_point!(
            Instant::now() - start_time.load(),
            "wrk3.tcp_connections.d",
            value = Int(1)
        ))
        .map_err(|_| FatalError::MetricsDisconnected)?;

    tokio::spawn(
        async move {
            let _ = conn.await.inspect_err(|err| {
                tracing::trace!(%err);
            });
            let _ = metrics.send(data_point!(
                Instant::now() - start_time.load(),
                "wrk3.tcp_disconnections.d",
                value = Int(1)
            ));
        }
        .instrument(tracing::trace_span!("conn")),
    );

    while let Some(message) = messages.next().await {
        match message {
            script::RuntimeMsg::Request { request, response } => {
                let timeout = request.timeout.map(Into::into);
                let request = script_request_to_http(host, request)?;
                match handle_request_message(request, timeout, &mut sender).await {
                    Ok(resp) => {
                        tokio::spawn(
                            async move {
                                discard_body(resp.body)
                                    .await
                                    .inspect_err(|err| tracing::trace!(%err))
                            }
                            .instrument(tracing::trace_span!("drain_body")),
                        );
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

async fn reactor_loop(
    addr: SocketAddr,
    messages: &mut (impl Stream<Item = RuntimeMsg> + Unpin),
    host: &str,
    start_time: Arc<AtomicCell<Instant>>,
    metrics: mpsc::UnboundedSender<DataPoint>,
) -> Result<(), FatalError> {
    loop {
        match handle_runtime_messages(addr, messages, host, start_time.clone(), metrics.clone())
            .await?
        {
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

    while let Some(point) = stream.next().await {
        trace_data_point(&point);
        serde_json::to_writer(
            &mut buf,
            &DataPointSer {
                metric: point.metric,
                time: point.timestamp,
                data: point.values,
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

struct Script {
    name: String,
    source: String,
}

#[derive(Clone)]
struct IterationInfo {
    num: usize,
}

async fn vu_loop(
    args: Arc<FxHashMap<String, String>>,
    script: Arc<Script>,
    sender: mpsc::Sender<RuntimeMsg>,
    metrics: mpsc::UnboundedSender<DataPoint>,
    start_time: Arc<AtomicCell<Instant>>,
    mut start_signal: impl Stream<Item = IterationInfo> + Unpin,
) -> anyhow::Result<()> {
    let state = VuState::new(
        args,
        NamedSource::new(&script.name, &script.source),
        RuntimeState {
            sender,
            metrics: metrics.clone(),
            start_time: start_time.clone(),
        },
    )?;

    let mut last_active = Instant::now();

    while let Some(info) = start_signal.next().await {
        let before_run = Instant::now();
        let inactive_time = before_run - last_active;

        state.seed_random(info.num as u64)?;
        state
            .run_main(MainCtx {
                iteration: info.num,
            })
            .await?;

        let after_run = Instant::now();
        last_active = after_run;
        let active_time = after_run - before_run;

        let ts = before_run - start_time.load();
        metrics.send(data_point!(
            ts,
            "wrk3.vu_active",
            duration = Dt(active_time.into())
        ))?;
        metrics.send(data_point!(
            ts,
            "wrk3.vu_inactive",
            duration = Dt(inactive_time.into())
        ))?;
    }

    Ok(())
}

#[derive(Clone)]
struct HostInfo {
    addr: SocketAddr,
    host: Arc<str>,
}

struct VuPool {
    start_time: Arc<AtomicCell<Instant>>,
    input_channels: Vec<mpsc::Sender<IterationInfo>>,
    vu_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl VuPool {
    async fn spawn(
        num: usize,
        host: HostInfo,
        metrics: mpsc::UnboundedSender<DataPoint>,
        args: ArgsMap,
        script: Arc<Script>,
    ) -> Self {
        let start_time = Arc::new(AtomicCell::new(Instant::now()));
        let mut input_channels = Vec::new();
        let mut vu_handles = Vec::new();

        let mut started_signals = Vec::new();

        for vu_idx in 0..num {
            let (input_tx, input_rx) = mpsc::channel(1);
            let (runtime_tx, runtime_rx) = mpsc::channel(1);

            let args = args.clone();
            let script = script.clone();
            let metrics = metrics.clone();
            let start_time = start_time.clone();

            let (vu_signal_tx, vu_signal_rx) = oneshot::channel();
            let (reactor_signal_tx, reactor_signal_rx) = oneshot::channel();

            started_signals.push(vu_signal_rx);
            started_signals.push(reactor_signal_rx);

            let host = host.clone();

            let vu_jh = tokio::spawn(
                async move {
                    let vu_jh = tokio::spawn({
                        let metrics = metrics.clone();
                        let start_time = start_time.clone();
                        async move {
                            vu_signal_tx.send(()).unwrap();
                            vu_loop(
                                args,
                                script,
                                runtime_tx,
                                metrics,
                                start_time,
                                ReceiverStream::new(input_rx),
                            )
                            .await
                        }
                        .instrument(tracing::trace_span!("script"))
                    });
                    let reactor_jh = tokio::spawn(
                        async move {
                            reactor_signal_tx.send(()).unwrap();
                            reactor_loop(
                                host.addr,
                                &mut ReceiverStream::new(runtime_rx),
                                &host.host,
                                start_time,
                                metrics,
                            )
                            .await
                        }
                        .instrument(tracing::trace_span!("reactor", addr = %host.addr)),
                    );

                    vu_jh.await.unwrap()?;
                    reactor_jh.await.unwrap()?;

                    Ok::<_, anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("vu", idx = vu_idx)),
            );

            input_channels.push(input_tx);
            vu_handles.push(vu_jh);
        }

        for signal in started_signals.drain(..) {
            signal.await.unwrap();
        }
        tracing::trace!("all vus spawned");

        VuPool {
            start_time,
            input_channels,
            vu_handles,
        }
    }

    fn set_start_time(&self, instant: Instant) {
        self.start_time.store(instant);
    }

    async fn join(mut self) -> anyhow::Result<()> {
        self.input_channels.clear();
        for jh in self.vu_handles.drain(..) {
            jh.await.unwrap()?;
        }
        Ok(())
    }

    fn count(&self) -> usize {
        self.input_channels.len()
    }

    fn start_iteration(&self, vu_idx: usize, info: IterationInfo) -> Result<(), StartError> {
        let tx = self
            .input_channels
            .get(vu_idx)
            .expect("vu idx should be in range");

        match tx.try_send(info) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(StartError::Occupied),
            Err(TrySendError::Closed(_)) => Err(StartError::Failed),
        }
    }
}

enum DispatchOk {
    Started,
    Skip,
}

trait Dispatcher {
    fn dispatch_iteration(&mut self, pool: &VuPool, info: IterationInfo) -> Result<DispatchOk, ()>;
}

#[derive(Default)]
struct RoundRobinDispatcher {
    next: usize,
}

impl Dispatcher for RoundRobinDispatcher {
    fn dispatch_iteration(&mut self, pool: &VuPool, info: IterationInfo) -> Result<DispatchOk, ()> {
        let res = match pool.start_iteration(self.next, info) {
            Ok(_) => Ok(DispatchOk::Started),
            Err(StartError::Occupied) => Ok(DispatchOk::Skip),
            Err(StartError::Failed) => Err(()),
        };
        self.next = (self.next + 1).rem_euclid(pool.count());
        res
    }
}

struct RetryDispatcher<D> {
    inner: D,
    max_iters: usize,
}

impl<D> Dispatcher for RetryDispatcher<D>
where
    D: Dispatcher,
{
    fn dispatch_iteration(&mut self, pool: &VuPool, info: IterationInfo) -> Result<DispatchOk, ()> {
        for _ in 0..self.max_iters {
            if matches!(
                self.inner.dispatch_iteration(pool, info.clone())?,
                DispatchOk::Started
            ) {
                return Ok(DispatchOk::Started);
            }
        }
        Ok(DispatchOk::Skip)
    }
}

enum StartError {
    Occupied,
    Failed,
}

fn trace_data_point(point: &DataPoint) {
    tracing::trace!(
        target: "wrk3::metrics",
        time = %point.timestamp,
        metric = point.metric,
        data = ?point.values,
    );
}

fn metrics_span() -> Span {
    tracing::trace_span!(target: "wrk3::metrics", "metrics")
}

async fn spawn_metrics_task(
    output_file: Option<&Path>,
) -> anyhow::Result<(
    tokio::task::JoinHandle<anyhow::Result<()>>,
    mpsc::UnboundedSender<DataPoint>,
)> {
    let (metrics_tx, metrics_rx) = mpsc::unbounded_channel::<DataPoint>();

    let metrics_jh = if let Some(output_path) = &output_file {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path)
            .await
            .with_context(|| OpenFile(output_path.into()))?;
        tokio::spawn(
            async move {
                collect_data_points_json_file(
                    &mut UnboundedReceiverStream::new(metrics_rx),
                    &mut file,
                )
                .await
            }
            .instrument(metrics_span()),
        )
    } else {
        tokio::spawn(
            async move {
                let mut stream = UnboundedReceiverStream::new(metrics_rx);
                while let Some(point) = stream.next().await {
                    trace_data_point(&point);
                }
                Ok(())
            }
            .instrument(metrics_span()),
        )
    };

    Ok((metrics_jh, metrics_tx))
}

#[derive(thiserror::Error, Debug)]
#[error("opening file {0}")]
struct OpenFile(PathBuf);

struct Stage {
    start_time: Instant,
    duration: Duration,
    request_delay: Duration,
    emitted: usize,
}

impl Stage {
    fn end_time(&self) -> Instant {
        self.start_time + self.duration
    }

    fn nth_event_time(&self, n: usize) -> Instant {
        self.start_time + (n as u32 * self.request_delay)
    }

    fn events_due(&self, now: Instant) -> usize {
        let now = std::cmp::min(now, self.end_time());
        if now <= self.start_time {
            return 0;
        }
        ((now - self.start_time).as_secs_f64() / self.request_delay.as_secs_f64()) as usize
    }
}

struct SteppedRateSchedule {
    current_time: Instant,
    stages: VecDeque<Stage>,
    start_time: Instant,
    total_duration: Duration,
}

trait Schedule {
    /// Advance the current time and determine how many events were supposed to have taken place
    fn advance(&mut self, now: Instant) -> usize;

    fn next_event_time(&mut self) -> Option<Instant>;

    fn progress(&self) -> f64;
}

impl Schedule for SteppedRateSchedule {
    fn advance(&mut self, now: Instant) -> usize {
        let mut events = 0;
        while let Some(stage) = self.stages.front_mut() {
            let due = stage.events_due(now);
            events += due - stage.emitted;
            stage.emitted = due;

            if now < stage.end_time() {
                break;
            }
            self.stages.pop_front();
        }
        self.current_time = now;
        events
    }

    fn next_event_time(&mut self) -> Option<Instant> {
        let stage = self.stages.front()?;
        Some(stage.nth_event_time(stage.emitted + 1))
    }

    fn progress(&self) -> f64 {
        let since_start = self.current_time - self.start_time;
        since_start.as_secs_f64() / self.total_duration.as_secs_f64()
    }
}

impl SteppedRateSchedule {
    fn new(start_time: Instant, script_stages: impl IntoIterator<Item = script::Stage>) -> Self {
        let mut stages = Vec::new();
        let mut stage_start = start_time;
        for stage in script_stages {
            let duration = stage.duration.into();
            stages.push(Stage {
                start_time: stage_start,
                duration,
                request_delay: Duration::from_secs_f64(stage.rate.recip()),
                emitted: 0,
            });
            stage_start += duration;
        }
        Self {
            current_time: start_time,
            stages: stages.into(),
            start_time,
            total_duration: stage_start - start_time,
        }
    }
}

async fn run_schedule(
    start_time: Instant,
    mut schedule: impl Schedule,
    mut dispatcher: impl Dispatcher,
    pool: &VuPool,
    metrics: mpsc::UnboundedSender<DataPoint>,
) -> anyhow::Result<()> {
    let mut iteration_num = 0;

    Span::current().pb_set_length(100);

    while let Some(next_time) = schedule.next_event_time() {
        let num_events = schedule.advance(Instant::now());
        tracing::trace!(num_events);

        Span::current().pb_set_position((schedule.progress() * 100.0) as u64);

        for _ in 0..num_events {
            let ts = Instant::now() - start_time;

            let res = dispatcher
                .dispatch_iteration(pool, IterationInfo { num: iteration_num })
                .map_err(|_| anyhow!("vu failed"))?;

            metrics.send(data_point!(ts, "wrk3.iterations.d", value = Int(1)))?;

            if matches!(res, DispatchOk::Skip) {
                tracing::warn!(iteration_num, "iteration skipped");
                metrics.send(data_point!(ts, "wrk3.skipped_iterations.d", value = Int(1)))?;
            } else {
                metrics.send(data_point!(
                    ts,
                    "wrk3.completed_iterations.d",
                    value = Int(1)
                ))?;
            }

            iteration_num += 1;
        }

        tracing::trace!(sleep_duration = ?(next_time - Instant::now()));
        tokio::time::sleep_until(next_time.into()).await;
    }

    Ok(())
}

fn create_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(|| {
            static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
            let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
            format!("worker-{id}")
        })
        .build()?;
    Ok(runtime)
}

fn run_cmd(cli: RunCmd) -> anyhow::Result<()> {
    let script =
        std::fs::read_to_string(&cli.script).with_context(|| OpenFile(cli.script.clone()))?;

    let script_name = cli.script.to_string_lossy().to_string();

    let args = parse_script_args(&script_name, &script, &cli.trailing)?;

    println!("{}", banner());

    println!("Version: {}", version_string());
    println!();

    if !args.is_empty() {
        println!("Script Args:");
        for (key, value) in &args {
            println!("  {key} => {value}")
        }
        println!();
    }

    let config = load_config(
        Arc::new(args.clone()),
        NamedSource::new(&script_name, &script),
    )?;
    if !config.stages.is_empty() {
        println!("Schedule:");
        for stage in &config.stages {
            println!("  * {} @ {}req/s", stage.duration, stage.rate);
        }
        println!();
    }

    let addr = resolve_connectable_address(&cli.host)
        .ok_or_else(|| anyhow!("failed to resolve address {}", cli.host))?;

    println!("Host: {} => {addr}", cli.host);
    println!();

    create_runtime()?.block_on(async {
        let (metrics_jh, metrics_tx) = spawn_metrics_task(cli.output.as_deref()).await?;

        let pool = VuPool::spawn(
            cli.vus,
            HostInfo {
                addr,
                host: cli.host.into(),
            },
            metrics_tx.clone(),
            Arc::new(args),
            Arc::new(Script {
                name: script_name,
                source: script,
            }),
        )
        .await;

        tokio::spawn(
            async move {
                let start_time = Instant::now();
                pool.set_start_time(start_time);

                Span::current().pb_set_style(
                    &ProgressStyle::with_template("[{elapsed_precise}] [{bar:50}] {percent}%")
                        .expect("template should be valid")
                        .progress_chars("=> "),
                );

                let schedule_res = run_schedule(
                    start_time,
                    SteppedRateSchedule::new(start_time, config.stages),
                    RetryDispatcher {
                        inner: RoundRobinDispatcher::default(),
                        max_iters: cli.dispatch_attempts,
                    },
                    &pool,
                    metrics_tx,
                )
                .await;

                // the errors from the pool will be better since it will have the actual lua errors
                pool.join().await?;

                schedule_res?;

                Ok::<_, anyhow::Error>(())
            }
            .instrument(tracing::trace_span!("schedule", indicatif.pb_show = true)),
        )
        .await
        .unwrap()?;

        metrics_jh.await.unwrap()?;

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

async fn get_first_msg(
    args: ArgsMap,
    source: NamedSource<'_>,
    seed: u64,
) -> anyhow::Result<RuntimeMsg> {
    let (metrics, metrics_rx) = mpsc::unbounded_channel();
    tokio::spawn(async {
        UnboundedReceiverStream::new(metrics_rx)
            .for_each(|_| async {})
            .await;
    });

    let (sender, mut rx) = mpsc::channel(1);
    let vu_state = VuState::new(
        args,
        source,
        RuntimeState {
            sender,
            metrics,
            start_time: Arc::new(AtomicCell::new(Instant::now())),
        },
    )?;

    vu_state.seed_random(seed)?;
    let complete_fut = vu_state.run_main(MainCtx { iteration: 0 });
    let msg_fut = rx.recv();

    let err_str = "main completed before causing any effects";
    tokio::select! {
        _ = complete_fut => bail!(err_str),
        msg = msg_fut => Ok(msg.ok_or_else(|| anyhow!(err_str))?)
    }
}

fn request_cmd(cli: RequestCmd) -> anyhow::Result<()> {
    let script =
        std::fs::read_to_string(&cli.script).with_context(|| OpenFile(cli.script.clone()))?;

    let script_name = cli.script.to_string_lossy().to_string();

    let args = parse_script_args(&script_name, &script, &cli.trailing)?;

    let addr = resolve_connectable_address(&cli.host)
        .ok_or_else(|| anyhow!("failed to resolve address {}", cli.host))?;
    println!("* Resolved {} to {}", cli.host, addr);

    create_runtime()?.block_on(async {
        let first_msg = get_first_msg(
            Arc::new(args),
            NamedSource::new(&script_name, &script),
            cli.seed,
        )
        .await?;

        let request = match first_msg {
            RuntimeMsg::Request { request, .. } => request,
            _ => bail!("the first effect must be a request"),
        };

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| FatalError::Connect(e.kind()))?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(FatalError::Handshake)?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        println!("* Connected to {addr}");

        println!("> {} {}", request.method, request.path);
        if let Some(map) = &request.headers {
            for (k, v) in map {
                println!("> {k}: {v}")
            }
        }
        if let Some(body) = &request.body {
            println!(">");
            for line in body.lines() {
                println!("> {line}");
            }
        }

        let request = script_request_to_http(&cli.host, request)?;
        let res = sender.send_request(request).await?;
        println!("* Request sent");

        println!(
            "< {} {}",
            res.status().as_u16(),
            res.status().canonical_reason().unwrap_or("")
        );
        for (k, v) in res.headers() {
            println!(
                "< {}: {}",
                k.as_str(),
                String::from_utf8_lossy(v.as_bytes())
            );
        }
        println!("<");
        for line in String::from_utf8_lossy(&res.into_body().collect().await?.to_bytes()).lines() {
            println!("< {line}");
        }

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    let args = Cli::parse();

    match args.cmd {
        Command::Run(cli) => run_cmd(cli),
        Command::Request(cli) => request_cmd(cli),
    }
}
