use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    num::{ParseFloatError, ParseIntError},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context as _;
use crossbeam::atomic::AtomicCell;
use mlua::{FromLua, IntoLua, LuaSerdeExt as _};
use rustc_hash::FxHashMap;
use serde::{Deserialize, de};
use tokio::sync::{mpsc, oneshot};

macro_rules! impl_from_lua_table {
    ($ty:ty, $($field:ident,)* $([$default_field:ident = $f:expr],)*) => {
        impl mlua::FromLua for $ty {
            fn from_lua(value: mlua::Value, lua: &mlua::Lua) -> mlua::Result<Self> {
                let table = mlua::Table::from_lua(value, lua)?;
                Ok(Self {
                    $($field: table.get(stringify!($field))?,)*
                    $($default_field: table.get(stringify!($default_field)).unwrap_or_else(|_| $f(lua)),)*
                })
            }
        }
    };
}

macro_rules! impl_lua_enum {
    ($ty:ty, $($str:literal => $variant:ident,)* _ => $err:expr $(,)?) => {
        impl mlua::FromLua for $ty {
            fn from_lua(
                value: mlua::prelude::LuaValue,
                lua: &mlua::prelude::Lua,
            ) -> mlua::prelude::LuaResult<Self> {
                let value_s = String::from_lua(value, lua)?;
                match value_s.to_lowercase().as_str() {
                    $($str => Ok(<$ty>::$variant),)*
                    _ => Err($err),
                }
            }
        }
    };
}

#[derive(Clone, Copy)]
pub struct LuaDt {
    duration: Duration,
}

impl mlua::UserData for LuaDt {
    fn add_methods<M: mlua::prelude::LuaUserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(mlua::MetaMethod::ToString, |_, this, (): ()| {
            Ok(format!("{}", humantime::format_duration(this.duration)))
        });
        methods.add_meta_method(mlua::MetaMethod::Sub, |_, this, rhs: LuaDt| {
            Ok(LuaDt {
                duration: this.duration - rhs.duration,
            })
        });
        methods.add_meta_method(mlua::MetaMethod::Eq, |_, this, rhs: LuaDt| {
            Ok(this.duration == rhs.duration)
        });
        methods.add_method("secs", |_, this, (): ()| Ok(this.duration.as_secs_f64()));
    }
}

impl FromStr for LuaDt {
    type Err = ParseDtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            duration: humantime::parse_duration(s).map_err(ParseDtError::InvalidFormat)?,
        })
    }
}

impl<'de> serde::Deserialize<'de> for LuaDt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = RawDtValue::deserialize(deserializer)?;
        value.into_lua_dt().map_err(de::Error::custom)
    }
}

impl serde::Serialize for LuaDt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{}", self.duration.as_nanos()).serialize(serializer)
    }
}

impl Debug for LuaDt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LuaDt")
            .field(&format!("{}", humantime::format_duration(self.duration)))
            .finish()
    }
}

impl Display for LuaDt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", humantime::format_duration(self.duration))
    }
}

impl From<LuaDt> for Duration {
    fn from(value: LuaDt) -> Self {
        value.duration
    }
}

impl From<Duration> for LuaDt {
    fn from(value: Duration) -> Self {
        Self { duration: value }
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum RawDtValue {
    I64(i64),
    F64(f64),
    String(String),
    Dt(LuaDt),
}

#[derive(thiserror::Error, Debug)]
pub enum ParseDtError {
    #[error("value must be positive")]
    MustBePositive,
    #[error("invalid dt format: {0}")]
    InvalidFormat(humantime::DurationError),
}

impl RawDtValue {
    fn into_lua_dt(self) -> Result<LuaDt, ParseDtError> {
        match self {
            RawDtValue::I64(i) => {
                if i.is_negative() {
                    return Err(ParseDtError::MustBePositive);
                }
                Ok(LuaDt {
                    duration: Duration::from_secs(i as u64),
                })
            }
            RawDtValue::F64(n) => {
                if n.is_sign_negative() {
                    return Err(ParseDtError::MustBePositive);
                }
                Ok(LuaDt {
                    duration: Duration::from_secs_f64(n),
                })
            }
            RawDtValue::String(s) => {
                let duration =
                    humantime::parse_duration(&s).map_err(ParseDtError::InvalidFormat)?;
                Ok(LuaDt { duration })
            }
            RawDtValue::Dt(dt) => Ok(dt),
        }
    }
}

impl FromLua for LuaDt {
    fn from_lua(value: mlua::Value, _lua: &mlua::Lua) -> mlua::prelude::LuaResult<Self> {
        if let Some(userdata) = value.as_userdata()
            && let Ok(dt) = userdata.borrow::<LuaDt>()
        {
            return Ok(*dt);
        }
        let value = match value {
            mlua::Value::Integer(i) => RawDtValue::I64(i),
            mlua::Value::Number(n) => RawDtValue::F64(n),
            mlua::Value::String(s) => RawDtValue::String(s.to_str()?.to_string()),
            _ => {
                return Err(mlua::Error::external(format!(
                    "{} cannot be converted to dt",
                    value.type_name()
                )));
            }
        };
        value.into_lua_dt().map_err(mlua::Error::external)
    }
}

#[derive(Clone, Debug)]
pub enum ArgValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Dt(LuaDt),
}

impl mlua::IntoLua for ArgValue {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::prelude::LuaResult<mlua::Value> {
        match self {
            ArgValue::Int(i) => i.into_lua(lua),
            ArgValue::Float(f) => f.into_lua(lua),
            ArgValue::Bool(b) => b.into_lua(lua),
            ArgValue::String(s) => s.into_lua(lua),
            ArgValue::Dt(lua_dt) => lua_dt.into_lua(lua),
        }
    }
}

impl Display for ArgValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArgValue::Int(i) => write!(f, "{i}"),
            ArgValue::Float(n) => write!(f, "{n}"),
            ArgValue::Bool(b) => write!(f, "{b}"),
            ArgValue::String(s) => write!(f, "{s}"),
            ArgValue::Dt(dt) => write!(f, "{dt}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ArgTy {
    Int,
    Float,
    Bool,
    String,
    Dt,
}

#[derive(Debug)]
pub struct ExpectedArg {
    pub name: String,
    pub ty: ArgTy,
    pub default_value: Option<ArgValue>,
}

pub type ArgsMap = Arc<FxHashMap<String, String>>;

#[derive(Clone)]
pub enum ArgsState {
    Runtime {
        args: ArgsMap,
    },
    Trace {
        expected_args: Arc<Mutex<Vec<ExpectedArg>>>,
    },
}

#[derive(thiserror::Error, Debug)]
enum ArgError {
    #[error(transparent)]
    IntFormat(ParseIntError),
    #[error(transparent)]
    FloatFormat(ParseFloatError),
    #[error("expected 'true' or 'false'")]
    ExpectedBool,
    #[error(transparent)]
    DtFormat(ParseDtError),
    #[error("missing required argument '{0}'")]
    Missing(String),
}

impl ArgsState {
    fn get_arg(
        &self,
        name: &str,
        expected_type: ArgTy,
        default_value: Option<ArgValue>,
    ) -> Result<ArgValue, ArgError> {
        match &self {
            ArgsState::Runtime { args } => {
                let Some(value_s) = args.get(name) else {
                    if let Some(default) = default_value {
                        return Ok(default);
                    }
                    return Err(ArgError::Missing(name.into()));
                };
                Ok(match expected_type {
                    ArgTy::Int => ArgValue::Int(value_s.parse().map_err(ArgError::IntFormat)?),
                    ArgTy::Float => {
                        ArgValue::Float(value_s.parse().map_err(ArgError::FloatFormat)?)
                    }
                    ArgTy::Bool => match value_s.as_str() {
                        "true" => ArgValue::Bool(true),
                        "false" => ArgValue::Bool(false),
                        _ => return Err(ArgError::ExpectedBool),
                    },
                    ArgTy::String => ArgValue::String(value_s.clone()),
                    ArgTy::Dt => ArgValue::Dt(value_s.parse().map_err(ArgError::DtFormat)?),
                })
            }
            ArgsState::Trace { expected_args } => {
                expected_args.lock().unwrap().push(ExpectedArg {
                    name: name.into(),
                    ty: expected_type,
                    default_value,
                });
                Ok(match expected_type {
                    ArgTy::Int => ArgValue::Int(0),
                    ArgTy::Float => ArgValue::Float(0.0),
                    ArgTy::Bool => ArgValue::Bool(false),
                    ArgTy::String => ArgValue::String("".into()),
                    ArgTy::Dt => ArgValue::Dt(LuaDt {
                        duration: Duration::ZERO,
                    }),
                })
            }
        }
    }
}

fn create_args_mod(lua: &mlua::Lua, state: ArgsState) -> mlua::Result<mlua::Value> {
    let m = lua.create_table()?;

    macro_rules! arg_method {
        ($lua:expr, $m:expr, $state:expr, $name:ident, $rust_ty:ty, $enum_ty:ident) => {{
            struct Opts {
                default: Option<$rust_ty>,
            }
            impl_from_lua_table!(Opts, default,);
            m.set(
                stringify!($name),
                lua.create_function({
                    let state = $state.clone();
                    move |_lua, (name, opts): (String, Option<Opts>)| {
                        if let Some(opts) = opts {
                            state
                                .get_arg(
                                    &name,
                                    ArgTy::$enum_ty,
                                    opts.default.map(ArgValue::$enum_ty),
                                )
                                .map_err(mlua::Error::external)
                        } else {
                            state
                                .get_arg(&name, ArgTy::$enum_ty, None)
                                .map_err(mlua::Error::external)
                        }
                    }
                })?,
            )?;
        }};
    }

    arg_method!(lua, m, state, int, i64, Int);
    arg_method!(lua, m, state, float, f64, Float);
    arg_method!(lua, m, state, bool, bool, Bool);
    arg_method!(lua, m, state, string, String, String);
    arg_method!(lua, m, state, dt, LuaDt, Dt);

    m.into_lua(lua)
}

pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl_lua_enum!(Method,
    "get" => Get,
    "post" => Post,
    "put" => Put,
    "delete" => Delete,
    "patch" => Patch,
    _ => mlua::Error::external("expected a valid HTTP method"),
);

pub struct Request {
    pub path: String,
    pub method: Method,
    pub headers: Option<FxHashMap<String, String>>,
    pub body: Option<String>,
    pub timeout: Option<LuaDt>,
}

impl_from_lua_table!(
    Request,
    path,
    [method = |_| Method::Get],
    [headers = |_| None],
    [body = |_| None],
    [timeout = |_| None],
);

pub enum ResponseError {
    TimedOut,
    Disconnected,
}

pub struct Response {
    pub status: u16,
    pub error: Option<ResponseError>,
}

impl mlua::UserData for Response {
    fn add_fields<F: mlua::prelude::LuaUserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("status", |_, this| Ok(this.status));
        fields.add_field_method_get("is_timeout", |_, this| {
            Ok(matches!(this.error, Some(ResponseError::TimedOut)))
        });
        fields.add_field_method_get("is_disconnect", |_, this| {
            Ok(matches!(this.error, Some(ResponseError::Disconnected)))
        });
    }
}

pub enum RuntimeMsg {
    Request {
        request: Request,
        response: oneshot::Sender<Response>,
    },
    Sleep {
        duration: Duration,
        response: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum MetricData {
    Int(i64),
    Float(f64),
    Dt(LuaDt),
    Bool(bool),
}

#[derive(Debug)]
pub struct DataPoint {
    pub metric: String,
    pub timestamp: LuaDt,
    pub values: FxHashMap<String, MetricData>,
}

macro_rules! data_point {
    ($ts:expr, $name:expr, $($key:ident = $variant:ident ( $value:expr )),* $(,)?) => {
        crate::script::DataPoint {
            metric: $name.into(),
            timestamp: $ts.into(),
            values: rustc_hash::FxHashMap::from_iter([
                $((stringify!($key).into(), crate::script::MetricData::$variant($value)),)*
            ]),
        }
    };
}

pub(crate) use data_point;

#[derive(Clone)]
pub struct RuntimeState {
    pub sender: mpsc::Sender<RuntimeMsg>,
    pub metrics: mpsc::UnboundedSender<DataPoint>,
    pub start_time: Arc<AtomicCell<Instant>>,
}

struct LuaMetric {
    metrics: mpsc::UnboundedSender<DataPoint>,
    start_time: Arc<AtomicCell<Instant>>,
    schema: MetricSchema,
}

impl mlua::UserData for LuaMetric {
    fn add_methods<M: mlua::prelude::LuaUserDataMethods<Self>>(methods: &mut M) {
        struct AddExtraArgs {
            at: Option<LuaDt>,
        }
        impl_from_lua_table!(AddExtraArgs, at,);

        methods.add_method(
            "add",
            |lua, this, (table, extra): (mlua::Table, Option<AddExtraArgs>)| {
                let mut datapoint = DataPoint {
                    metric: this.schema.name.clone(),
                    timestamp: extra.and_then(|e| e.at).unwrap_or_else(|| LuaDt {
                        duration: Instant::now() - this.start_time.load(),
                    }),
                    values: FxHashMap::default(),
                };
                for pair in table.pairs() {
                    let (key, value): (String, mlua::Value) = pair?;
                    let value = if let Some(schema_ty) = this.schema.cols.get(&key) {
                        match schema_ty {
                            ColType::Dt => MetricData::Dt(LuaDt::from_lua(value, lua)?),
                            ColType::Int => MetricData::Int(i64::from_lua(value, lua)?),
                            ColType::Float => MetricData::Float(f64::from_lua(value, lua)?),
                            ColType::Bool => MetricData::Bool(bool::from_lua(value, lua)?),
                        }
                    } else {
                        match value {
                            mlua::Value::Boolean(b) => MetricData::Bool(b),
                            mlua::Value::UserData(_) => {
                                MetricData::Dt(LuaDt::from_lua(value, lua)?)
                            }
                            mlua::Value::Integer(i) => MetricData::Int(i),
                            mlua::Value::Number(n) => MetricData::Float(n),
                            _ => return Err(mlua::Error::external("unsupported value type")),
                        }
                    };
                    datapoint.values.insert(key, value);
                }
                this.metrics
                    .send(datapoint)
                    .map_err(|_| mlua::Error::external("disconnected from runtime"))?;
                Ok(())
            },
        );
    }
}

impl serde::Serialize for MetricData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MetricData::Int(i) => i.serialize(serializer),
            MetricData::Float(f) => f.serialize(serializer),
            MetricData::Dt(lua_dt) => lua_dt.serialize(serializer),
            MetricData::Bool(b) => b.serialize(serializer),
        }
    }
}

fn create_runtime_fns(lua: &mlua::Lua, m: &mlua::Table, state: RuntimeState) -> mlua::Result<()> {
    m.set(
        "request",
        lua.create_async_function({
            let state = state.clone();
            move |_lua, request: Request| {
                let state = state.clone();
                async move {
                    let (response, res) = oneshot::channel();
                    state
                        .sender
                        .send(RuntimeMsg::Request { request, response })
                        .await
                        .map_err(|_| mlua::Error::external("not connected to runtime"))?;
                    res.await
                        .map_err(|_| mlua::Error::external("not connected to runtime"))
                }
            }
        })?,
    )?;

    m.set(
        "sleep",
        lua.create_async_function({
            let state = state.clone();
            move |_lua, duration: LuaDt| {
                let state = state.clone();
                async move {
                    let (response, res) = oneshot::channel();
                    state
                        .sender
                        .send(RuntimeMsg::Sleep {
                            duration: duration.duration,
                            response,
                        })
                        .await
                        .map_err(|_| mlua::Error::external("not connected to runtime"))?;
                    res.await
                        .map_err(|_| mlua::Error::external("not connected to runtime"))?;
                    Ok(())
                }
            }
        })?,
    )?;

    m.set(
        "metric",
        lua.create_function({
            let state = state.clone();
            move |_lua, schema: MetricSchema| {
                Ok(LuaMetric {
                    metrics: state.metrics.clone(),
                    start_time: state.start_time.clone(),
                    schema,
                })
            }
        })?,
    )?;

    Ok(())
}

pub struct Wrk3State {
    pub args_state: ArgsState,
    pub runtime_state: RuntimeState,
}

fn create_wrk3_mod(lua: &mlua::Lua, state: Wrk3State) -> mlua::Result<mlua::Value> {
    let m = lua.create_table()?;

    m.set("args", create_args_mod(lua, state.args_state)?)?;

    m.set("dt", lua.create_function(|_lua, dt: LuaDt| Ok(dt))?)?;

    m.set(
        "now",
        lua.create_function({
            let start_time = state.runtime_state.start_time.clone();
            move |_lua, (): ()| {
                Ok(LuaDt {
                    duration: Instant::now() - start_time.load(),
                })
            }
        })?,
    )?;

    m.set(
        "formdata",
        lua.create_function(|_, table: mlua::Table| {
            let mut s = form_urlencoded::Serializer::new(String::new());
            for pair in table.pairs::<String, String>() {
                let (k, v) = pair?;
                s.append_pair(&k, &v);
            }
            Ok(s.finish())
        })?,
    )?;

    m.set(
        "json",
        lua.create_function(|lua, value: mlua::Value| {
            let json_value: serde_json::Value = lua.from_value(value)?;
            serde_json::to_string(&json_value)
                .context("serializing value to JSON")
                .map_err(mlua::Error::external)
        })?,
    )?;

    create_runtime_fns(lua, &m, state.runtime_state)?;

    m.into_lua(lua)
}

#[derive(Copy, Clone)]
pub struct NamedSource<'a> {
    name: &'a str,
    code: &'a str,
}

impl<'a> NamedSource<'a> {
    pub fn new(name: &'a str, code: &'a str) -> Self {
        Self { name, code }
    }
}

impl mlua::chunk::AsChunk for NamedSource<'_> {
    fn source<'a>(&self) -> std::io::Result<Cow<'a, [u8]>>
    where
        Self: 'a,
    {
        Ok(Cow::Borrowed(self.code.as_bytes()))
    }

    fn name(&self) -> Option<String> {
        // gets rid of `[string "<name>"]` formatting of filename
        Some(format!("@{}", self.name))
    }
}

pub fn create_lua(state: Wrk3State) -> mlua::Result<mlua::Lua> {
    let lua = mlua::Lua::new();
    lua.register_module("wrk3", create_wrk3_mod(&lua, state)?)?;
    Ok(lua)
}

pub fn trace_args(script: &str) -> mlua::Result<Vec<ExpectedArg>> {
    let expected_args = Arc::new(Mutex::new(Vec::new()));

    let module = Wrk3State {
        args_state: ArgsState::Trace {
            expected_args: expected_args.clone(),
        },
        runtime_state: RuntimeState {
            sender: mpsc::channel(1).0,
            metrics: mpsc::unbounded_channel().0,
            start_time: Arc::new(AtomicCell::new(Instant::now())),
        },
    };

    let lua = create_lua(module)?;
    // tracing the args involves supplying dummy values. therefore the script might fail, but we don't care
    let _ = lua.load(script).exec();

    Ok(std::mem::take(&mut expected_args.lock().unwrap()))
}

#[derive(Deserialize, Debug)]
pub struct WorkloadConfig {
    pub stages: Vec<Stage>,
}

#[derive(Deserialize, Debug)]
pub struct Stage {
    pub duration: LuaDt,
    pub rate: f64,
}

impl_from_lua_table!(WorkloadConfig, stages,);
impl_from_lua_table!(Stage, duration, rate,);

enum ColType {
    Dt,
    Int,
    Float,
    Bool,
}

impl_lua_enum!(ColType,
    "dt" => Dt,
    "int" => Int,
    "float" => Float,
    "bool" => Bool,
    _ => mlua::Error::external("expected a valid column type"),
);

struct MetricSchema {
    name: String,
    cols: FxHashMap<String, ColType>,
}
impl_from_lua_table!(MetricSchema, name, cols,);

pub fn load_config(args: ArgsMap, source: NamedSource) -> mlua::Result<WorkloadConfig> {
    let lua = create_lua(Wrk3State {
        args_state: ArgsState::Runtime { args },
        runtime_state: RuntimeState {
            sender: mpsc::channel(1).0,
            metrics: mpsc::unbounded_channel().0,
            start_time: Arc::new(AtomicCell::new(Instant::now())),
        },
    })?;

    struct ModuleResult {
        config: WorkloadConfig,
    }

    impl_from_lua_table!(ModuleResult, config,);

    let module: ModuleResult = lua.load(source).eval()?;
    Ok(module.config)
}

pub struct VuState {
    // must be kept alive
    #[expect(unused)]
    lua: mlua::Lua,
    main: mlua::Function,
    randomseed_fn: mlua::Function,
}

pub struct MainCtx {
    pub iteration: usize,
}

impl mlua::UserData for MainCtx {
    fn add_fields<F: mlua::prelude::LuaUserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("iteration", |_, this| Ok(this.iteration));
    }
}

impl VuState {
    pub fn new(args: ArgsMap, source: NamedSource, state: RuntimeState) -> mlua::Result<Self> {
        let lua = create_lua(Wrk3State {
            args_state: ArgsState::Runtime { args },
            runtime_state: state,
        })?;

        struct ModuleResult {
            main: mlua::Function,
        }
        impl_from_lua_table!(ModuleResult, main,);

        let module: ModuleResult = lua.load(source).eval()?;

        let randomseed_fn = lua
            .globals()
            .get::<mlua::Table>("math")?
            .get("randomseed")?;

        Ok(Self {
            lua,
            main: module.main,
            randomseed_fn,
        })
    }

    pub fn seed_random(&self, seed: u64) -> mlua::Result<()> {
        self.randomseed_fn.call::<()>(seed)?;
        Ok(())
    }

    pub async fn run_main(&self, ctx: MainCtx) -> mlua::Result<()> {
        self.main.call_async(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    fn run_script(src: &str) {
        let lua = create_lua(Wrk3State {
            args_state: ArgsState::Runtime {
                args: Default::default(),
            },
            runtime_state: RuntimeState {
                sender: mpsc::channel(1).0,
                metrics: mpsc::unbounded_channel().0,
                start_time: Arc::new(AtomicCell::new(Instant::now())),
            },
        })
        .unwrap();
        lua.load(src).exec().unwrap();
    }

    #[test]
    fn dt_arithmetic() {
        run_script(indoc!(
            "
            local w = require 'wrk3'
            assert(w.dt'10s' - 2 == w.dt'8s')
            assert(w.dt'2s':secs() == 2)
            "
        ));
    }

    #[test]
    fn traces_args() {
        let args = trace_args(indoc!(
            "
            local w = require 'wrk3'

            w.args.int 'foo'
            w.args.string('bar', { default = 'default value' })
            w.args.dt('a', { default = '10s' })
            "
        ))
        .unwrap();
        insta::assert_debug_snapshot!(args, @r#"
        [
            ExpectedArg {
                name: "foo",
                ty: Int,
                default_value: None,
            },
            ExpectedArg {
                name: "bar",
                ty: String,
                default_value: Some(
                    String(
                        "default value",
                    ),
                ),
            },
            ExpectedArg {
                name: "a",
                ty: Dt,
                default_value: Some(
                    Dt(
                        LuaDt(
                            "10s",
                        ),
                    ),
                ),
            },
        ]
        "#);
    }

    #[test]
    fn loads_config() {
        let script = indoc!(
            "
            local w = require 'wrk3'
            local M = {}
            M.config = {
                stages = {
                    { duration = '10s', rate = 20.5 },
                    { duration = w.args.dt 'duration', rate = 300 },
                }
            }
            return M
            "
        );
        let config = load_config(
            ArgsMap::new(FxHashMap::from_iter([("duration".into(), "5s".into())])),
            NamedSource::new("input.lua", script),
        )
        .unwrap();
        insta::assert_debug_snapshot!(config, @r#"
        Config {
            stages: [
                Stage {
                    duration: LuaDt(
                        "10s",
                    ),
                    rate: 20.5,
                },
                Stage {
                    duration: LuaDt(
                        "5s",
                    ),
                    rate: 300.0,
                },
            ],
        }
        "#);
    }
}
