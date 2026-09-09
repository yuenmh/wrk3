use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    fmt::Debug,
    num::{ParseFloatError, ParseIntError},
    ops::Deref,
    rc::Rc,
    str::FromStr,
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use hyper::client::conn::http1;
use mlua::{FromLua, IntoLua};
use rustc_hash::FxHashMap;
use serde::{Deserialize, de};
use tokio::sync::{mpsc, oneshot};

use crate::script::Method::Get;

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

#[derive(Clone, Copy)]
struct LuaDt {
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

impl Debug for LuaDt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LuaDt")
            .field(&format!("{}", humantime::format_duration(self.duration)))
            .finish()
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
enum ParseDtError {
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

#[derive(Clone)]
struct RcStr(Rc<str>);

impl<S> From<S> for RcStr
where
    S: Into<Rc<str>>,
{
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

impl Debug for RcStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for RcStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromLua for RcStr {
    fn from_lua(value: mlua::Value, lua: &mlua::Lua) -> mlua::Result<Self> {
        let s = String::from_lua(value, lua)?;
        Ok(Self(s.into()))
    }
}

impl Deref for RcStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[derive(Clone, Debug)]
enum ArgValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(RcStr),
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

#[derive(Clone, Copy, Debug)]
enum ArgTy {
    Int,
    Float,
    Bool,
    String,
    Dt,
}

#[derive(Debug)]
struct ExpectedArg {
    name: String,
    ty: ArgTy,
    default_value: Option<ArgValue>,
}

type ArgsMap = Rc<FxHashMap<String, String>>;

#[derive(Clone)]
enum ArgsState {
    Runtime {
        args: ArgsMap,
    },
    Trace {
        expected_args: Rc<RefCell<Vec<ExpectedArg>>>,
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
                    ArgTy::String => ArgValue::String(value_s.clone().into()),
                    ArgTy::Dt => ArgValue::Dt(value_s.parse().map_err(ArgError::DtFormat)?),
                })
            }
            ArgsState::Trace { expected_args } => {
                expected_args.borrow_mut().push(ExpectedArg {
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
    arg_method!(lua, m, state, string, RcStr, String);
    arg_method!(lua, m, state, dt, LuaDt, Dt);

    m.into_lua(lua)
}

enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl FromLua for Method {
    fn from_lua(
        value: mlua::prelude::LuaValue,
        lua: &mlua::prelude::Lua,
    ) -> mlua::prelude::LuaResult<Self> {
        let value_s = String::from_lua(value, lua)?;
        match value_s.to_lowercase().as_str() {
            "get" => Ok(Method::Get),
            "post" => Ok(Method::Post),
            "put" => Ok(Method::Put),
            "delete" => Ok(Method::Delete),
            "patch" => Ok(Method::Patch),
            _ => Err(mlua::Error::external("expected a valid HTTP method")),
        }
    }
}

struct Request {
    path: String,
    method: Method,
    headers: Option<FxHashMap<String, String>>,
    body: Option<String>,
    timeout: Option<LuaDt>,
}

impl_from_lua_table!(
    Request,
    path,
    [method = |_| Method::Get],
    [headers = |_| None],
    [body = |_| None],
    [timeout = |_| None],
);

enum ResponseError {
    TimedOut,
    Disconnected,
}

struct Response {
    status: u16,
    error: Option<ResponseError>,
}

impl mlua::UserData for Response {}

enum RuntimeMsg {
    Request {
        request: Request,
        response: oneshot::Sender<Response>,
    },
    Sleep {
        duration: Duration,
        response: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
struct RuntimeState {
    sender: mpsc::Sender<RuntimeMsg>,
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

    Ok(())
}

struct Wrk3State {
    args_state: ArgsState,
    runtime_state: RuntimeState,
    start_time: Rc<Cell<Instant>>,
}

fn create_wrk3_mod(lua: &mlua::Lua, state: Wrk3State) -> mlua::Result<mlua::Value> {
    let m = lua.create_table()?;

    m.set("args", create_args_mod(lua, state.args_state)?)?;

    m.set("dt", lua.create_function(|_lua, dt: LuaDt| Ok(dt))?)?;

    m.set(
        "now",
        lua.create_function({
            let start_time = state.start_time.clone();
            move |_lua, (): ()| {
                Ok(LuaDt {
                    duration: Instant::now() - start_time.get(),
                })
            }
        })?,
    )?;

    create_runtime_fns(lua, &m, state.runtime_state)?;

    m.into_lua(lua)
}

#[derive(Copy, Clone)]
struct NamedSource<'a> {
    name: &'a str,
    code: &'a str,
}

impl<'a> NamedSource<'a> {
    fn new(name: &'a str, code: &'a str) -> Self {
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
        Some(self.name.into())
    }
}

fn create_lua(state: Wrk3State) -> mlua::Result<mlua::Lua> {
    let lua = mlua::Lua::new();
    lua.register_module("wrk3", create_wrk3_mod(&lua, state)?)?;
    Ok(lua)
}

fn trace_args(script: &str) -> mlua::Result<Vec<ExpectedArg>> {
    let expected_args = Rc::new(RefCell::new(Vec::new()));

    let module = Wrk3State {
        args_state: ArgsState::Trace {
            expected_args: expected_args.clone(),
        },
        runtime_state: RuntimeState {
            sender: mpsc::channel(1).0,
        },
        start_time: Rc::new(Cell::new(Instant::now())),
    };

    let lua = create_lua(module)?;
    // tracing the args involves supplying dummy values. therefore the script might fail, but we don't care
    let _ = lua.load(script).exec();

    Ok(expected_args.take())
}

#[derive(Deserialize, Debug)]
struct Config {
    stages: Vec<Stage>,
}

#[derive(Deserialize, Debug)]
struct Stage {
    duration: LuaDt,
    rate: f64,
}

impl_from_lua_table!(Config, stages,);
impl_from_lua_table!(Stage, duration, rate,);

fn load_config(args: ArgsMap, source: NamedSource) -> mlua::Result<Config> {
    let lua = create_lua(Wrk3State {
        args_state: ArgsState::Runtime { args },
        runtime_state: RuntimeState {
            sender: mpsc::channel(1).0,
        },
        start_time: Rc::new(Cell::new(Instant::now())),
    })?;

    struct ModuleResult {
        config: Config,
    }

    impl_from_lua_table!(ModuleResult, config,);

    let module: ModuleResult = lua.load(source).eval()?;
    Ok(module.config)
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
            },
            start_time: Rc::new(Cell::new(Instant::now())),
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
