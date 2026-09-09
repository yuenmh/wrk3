use std::{
    borrow::Cow,
    cell::RefCell,
    fmt::Debug,
    num::{ParseFloatError, ParseIntError},
    ops::Deref,
    rc::Rc,
    str::FromStr,
    time::Duration,
};

use mlua::FromLua;
use rustc_hash::FxHashMap;
use serde::{Deserialize, de};

macro_rules! impl_from_lua_table {
    ($ty:ty, $($field:ident),* $(,)?) => {
        impl mlua::FromLua for $ty {
            fn from_lua(value: mlua::Value, lua: &mlua::Lua) -> mlua::Result<Self> {
                let table = mlua::Table::from_lua(value, lua)?;
                Ok(Self {
                    $($field: table.get(stringify!($field))?,)*
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

struct Args {
    state: ArgsState,
}

impl Args {
    fn get_arg(
        &self,
        name: &str,
        expected_type: ArgTy,
        default_value: Option<ArgValue>,
    ) -> Result<ArgValue, ArgError> {
        match &self.state {
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

impl mlua::UserData for Args {
    fn add_methods<M: mlua::prelude::LuaUserDataMethods<Self>>(methods: &mut M) {
        macro_rules! arg_method {
            ($methods:expr, $name:ident, $rust_ty:ty, $enum_ty:ident) => {{
                struct Opts {
                    default: Option<$rust_ty>,
                }
                impl_from_lua_table!(Opts, default);
                $methods.add_method(
                    stringify!($name),
                    |_lua, this, (arg_name, opts): (String, Option<Opts>)| {
                        if let Some(opts) = opts {
                            this.get_arg(
                                &arg_name,
                                ArgTy::$enum_ty,
                                opts.default.map(ArgValue::$enum_ty),
                            )
                            .map_err(mlua::Error::external)
                        } else {
                            this.get_arg(&arg_name, ArgTy::$enum_ty, None)
                                .map_err(mlua::Error::external)
                        }
                    },
                );
            }};
        }

        arg_method!(methods, int, i64, Int);
        arg_method!(methods, float, f64, Float);
        arg_method!(methods, bool, bool, Bool);
        arg_method!(methods, string, RcStr, String);
        arg_method!(methods, dt, LuaDt, Dt);
    }
}

struct Wrk3Module {
    args_state: ArgsState,
}

impl mlua::UserData for Wrk3Module {
    fn add_fields<F: mlua::prelude::LuaUserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("args", |_lua, module| {
            Ok(Args {
                state: module.args_state.clone(),
            })
        });
    }
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

fn create_lua(module: Wrk3Module) -> mlua::Result<mlua::Lua> {
    let lua = mlua::Lua::new();
    lua.register_module("wrk3", module)?;
    Ok(lua)
}

fn trace_args(script: &str) -> mlua::Result<Vec<ExpectedArg>> {
    let expected_args = Rc::new(RefCell::new(Vec::new()));

    let module = Wrk3Module {
        args_state: ArgsState::Trace {
            expected_args: expected_args.clone(),
        },
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

impl_from_lua_table!(Config, stages);
impl_from_lua_table!(Stage, duration, rate);

fn load_config(args: ArgsMap, source: NamedSource) -> mlua::Result<Config> {
    let lua = create_lua(Wrk3Module {
        args_state: ArgsState::Runtime { args },
    })?;

    struct ModuleResult {
        config: Config,
    }

    impl_from_lua_table!(ModuleResult, config);

    let module: ModuleResult = lua.load(source).eval()?;
    Ok(module.config)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn traces_args() {
        let args = trace_args(indoc!(
            "
            local w = require 'wrk3'

            w.args:int 'foo'
            w.args:string('bar', { default = 'default value' })
            w.args:dt('a', { default = '10s' })
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
                    { duration = w.args:dt 'duration', rate = 300 },
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
