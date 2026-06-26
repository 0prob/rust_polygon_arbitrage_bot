use alloy::primitives::Address;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArbError {
    #[error("address parse failed: {0}")]
    AddressParse(String),

    #[error("overflow in {context}: {operation}")]
    Overflow {
        context: &'static str,
        operation: &'static str,
    },

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("simulation error: {0}")]
    Simulation(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("index out of bounds: token {token} >= {count}")]
    IndexOutOfBounds { token: u32, count: usize },

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("client build failed: {0}")]
    ClientBuild(String),

    #[error("HTTP client error: {0}")]
    HttpClient(String),

    #[error("signal handler error: {0}")]
    Signal(String),

    #[error("math literal parse error: {0}")]
    MathLiteral(String),

    #[error("pool discovery error: {0}")]
    PoolDiscovery(String),

    #[error("thread pool error: {0}")]
    ThreadPool(String),

    #[error("init error: {0}")]
    InitFailure(String),

    #[error("retry exhausted: {0} after {1} attempts")]
    RetryExhausted(String, u32),

    #[error("fetch error: {0}")]
    FetchError(String),
}

impl From<std::num::ParseIntError> for ArbError {
    fn from(e: std::num::ParseIntError) -> Self {
        ArbError::MathLiteral(e.to_string())
    }
}

impl From<alloy::dyn_abi::Error> for ArbError {
    fn from(e: alloy::dyn_abi::Error) -> Self {
        ArbError::Internal(e.to_string())
    }
}

/// Parse a hex address string, returning a structured error on failure.
pub fn parse_addr(hex: &str) -> Result<Address, ArbError> {
    hex.parse()
        .map_err(|e| ArbError::AddressParse(format!("{hex}: {e}")))
}
