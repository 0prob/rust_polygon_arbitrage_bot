#[cfg(feature = "tokio-console")]
pub fn init() {
    if std::env::var_os("RPBOT_TOKIO_CONSOLE").is_some() {
        console_subscriber::init();
    }
}

#[cfg(not(feature = "tokio-console"))]
pub fn init() {}
