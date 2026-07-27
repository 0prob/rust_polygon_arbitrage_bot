use anyhow::Context;

pub fn build() -> anyhow::Result<tokio::runtime::Runtime> {
    crate::console::init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rpbot-tokio")
        .build()
        .context("failed to build tokio runtime")
}
