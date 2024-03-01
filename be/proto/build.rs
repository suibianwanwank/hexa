use std::env;
use std::io::Result;

use tonic_build::configure;

fn main() -> Result<()> {
    let common_config = configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/protos");

    common_config
        .clone()
        .compile(&["../../proto/datafusion.proto"], &["../../proto"])?;

    common_config.compile(&["../../proto/execute.proto"], &["../../proto"])?;

    Ok(())
}
