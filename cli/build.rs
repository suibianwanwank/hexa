use std::path::Path;
use tonic_build::configure;

const PROTO_PATH: &str = "../proto";

fn main() -> Result<(), String> {
    let proto_dir = Path::new(PROTO_PATH);

    let cp = format!("{}/{}", PROTO_PATH, "cli.proto");
    let cli_path = Path::new(cp.as_str());

    // proto definitions has to be there
    let descriptor_path = proto_dir.join("proto_descriptor.bin");

    configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_path)
        .out_dir("src/generated")
        .compile(&[cli_path,], &[proto_dir])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    Ok(())
}


