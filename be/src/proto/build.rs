use std::path::Path;
use tonic_build::configure;


// fn main() -> Result<()> {
//     let common_config = configure()
//         .build_server(true)
//         .build_client(true)
//         .out_dir("src/protos");
//
//     common_config
//         .clone()
//         .compile(&["../../../proto/datafusion.proto"], &["../../../proto"])?;
//
//     common_config.compile(&["../../../proto/execute.proto"], &["../../../proto"])?;
//
//     Ok(())
// }

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

// fn main() -> Result<(), String> {
//     let proto_dir = Path::new("D:/code_project/hexa/proto");
//     let proto_path = Path::new("D:/code_project/hexa/proto/datafusion.proto");
//
//     // proto definitions has to be there
//     let descriptor_path = proto_dir.join("proto_descriptor.bin");
//
//     prost_build::Config::new()
//         .file_descriptor_set_path(&descriptor_path)
//         .out_dir("src")
//         .compile_well_known_types()
//         .extern_path(".google.protobuf", "::pbjson_types")
//         .compile_protos(&[proto_path], &["D:/code_project/hexa/proto\
//         "])
//         .map_err(|e| format!("protobuf compilation failed: {e}"))?;
//
//     let descriptor_set = std::fs::read(&descriptor_path)
//         .unwrap_or_else(|e| panic!("Cannot read {:?}: {}", &descriptor_path, e));
//
//     pbjson_build::Builder::new()
//         .out_dir("src")
//         .register_descriptors(&descriptor_set)
//         .unwrap_or_else(|e| {
//             panic!("Cannot register descriptors {:?}: {}", &descriptor_set, e)
//         })
//         .build(&[".datafusion"])
//         .map_err(|e| format!("pbjson compilation failed: {e}"))?;
//
//     let prost = Path::new("src/datafusion.rs");
//     let pbjson = Path::new("src/datafusion.serde.rs");
//
//     std::fs::copy(prost, "src/generated/prost.rs").unwrap();
//     std::fs::copy(pbjson, "src/generated/pbjson.rs").unwrap();
//
//     Ok(())
// }

fn main() -> Result<(), String> {
    let proto_dir = Path::new("D:/code_project/hexa/proto");
    let proto_path = Path::new("D:/code_project/hexa/proto/datafusion.proto");
    let execute_path = Path::new("D:/code_project/hexa/proto/execute.proto");
    let cli_path = Path::new("D:/code_project/hexa/proto/cli.proto");

    // proto definitions has to be there
    let descriptor_path = proto_dir.join("proto_descriptor.bin");

    // prost_build::Config::new()
    //     .file_descriptor_set_path(&descriptor_path)
    //     .out_dir("src")
    //     .compile_well_known_types()
    //     .extern_path(".google.protobuf", "::pbjson_types")
    //     .compile_protos(&[proto_path], &["D:/code_project/hexa/proto\
    //     "])
    //     .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    configure()
        .file_descriptor_set_path(&descriptor_path)
        .out_dir("src")
        .build_server(true)
        .build_client(true)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(&[proto_path], &["D:/code_project/hexa/proto\
        "])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .out_dir("src")
        .compile(&[execute_path, cli_path], &["D:/code_project/hexa/proto\
        "])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let prost = Path::new("src/datafusion.rs");
    let execute = Path::new("src/execute.rs");

    std::fs::copy(prost, "src/generated/prost.rs").unwrap();
    std::fs::copy(execute, "src/generated/execute.rs").unwrap();


    Ok(())
}


