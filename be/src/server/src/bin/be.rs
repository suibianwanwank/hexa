use clap::Parser;
use hexa_server::server::{BeServer, Server};
use mimalloc_rust::GlobalMiMalloc;

#[global_allocator]
static GLOBAL_MALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Parser)]
#[clap(name = "HexaDB")]
#[clap(version)]
#[clap(about = "CLI for HexaDB", long_about = None)]
pub struct Cli {
    #[clap(short, long, value_parser, default_value_t = String::from("8888"))]
    pub bind: String,

    #[clap(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

fn main() {
    let cli = Cli::parse();

    BeServer::run(cli.verbose, cli.bind.as_str());
}