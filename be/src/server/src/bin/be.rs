use clap::Parser;
use mimalloc::MiMalloc;
use hexa_server::server::{BeServer, Server};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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