use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn global_log_init(verbose: Verbose) {

    let level = Level::from(verbose);

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[derive(Debug)]
pub enum Verbose {
    Info,
    Debug,
    Trace,
}

impl From<u8> for Verbose {
    fn from(v: u8) -> Self {
        match v {
            1 => Verbose::Debug,
            2 => Verbose::Trace,
            _ => Verbose::Info,
        }
    }
}

impl From<Verbose> for Level {
    fn from(v: Verbose) -> Self {
        match v {
            Verbose::Info => Level::INFO,
            Verbose::Debug => Level::DEBUG,
            Verbose::Trace => Level::TRACE,
        }
    }
}
