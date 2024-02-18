use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn global_log_init(verbose: Verbose) {

    let level = Level::from(verbose);

    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(level)
        // completes the builder.
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
