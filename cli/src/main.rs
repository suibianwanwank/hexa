mod generated;

use clap::Parser;
use owo_colors::OwoColorize;
use rustyline::completion::FilenameCompleter;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::highlight::MatchingBracketHighlighter;
use rustyline::hint::Hinter;
use rustyline::history::SearchDirection;
use rustyline::validate::MatchingBracketValidator;
use rustyline::{
    Completer, CompletionType, Config, Context, EditMode, Editor, Helper, Hinter,
    Validator,
};
use snafu::{ResultExt, Snafu};
use std::borrow::Cow;
use std::borrow::Cow::{Borrowed, Owned};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{transport, Status};
use tracing::info;
use crate::generated::proto::cli_bridge_client::CliBridgeClient;
use crate::generated::proto::SqlJobRequest;

#[derive(Parser)]
#[clap(name = "Hexa-Cli")]
#[clap(version)]
#[clap(about = "CLI for HexaDB", long_about = None)]
pub struct Cli {
    /// hexa fe grpc address, default:127.0.0.1:9065
    #[clap(short, long, value_parser, default_value_t = String::from("127.0.0.1:9065"))]
    pub bind: String,

    #[clap(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

const PROMPT: &str = "Hexa> ";

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let client = create_grpc_client(cli.bind).await?;

    run_cli_application(client).await
}

async fn run_cli_application(mut client: CliBridgeClient<Channel>) -> Result<()> {
    let config = Config::builder()
        .history_ignore_space(true)
        .auto_add_history(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();

    let history = rustyline::sqlite_history::SQLiteHistory::open(config, "history.sqlite3")
        .context(OpenHistoryFileSnafu {})?;

    let mut rl = Editor::with_history(config, history).context(StartCliServerSnafu {})?;
    rl.set_helper(Some(SqlCliHelper::new()));

    loop {
        let readline = rl.readline(PROMPT);

        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                match send_sql_job(&mut client, &line).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                eprintln!("Process will be closed");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Cli Error: {err:?}");
                break;
            }
        }
    }
    Ok(())
    // rl.append_history("history.txt").map_err(return_cli_error)
}

async fn create_grpc_client(bind: String) -> Result<CliBridgeClient<Channel>> {
    info!("Bind hexa server address:{}", bind);
    let dst = if !bind.to_lowercase().starts_with("http") {
        format!("https://{}", bind)
    } else {
        bind
    };

    CliBridgeClient::connect(dst.clone())
        .await
        .context(GrpcConnectSnafu { address: dst })
}

async fn send_sql_job(client: &mut CliBridgeClient<Channel>, sql: &str) -> Result<(), Status> {
    let mut stream = client
        .submit_sql_job(SqlJobRequest { sql: sql.into() })
        .await?
        .into_inner();

    while let Some(item) = stream.next().await {
        println!("{}", item?.content);
    }

    Ok(())
}

#[derive(Debug, Snafu)]
#[snafu()]
pub enum CliError {
    #[snafu(display("Can't connect to the destination address, please check if the connection:{address} information is correct!"))]
    GrpcConnect {
        source: transport::Error,
        address: String,
    },

    #[snafu(display("Failed to initialize cli history file, check file permissions!"))]
    OpenHistoryFile { source: ReadlineError },

    #[snafu(display("cli startup configuration, check if the operating system matches the cli, check the logs or contact the administrator!"))]
    StartCliServer { source: ReadlineError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "select", "FROM", "from", "WHERE", "where", "JOIN", "join", "SHOW", "show", "CREATE",
    "create", "SCHEMA", "schema", "CATALOG", "catalog", "TABLE", "table",
];

fn complete_keyword(line: &str) -> Option<String> {
    if line.ends_with(' ') {
        return None;
    }

    if let Some(pre) = line.split_whitespace().last() {
        for &item in SQL_KEYWORDS {
            if !item.starts_with(pre) || item.eq(pre) {
                continue;
            }
            return Some(item[pre.len()..].to_owned());
        }
    }
    None

    // SQL_KEYWORDS
    //     .iter()
    //     .filter(|&keyword| keyword.starts_with(&line))
    //     .map(|&keyword| keyword[line.len()..].to_owned())
    //     .next()
}

struct SqlCliHinter;

impl Hinter for SqlCliHinter {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        if line.is_empty() || pos < line.len() || line.ends_with(';') {
            return None;
        }

        // completion based on keywords
        if let Some(completion) = complete_keyword(line) {
            return Some(completion);
        }

        // completion based on history
        let start = if ctx.history_index() == ctx.history().len() {
            ctx.history_index().saturating_sub(1)
        } else {
            ctx.history_index()
        };

        if let Some(sr) = ctx
            .history()
            .starts_with(line, 0, SearchDirection::Reverse)
            .unwrap_or(None)
        {
            if sr.entry == line || sr.entry.len() <= line.len() {
                return None;
            }
            return Some(sr.entry[line.len()..].to_owned());
        }

        None
    }
}

#[derive(Helper, Completer, Validator, Hinter)]
struct SqlCliHelper {
    #[rustyline(Completer)]
    completer: FilenameCompleter,
    highlighter: MatchingBracketHighlighter,
    #[rustyline(Validator)]
    validator: MatchingBracketValidator,
    #[rustyline(Hinter)]
    hinter: SqlCliHinter,
    colored_prompt: String,
}

impl SqlCliHelper {
    pub fn new() -> Self {
        SqlCliHelper {
            completer: FilenameCompleter::new(),
            highlighter: MatchingBracketHighlighter::new(),
            colored_prompt: PROMPT.bright_green().to_string(),
            hinter: SqlCliHinter {},
            validator: MatchingBracketValidator::new(),
        }
    }
}

impl Highlighter for SqlCliHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(hint.bright_black().to_string())
    }

    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let mut result = String::new();

        // split rows by spaces
        let tokens = line.split_inclusive(' ');
        for token in tokens {
            // remove trailing spaces to match
            let n = token.trim_end();

            if SQL_KEYWORDS.contains(&n) {
                // match to keyword, highlight and special display
                result.push_str(token.bright_yellow().to_string().as_str());
                continue;
            }

            result.push_str(token);
        }
        Owned(result)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}
