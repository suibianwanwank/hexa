use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub enum DatasourceError {
    #[snafu(display("got sqlx error! detail:{detail}"))]
    Sqlx {
        detail: &'static str,
        source: sqlx::error::Error,
    },
}

pub type Result<T, E = DatasourceError> = std::result::Result<T, E>;
