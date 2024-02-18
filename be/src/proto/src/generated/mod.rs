#[allow(clippy::all)]
#[rustfmt::skip]
#[cfg(not(docsrs))]
#[allow(non_camel_case_types)]
pub mod datafusion{
    include!("prost.rs");
    include!("execute.rs");
}
