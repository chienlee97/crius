#![allow(dead_code)]

use crate::crs::error::{CliError, CommandResult};

pub(crate) async fn not_available() -> Result<CommandResult, CliError> {
    Err(CliError::not_implemented("crs streaming"))
}
