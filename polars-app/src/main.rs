use expression::run_polars_expression;
use utils::prelude::*;

mod expression;

fn main() -> Result<()> {
  let df = polars_rw::polars_read()?;

  run_polars_expression(&df)?;

  Ok(())
}
