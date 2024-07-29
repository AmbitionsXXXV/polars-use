// use expression::run_polars_expression;
use polars_rw::merge::merge_df;
use utils::prelude::*;

mod expression;

fn main() -> Result<()> {
  let df = polars_rw::polars_read()?;

  // run_polars_expression(&df)?;

  merge_df()?;

  Ok(())
}
