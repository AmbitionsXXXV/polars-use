use polars::prelude::*;
use polars_rw::fold_fn::{concat_string, conditional_aggregation, sum_manual};
use utils::prelude::Result;

fn main() -> Result<()> {
  let df = df!(
      "price" => &[100, 200, 300],
      "quantity" => &[2, 3, 4],
  )?;

  sum_manual(&df)?;

  conditional_aggregation(&df)?;

  concat_string()?;

  Ok(())
}
