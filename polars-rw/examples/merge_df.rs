use polars_rw::merge::merge_df;
use utils::prelude::Result;

fn main() -> Result<()> {
  merge_df()?;

  Ok(())
}
