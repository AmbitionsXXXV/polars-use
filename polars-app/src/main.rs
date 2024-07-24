use anyhow::{Ok, Result};
use polars_rw::expression::{
  polars_filter, polars_group_by, polars_select, polars_with_column,
  polars_with_columns,
};

fn main() -> Result<()> {
  let df = polars_rw::polars_read()?;

  // ======polars 表达式======
  // 1. select
  // 2. filter
  // 3. with_columns
  // 4. group_by

  // select
  polars_select(&df)?;

  // filter
  polars_filter(&df)?;

  // with_columns
  // 一次添加多列
  polars_with_columns(&df)?;

  // with_column
  // 一次添加一列
  polars_with_column(&df)?;

  // group_by
  polars_group_by()?;

  Ok(())
}
