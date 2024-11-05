use std::fs::File;

use anyhow::Result;
use polars::prelude::*;

fn main() -> Result<()> {
  let df = CsvReadOptions::default()
    .try_into_reader_with_file_path(Some("assets/EmmoBillingDetails.csv".into()))?
    .finish()?;

  let mut df = df
    .lazy()
    .with_column(lit("CNY").alias("币种"))
    .select([
      col("时间"),
      col("类型"),
      col("类别"),
      col("币种"),
      col("金额"),
      col("备注"),
    ])
    .with_column(
      col("金额")
        .str()
        .replace(lit(r"[+-]"), lit(""), false)
        .alias("金额"),
    )
    .collect()?;

  println!("{:?}", df);

  CsvWriter::new(File::create("assets/output.csv")?).finish(&mut df)?;

  Ok(())
}
