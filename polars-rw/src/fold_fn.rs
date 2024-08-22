use polars::{
  lazy::dsl::{col, concat_str, fold_exprs, lit},
  prelude::*,
};
use utils::prelude::Result;

// 手动求和
pub fn sum_manual(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    .select([
      fold_exprs(lit(0), |acc, x| (acc + x).map(Some), [col("*")]).alias("sum")
    ])
    .collect()?;

  println!("sum_manual {:?}", out);

  Ok(())
}

pub fn conditional_aggregation(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    .filter(fold_exprs(
      lit(false),
      |acc, x| acc.bitor(&x).map(Some),
      [col("*").gt(150)],
    ))
    .collect()?;

  println!("conditional_aggregation {}", out);

  Ok(())
}

pub fn concat_string() -> Result<()> {
  let df = df!(
      "symbol" => &["AAPL", "GOOGL", "AMZN"],
      "price" => &[150, 2800, 3400],
  )?;

  let out = df
    .lazy()
    .select([concat_str([col("symbol"), col("price")], "", false).alias("combined")])
    .collect()?;

  println!("concat_str {:?}", out);

  Ok(())
}
