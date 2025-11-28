use polars::{
  lazy::dsl::{col, concat_str, sum_horizontal},
  prelude::*,
};
use utils::prelude::Result;

// 手动求和 - 使用 sum_horizontal 进行水平求和
pub fn sum_manual(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    .select([
      // 使用 sum_horizontal 进行水平求和，第二个参数表示是否忽略空值
      sum_horizontal([col("*")], true)?.alias("sum"),
    ])
    .collect()?;

  println!("sum_manual {:?}", out);

  Ok(())
}

pub fn conditional_aggregation(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    .filter(
      // 使用 any_horizontal 进行条件聚合
      any_horizontal([col("*").gt(150)])?,
    )
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
