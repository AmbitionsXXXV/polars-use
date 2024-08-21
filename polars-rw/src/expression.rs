use chrono::prelude::*;
use polars::prelude::*;
use utils::prelude::*;

pub fn polars_select(df: &DataFrame) -> Result<()> {
  // col("*") 选择所有列
  let out = df.clone().lazy().select([col("*")]).collect()?;

  println!("{:?}", out);

  // col("integer") 选择指定列
  let out = df
    .clone()
    .lazy()
    .select([col("integer"), col("string")])
    .collect()?;

  println!("{:?}", out);

  Ok(())
}

pub fn polars_filter(df: &DataFrame) -> Result<()> {
  let start_date = NaiveDate::from_ymd_opt(2025, 12, 2)
    .unwrap()
    .and_hms_opt(0, 0, 0)
    .unwrap();
  let end_date = NaiveDate::from_ymd_opt(2025, 12, 3)
    .unwrap()
    .and_hms_opt(0, 0, 0)
    .unwrap();

  let out = df
    .clone()
    .lazy()
    .filter(
      col("date")
        // **注意** 在这里 lit() 全称是 literal。在 Polars 中，lit() 函数用于将一个常量值转换为 Polars 表达式，使其可以在查询中使用。
        .gt_eq(lit(start_date))
        .and(col("date").lt_eq(lit(end_date))),
    )
    .collect()?;

  println!("filter 1 {:?}", out);

  let out = df
    .clone()
    .lazy()
    .filter(
      col("integer")
        .lt_eq(2) // 过滤条件：列 "integer" 的值小于或等于 3
        .and(col("float").is_not_null()), // 过滤条件：列 "float" 的值不是空值
    )
    .collect()?; // 收集结果并执行计算

  println!("filter 2 {}", out); // 打印过滤后的 DataFrame

  Ok(())
}

// region: Polars 表达式 -- with_column() 和 with_columns()
pub fn polars_with_column(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    // 通过 sum() 函数计算整数列 "integer" 的总和，并将结果存储在新列 "new_integer" 中
    // .with_column(col("integer").sum().alias("new_integer"))
    //
    // 通过 max() 函数计算整数列 "integer" 的最大值，并将结果存储在新列 "new_integer" 中
    // .with_column(col("integer").max().alias("new_integer"))
    //
    // 通过 min() 函数计算整数列 "integer" 的最小值，并将结果存储在新列 "new_integer" 中
    // .with_column(col("integer").min().alias("new_integer"))
    //
    // 通过 mean() 函数计算整数列 "integer" 的平均值，并将结果存储在新列 "new_integer" 中
    // .with_column(col("integer").mean().alias("new_integer"))
    //
    // 通过 median() 函数计算整数列 "integer" 的中位数，并将结果存储在新列 "new_integer" 中
    .with_column(col("integer").median().alias("new_integer"))
    .collect()?;

  println!("{:?}", out);

  Ok(())
}

pub fn polars_with_columns(df: &DataFrame) -> Result<()> {
  let out = df
    .clone()
    .lazy()
    .with_columns([
      col("integer").sum().alias("new_integer"),
      (col("integer") + lit(42)).alias("integer+42"), // 新列 integer+42：列 integer 的值加 42
    ])
    .collect()?;

  println!("{:?}", out);

  Ok(())
}

// endregion: Polars 表达式 -- with_column() 和 with_columns()

// len() 和 count() 的主要区别:
//
// 1. 空值处理:
//    - len(): 计算每个组的所有行，包括包含空值的行。
//    - count(): 只计算非空值。当用 col("*").count() 时，计算至少有一个非空值的行。
//
// 2. 结果类型:
//    - len(): 通常返回 i64 类型。
//    - count(): 通常返回 u32 类型。
//
// 3. 性能:
//    - len(): 通常更快，因为只需计算行数。
//    - count(): 可能稍慢，因为需要检查每个值是否为空。
//
// 4. 应用范围:
//    - len(): 是一个独立的聚合函数，应用于整个组。
//    - count(): 可以应用于特定列或所有列 (使用 col("*"))。
//
// 5. 使用场景:
//    - len(): 当你想包括所有行，或确定数据没有空值时使用。
//    - count(): 当你只想计算非空值，或需要对特定列进行计数时使用。
//
// 注意: 如果数据中没有全为空值的行，col("*").count() 和 len() 可能会得到相同的结果。
// 但是，如果存在某些行在所有列上都是空值，结果可能会不同。
pub fn polars_group_by() -> Result<()> {
  let df2: DataFrame =
    df!("x" => 0..8, "y"=> &["A", "A", "A", "B", "B", "C", "X", "X"])
      .expect("should not fail");

  // 按列 "y" 进行分组，并聚合
  let out = df2.clone().lazy().group_by(["y"]).agg([len()]).collect()?;

  println!("group_by 1 {}", out);

  // 按列 "y" 进行分组，并聚合多个统计量
  let out = df2
    .clone()
    .lazy()
    .group_by(["y"])
    .agg([col("*").count().alias("count"), col("*").sum().alias("sum")])
    .collect()?;

  println!("{}", out);

  Ok(())
}

pub fn polars_combination_expression() -> Result<()> {
  let df = df!("a"=> [1, 2, 3, 4], "b" => [2, 3, 4, 5], "c" => [3, 4, 5, 6], "d" => [4, 5, 6, 7])?;

  // 创建并选择列（排除c、d列）
  let out = df
    .clone()
    .lazy()
    .with_columns([(col("a") * col("b")).alias("a * b")])
    .select([col("*").exclude(["c", "d"])])
    .collect()?;

  println!("{}", out);

  // 创建并选择列（排除d列）
  let out = df
    .clone()
    .lazy()
    .with_columns([(col("a") * col("b")).alias("a * b")])
    .select([col("*").exclude(["d"])])
    .collect()?;

  println!("{}", out);

  Ok(())
}
