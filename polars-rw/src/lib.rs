use chrono::prelude::*;
use polars::prelude::*;
use utils::prelude::*;

pub mod expression;

pub fn polars_read() -> Result<DataFrame> {
  // 创建一个 DataFrame，包含四列数据：整数、日期、浮点数和字符串
  let df: DataFrame = df!(
    "integer" => &[1, 2, 3, 9], // 整数列
    "date" => &[ // 日期列
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(), // 第一天
        NaiveDate::from_ymd_opt(2025, 12, 2).unwrap().and_hms_opt(0, 0, 0).unwrap(), // 第二天
        NaiveDate::from_ymd_opt(2025, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(), // 第三天
        NaiveDate::from_ymd_opt(2025, 1, 7).unwrap().and_hms_opt(0, 0, 0).unwrap(), // 第三天
    ],
    "float" => &[4.0, 5.0, 6.0, 9.0], // 浮点数列
    "string" => &["a", "b", "c", "z"], // 字符串列
  )
  .unwrap(); // 创建 DataFrame 成功后，解除 Result 包装

  println!("{:?}", df);

  Ok(df)
}
