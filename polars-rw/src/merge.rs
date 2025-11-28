use polars::prelude::*;
use rand::Rng;
use utils::prelude::*;

pub fn merge_df() -> Result<()> {
  let mut rng = rand::rng();

  let df1: DataFrame = df!(
    "a" => 0..8,
    "b" => (0..8).map(|_| rng.random::<f64>()).collect::<Vec<f64>>()
  )?;

  let df2: DataFrame = df!(
      "x" => 1..9,
      "y" => &["A", "A", "A", "B", "B", "C", "X", "X"]
  )?;

  // 使用专用的 left_join 方法
  let joined = df1.left_join(&df2, ["a"], ["x"])?;

  println!("<<<left joined {:?}", joined);

  // 使用 join 方法并传入 JoinArgs 和 None 作为 options
  let joined = df1.join(&df2, ["a"], ["x"], JoinArgs::new(JoinType::Right), None)?;

  println!("<<<right joined222 {:?}", joined);

  // 使用专用的 inner_join 方法
  let joined = df1.inner_join(&df2, ["a"], ["x"])?;

  println!("<<<inner joined {:?}", joined);

  // 使用专用的 full_join 方法
  let joined = df1.full_join(&df2, ["a"], ["x"])?;

  println!("<<<outer joined {:?}", joined);

  // hstack 作用是将两个 DataFrame 水平合并
  let stacked = df1.hstack(df2.clone().get_columns())?;

  println!("<<<stacked {:?}", stacked);

  let mut schema = Schema::default();
  schema.with_column("symbol".into(), DataType::String);

  println!("<<<schema {:?}", schema);

  Ok(())
}
