use polars::prelude::*;
use utils::prelude::*;

use rand::Rng;

pub fn merge_df() -> Result<()> {
  let mut rng = rand::thread_rng();

  let df1: DataFrame = df!(
    "a" => 0..8,
    "b" => (0..8).map(|_| rng.gen::<f64>()).collect::<Vec<f64>>()
  )?;

  let df2: DataFrame = df!(
      "x" => 1..9,
      "y" => &["A", "A", "A", "B", "B", "C", "X", "X"]
  )?;

  let joined = df1.join(&df2, ["a"], ["x"], JoinType::Left.into())?;

  println!("<<<left joined {:?}", joined);

  let joined = df1.join(&df2, ["a"], ["x"], JoinType::Right.into())?;

  println!("<<<right joined222 {:?}", joined);

  let joined = df1.join(&df2, ["a"], ["x"], JoinArgs::new(JoinType::Inner))?;

  println!("<<<inner joined {:?}", joined);

  let joined = df1.join(&df2, ["a"], ["x"], JoinArgs::new(JoinType::Full))?;

  println!("<<<outer joined {:?}", joined);

  // hstack 作用是将两个 DataFrame 水平合并
  let stacked = df1.hstack(df2.clone().get_columns())?;

  println!("<<<stacked {:?}", stacked);

  let mut schema = Schema::new();
  schema.with_column(
    "symbol".into(),
    DataType::Categorical(None, Default::default()),
  );

  println!("<<<schema {:?}", schema);

  Ok(())
}
