test_that("big_group_by groups data correctly", {
  df <- big_group_by(mtcars, cyl)
  expect_true(inherits(df, "grouped_df"))
})
