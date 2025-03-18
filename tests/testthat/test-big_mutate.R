test_that("big_mutate adds a new column", {
  df <- big_mutate(mtcars, new_col = mpg * 2)
  expect_true("new_col" %in% names(df))
  expect_equal(df$new_col, df$mpg * 2)
})
