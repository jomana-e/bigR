test_that("big_select selects correct columns", {
  df <- big_select(mtcars, mpg, cyl)
  expect_equal(ncol(df), 2)
  expect_equal(names(df), c("mpg", "cyl"))
})
