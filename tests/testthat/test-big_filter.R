test_that("big_filter filters rows correctly", {
  df <- big_filter(mtcars, mpg > 20)
  expect_true(all(df$mpg > 20))
})
