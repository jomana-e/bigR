test_that("big_arrange sorts data correctly", {
  df <- big_arrange(mtcars, desc(mpg))
  expect_equal(df$mpg, sort(mtcars$mpg, decreasing = TRUE))
})
