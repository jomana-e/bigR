test_that("big_optimize suggests the right backend", {
  df <- mtcars
  expect_match(big_optimize(df), "Data is small")
})
