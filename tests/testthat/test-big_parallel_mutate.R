test_that("big_parallel_mutate works", {
  df <- big_parallel_mutate(mtcars, mpg2 = mpg * 2, n_cores = 2)
  expect_true("mpg2" %in% names(df))
})
