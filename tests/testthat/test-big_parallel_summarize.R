test_that("big_parallel_summarize works", {
  df <- big_parallel_summarize(mtcars, mean_mpg = mean(mpg), n_cores = 2)
  expect_true("mean_mpg" %in% names(df))
})
