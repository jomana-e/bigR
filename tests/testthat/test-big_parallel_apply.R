test_that("big_parallel_apply processes data in parallel", {
  df <- big_parallel_apply(mtcars, function(df) dplyr::mutate(df, mpg2 = mpg * 2), n_cores = 2)
  expect_true("mpg2" %in% names(df))
})
