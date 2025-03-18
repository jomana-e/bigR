test_that("big_chunk_apply processes data in chunks", {
  test_file <- tempfile(fileext = ".csv")
  write.csv(mtcars, test_file, row.names = FALSE)

  result <- big_chunk_apply(test_file, function(df) dplyr::mutate(df, mpg2 = mpg * 2), chunk_size = 5)
  expect_true("mpg2" %in% names(result))
})
