test_that("big_write_parquet writes Parquet files", {
  test_file <- tempfile(fileext = ".parquet")
  big_write_parquet(mtcars, test_file, backend = "arrow")

  df <- arrow::read_parquet(test_file)
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})
