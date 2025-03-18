test_that("big_read_parquet reads Parquet files", {
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(mtcars, test_file)

  df <- big_read_parquet(test_file, backend = "auto")
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})
