test_that("big_read_csv works with small CSV files", {
  test_file <- tempfile(fileext = ".csv")
  write.csv(mtcars, test_file, row.names = FALSE)

  df <- big_read_csv(test_file, backend = "auto")
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})
