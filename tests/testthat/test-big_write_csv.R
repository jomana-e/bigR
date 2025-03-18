test_that("big_write_csv writes CSV files correctly", {
  test_file <- tempfile(fileext = ".csv")

  big_write_csv(mtcars, test_file, backend = "auto")

  df <- readr::read_csv(test_file)
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})
