test_that("big_chunk_map writes processed data", {
  input_file <- tempfile(fileext = ".csv")
  output_file <- tempfile(fileext = ".csv")

  write.csv(mtcars, input_file, row.names = FALSE)

  big_chunk_map(input_file, function(df) dplyr::mutate(df, mpg2 = mpg * 2), chunk_size = 5, output_file)

  df <- readr::read_csv(output_file)
  expect_true("mpg2" %in% names(df))
})
