test_that("big_read_csv works with small CSV files", {
  test_file <- tempfile(fileext = ".csv")
  write.csv(mtcars, test_file, row.names = FALSE)

  df <- big_read_csv(test_file, backend = "auto")
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})

test_that("big_read_csv works with different file sizes", {
  # Generate test files of different sizes
  files <- generate_test_files(sizes = c(1e3, 1e4, 1e5), formats = "csv")
  
  # Test each file size
  for (file in files) {
    df <- big_read_csv(file, backend = "auto")
    expect_equal(nrow(df), as.numeric(gsub(".*_([0-9]+)_.*", "\\1", basename(file))))
    expect_true(all(c("id", "num_1", "cat_1") %in% names(df)))
  }
  
  cleanup_test_files(files)
})

test_that("big_read_csv works with different backends", {
  # Generate a medium-sized test file
  files <- generate_test_files(sizes = 1e4, formats = "csv")
  file <- files[[1]]
  
  # Test each backend
  backends <- c("data.table", "arrow", "duckdb")
  for (backend in backends) {
    df <- big_read_csv(file, backend = backend)
    expect_equal(nrow(df), 1e4)
    expect_true(all(c("id", "num_1", "cat_1") %in% names(df)))
  }
  
  cleanup_test_files(files)
})

test_that("big_read_csv handles missing values correctly", {
  # Create test data with missing values
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  df$cat_1[sample(1:1000, 100)] <- NA
  
  test_file <- tempfile(fileext = ".csv")
  write.csv(df, test_file, row.names = FALSE)
  
  # Test reading with different backends
  backends <- c("data.table", "arrow", "duckdb")
  for (backend in backends) {
    result <- big_read_csv(test_file, backend = backend)
    expect_equal(sum(is.na(result$num_1)), 100)
    expect_equal(sum(is.na(result$cat_1)), 100)
  }
  
  unlink(test_file)
})

test_that("big_read_csv fails gracefully with invalid files", {
  # Test non-existent file
  expect_error(big_read_csv("nonexistent.csv"))
  
  # Test invalid file format
  invalid_file <- tempfile(fileext = ".csv")
  writeLines("invalid,csv,format\na,b", invalid_file)
  expect_error(big_read_csv(invalid_file))
  
  unlink(invalid_file)
})

test_that("big_read_csv respects chunk_size parameter", {
  files <- generate_test_files(sizes = 1e4, formats = "csv")
  file <- files[[1]]
  
  # Test different chunk sizes
  chunk_sizes <- c(100, 1000, 5000)
  for (chunk_size in chunk_sizes) {
    df <- big_read_csv(file, chunk_size = chunk_size)
    expect_equal(nrow(df), 1e4)
  }
  
  cleanup_test_files(files)
})
