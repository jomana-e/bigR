test_that("big_read_parquet reads Parquet files with different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    test_file <- tempfile(fileext = ".parquet")
    
    # Write test file
    arrow::write_parquet(df, test_file)
    
    # Read with different backends
    backends <- c("arrow", "data.table")
    for (backend in backends) {
      df_read <- big_read_parquet(test_file, backend = backend)
      expect_equal(nrow(df_read), size)
      expect_equal(ncol(df_read), ncol(df))
      expect_equal(names(df_read), names(df))
      expect_equal(df_read, df)
    }
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_read_parquet handles different data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  df$logical <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  df$integer <- as.integer(1:1000)
  
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  
  df_read <- big_read_parquet(test_file, backend = "arrow")
  
  # Verify data types are preserved
  expect_type(df_read$num_1, "double")
  expect_type(df_read$cat_1, "character")
  expect_s3_class(df_read$date, "Date")
  expect_type(df_read$logical, "logical")
  expect_type(df_read$integer, "integer")
  
  # Clean up
  unlink(test_file)
})

test_that("big_read_parquet works with different compression types", {
  df <- generate_test_data(n_rows = 1000)
  compression_types <- c("snappy", "gzip", "none")
  
  for (compression in compression_types) {
    test_file <- tempfile(fileext = ".parquet")
    arrow::write_parquet(df, test_file, compression = compression)
    
    df_read <- big_read_parquet(test_file, backend = "arrow")
    expect_equal(df_read, df)
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_read_parquet handles column selection", {
  df <- generate_test_data(n_rows = 1000)
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  
  # Select specific columns
  cols <- c("num_1", "cat_1")
  df_read <- big_read_parquet(test_file, columns = cols, backend = "arrow")
  expect_equal(names(df_read), cols)
  expect_equal(df_read$num_1, df$num_1)
  expect_equal(df_read$cat_1, df$cat_1)
  
  # Clean up
  unlink(test_file)
})

test_that("big_read_parquet handles row filtering", {
  df <- generate_test_data(n_rows = 1000)
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  
  # Test row filtering
  df_read <- big_read_parquet(test_file, filter = num_1 > 0, backend = "arrow")
  expect_true(all(df_read$num_1 > 0))
  expect_true(nrow(df_read) <= nrow(df))
  
  # Clean up
  unlink(test_file)
})

test_that("big_read_parquet handles chunked reading", {
  df <- generate_test_data(n_rows = 1000)
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  
  # Test with different chunk sizes
  chunk_sizes <- c(100, 250, 500)
  for (chunk_size in chunk_sizes) {
    df_read <- big_read_parquet(test_file, chunk_size = chunk_size, backend = "arrow")
    expect_equal(df_read, df)
  }
  
  # Clean up
  unlink(test_file)
})

test_that("big_read_parquet fails gracefully with invalid inputs", {
  # Non-existent file
  expect_error(big_read_parquet("nonexistent.parquet"))
  
  # Invalid backend
  df <- generate_test_data(n_rows = 10)
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  expect_error(big_read_parquet(test_file, backend = "invalid"))
  
  # Invalid column names
  expect_error(big_read_parquet(test_file, columns = c("nonexistent")))
  
  # Invalid filter expression
  expect_error(big_read_parquet(test_file, filter = nonexistent > 0))
  
  # Clean up
  unlink(test_file)
})

test_that("big_read_parquet handles memory efficiently", {
  # Generate large dataset
  df <- generate_test_data(n_rows = 1e5)
  test_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df, test_file)
  
  # Monitor memory usage during read
  start_mem <- pryr::mem_used()
  df_read <- big_read_parquet(test_file, chunk_size = 1000, backend = "arrow")
  end_mem <- pryr::mem_used()
  
  # Verify memory usage is reasonable (less than 3x data size)
  expect_lt(end_mem - start_mem, object.size(df) * 3)
  
  # Clean up
  unlink(test_file)
})
