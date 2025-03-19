test_that("big_write_parquet writes Parquet files with different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    test_file <- tempfile(fileext = ".parquet")
    
    # Write parquet file
    big_write_parquet(df, test_file, backend = "arrow")
    
    # Verify file exists and has content
    expect_true(file.exists(test_file))
    expect_gt(file.size(test_file), 0)
    
    # Read and verify content
    df_read <- arrow::read_parquet(test_file)
    expect_equal(nrow(df_read), size)
    expect_equal(ncol(df_read), ncol(df))
    expect_equal(names(df_read), names(df))
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_write_parquet handles different data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  df$logical <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  df$integer <- as.integer(1:1000)
  
  test_file <- tempfile(fileext = ".parquet")
  big_write_parquet(df, test_file, backend = "arrow")
  
  df_read <- arrow::read_parquet(test_file)
  
  # Verify data types are preserved
  expect_type(df_read$num_1, "double")
  expect_type(df_read$cat_1, "character")
  expect_s3_class(df_read$date, "Date")
  expect_type(df_read$logical, "logical")
  expect_type(df_read$integer, "integer")
  
  # Clean up
  unlink(test_file)
})

test_that("big_write_parquet works with different compression options", {
  df <- generate_test_data(n_rows = 1000)
  compression_types <- c("snappy", "gzip", "none")
  
  for (compression in compression_types) {
    test_file <- tempfile(fileext = ".parquet")
    big_write_parquet(df, test_file, compression = compression, backend = "arrow")
    
    # Verify file exists and has content
    expect_true(file.exists(test_file))
    expect_gt(file.size(test_file), 0)
    
    # Read and verify content
    df_read <- arrow::read_parquet(test_file)
    expect_equal(df_read, df)
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_write_parquet works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  backends <- c("arrow", "data.table")
  
  for (backend in backends) {
    test_file <- tempfile(fileext = ".parquet")
    big_write_parquet(df, test_file, backend = backend)
    
    # Verify file exists and has content
    expect_true(file.exists(test_file))
    expect_gt(file.size(test_file), 0)
    
    # Read and verify content
    df_read <- arrow::read_parquet(test_file)
    expect_equal(df_read, df)
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_write_parquet handles grouped data", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  test_file <- tempfile(fileext = ".parquet")
  big_write_parquet(grouped_df, test_file, backend = "arrow")
  
  # Verify file exists and has content
  expect_true(file.exists(test_file))
  expect_gt(file.size(test_file), 0)
  
  # Read and verify content
  df_read <- arrow::read_parquet(test_file)
  expect_equal(df_read, as.data.frame(grouped_df))
  
  # Clean up
  unlink(test_file)
})

test_that("big_write_parquet fails gracefully with invalid inputs", {
  df <- generate_test_data(n_rows = 1000)
  
  # Invalid file path
  expect_error(big_write_parquet(df, "/nonexistent/directory/file.parquet"))
  
  # Invalid compression type
  expect_error(big_write_parquet(df, tempfile(), compression = "invalid"))
  
  # Invalid backend
  expect_error(big_write_parquet(df, tempfile(), backend = "invalid"))
  
  # Empty data frame
  expect_error(big_write_parquet(data.frame()))
  
  # NULL input
  expect_error(big_write_parquet(NULL))
})
