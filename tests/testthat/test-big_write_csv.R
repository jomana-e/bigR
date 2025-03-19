test_that("big_write_csv writes CSV files correctly", {
  test_file <- tempfile(fileext = ".csv")

  big_write_csv(mtcars, test_file, backend = "auto")

  df <- readr::read_csv(test_file)
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(ncol(df), ncol(mtcars))
})

test_that("big_write_csv writes CSV files with different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    test_file <- tempfile(fileext = ".csv")
    
    # Write CSV file
    big_write_csv(df, test_file)
    
    # Verify file exists and has content
    expect_true(file.exists(test_file))
    expect_gt(file.size(test_file), 0)
    
    # Read and verify content
    df_read <- data.table::fread(test_file)
    expect_equal(nrow(df_read), size)
    expect_equal(ncol(df_read), ncol(df))
    expect_equal(names(df_read), names(df))
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_write_csv handles different data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  df$logical <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  df$integer <- as.integer(1:1000)
  
  test_file <- tempfile(fileext = ".csv")
  big_write_csv(df, test_file)
  
  df_read <- data.table::fread(test_file)
  
  # Verify data types are preserved (note: some types may be converted as per CSV limitations)
  expect_type(df_read$num_1, "double")
  expect_type(df_read$cat_1, "character")
  expect_type(df_read$logical, "logical")
  expect_type(df_read$integer, "integer")
  
  # Clean up
  unlink(test_file)
})

test_that("big_write_csv works with different compression options", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test gzip compression
  test_file_gz <- tempfile(fileext = ".csv.gz")
  big_write_csv(df, test_file_gz)
  expect_true(file.exists(test_file_gz))
  df_read_gz <- data.table::fread(test_file_gz)
  expect_equal(df_read_gz, df)
  unlink(test_file_gz)
  
  # Test bzip2 compression
  test_file_bz2 <- tempfile(fileext = ".csv.bz2")
  big_write_csv(df, test_file_bz2)
  expect_true(file.exists(test_file_bz2))
  df_read_bz2 <- data.table::fread(test_file_bz2)
  expect_equal(df_read_bz2, df)
  unlink(test_file_bz2)
  
  # Test xz compression
  test_file_xz <- tempfile(fileext = ".csv.xz")
  big_write_csv(df, test_file_xz)
  expect_true(file.exists(test_file_xz))
  df_read_xz <- data.table::fread(test_file_xz)
  expect_equal(df_read_xz, df)
  unlink(test_file_xz)
})

test_that("big_write_csv works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  backends <- c("data.table", "arrow")
  
  for (backend in backends) {
    test_file <- tempfile(fileext = ".csv")
    big_write_csv(df, test_file, backend = backend)
    
    # Verify file exists and has content
    expect_true(file.exists(test_file))
    expect_gt(file.size(test_file), 0)
    
    # Read and verify content
    df_read <- data.table::fread(test_file)
    expect_equal(df_read, df)
    
    # Clean up
    unlink(test_file)
  }
})

test_that("big_write_csv handles special characters and quoting", {
  df <- generate_test_data(n_rows = 100)
  df$special <- paste0("text,with,commas ", sample(letters, 100, replace = TRUE))
  df$quoted <- paste0('"quoted"text"', sample(letters, 100, replace = TRUE))
  
  test_file <- tempfile(fileext = ".csv")
  big_write_csv(df, test_file)
  
  df_read <- data.table::fread(test_file)
  expect_equal(df_read$special, df$special)
  expect_equal(df_read$quoted, df$quoted)
  
  # Clean up
  unlink(test_file)
})

test_that("big_write_csv handles grouped data", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  test_file <- tempfile(fileext = ".csv")
  big_write_csv(grouped_df, test_file)
  
  # Verify file exists and has content
  expect_true(file.exists(test_file))
  expect_gt(file.size(test_file), 0)
  
  # Read and verify content
  df_read <- data.table::fread(test_file)
  expect_equal(df_read, as.data.frame(grouped_df))
  
  # Clean up
  unlink(test_file)
})

test_that("big_write_csv fails gracefully with invalid inputs", {
  df <- generate_test_data(n_rows = 1000)
  
  # Invalid file path
  expect_error(big_write_csv(df, "/nonexistent/directory/file.csv"))
  
  # Invalid backend
  expect_error(big_write_csv(df, tempfile(), backend = "invalid"))
  
  # Empty data frame
  expect_error(big_write_csv(data.frame()))
  
  # NULL input
  expect_error(big_write_csv(NULL))
  
  # Invalid compression
  expect_error(big_write_csv(df, tempfile(fileext = ".csv.invalid")))
})
