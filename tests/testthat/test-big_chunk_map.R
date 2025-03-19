test_that("big_chunk_map writes processed data", {
  input_file <- tempfile(fileext = ".csv")
  output_file <- tempfile(fileext = ".csv")
  on.exit({
    if (file.exists(input_file)) unlink(input_file)
    if (file.exists(output_file)) unlink(output_file)
  })

  write.csv(mtcars, input_file, row.names = FALSE)
  
  # Process data and write to output file
  big_chunk_map(
    input_file, 
    function(df) dplyr::mutate(df, mpg2 = mpg * 2), 
    chunk_size = 5, 
    output_file = output_file
  )

  # Verify output file exists and contains expected data
  expect_true(file.exists(output_file))
  df <- readr::read_csv(output_file)
  expect_true("mpg2" %in% names(df))
  expect_equal(nrow(df), nrow(mtcars))
  expect_equal(df$mpg2, mtcars$mpg * 2)
})

# Tests for big_chunk_map function

test_that("big_chunk_map processes data in chunks", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test with different chunk sizes
  chunk_sizes <- c(100, 1000, 5000)
  for (size in chunk_sizes) {
    result <- big_chunk_map(df, function(chunk) {
      chunk$num_1 <- chunk$num_1 * 2
      return(chunk)
    }, chunk_size = size)
    
    expect_equal(nrow(result), 1000)
    expect_true(all(result$num_1 == df$num_1 * 2))
  }
})

test_that("big_chunk_map handles different return types", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test numeric vector return
  result <- big_chunk_map(df, function(chunk) chunk$num_1 + 1, chunk_size = 100)
  expect_equal(length(result), 1000)
  expect_true(all(result == df$num_1 + 1))
  
  # Test character vector return
  result <- big_chunk_map(df, function(chunk) as.character(chunk$num_1), chunk_size = 100)
  expect_equal(length(result), 1000)
  expect_type(result, "character")
  
  # Test data frame return
  result <- big_chunk_map(df, function(chunk) {
    chunk$new_col <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100)
  expect_equal(nrow(result), 1000)
  expect_true("new_col" %in% names(result))
})

test_that("big_chunk_map handles memory efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Test with different chunk sizes
  chunk_sizes <- c(100, 1000, 5000)
  memory_usage <- numeric(length(chunk_sizes))
  
  for (i in seq_along(chunk_sizes)) {
    gc()  # Force garbage collection
    result <- big_chunk_map(df, function(chunk) {
      chunk$num_1 <- chunk$num_1 * 2
      return(chunk)
    }, chunk_size = chunk_sizes[i])
    gc()  # Force garbage collection
    memory_usage[i] <- as.numeric(object.size(result))
  }
  
  # Larger chunks should use more memory
  expect_true(all(diff(memory_usage) >= 0))
})

test_that("big_chunk_map works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_chunk_map(df, function(chunk) {
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100)
  expect_equal(nrow(result_dt), 1000)
  
  # Test with Arrow backend
  big_backend("arrow")
  result_arrow <- big_chunk_map(df, function(chunk) {
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100)
  expect_equal(nrow(result_arrow), 1000)
  
  # Reset backend
  big_backend("auto")
})

test_that("big_chunk_map handles complex transformations", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_chunk_map(df, function(chunk) {
    chunk$scaled <- scale(chunk$num_1)
    chunk$category <- cut(chunk$num_1, breaks = 3)
    chunk$rank <- rank(chunk$num_1)
    return(chunk)
  }, chunk_size = 100)
  
  expect_equal(nrow(result), 1000)
  expect_true(all(c("scaled", "category", "rank") %in% names(result)))
  expect_true(is.numeric(result$scaled))
  expect_true(is.factor(result$category))
  expect_true(is.numeric(result$rank))
})

test_that("big_chunk_map preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  df$logical <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  
  result <- big_chunk_map(df, function(chunk) {
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100)
  
  expect_type(result$num_1, "double")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
  expect_type(result$logical, "logical")
})

test_that("big_chunk_map handles progress reporting", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test with progress bar
  result <- big_chunk_map(df, function(chunk) {
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100, progress = TRUE)
  
  expect_equal(nrow(result), 1000)
  expect_true(all(result$num_1 == df$num_1 * 2))
})

test_that("big_chunk_map handles parallel processing", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test sequential vs parallel
  result_seq <- big_chunk_map(df, function(chunk) {
    Sys.sleep(0.1)  # Simulate expensive computation
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100, parallel = FALSE)
  
  result_par <- big_chunk_map(df, function(chunk) {
    Sys.sleep(0.1)  # Simulate expensive computation
    chunk$num_1 <- chunk$num_1 * 2
    return(chunk)
  }, chunk_size = 100, parallel = TRUE)
  
  expect_equal(result_seq, result_par)
})

test_that("big_chunk_map handles errors in chunks", {
  df <- generate_test_data(n_rows = 1000)
  
  # Function that fails on negative numbers
  expect_error(
    big_chunk_map(
      df,
      function(chunk) {
        if (any(chunk$num_1 < 0)) {
          stop("Negative numbers not allowed")
        }
        sqrt(chunk$num_1)
      },
      chunk_size = 100
    )
  )
})

test_that("big_chunk_map fails gracefully with invalid inputs", {
  df <- generate_test_data(n_rows = 1000)
  
  # Invalid chunk size
  expect_error(
    big_chunk_map(df, function(chunk) chunk, chunk_size = 0)
  )
  
  # Invalid function
  expect_error(
    big_chunk_map(df, "not_a_function", chunk_size = 100)
  )
  
  # NULL input
  expect_error(
    big_chunk_map(NULL, function(chunk) chunk, chunk_size = 100)
  )
  
  # Empty data frame
  expect_error(
    big_chunk_map(data.frame(), function(chunk) chunk, chunk_size = 100)
  )
})
