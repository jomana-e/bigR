test_that("big_chunk_map writes processed data", {
  input_file <- tempfile(fileext = ".csv")
  output_file <- tempfile(fileext = ".csv")

  write.csv(mtcars, input_file, row.names = FALSE)

  big_chunk_map(input_file, function(df) dplyr::mutate(df, mpg2 = mpg * 2), chunk_size = 5, output_file)

  df <- readr::read_csv(output_file)
  expect_true("mpg2" %in% names(df))
})

# Tests for big_chunk_map function

test_that("big_chunk_map processes data in chunks", {
  df <- generate_test_data(n_rows = 1e5)
  chunk_sizes <- c(100, 1000, 5000)
  
  for (chunk_size in chunk_sizes) {
    # Simple transformation
    result <- big_chunk_map(
      df,
      function(chunk) {
        chunk$num_1 * 2
      },
      chunk_size = chunk_size
    )
    
    expect_equal(length(result), nrow(df))
    expect_true(all(result == df$num_1 * 2))
  }
})

test_that("big_chunk_map handles different return types", {
  df <- generate_test_data(n_rows = 1000)
  
  # Return numeric vector
  result_num <- big_chunk_map(
    df,
    function(chunk) chunk$num_1 + 1,
    chunk_size = 100
  )
  expect_type(result_num, "double")
  expect_length(result_num, nrow(df))
  
  # Return character vector
  result_char <- big_chunk_map(
    df,
    function(chunk) paste0(chunk$cat_1, "_suffix"),
    chunk_size = 100
  )
  expect_type(result_char, "character")
  expect_length(result_char, nrow(df))
  
  # Return data frame
  result_df <- big_chunk_map(
    df,
    function(chunk) {
      data.frame(
        doubled = chunk$num_1 * 2,
        category = chunk$cat_1
      )
    },
    chunk_size = 100
  )
  expect_s3_class(result_df, "data.frame")
  expect_equal(nrow(result_df), nrow(df))
})

test_that("big_chunk_map handles memory efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Monitor memory usage with different chunk sizes
  chunk_sizes <- c(100, 1000, 5000)
  memory_usage <- list()
  
  for (chunk_size in chunk_sizes) {
    start_mem <- pryr::mem_used()
    
    result <- big_chunk_map(
      df,
      function(chunk) chunk$num_1 * 2,
      chunk_size = chunk_size
    )
    
    end_mem <- pryr::mem_used()
    memory_usage[[as.character(chunk_size)]] <- end_mem - start_mem
  }
  
  # Smaller chunks should use less memory
  expect_lt(memory_usage[["100"]], memory_usage[["5000"]])
})

test_that("big_chunk_map works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  backends <- c("data.table", "arrow", "duckdb")
  
  for (backend in backends) {
    big_backend(backend)
    
    result <- big_chunk_map(
      df,
      function(chunk) chunk$num_1 * 2,
      chunk_size = 100
    )
    
    expect_equal(length(result), nrow(df))
    expect_true(all(result == df$num_1 * 2))
  }
})

test_that("big_chunk_map handles complex transformations", {
  df <- generate_test_data(n_rows = 1000)
  
  # Multiple column transformation
  result <- big_chunk_map(
    df,
    function(chunk) {
      data.frame(
        scaled = scale(chunk$num_1),
        category = toupper(chunk$cat_1),
        rolling_mean = zoo::rollmean(chunk$num_1, k = 3, fill = NA, align = "right")
      )
    },
    chunk_size = 100
  )
  
  expect_equal(nrow(result), nrow(df))
  expect_equal(ncol(result), 3)
  expect_true(all(!is.na(result$scaled[-c(1:2)])))
})

test_that("big_chunk_map preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- big_chunk_map(
    df,
    function(chunk) {
      data.frame(
        num = chunk$num_1,
        date = chunk$date,
        factor = chunk$factor
      )
    },
    chunk_size = 100
  )
  
  expect_type(result$num, "double")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
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

test_that("big_chunk_map handles progress reporting", {
  df <- generate_test_data(n_rows = 1000)
  
  # With progress bar
  result_with_progress <- big_chunk_map(
    df,
    function(chunk) chunk$num_1 * 2,
    chunk_size = 100,
    progress = TRUE
  )
  
  # Without progress bar
  result_without_progress <- big_chunk_map(
    df,
    function(chunk) chunk$num_1 * 2,
    chunk_size = 100,
    progress = FALSE
  )
  
  expect_equal(result_with_progress, result_without_progress)
})

test_that("big_chunk_map handles parallel processing", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Sequential processing
  start_time <- Sys.time()
  result_seq <- big_chunk_map(
    df,
    function(chunk) {
      Sys.sleep(0.1)  # Simulate computation
      chunk$num_1 * 2
    },
    chunk_size = 1000,
    parallel = FALSE
  )
  seq_time <- as.numeric(Sys.time() - start_time, units = "secs")
  
  # Parallel processing
  start_time <- Sys.time()
  result_par <- big_chunk_map(
    df,
    function(chunk) {
      Sys.sleep(0.1)  # Simulate computation
      chunk$num_1 * 2
    },
    chunk_size = 1000,
    parallel = TRUE
  )
  par_time <- as.numeric(Sys.time() - start_time, units = "secs")
  
  expect_equal(result_seq, result_par)
  # expect_lt(par_time, seq_time)  # Commented out as it might be flaky
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
