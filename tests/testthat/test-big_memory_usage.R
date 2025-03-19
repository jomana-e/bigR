test_that("big_memory_usage reports correct memory size", {
  df <- mtcars
  mem_size <- big_memory_usage(df)

  expect_true(is.numeric(mem_size))
  expect_true(mem_size > 0)
})

test_that("big_memory_usage tracks memory usage correctly", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Get initial memory usage
  initial_usage <- big_memory_usage()
  expect_type(initial_usage, "double")
  expect_gte(initial_usage, 0)
  
  # Create large object and check memory increase
  large_matrix <- matrix(rnorm(1e6), ncol = 100)
  new_usage <- big_memory_usage()
  expect_gt(new_usage, initial_usage)
  
  # Remove large object and check memory decrease
  rm(large_matrix)
  gc()
  final_usage <- big_memory_usage()
  expect_lt(final_usage, new_usage)
})

test_that("big_memory_usage works with different units", {
  units <- c("bytes", "kb", "mb", "gb")
  
  for (unit in units) {
    usage <- big_memory_usage(unit = unit)
    expect_type(usage, "double")
    expect_gte(usage, 0)
  }
})

test_that("big_memory_usage handles large data operations efficiently", {
  # Generate test data
  df <- generate_test_data(n_rows = 1e5)
  
  # Monitor memory during group by and summarize
  start_mem <- big_memory_usage()
  
  result <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      mean_num = mean(num_1),
      sd_num = sd(num_1),
      count = n()
    ) %>%
    collect()
  
  end_mem <- big_memory_usage()
  
  # Memory usage should be reasonable (less than 3x data size)
  expect_lt(end_mem - start_mem, object.size(df) * 3)
})

test_that("big_memory_usage tracks backend-specific memory usage", {
  df <- generate_test_data(n_rows = 1e5)
  
  backends <- c("data.table", "arrow", "duckdb")
  memory_usage <- list()
  
  for (backend in backends) {
    big_backend(backend)
    
    # Monitor memory during operation
    start_mem <- big_memory_usage()
    
    result <- df %>%
      big_filter(num_1 > 0) %>%
      big_select(num_1, cat_1) %>%
      collect()
    
    end_mem <- big_memory_usage()
    memory_usage[[backend]] <- end_mem - start_mem
    
    # Memory usage should be reasonable
    expect_lt(memory_usage[[backend]], object.size(df) * 3)
  }
})

test_that("big_memory_usage handles chunked operations efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  chunk_sizes <- c(1000, 5000, 10000)
  memory_usage <- list()
  
  for (chunk_size in chunk_sizes) {
    start_mem <- big_memory_usage()
    
    result <- df %>%
      big_chunk_map(
        function(chunk) {
          chunk$num_1 * 2
        },
        chunk_size = chunk_size
      )
    
    end_mem <- big_memory_usage()
    memory_usage[[as.character(chunk_size)]] <- end_mem - start_mem
  }
  
  # Smaller chunks should use less memory
  expect_lt(memory_usage[["1000"]], memory_usage[["10000"]])
})

test_that("big_memory_usage handles memory limits", {
  # Set a low memory limit
  old_limit <- options(bigr.memory.limit = 100 * 1024 * 1024)  # 100MB
  on.exit(options(old_limit))
  
  df <- generate_test_data(n_rows = 1e5)
  
  # Operation that would exceed memory limit
  expect_error(
    df %>%
      big_mutate(
        new_col = lapply(1:nrow(df), function(i) rnorm(1000))
      ) %>%
      collect()
  )
})

test_that("big_memory_usage handles concurrent operations", {
  df1 <- generate_test_data(n_rows = 1e4)
  df2 <- generate_test_data(n_rows = 1e4)
  
  # Monitor memory during concurrent operations
  start_mem <- big_memory_usage()
  
  future::plan(future::multisession)
  results <- future.apply::future_lapply(1:2, function(i) {
    if (i == 1) {
      df1 %>%
        big_group_by(cat_1) %>%
        big_summarize(mean_num = mean(num_1)) %>%
        collect()
    } else {
      df2 %>%
        big_filter(num_1 > 0) %>%
        big_select(num_1, cat_1) %>%
        collect()
    }
  })
  future::plan(future::sequential)
  
  end_mem <- big_memory_usage()
  
  # Memory usage should be reasonable
  expect_lt(end_mem - start_mem, object.size(df1) * 4)
})

test_that("big_memory_usage reports peak memory usage", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Get initial peak
  initial_peak <- big_memory_usage(type = "peak")
  
  # Create and remove large objects
  large_matrix1 <- matrix(rnorm(1e6), ncol = 100)
  rm(large_matrix1)
  gc()
  
  large_matrix2 <- matrix(rnorm(5e5), ncol = 50)
  rm(large_matrix2)
  gc()
  
  # Peak should reflect highest usage
  final_peak <- big_memory_usage(type = "peak")
  expect_gte(final_peak, initial_peak)
})

test_that("big_memory_usage handles memory cleanup", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Create some temporary objects
  temp_data <- list()
  for (i in 1:10) {
    temp_data[[i]] <- matrix(rnorm(1e4), ncol = 10)
  }
  
  # Record memory usage
  usage_before_cleanup <- big_memory_usage()
  
  # Clean up
  rm(temp_data)
  gc()
  
  # Check memory usage after cleanup
  usage_after_cleanup <- big_memory_usage()
  expect_lt(usage_after_cleanup, usage_before_cleanup)
})
