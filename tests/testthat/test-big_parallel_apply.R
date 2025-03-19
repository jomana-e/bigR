test_that("big_parallel_apply scales with data size", {
  sizes <- c(1e3, 1e4, 1e5)
  times <- numeric(length(sizes))
  
  for (i in seq_along(sizes)) {
    df <- generate_test_data(n_rows = sizes[i])
    
    # Measure execution time
    start_time <- Sys.time()
    result <- big_parallel_apply(df, function(chunk) {
      chunk$num_1 <- chunk$num_1 * 2
      return(chunk)
    })
    end_time <- Sys.time()
    times[i] <- as.numeric(end_time - start_time)
    
    # Verify results
    expect_equal(nrow(result), sizes[i])
    expect_equal(result$num_1, df$num_1 * 2)
  }
  
  # Verify that processing time increases sub-linearly with data size
  # (due to parallel processing)
  ratios <- times[-1] / times[-length(times)]
  expect_true(all(ratios < 10))  # Should scale better than linear
})

test_that("big_parallel_apply works with different core counts", {
  df <- generate_test_data(n_rows = 1e4)
  core_counts <- c(1, 2, parallel::detectCores())
  
  for (cores in core_counts) {
    result <- big_parallel_apply(df, function(chunk) {
      chunk$num_1 <- chunk$num_1 * 2
      return(chunk)
    }, n_cores = cores)
    
    expect_equal(result$num_1, df$num_1 * 2)
  }
})

test_that("big_parallel_apply handles complex operations", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Complex transformation function
  transform_fn <- function(chunk) {
    chunk$scaled <- scale(chunk$num_1)
    chunk$category <- cut(chunk$num_1, breaks = 3)
    chunk$rolling_mean <- stats::filter(chunk$num_1, rep(1/3, 3), sides = 1)
    return(chunk)
  }
  
  result <- big_parallel_apply(df, transform_fn)
  
  expect_true(all(c("scaled", "category", "rolling_mean") %in% names(result)))
  expect_equal(nrow(result), nrow(df))
})

test_that("big_parallel_apply handles errors gracefully", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Function that will fail on some rows
  error_fn <- function(chunk) {
    chunk$result <- sqrt(chunk$num_1)  # Will fail for negative numbers
    return(chunk)
  }
  
  # Should throw an error rather than fail silently
  expect_error(big_parallel_apply(df, error_fn))
})

test_that("big_parallel_apply maintains data consistency", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Function that depends on row order
  order_dependent_fn <- function(chunk) {
    chunk$cumsum <- cumsum(chunk$num_1)
    return(chunk)
  }
  
  # Run multiple times and verify results are consistent
  result1 <- big_parallel_apply(df, order_dependent_fn)
  result2 <- big_parallel_apply(df, order_dependent_fn)
  
  expect_equal(result1$cumsum, result2$cumsum)
})
