test_that("big_parallel_mutate works", {
  df <- big_parallel_mutate(mtcars, mpg2 = mpg * 2, n_cores = 2)
  expect_true("mpg2" %in% names(df))
})

# Tests for big_parallel_mutate function

test_that("big_parallel_mutate handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    
    result <- big_parallel_mutate(df,
      doubled = num_1 * 2,
      squared = num_1^2,
      category = toupper(cat_1)
    )
    
    expect_equal(nrow(result), size)
    expect_equal(result$doubled, df$num_1 * 2)
    expect_equal(result$squared, df$num_1^2)
    expect_equal(result$category, toupper(df$cat_1))
  }
})

test_that("big_parallel_mutate works with different numbers of cores", {
  df <- generate_test_data(n_rows = 1e4)
  cores <- c(1, 2, parallel::detectCores())
  
  for (n_cores in cores) {
    result <- big_parallel_mutate(df,
      doubled = num_1 * 2,
      squared = num_1^2,
      n_cores = n_cores
    )
    
    expect_equal(nrow(result), nrow(df))
    expect_equal(result$doubled, df$num_1 * 2)
    expect_equal(result$squared, df$num_1^2)
  }
})

test_that("big_parallel_mutate handles complex transformations", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_parallel_mutate(df,
    scaled = scale(num_1),
    category = toupper(cat_1),
    rolling_mean = zoo::rollmean(num_1, k = 3, fill = NA, align = "right"),
    conditional = ifelse(num_1 > 0, "positive", "negative")
  )
  
  expect_equal(nrow(result), nrow(df))
  expect_true(all(!is.na(result$scaled[-c(1:2)])))
  expect_equal(result$category, toupper(df$cat_1))
  expect_true(all(result$conditional %in% c("positive", "negative")))
})

test_that("big_parallel_mutate preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- big_parallel_mutate(df,
    num = num_1,
    date_plus_1 = date + 1,
    factor_upper = factor(toupper(as.character(factor)))
  )
  
  expect_type(result$num, "double")
  expect_s3_class(result$date_plus_1, "Date")
  expect_s3_class(result$factor_upper, "factor")
})

test_that("big_parallel_mutate handles grouped operations", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  result <- big_parallel_mutate(grouped_df,
    mean_by_group = mean(num_1),
    count = n(),
    rank = rank(num_1)
  )
  
  expect_true(inherits(result, "grouped_df"))
  expect_true(all(c("mean_by_group", "count", "rank") %in% names(result)))
})

test_that("big_parallel_mutate handles memory efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Monitor memory usage
  start_mem <- pryr::mem_used()
  
  result <- big_parallel_mutate(df,
    doubled = num_1 * 2,
    squared = num_1^2,
    cubed = num_1^3
  )
  
  end_mem <- pryr::mem_used()
  
  # Memory usage should be reasonable (less than 4x data size)
  expect_lt(end_mem - start_mem, object.size(df) * 4)
})

test_that("big_parallel_mutate works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  backends <- c("data.table", "arrow", "duckdb")
  
  for (backend in backends) {
    big_backend(backend)
    
    result <- big_parallel_mutate(df,
      doubled = num_1 * 2,
      category = toupper(cat_1)
    )
    
    expect_equal(nrow(result), nrow(df))
    expect_equal(result$doubled, df$num_1 * 2)
    expect_equal(result$category, toupper(df$cat_1))
  }
})

test_that("big_parallel_mutate handles window functions", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_parallel_mutate(df,
    lag_num = lag(num_1),
    lead_num = lead(num_1),
    cumsum_num = cumsum(num_1),
    rolling_mean = zoo::rollmean(num_1, k = 3, fill = NA, align = "right")
  )
  
  expect_equal(nrow(result), nrow(df))
  expect_true(all(c("lag_num", "lead_num", "cumsum_num", "rolling_mean") %in% names(result)))
})

test_that("big_parallel_mutate handles errors gracefully", {
  df <- generate_test_data(n_rows = 1000)
  
  # Non-existent column
  expect_error(
    big_parallel_mutate(df, new_col = nonexistent_col * 2)
  )
  
  # Invalid expression
  expect_error(
    big_parallel_mutate(df, new_col = stop("Error"))
  )
  
  # Invalid number of cores
  expect_error(
    big_parallel_mutate(df, new_col = num_1 * 2, n_cores = -1)
  )
})

test_that("big_parallel_mutate handles progress reporting", {
  df <- generate_test_data(n_rows = 1000)
  
  # With progress bar
  result_with_progress <- big_parallel_mutate(df,
    doubled = num_1 * 2,
    progress = TRUE
  )
  
  # Without progress bar
  result_without_progress <- big_parallel_mutate(df,
    doubled = num_1 * 2,
    progress = FALSE
  )
  
  expect_equal(result_with_progress, result_without_progress)
})
