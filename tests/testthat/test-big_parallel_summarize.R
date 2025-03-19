test_that("big_parallel_summarize works", {
  df <- big_parallel_summarize(mtcars, mean_mpg = mean(mpg), n_cores = 2)
  expect_true("mean_mpg" %in% names(df))
})

# Tests for big_parallel_summarize function

test_that("big_parallel_summarize handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    
    result <- big_parallel_summarize(df,
      mean_num = mean(num_1),
      sd_num = sd(num_1),
      count = n(),
      min_num = min(num_1),
      max_num = max(num_1)
    )
    
    expect_equal(nrow(result), 1)
    expect_equal(ncol(result), 5)
    expect_true(all(c("mean_num", "sd_num", "count", "min_num", "max_num") %in% names(result)))
    expect_equal(result$count, size)
  }
})

test_that("big_parallel_summarize works with different numbers of cores", {
  df <- generate_test_data(n_rows = 1e4)
  cores <- c(1, 2, parallel::detectCores())
  
  for (n_cores in cores) {
    result <- big_parallel_summarize(df,
      mean_num = mean(num_1),
      sd_num = sd(num_1),
      n_cores = n_cores
    )
    
    expect_equal(nrow(result), 1)
    expect_equal(ncol(result), 2)
    expect_true(all(c("mean_num", "sd_num") %in% names(result)))
  }
})

test_that("big_parallel_summarize handles complex calculations", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_parallel_summarize(df,
    correlation = cor(num_1, num_2),
    zscore_mean = mean(scale(num_1)),
    quantile_75 = quantile(num_1, 0.75),
    unique_cats = n_distinct(cat_1)
  )
  
  expect_equal(nrow(result), 1)
  expect_true(abs(result$correlation) <= 1)
  expect_true(abs(result$zscore_mean) < 1e-10)  # Should be very close to 0
  expect_true(result$unique_cats > 0)
})

test_that("big_parallel_summarize handles grouped operations", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  result <- big_parallel_summarize(grouped_df,
    mean_num = mean(num_1),
    count = n(),
    sd_num = sd(num_1)
  )
  
  expect_gt(nrow(result), 1)
  expect_true(all(c("cat_1", "mean_num", "count", "sd_num") %in% names(result)))
  expect_true(all(result$count > 0))
})

test_that("big_parallel_summarize handles memory efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  
  # Monitor memory usage
  start_mem <- pryr::mem_used()
  
  result <- big_parallel_summarize(df,
    mean_num = mean(num_1),
    sd_num = sd(num_1),
    count = n(),
    correlation = cor(num_1, num_2)
  )
  
  end_mem <- pryr::mem_used()
  
  # Memory usage should be reasonable (less than 3x data size)
  expect_lt(end_mem - start_mem, object.size(df) * 3)
})

test_that("big_parallel_summarize works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  backends <- c("data.table", "arrow", "duckdb")
  
  for (backend in backends) {
    big_backend(backend)
    
    result <- big_parallel_summarize(df,
      mean_num = mean(num_1),
      count = n()
    )
    
    expect_equal(nrow(result), 1)
    expect_equal(ncol(result), 2)
    expect_equal(result$count, nrow(df))
  }
})

test_that("big_parallel_summarize handles window functions", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  result <- big_parallel_summarize(grouped_df,
    mean_num = mean(num_1),
    relative_count = n() / nrow(df),
    rank = min(rank(num_1))
  )
  
  expect_gt(nrow(result), 1)
  expect_true(all(c("mean_num", "relative_count", "rank") %in% names(result)))
  expect_true(sum(result$relative_count) == 1)
})

test_that("big_parallel_summarize handles missing values", {
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  
  result <- big_parallel_summarize(df,
    mean_num = mean(num_1, na.rm = TRUE),
    na_count = sum(is.na(num_1)),
    complete_count = sum(!is.na(num_1))
  )
  
  expect_equal(result$na_count, 100)
  expect_equal(result$complete_count, 900)
  expect_false(is.na(result$mean_num))
})

test_that("big_parallel_summarize handles errors gracefully", {
  df <- generate_test_data(n_rows = 1000)
  
  # Non-existent column
  expect_error(
    big_parallel_summarize(df, mean_nonexistent = mean(nonexistent))
  )
  
  # Invalid expression
  expect_error(
    big_parallel_summarize(df, error = stop("Error"))
  )
  
  # Invalid number of cores
  expect_error(
    big_parallel_summarize(df, mean_num = mean(num_1), n_cores = -1)
  )
})

test_that("big_parallel_summarize handles progress reporting", {
  df <- generate_test_data(n_rows = 1000)
  
  # With progress bar
  result_with_progress <- big_parallel_summarize(df,
    mean_num = mean(num_1),
    progress = TRUE
  )
  
  # Without progress bar
  result_without_progress <- big_parallel_summarize(df,
    mean_num = mean(num_1),
    progress = FALSE
  )
  
  expect_equal(result_with_progress, result_without_progress)
})

test_that("big_parallel_summarize handles multiple grouping variables", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1, cat_2)
  
  result <- big_parallel_summarize(grouped_df,
    mean_num = mean(num_1),
    count = n()
  )
  
  expect_true(all(c("cat_1", "cat_2", "mean_num", "count") %in% names(result)))
  expect_equal(sum(result$count), nrow(df))
})
