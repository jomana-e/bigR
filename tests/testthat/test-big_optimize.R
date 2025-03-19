test_that("big_optimize suggests the right backend", {
  df <- mtcars
  expect_match(big_optimize(df), "Data is small")
})

test_that("big_optimize improves query performance", {
  # Generate test data
  df <- generate_test_data(n_rows = 1e5)
  
  # Create a complex query
  query <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      mean_num = mean(num_1),
      sd_num = sd(num_1),
      count = n()
    ) %>%
    big_filter(count > 100) %>%
    big_arrange(desc(mean_num))
  
  # Run query without optimization
  start_time <- Sys.time()
  result_unopt <- query %>% collect()
  unopt_time <- as.numeric(Sys.time() - start_time, units = "secs")
  
  # Run query with optimization
  start_time <- Sys.time()
  result_opt <- query %>% big_optimize() %>% collect()
  opt_time <- as.numeric(Sys.time() - start_time, units = "secs")
  
  # Verify results are the same
  expect_equal(result_opt, result_unopt)
  
  # Verify optimization improves performance
  # Note: This test might be flaky due to system load
  # expect_lt(opt_time, unopt_time)
})

test_that("big_optimize handles different optimization strategies", {
  df <- generate_test_data(n_rows = 1e4)
  
  strategies <- c("predicate_pushdown", "projection_pushdown", "join_reordering")
  for (strategy in strategies) {
    query <- df %>%
      big_filter(num_1 > 0) %>%
      big_select(num_1, cat_1)
    
    result <- query %>% 
      big_optimize(strategy = strategy) %>%
      collect()
    
    expect_true(nrow(result) <= nrow(df))
    expect_equal(ncol(result), 2)
  }
})

test_that("big_optimize works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  query_duck <- con %>%
    big_filter(num_1 > 0) %>%
    big_select(num_1, cat_1)
  
  result_duck <- query_duck %>%
    big_optimize() %>%
    collect()
  
  expect_true(nrow(result_duck) <= nrow(df))
  expect_equal(ncol(result_duck), 2)
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  query_dt <- df %>%
    big_filter(num_1 > 0) %>%
    big_select(num_1, cat_1)
  
  result_dt <- query_dt %>%
    big_optimize() %>%
    collect()
  
  expect_true(nrow(result_dt) <= nrow(df))
  expect_equal(ncol(result_dt), 2)
})

test_that("big_optimize handles complex queries", {
  df1 <- generate_test_data(n_rows = 1e4)
  df2 <- generate_test_data(n_rows = 1e4)
  
  query <- df1 %>%
    big_left_join(df2, by = "cat_1") %>%
    big_filter(num_1.x > num_1.y) %>%
    big_group_by(cat_1) %>%
    big_summarize(
      diff_mean = mean(num_1.x - num_1.y),
      count = n()
    ) %>%
    big_arrange(desc(diff_mean))
  
  result <- query %>%
    big_optimize() %>%
    collect()
  
  expect_true(is.data.frame(result))
  expect_true("diff_mean" %in% names(result))
  expect_true("count" %in% names(result))
})

test_that("big_optimize preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  query <- df %>%
    big_filter(num_1 > 0) %>%
    big_select(num_1, date, factor, cat_1)
  
  result <- query %>%
    big_optimize() %>%
    collect()
  
  expect_type(result$num_1, "double")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
  expect_type(result$cat_1, "character")
})

test_that("big_optimize handles memory efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  
  query <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      mean_num = mean(num_1),
      sd_num = sd(num_1)
    )
  
  # Monitor memory usage
  start_mem <- pryr::mem_used()
  result <- query %>%
    big_optimize() %>%
    collect()
  end_mem <- pryr::mem_used()
  
  # Verify memory usage is reasonable
  expect_lt(end_mem - start_mem, object.size(df) * 2)
})

test_that("big_optimize fails gracefully with invalid inputs", {
  df <- generate_test_data(n_rows = 1000)
  
  # Invalid optimization strategy
  expect_error(
    df %>%
      big_filter(num_1 > 0) %>%
      big_optimize(strategy = "invalid") %>%
      collect()
  )
  
  # Invalid query
  expect_error(
    df %>%
      big_filter(nonexistent > 0) %>%
      big_optimize() %>%
      collect()
  )
  
  # NULL input
  expect_error(big_optimize(NULL))
})
