test_that("big_summarize calculates aggregations correctly", {
  df <- big_group_by(mtcars, cyl) %>% big_summarize(mean_mpg = mean(mpg))
  expect_true("mean_mpg" %in% names(df))
  expect_equal(nrow(df), length(unique(mtcars$cyl)))
})

test_that("big_summarize handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    result <- big_summarize(df,
      count = n(),
      mean_val = mean(num_1),
      sd_val = sd(num_1)
    )
    
    expect_equal(result$count, size)
    expect_true(!is.na(result$mean_val))
    expect_true(!is.na(result$sd_val))
  }
})

test_that("big_summarize works with grouped data", {
  df <- generate_test_data(n_rows = 1e4)
  
  result <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      count = n(),
      mean_val = mean(num_1),
      sd_val = sd(num_1),
      min_val = min(num_1),
      max_val = max(num_1)
    )
  
  # Check group-wise calculations
  for (cat in unique(df$cat_1)) {
    subset <- df[df$cat_1 == cat, ]
    group_result <- result[result$cat_1 == cat, ]
    expect_equal(group_result$count, nrow(subset))
    expect_equal(group_result$mean_val, mean(subset$num_1))
    expect_equal(group_result$min_val, min(subset$num_1))
  }
})

test_that("big_summarize works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- con %>%
    big_summarize(
      "COUNT(*) as count",
      "AVG(num_1) as mean_val",
      "MIN(num_1) as min_val"
    )
  expect_equal(result_duckdb$count, 1e4)
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_summarize(df,
    count = n(),
    mean_val = mean(num_1),
    min_val = min(num_1)
  )
  expect_equal(result_dt$count, 1e4)
})

test_that("big_summarize handles missing values correctly", {
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  df$num_2[sample(1:1000, 200)] <- NA
  
  result <- big_summarize(df,
    count = n(),
    complete = sum(!is.na(num_1)),
    mean_val = mean(num_1, na.rm = TRUE),
    na_count = sum(is.na(num_1)),
    both_na = sum(is.na(num_1) & is.na(num_2))
  )
  
  expect_equal(result$count, 1000)
  expect_equal(result$complete, 900)
  expect_equal(result$na_count, 100)
  expect_true(!is.na(result$mean_val))
})

test_that("big_summarize handles complex calculations", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_summarize(df,
    correlation = cor(num_1, num_2),
    zscore_mean = mean(scale(num_1)),
    quantile_75 = quantile(num_1, 0.75),
    cat_counts = length(unique(cat_1))
  )
  
  expect_true(abs(result$correlation) <= 1)
  expect_true(abs(result$zscore_mean) < 0.1)  # Should be close to 0
  expect_true(result$quantile_75 > median(df$num_1))
  expect_equal(result$cat_counts, length(unique(df$cat_1)))
})

test_that("big_summarize works with window functions", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      count = n(),
      pct = n() / nrow(df) * 100,
      rank = min(rank(num_1))
    )
  
  expect_equal(sum(result$count), 1000)
  expect_equal(sum(result$pct), 100, tolerance = 0.01)
  expect_true(all(result$rank >= 1))
})

test_that("big_summarize fails gracefully with invalid expressions", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test invalid column reference
  expect_error(big_summarize(df, mean_val = mean(nonexistent_col)))
  
  # Test invalid function
  expect_error(big_summarize(df, result = invalid_function(num_1)))
  
  # Test invalid grouping
  expect_error(
    df %>%
      big_group_by(nonexistent_group) %>%
      big_summarize(count = n())
  )
})
