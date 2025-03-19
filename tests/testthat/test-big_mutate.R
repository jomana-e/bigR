test_that("big_mutate adds a new column", {
  df <- big_mutate(mtcars, new_col = mpg * 2)
  expect_true("new_col" %in% names(df))
  expect_equal(df$new_col, df$mpg * 2)
})

test_that("big_mutate handles different data sizes", {
  # Test with different data sizes
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    result <- big_mutate(df, new_col = num_1 * 2)
    expect_equal(nrow(result), size)
    expect_equal(result$new_col, df$num_1 * 2)
  }
})

test_that("big_mutate works with complex expressions", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Multiple mutations
  result <- big_mutate(df,
    sum_cols = num_1 + num_2,
    mean_cols = (num_1 + num_2) / 2,
    category = ifelse(num_1 > 0, "positive", "negative")
  )
  
  expect_equal(result$sum_cols, df$num_1 + df$num_2)
  expect_equal(result$mean_cols, (df$num_1 + df$num_2) / 2)
  expect_equal(result$category, ifelse(df$num_1 > 0, "positive", "negative"))
})

test_that("big_mutate works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- big_mutate(con, new_col = num_1 * 2)
  expect_equal(result_duckdb$new_col, df$num_1 * 2)
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_mutate(df, new_col = num_1 * 2)
  expect_equal(result_dt$new_col, df$num_1 * 2)
})

test_that("big_mutate handles missing values correctly", {
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  
  # Test NA handling in different operations
  result <- big_mutate(df,
    plus_one = num_1 + 1,
    mult_two = num_1 * 2,
    is_missing = is.na(num_1)
  )
  
  expect_equal(sum(is.na(result$plus_one)), 100)
  expect_equal(sum(is.na(result$mult_two)), 100)
  expect_equal(sum(result$is_missing), 100)
})

test_that("big_mutate fails gracefully with invalid expressions", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test invalid column reference
  expect_error(big_mutate(df, new_col = nonexistent_col * 2))
  
  # Test invalid expression
  expect_error(big_mutate(df, new_col = log("invalid")))
})
