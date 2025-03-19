test_that("big_filter filters rows correctly", {
  df <- big_filter(mtcars, mpg > 20)
  expect_true(all(df$mpg > 20))
})

test_that("big_filter handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    result <- big_filter(df, num_1 > 0)
    expect_true(nrow(result) <= size)
    expect_true(all(result$num_1 > 0))
  }
})

test_that("big_filter works with complex conditions", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Multiple conditions
  result <- big_filter(df,
    num_1 > 0,
    num_2 < 1,
    cat_1 == "a"
  )
  
  expect_true(all(result$num_1 > 0))
  expect_true(all(result$num_2 < 1))
  expect_true(all(result$cat_1 == "a"))
  
  # Logical operators
  result <- big_filter(df, num_1 > 0 & num_2 < 0 | cat_1 == "a")
  expect_true(all(result$num_1 > 0 & result$num_2 < 0 | result$cat_1 == "a"))
})

test_that("big_filter works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  condition <- quote(num_1 > 0)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- big_filter(con, !!condition)
  expect_true(all(result_duckdb$num_1 > 0))
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_filter(df, !!condition)
  expect_true(all(result_dt$num_1 > 0))
})

test_that("big_filter handles missing values correctly", {
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  
  # Filter with NA handling
  result_na <- big_filter(df, !is.na(num_1))
  expect_equal(nrow(result_na), 900)
  expect_equal(sum(is.na(result_na$num_1)), 0)
  
  # Filter with NA in condition
  result_gt <- big_filter(df, num_1 > 0)
  expect_true(all(result_gt$num_1 > 0))
  expect_equal(sum(is.na(result_gt$num_1)), 0)
})

test_that("big_filter fails gracefully with invalid conditions", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test invalid column reference
  expect_error(big_filter(df, nonexistent_col > 0))
  
  # Test invalid comparison
  expect_error(big_filter(df, num_1 > "invalid"))
  
  # Test malformed expression
  expect_error(big_filter(df, >))
})

test_that("big_filter preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- big_filter(df, num_1 > 0)
  
  expect_type(result$num_1, "double")
  expect_type(result$cat_1, "character")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
})

test_that("big_filter works with grouped data", {
  df <- generate_test_data(n_rows = 1000)
  
  # Filter after grouping
  result <- df %>%
    big_group_by(cat_1) %>%
    big_filter(num_1 > mean(num_1))
  
  # Check that filtering was done within groups
  by_group <- split(df, df$cat_1)
  for (group in names(by_group)) {
    group_data <- by_group[[group]]
    group_result <- result[result$cat_1 == group, ]
    expect_true(all(group_result$num_1 > mean(group_data$num_1)))
  }
})
