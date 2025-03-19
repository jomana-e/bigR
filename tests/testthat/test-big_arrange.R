test_that("big_arrange sorts data correctly", {
  df <- big_arrange(mtcars, desc(mpg))
  expect_equal(df$mpg, sort(mtcars$mpg, decreasing = TRUE))
})

test_that("big_arrange handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    
    # Test ascending order
    result <- big_arrange(df, num_1)
    expect_equal(nrow(result), size)
    expect_true(all(diff(result$num_1) >= 0))
    
    # Test descending order
    result_desc <- big_arrange(df, desc(num_1))
    expect_true(all(diff(result_desc$num_1) <= 0))
  }
})

test_that("big_arrange works with multiple sort columns", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Sort by multiple columns
  result <- big_arrange(df, cat_1, desc(num_1), num_2)
  
  # Check primary sort
  expect_true(all(diff(as.character(result$cat_1)) >= 0))
  
  # Check secondary sort within groups
  by_cat <- split(result, result$cat_1)
  for (group in by_cat) {
    expect_true(all(diff(group$num_1) <= 0))  # descending
    
    # Check tertiary sort within equal num_1 values
    equal_num1 <- split(group, group$num_1)
    for (subgroup in equal_num1) {
      expect_true(all(diff(subgroup$num_2) >= 0))  # ascending
    }
  }
})

test_that("big_arrange works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- big_arrange(con, "num_1 ASC")
  expect_true(all(diff(result_duckdb$num_1) >= 0))
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_arrange(df, num_1)
  expect_true(all(diff(result_dt$num_1) >= 0))
})

test_that("big_arrange handles missing values correctly", {
  df <- generate_test_data(n_rows = 1000)
  df$num_1[sample(1:1000, 100)] <- NA
  df$cat_1[sample(1:1000, 100)] <- NA
  
  # Test numeric NA handling
  result_num <- big_arrange(df, num_1)
  expect_equal(sum(is.na(head(result_num$num_1, 100))), 100)  # NAs first
  
  # Test character NA handling
  result_char <- big_arrange(df, cat_1)
  expect_equal(sum(is.na(head(result_char$cat_1, 100))), 100)  # NAs first
  
  # Test desc NA handling
  result_desc <- big_arrange(df, desc(num_1))
  expect_equal(sum(is.na(tail(result_desc$num_1, 100))), 100)  # NAs last
})

test_that("big_arrange preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- big_arrange(df, date, factor)
  
  expect_type(result$num_1, "double")
  expect_type(result$cat_1, "character")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
})

test_that("big_arrange works with expressions", {
  df <- generate_test_data(n_rows = 1000)
  
  # Sort by computed values
  result <- big_arrange(df, abs(num_1), num_1^2)
  expect_true(all(diff(abs(result$num_1)) >= 0))
  
  # Sort by string operations
  result_str <- big_arrange(df, toupper(cat_1))
  expect_true(all(diff(toupper(result_str$cat_1)) >= 0))
})

test_that("big_arrange maintains row integrity", {
  df <- generate_test_data(n_rows = 1000)
  df$row_id <- 1:1000
  
  # Sort and verify all original data is preserved
  result <- big_arrange(df, num_1)
  expect_equal(sort(result$row_id), 1:1000)
  expect_equal(nrow(result), 1000)
  expect_equal(ncol(result), ncol(df))
})
