test_that("big_select selects correct columns", {
  df <- big_select(mtcars, mpg, cyl)
  expect_equal(ncol(df), 2)
  expect_equal(names(df), c("mpg", "cyl"))
})

test_that("big_select handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    
    # Select subset of columns
    result <- big_select(df, num_1, cat_1)
    expect_equal(ncol(result), 2)
    expect_equal(names(result), c("num_1", "cat_1"))
    expect_equal(nrow(result), size)
    
    # Select all columns
    result_all <- big_select(df, everything())
    expect_equal(ncol(result_all), ncol(df))
    expect_equal(nrow(result_all), size)
  }
})

test_that("big_select works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- big_select(con, "num_1", "cat_1")
  expect_equal(names(result_duckdb), c("num_1", "cat_1"))
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- big_select(df, num_1, cat_1)
  expect_equal(names(result_dt), c("num_1", "cat_1"))
})

test_that("big_select handles column renaming", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_select(df,
    number_one = num_1,
    category = cat_1,
    value = num_2
  )
  
  expect_equal(names(result), c("number_one", "category", "value"))
  expect_equal(result$number_one, df$num_1)
  expect_equal(result$category, df$cat_1)
  expect_equal(result$value, df$num_2)
})

test_that("big_select preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- big_select(df, num_1, date, factor, cat_1)
  
  expect_type(result$num_1, "double")
  expect_type(result$cat_1, "character")
  expect_s3_class(result$date, "Date")
  expect_s3_class(result$factor, "factor")
})

test_that("big_select works with helper functions", {
  df <- generate_test_data(n_rows = 1000)
  
  # starts_with
  result_starts <- big_select(df, starts_with("num"))
  expect_true(all(grepl("^num", names(result_starts))))
  
  # contains
  result_contains <- big_select(df, contains("_"))
  expect_true(all(grepl("_", names(result_contains))))
  
  # matches
  result_matches <- big_select(df, matches("^[nc]"))
  expect_true(all(grepl("^[nc]", names(result_matches))))
})

test_that("big_select handles grouped data", {
  df <- generate_test_data(n_rows = 1000)
  grouped_df <- big_group_by(df, cat_1)
  
  result <- big_select(grouped_df, num_1, num_2)
  expect_true(inherits(result, "grouped_df"))
  expect_equal(ncol(result), 3)  # includes grouping column
  expect_true("cat_1" %in% names(result))
})

test_that("big_select fails gracefully with invalid inputs", {
  df <- generate_test_data(n_rows = 1000)
  
  # Non-existent column
  expect_error(big_select(df, nonexistent_col))
  
  # Empty selection
  expect_error(big_select(df))
  
  # Invalid column name type
  expect_error(big_select(df, 1))
})

test_that("big_select works with complex expressions", {
  df <- generate_test_data(n_rows = 1000)
  
  result <- big_select(df,
    scaled_num = scale(num_1),
    category = toupper(cat_1),
    ratio = num_1 / num_2
  )
  
  expect_equal(names(result), c("scaled_num", "category", "ratio"))
  expect_true(is.numeric(result$scaled_num))
  expect_true(is.character(result$category))
  expect_true(is.numeric(result$ratio))
})
