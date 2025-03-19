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
  df <- generate_test_data(n_rows = 1000)
  
  # Test sorting by multiple columns
  result <- big_arrange(df, cat_1, desc(num_1), num_2)
  
  # Check that cat_1 is sorted
  expect_true(all(result$cat_1[1:(nrow(result)-1)] <= result$cat_1[2:nrow(result)]))
  
  # Within each cat_1 group, num_1 should be descending
  cat_groups <- split(result, result$cat_1)
  for (group in cat_groups) {
    expect_true(all(group$num_1[1:(nrow(group)-1)] >= group$num_1[2:nrow(group)]))
  }
})

test_that("big_arrange works with different backends", {
  df <- generate_test_data(n_rows = 1000)
  
  # Test data.table backend
  big_backend("data.table")
  result_dt <- big_arrange(df, num_1)
  expect_true(all(result_dt$num_1[1:(nrow(result_dt)-1)] <= result_dt$num_1[2:nrow(result_dt)]))
  
  # Test Arrow backend
  big_backend("arrow")
  result_arrow <- big_arrange(df, num_1)
  expect_true(all(result_arrow$num_1[1:(nrow(result_arrow)-1)] <= result_arrow$num_1[2:nrow(result_arrow)]))
  
  # Reset backend
  big_backend("auto")
})

test_that("big_arrange handles missing values correctly", {
  # Test with numeric missing values
  df_num <- generate_test_data(n_rows = 1000)
  df_num$num_1[1:100] <- NA
  result_num <- big_arrange(df_num, num_1)
  expect_equal(sum(is.na(head(result_num$num_1, 100))), 100)
  expect_true(all(result_num$num_1[101:nrow(result_num)] >= -Inf))
  
  # Test with character missing values
  df_char <- generate_test_data(n_rows = 1000)
  df_char$cat_1[1:100] <- NA
  result_char <- big_arrange(df_char, cat_1)
  expect_equal(sum(is.na(head(result_char$cat_1, 100))), 100)
  expect_true(all(result_char$cat_1[101:nrow(result_char)] >= ""))
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
  
  # Test sorting by absolute values
  result_abs <- big_arrange(df, abs(num_1))
  expect_true(all(abs(result_abs$num_1[1:(nrow(result_abs)-1)]) <= abs(result_abs$num_1[2:nrow(result_abs)])))
  
  # Test sorting by transformed strings
  result_str <- big_arrange(df, toupper(cat_1))
  expect_true(all(toupper(result_str$cat_1[1:(nrow(result_str)-1)]) <= toupper(result_str$cat_1[2:nrow(result_str)])))
})

test_that("big_arrange maintains row integrity", {
  df <- generate_test_data(n_rows = 1000)
  df$id <- 1:1000
  
  result <- big_arrange(df, num_1)
  
  # Check that all IDs are present
  expect_equal(sort(result$id), 1:1000)
  
  # Check that row relationships are maintained
  for (i in 1:(nrow(result)-1)) {
    if (result$num_1[i] == result$num_1[i+1]) {
      expect_true(result$id[i] < result$id[i+1])
    }
  }
})
