test_that("big_group_by groups data correctly", {
  df <- big_group_by(mtcars, cyl)
  expect_true(inherits(df, "grouped_df"))
})

test_that("big_group_by handles different data sizes", {
  sizes <- c(1e3, 1e4, 1e5)
  for (size in sizes) {
    df <- generate_test_data(n_rows = size)
    result <- df %>%
      big_group_by(cat_1) %>%
      big_summarize(mean_val = mean(num_1))
    
    expect_equal(nrow(result), length(unique(df$cat_1)))
    expect_true(all(table(df$cat_1) > 0))
  }
})

test_that("big_group_by works with multiple grouping variables", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Group by multiple columns
  result <- df %>%
    big_group_by(cat_1, cat_2) %>%
    big_summarize(
      count = n(),
      mean_val = mean(num_1)
    )
  
  # Check group structure
  expect_equal(
    nrow(result),
    length(unique(paste(df$cat_1, df$cat_2)))
  )
  
  # Verify group calculations
  manual_check <- aggregate(
    df$num_1,
    by = list(cat_1 = df$cat_1, cat_2 = df$cat_2),
    FUN = mean
  )
  expect_equal(nrow(result), nrow(manual_check))
})

test_that("big_group_by works with different backends", {
  df <- generate_test_data(n_rows = 1e4)
  
  # Test with DuckDB backend
  con <- create_test_db(df)
  result_duckdb <- con %>%
    big_group_by("cat_1") %>%
    big_summarize("AVG(num_1) as mean_val")
  expect_equal(nrow(result_duckdb), length(unique(df$cat_1)))
  DBI::dbDisconnect(con)
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(mean_val = mean(num_1))
  expect_equal(nrow(result_dt), length(unique(df$cat_1)))
})

test_that("big_group_by handles missing values correctly", {
  df <- generate_test_data(n_rows = 1000)
  df$cat_1[sample(1:1000, 100)] <- NA
  df$num_1[sample(1:1000, 100)] <- NA
  
  result <- df %>%
    big_group_by(cat_1) %>%
    big_summarize(
      count = n(),
      na_count = sum(is.na(num_1)),
      mean_val = mean(num_1, na.rm = TRUE)
    )
  
  expect_true("NA" %in% result$cat_1 | NA %in% result$cat_1)
  expect_true(all(!is.na(result$mean_val)))
})

test_that("big_group_by preserves data types", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  
  result <- df %>%
    big_group_by(factor, cat_1) %>%
    big_summarize(
      count = n(),
      mean_date = mean(as.numeric(date))
    )
  
  expect_s3_class(result$factor, "factor")
  expect_type(result$cat_1, "character")
})

test_that("big_group_by works with complex grouping expressions", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  
  result <- df %>%
    big_group_by(
      year = format(date, "%Y"),
      month = format(date, "%m"),
      cat_group = paste(cat_1, cat_2)
    ) %>%
    big_summarize(count = n())
  
  expect_true(all(c("year", "month", "cat_group") %in% names(result)))
  expect_true(all(result$count > 0))
})

test_that("big_group_by maintains group ordering", {
  df <- generate_test_data(n_rows = 1000)
  df$ordered_cat <- ordered(df$cat_1)
  
  result <- df %>%
    big_group_by(ordered_cat) %>%
    big_summarize(count = n())
  
  expect_true(is.ordered(result$ordered_cat))
  expect_equal(levels(result$ordered_cat), levels(df$ordered_cat))
})
