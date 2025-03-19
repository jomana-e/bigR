test_that("big_backend sets and gets the correct backend", {
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  big_backend("data.table")
  expect_equal(big_backend(), "data.table")
  
  big_backend("arrow")
  expect_equal(big_backend(), "arrow")
  
  big_backend("duckdb")
  expect_equal(big_backend(), "duckdb")
  
  big_backend("auto")
  expect_equal(big_backend(), "auto")
})

test_that("big_backend maintains state between operations", {
  df <- generate_test_data(n_rows = 1000)
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  # Test with data.table backend
  big_backend("data.table")
  result_dt <- df %>%
    big_filter(num_1 > 0) %>%
    big_select(num_1, cat_1) %>%
    as.data.frame()
  
  expect_equal(big_backend(), "data.table")
  expect_s3_class(result_dt, "data.frame")
  expect_true(all(result_dt$num_1 > 0))
})

test_that("big_backend handles backend-specific features", {
  df <- generate_test_data(n_rows = 1000)
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  # Test DuckDB-specific features
  big_backend("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, "df", df)
  result_sql <- DBI::dbGetQuery(con, "SELECT * FROM df WHERE num_1 > 0")
  expect_true(is.data.frame(result_sql))
  expect_true(all(result_sql$num_1 > 0))
  DBI::dbDisconnect(con)
  
  # Test Arrow-specific features
  big_backend("arrow")
  arrow_table <- arrow::arrow_table(df)
  expect_true(inherits(arrow_table, "Table"))
  
  # Test data.table-specific features
  big_backend("data.table")
  dt <- data.table::as.data.table(df)
  expect_true(data.table::is.data.table(dt))
})

test_that("big_backend preserves data types across backends", {
  df <- generate_test_data(n_rows = 1000)
  df$date <- Sys.Date() + 1:1000
  df$factor <- factor(sample(letters[1:3], 1000, replace = TRUE))
  df$logical <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  backends <- c("data.table", "arrow", "duckdb")
  results <- list()
  
  for (backend in backends) {
    big_backend(backend)
    results[[backend]] <- as.data.frame(df[, c("num_1", "date", "factor", "logical")])
    
    # Verify data types
    expect_type(results[[backend]]$num_1, "double")
    expect_s3_class(results[[backend]]$date, "Date")
    expect_type(results[[backend]]$logical, "logical")
  }
  
  # Verify results are consistent across backends
  expect_equal(results$data.table, results$arrow)
  expect_equal(results$data.table, results$duckdb)
})

test_that("big_backend handles large datasets efficiently", {
  df <- generate_test_data(n_rows = 1e5)
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  backends <- c("data.table", "arrow", "duckdb")
  times <- list()
  
  for (backend in backends) {
    big_backend(backend)
    
    # Measure execution time
    start_time <- Sys.time()
    result <- as.data.frame(df[, c("cat_1", "num_1")])
    times[[backend]] <- as.numeric(Sys.time() - start_time, units = "secs")
    
    # Verify result structure
    expect_true(is.data.frame(result))
    expect_true(all(c("cat_1", "num_1") %in% names(result)))
  }
  
  # All backends should complete in reasonable time
  expect_true(all(unlist(times) < 60))
})

test_that("big_backend handles concurrent operations", {
  df1 <- generate_test_data(n_rows = 1000)
  df2 <- generate_test_data(n_rows = 1000)
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  # Test concurrent operations with different backends
  big_backend("data.table")
  op1 <- df1[df1$num_1 > 0, ]
  
  big_backend("arrow")
  op2 <- df2[df2$num_1 < 0, ]
  
  # Results should reflect the backend at collection time
  result1 <- as.data.frame(op1)
  result2 <- as.data.frame(op2)
  
  expect_true(all(result1$num_1 > 0))
  expect_true(all(result2$num_1 < 0))
})

test_that("big_backend fails gracefully with invalid inputs", {
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  # Invalid backend name
  expect_error(big_backend("invalid_backend"), "Invalid backend")
  
  # NULL backend
  expect_error(big_backend(NULL))
  
  # Empty string backend
  expect_error(big_backend(""), "Invalid backend")
  
  # Numeric backend
  expect_error(big_backend(1), "Backend must be a single character string")
})

test_that("big_backend handles temporary backend switches", {
  old_backend <- big_backend()
  on.exit(big_backend(old_backend))
  
  withr::with_options(
    list(bigR.backend = "arrow"),
    {
      expect_equal(big_backend(), "arrow")
      df <- generate_test_data(n_rows = 100)
      result <- as.data.frame(df[df$num_1 > 0, ])
      expect_true(is.data.frame(result))
    }
  )
  
  # Should revert to original backend
  expect_equal(big_backend(), old_backend)
})
