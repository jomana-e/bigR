# Helper functions for generating test data

#' Generate a test dataset of specified size
#' @param n_rows Number of rows
#' @param n_cols Number of columns (numeric)
#' @param n_cat_cols Number of categorical columns
#' @param n_categories Number of unique categories per categorical column
generate_test_data <- function(n_rows = 1000, n_cols = 5, n_cat_cols = 2, n_categories = 10) {
  # Generate numeric columns
  numeric_data <- matrix(rnorm(n_rows * n_cols), nrow = n_rows)
  colnames(numeric_data) <- paste0("num_", 1:n_cols)
  
  # Generate categorical columns
  cat_data <- matrix(
    sample(letters[1:n_categories], n_rows * n_cat_cols, replace = TRUE),
    nrow = n_rows
  )
  colnames(cat_data) <- paste0("cat_", 1:n_cat_cols)
  
  # Combine and convert to data frame
  df <- data.frame(
    id = 1:n_rows,
    as.data.frame(numeric_data),
    as.data.frame(cat_data)
  )
  
  return(df)
}

#' Generate test data files of different sizes
#' @param sizes List of sizes to generate (in rows)
#' @param formats Vector of formats to generate ("csv" and/or "parquet")
generate_test_files <- function(sizes = c(1e3, 1e4, 1e5), formats = c("csv", "parquet")) {
  files <- list()
  
  for (size in sizes) {
    data <- generate_test_data(n_rows = size)
    
    if ("csv" %in% formats) {
      csv_file <- tempfile(pattern = paste0("test_", size, "_"), fileext = ".csv")
      write.csv(data, csv_file, row.names = FALSE)
      files[[paste0("csv_", size)]] <- csv_file
    }
    
    if ("parquet" %in% formats) {
      parquet_file <- tempfile(pattern = paste0("test_", size, "_"), fileext = ".parquet")
      arrow::write_parquet(data, parquet_file)
      files[[paste0("parquet_", size)]] <- parquet_file
    }
  }
  
  return(files)
}

#' Clean up test files
#' @param files List of file paths to clean up
cleanup_test_files <- function(files) {
  lapply(files, unlink)
}

#' Create a DuckDB connection with test data
#' @param data Data frame to load
#' @param table_name Name of the table to create
create_test_db <- function(data, table_name = "test_table") {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, table_name, data)
  return(con)
} 