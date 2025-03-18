#' Write a large dataset to a Parquet file efficiently
#'
#' @param data A dataframe to write
#' @param file Path to save the Parquet file
#' @param backend Choose backend: "auto", "arrow", "duckdb"
#' @examples
#' \dontrun{
#' # Basic usage with automatic backend selection
#' big_write_parquet(data, "output.parquet")
#'
#' # Using Arrow backend explicitly
#' big_write_parquet(data, "output.parquet", backend = "arrow")
#'
#' # Writing processed data
#' data %>%
#'   big_filter(value > 0) %>%
#'   big_mutate(log_value = log(value)) %>%
#'   big_write_parquet("processed.parquet")
#'
#' # Using DuckDB for very large datasets
#' big_write_parquet(large_data, "large_output.parquet", backend = "duckdb")
#'
#' # Writing with compression
#' big_write_parquet(data, "compressed.parquet", backend = "arrow")
#' }
#' @export
big_write_parquet <- function(data, file, backend = "auto") {
  if (backend == "auto") {
    backend <- "arrow"
  }

  if (backend == "arrow") {
    arrow::write_parquet(data, file)
  } else if (backend == "duckdb") {
    con <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbWriteTable(con, "temp_table", data, overwrite = TRUE)
    DBI::dbExecute(con, sprintf("COPY temp_table TO '%s' (FORMAT 'parquet')", file))
    DBI::dbDisconnect(con)
  } else {
    stop("Invalid backend.")
  }
}
