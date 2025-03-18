#' Write a large dataset to a Parquet file efficiently
#'
#' @param data A dataframe to write
#' @param file Path to save the Parquet file
#' @param backend Choose backend: "auto", "arrow", "duckdb"
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
