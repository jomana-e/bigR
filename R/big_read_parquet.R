#' Read a large Parquet file using an optimized backend
#'
#' @param file Path to the Parquet file
#' @param backend Choose backend: "auto", "arrow", "duckdb"
#' @return A dataframe
#' @export
big_read_parquet <- function(file, backend = "auto") {
  if (backend == "auto") {
    backend <- "arrow" # Arrow is optimized for Parquet
  }

  if (backend == "arrow") {
    return(arrow::read_parquet(file))
  } else if (backend == "duckdb") {
    con <- DBI::dbConnect(duckdb::duckdb())
    return(DBI::dbGetQuery(con, sprintf("SELECT * FROM parquet_scan('%s')", file)))
  } else {
    stop("Invalid backend. Choose 'arrow' or 'duckdb'.")
  }
}
