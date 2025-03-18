#' Read a large Parquet file using an optimized backend
#'
#' @param file Path to the Parquet file
#' @param backend Choose backend: "auto", "arrow", "duckdb"
#' @return A dataframe
#' @examples
#' \dontrun{
#' # Basic usage with automatic backend selection
#' data <- big_read_parquet("data.parquet")
#'
#' # Using Arrow backend explicitly
#' data <- big_read_parquet("data.parquet", backend = "arrow")
#'
#' # Using DuckDB for large files
#' data <- big_read_parquet("large_data.parquet", backend = "duckdb")
#'
#' # Reading and processing
#' data <- big_read_parquet("data.parquet") %>%
#'   big_filter(value > 0) %>%
#'   big_mutate(log_value = log(value))
#'
#' # Reading partitioned Parquet files
#' data <- big_read_parquet("partitioned_dataset/")
#' }
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
