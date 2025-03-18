#' Read a large CSV file using an optimized backend
#'
#' @param file Path to the CSV file
#' @param backend Choose backend: "auto", "data.table", "arrow", or "duckdb"
#' @param chunk_size Optional chunk size for out-of-core processing
#' @return A dataframe or a DuckDB connection
#' @examples
#' \dontrun{
#' # Basic usage with automatic backend selection
#' data <- big_read_csv("large_dataset.csv")
#'
#' # Specify a backend explicitly
#' data <- big_read_csv("large_dataset.csv", backend = "data.table")
#'
#' # Read a very large file with DuckDB
#' data <- big_read_csv("huge_dataset.csv", backend = "duckdb")
#'
#' # Process data in chunks
#' data <- big_read_csv("large_dataset.csv", chunk_size = 10000)
#' }
#' @export
big_read_csv <- function(file, backend = "auto", chunk_size = NULL) {
  file_size <- file.info(file)$size / (1024^3) # Convert bytes to GB

  # Automatic backend selection
  if (backend == "auto") {
    if (file_size < 1) backend <- "readr"
    else if (file_size < 5) backend <- "data.table"
    else backend <- "duckdb"
  }

  # Read data using the selected backend
  if (backend == "readr") return(readr::read_csv(file))
  if (backend == "data.table") return(data.table::fread(file))
  if (backend == "arrow") return(arrow::read_csv_arrow(file))
  if (backend == "duckdb") {
    con <- DBI::dbConnect(duckdb::duckdb())
    return(DBI::dbGetQuery(con, sprintf("SELECT * FROM read_csv_auto('%s')", file)))
  }

  stop("Invalid backend. Choose from 'auto', 'data.table', 'arrow', or 'duckdb'.")
}
