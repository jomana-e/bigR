#' Write a large dataset to a CSV efficiently
#'
#' @param data A dataframe to write
#' @param file Path to save the file
#' @param backend Choose backend: "auto", "data.table", "arrow", "duckdb"
#' @param chunk_size Optional chunk size for writing in parts
#' @examples
#' \dontrun{
#' # Basic usage with automatic backend selection
#' big_write_csv(data, "output.csv")
#'
#' # Using data.table for fast writing
#' big_write_csv(data, "output.csv", backend = "data.table")
#'
#' # Writing large dataset in chunks
#' big_write_csv(large_data, "output.csv", chunk_size = 10000)
#'
#' # Writing processed data
#' data %>%
#'   big_filter(value > 0) %>%
#'   big_mutate(log_value = log(value)) %>%
#'   big_write_csv("processed_data.csv")
#'
#' # Using Arrow backend for compression
#' big_write_csv(data, "compressed.csv.gz", backend = "arrow")
#' }
#' @export
big_write_csv <- function(data, file, backend = "auto", chunk_size = NULL) {
  if (backend == "auto") backend <- "data.table"  # Default to fastest method

  if (backend == "readr") {
    readr::write_csv(data, file)
  } else if (backend == "data.table") {
    data.table::fwrite(data, file)
  } else if (backend == "arrow") {
    arrow::write_csv_arrow(data, file)
  } else {
    stop("Invalid backend.")
  }
}
