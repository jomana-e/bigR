#' Write a large dataset to a CSV efficiently
#'
#' @param data A dataframe to write
#' @param file Path to save the file
#' @param backend Choose backend: "auto", "data.table", "arrow", "duckdb"
#' @param chunk_size Optional chunk size for writing in parts
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
