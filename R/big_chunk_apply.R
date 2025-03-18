#' Apply a function to a large dataset in chunks
#'
#' @param file Path to the CSV file
#' @param FUN Function to apply to each chunk
#' @param chunk_size Number of rows per chunk
#' @return Combined result after applying the function
#' @export
big_chunk_apply <- function(file, FUN, chunk_size = 10000) {
  con <- readr::read_csv(file, col_names = TRUE, progress = FALSE) # Read CSV directly
  results <- list()

  for (i in seq(1, nrow(con), by = chunk_size)) {
    chunk <- con[i:min(i + chunk_size - 1, nrow(con)), , drop = FALSE]
    processed <- FUN(chunk)
    results <- append(results, list(processed))
  }

  return(dplyr::bind_rows(results))
}
