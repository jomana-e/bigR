#' Map a function over dataset chunks and write results to a file
#'
#' @param file Path to the input CSV file
#' @param FUN Function to apply to each chunk
#' @param chunk_size Number of rows per chunk
#' @param output_file Path to save the transformed file
#' @export
big_chunk_map <- function(file, FUN, chunk_size = 10000, output_file) {
  df <- readr::read_csv(file, col_names = TRUE, progress = FALSE)  # Read entire dataset
  results <- list()

  for (i in seq(1, nrow(df), by = chunk_size)) {
    chunk <- df[i:min(i + chunk_size - 1, nrow(df)), , drop = FALSE]
    processed <- FUN(chunk)
    results <- append(results, list(processed))
  }

  # Write results to output file
  readr::write_csv(dplyr::bind_rows(results), output_file)
}
