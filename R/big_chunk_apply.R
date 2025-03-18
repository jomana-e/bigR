#' Apply a function to a large dataset in chunks
#'
#' @param file Path to the CSV file
#' @param FUN Function to apply to each chunk
#' @param chunk_size Number of rows per chunk
#' @return Combined result after applying the function
#' @examples
#' \dontrun{
#' # Simple transformation on chunks
#' result <- big_chunk_apply("large_data.csv",
#'   FUN = function(chunk) {
#'     chunk %>%
#'       dplyr::mutate(new_col = value * 2)
#'   }
#' )
#'
#' # Process chunks with custom chunk size
#' result <- big_chunk_apply("large_data.csv",
#'   FUN = function(chunk) {
#'     chunk %>%
#'       dplyr::filter(value > 0) %>%
#'       dplyr::mutate(log_value = log(value))
#'   },
#'   chunk_size = 5000
#' )
#'
#' # Complex chunk processing with aggregation
#' result <- big_chunk_apply("large_data.csv",
#'   FUN = function(chunk) {
#'     chunk %>%
#'       dplyr::group_by(category) %>%
#'       dplyr::summarize(
#'         mean_val = mean(value),
#'         count = n()
#'       )
#'   }
#' )
#' }
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
