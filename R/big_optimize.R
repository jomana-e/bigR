#' Suggest optimizations based on dataset size
#'
#' @importFrom utils object.size
#'
#' @param data A dataframe
#' @return Suggested optimization
#' @export
big_optimize <- function(data) {
  mem_usage <- object.size(data) / (1024^2) # Convert bytes to MB
  if (mem_usage < 100) {
    return("Data is small; use dplyr.")
  } else if (mem_usage < 1000) {
    return("Consider using data.table.")
  } else {
    return("Use DuckDB for best performance.")
  }
}
