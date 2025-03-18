#' Suggest optimizations based on dataset size
#'
#' @importFrom utils object.size
#'
#' @param data A dataframe
#' @return Suggested optimization
#' @examples
#' \dontrun{
#' # Small dataset
#' small_df <- data.frame(x = 1:1000)
#' suggestion <- big_optimize(small_df)
#' print(suggestion)  # Will suggest using dplyr
#'
#' # Medium dataset
#' medium_df <- data.frame(
#'   x = rnorm(100000),
#'   y = runif(100000)
#' )
#' suggestion <- big_optimize(medium_df)
#' print(suggestion)  # Will suggest using data.table
#'
#' # Large dataset
#' large_df <- data.frame(
#'   x = rnorm(1000000),
#'   y = runif(1000000),
#'   z = letters[1:1000000]
#' )
#' suggestion <- big_optimize(large_df)
#' print(suggestion)  # Will suggest using DuckDB
#' }
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
