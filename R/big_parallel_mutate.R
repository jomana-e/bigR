#' Parallelized version of mutate()
#'
#' @param data A dataframe
#' @param n_cores Number of CPU cores to use for parallel execution.
#' @param ... Mutate expressions
#' @return Mutated dataframe
#' @examples
#' \dontrun{
#' # Basic parallel mutation
#' result <- big_parallel_mutate(data,
#'   new_col = expensive_calculation(value)
#' )
#'
#' # Multiple parallel mutations
#' result <- big_parallel_mutate(data,
#'   squared = value^2,
#'   logged = log(value),
#'   normalized = scale(value)
#' )
#'
#' # Specify number of cores
#' result <- big_parallel_mutate(data,
#'   complex_calc = expensive_function(col1, col2),
#'   n_cores = 4
#' )
#'
#' # Combining with other operations
#' result <- data %>%
#'   big_filter(value > 0) %>%
#'   big_parallel_mutate(
#'     log_value = log(value),
#'     z_score = scale(value)
#'   )
#' }
#' @export
big_parallel_mutate <- function(data, ..., n_cores = parallel::detectCores()) {
  big_parallel_apply(data, function(df) dplyr::mutate(df, ...), n_cores)
}
