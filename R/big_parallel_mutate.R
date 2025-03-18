#' Parallelized version of mutate()
#'
#' @param data A dataframe
#' @param n_cores Number of CPU cores to use for parallel execution.
#' @param ... Mutate expressions
#' @return Mutated dataframe
#' @export
big_parallel_mutate <- function(data, ..., n_cores = parallel::detectCores()) {
  big_parallel_apply(data, function(df) dplyr::mutate(df, ...), n_cores)
}
