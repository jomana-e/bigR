#' Parallelized version of summarize()
#'
#' @param data A dataframe
#' @param n_cores Number of CPU cores to use for parallel execution.
#' @param ... Summarization expressions
#' @return Summarized dataframe
#' @export
big_parallel_summarize <- function(data, ..., n_cores = parallel::detectCores()) {
  big_parallel_apply(data, function(df) dplyr::summarize(df, ...), n_cores)
}
