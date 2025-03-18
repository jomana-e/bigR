#' Parallelized version of summarize()
#'
#' @param data A dataframe
#' @param ... Summarization expressions
#' @return Summarized dataframe
#' @export
big_parallel_summarize <- function(data, ..., n_cores = parallel::detectCores()) {
  big_parallel_apply(data, function(df) dplyr::summarize(df, ...), n_cores)
}
