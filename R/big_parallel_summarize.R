#' Parallelized version of summarize()
#'
#' @param data A dataframe
#' @param n_cores Number of CPU cores to use for parallel execution.
#' @param ... Summarization expressions
#' @return Summarized dataframe
#' @examples
#' \dontrun{
#' # Basic parallel summarization
#' result <- big_parallel_summarize(data,
#'   mean_val = mean(value),
#'   sd_val = sd(value),
#'   count = n()
#' )
#'
#' # Complex calculations in parallel
#' result <- big_parallel_summarize(data,
#'   quantiles = quantile(value, probs = seq(0, 1, 0.1)),
#'   correlation = cor(value1, value2),
#'   custom_stat = expensive_stat_function(value)
#' )
#'
#' # Specify number of cores
#' result <- big_parallel_summarize(data,
#'   bootstrap_mean = mean(replicate(1000, mean(sample(value, replace = TRUE)))),
#'   n_cores = 4
#' )
#'
#' # Combining with other operations
#' result <- data %>%
#'   big_filter(value > 0) %>%
#'   big_group_by(category) %>%
#'   big_parallel_summarize(
#'     mean_val = mean(value),
#'     sd_val = sd(value),
#'     q95 = quantile(value, 0.95)
#'   )
#' }
#' @export
big_parallel_summarize <- function(data, ..., n_cores = parallel::detectCores()) {
  big_parallel_apply(data, function(df) dplyr::summarize(df, ...), n_cores)
}
