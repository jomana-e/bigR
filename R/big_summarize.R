#' Summarize a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Summarization expressions
#' @return A summarized dataframe
#' @examples
#' \dontrun{
#' # Basic summary statistics
#' summary <- big_summarize(data,
#'   mean_val = mean(value),
#'   sd_val = sd(value),
#'   count = n()
#' )
#'
#' # Group-wise summary with big_group_by
#' summary <- data %>%
#'   big_group_by(category) %>%
#'   big_summarize(
#'     avg = mean(value),
#'     total = sum(value),
#'     count = n()
#'   )
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' summary <- big_summarize(con,
#'   "AVG(value) as mean_val",
#'   "COUNT(*) as count"
#' )
#' }
#' @export
big_summarize <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    summarization_exprs <- rlang::expr_text(rlang::quos(...))
    query <- sprintf("SELECT %s FROM temp_table", summarization_exprs)
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::summarize(data, ...))
  }
}
