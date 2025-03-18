#' Summarize a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Summarization expressions
#' @return A summarized dataframe
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
