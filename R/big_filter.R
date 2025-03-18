#' Filter rows from a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Filter conditions
#' @return The filtered dataset
#' @export
big_filter <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    filter_cond <- rlang::expr_text(rlang::quos(...)[[1]])
    query <- sprintf("SELECT * FROM temp_table WHERE %s", filter_cond)
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::filter(data, ...))
  }
}
