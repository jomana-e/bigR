#' Sort a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Columns to sort by
#' @return A sorted dataframe
#' @export
big_arrange <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    sort_cols <- rlang::expr_text(rlang::quos(...))
    query <- sprintf("SELECT * FROM temp_table ORDER BY %s", paste(sort_cols, collapse = ", "))
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::arrange(data, ...))
  }
}
