#' Select columns from a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Columns to select
#' @return The selected columns
#' @export
big_select <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    cols <- rlang::quos(...)
    query <- sprintf("SELECT %s FROM temp_table", paste0(cols, collapse = ", "))
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::select(data, ...))
  }
}
