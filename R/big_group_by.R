#' Group a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Columns to group by
#' @return A grouped dataframe or DuckDB query result
#' @export
big_group_by <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    group_cols <- rlang::expr_text(rlang::quos(...))
    query <- sprintf("SELECT * FROM temp_table GROUP BY %s", paste(group_cols, collapse = ", "))
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::group_by(data, ...))
  }
}
