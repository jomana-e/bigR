#' Mutate columns in a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Mutate expressions
#' @return The mutated dataset
#' @examples
#' \dontrun{
#' # Basic mutation
#' result <- big_mutate(data, new_col = col1 * 2)
#'
#' # Multiple mutations
#' result <- big_mutate(data,
#'   new_col1 = col1 * 2,
#'   new_col2 = col2 + 10,
#'   ratio = col1 / col2
#' )
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' result <- big_mutate(con, transformed = log(value))
#' }
#' @export
big_mutate <- function(data, ...) {
  if (inherits(data, "duckdb_connection")) {
    mutate_exprs <- rlang::expr_text(rlang::quos(...)[[1]])
    query <- sprintf("SELECT *, %s FROM temp_table", mutate_exprs)
    return(DBI::dbGetQuery(data, query))
  } else {
    return(dplyr::mutate(data, ...))
  }
}
