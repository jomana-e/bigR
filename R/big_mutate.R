#' Mutate columns in a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Mutate expressions
#' @return The mutated dataset
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
