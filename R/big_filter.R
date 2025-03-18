#' Filter rows from a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Filter conditions
#' @return The filtered dataset
#' @examples
#' \dontrun{
#' # Basic filtering
#' result <- big_filter(data, value > 100)
#'
#' # Multiple conditions
#' result <- big_filter(data,
#'   value > 100,
#'   category == "A",
#'   date >= as.Date("2023-01-01")
#' )
#'
#' # Combining with other bigR functions
#' result <- data %>%
#'   big_filter(value > 0) %>%
#'   big_mutate(log_value = log(value))
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' result <- big_filter(con, "value > 100 AND category = 'A'")
#' }
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
