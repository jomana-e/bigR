#' Sort a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Columns to sort by
#' @return A sorted dataframe
#' @examples
#' \dontrun{
#' # Basic sorting
#' result <- big_arrange(data, value)
#'
#' # Sort in descending order
#' result <- big_arrange(data, desc(value))
#'
#' # Multiple sort columns
#' result <- big_arrange(data,
#'   category,
#'   desc(value),
#'   date
#' )
#'
#' # Combining with other bigR functions
#' result <- data %>%
#'   big_filter(value > 0) %>%
#'   big_arrange(desc(value)) %>%
#'   big_select(date, value, category)
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' result <- big_arrange(con, "category ASC", "value DESC")
#' }
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
