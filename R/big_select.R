#' Select columns from a large dataset efficiently
#'
#' @param data A dataframe or a database connection
#' @param ... Columns to select
#' @return The selected columns
#' @examples
#' \dontrun{
#' # Basic column selection
#' result <- big_select(data, col1, col2, col3)
#'
#' # Using column helpers
#' result <- big_select(data, starts_with("var_"))
#'
#' # Renaming columns during selection
#' result <- big_select(data,
#'   new_name1 = old_name1,
#'   new_name2 = old_name2
#' )
#'
#' # Combining with other bigR functions
#' result <- data %>%
#'   big_select(date, value, category) %>%
#'   big_filter(value > 0) %>%
#'   big_group_by(category)
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' result <- big_select(con, "date", "SUM(value) as total")
#' }
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
