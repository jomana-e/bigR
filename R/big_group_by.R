#' Group a large dataset efficiently
#'
#' @param data A dataframe or DuckDB connection
#' @param ... Columns to group by
#' @return A grouped dataframe or DuckDB query result
#' @examples
#' \dontrun{
#' # Basic grouping
#' result <- data %>%
#'   big_group_by(category) %>%
#'   big_summarize(
#'     count = n(),
#'     avg_value = mean(value)
#'   )
#'
#' # Multiple grouping variables
#' result <- data %>%
#'   big_group_by(category, year, month) %>%
#'   big_summarize(
#'     total = sum(value),
#'     count = n()
#'   )
#'
#' # Using with DuckDB connection
#' con <- DBI::dbConnect(duckdb::duckdb())
#' result <- con %>%
#'   big_group_by("category", "EXTRACT(year FROM date)") %>%
#'   big_summarize("SUM(value) as total")
#'
#' # Combining with other operations
#' result <- data %>%
#'   big_filter(value > 0) %>%
#'   big_group_by(category) %>%
#'   big_summarize(
#'     mean_val = mean(value),
#'     sd_val = sd(value)
#'   )
#' }
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
