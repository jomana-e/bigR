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
    # Handle DuckDB connection
    sort_cols <- sapply(rlang::enquos(...), function(x) {
      if (rlang::quo_is_call(x) && rlang::call_name(x) == "desc") {
        paste(rlang::as_label(rlang::quo_get_expr(x)[[2]]), "DESC")
      } else {
        paste(rlang::as_label(x), "ASC")
      }
    })
    query <- sprintf("SELECT * FROM temp_table ORDER BY %s", paste(sort_cols, collapse = ", "))
    return(DBI::dbGetQuery(data, query))
  } else {
    # Create a copy of the data frame to avoid modifying the original
    df <- data.frame(data, stringsAsFactors = FALSE)
    
    # Store original NA positions and replace NAs with sentinel values
    na_positions <- list()
    for (col in names(df)) {
      if (any(is.na(df[[col]]))) {
        na_positions[[col]] <- which(is.na(df[[col]]))
        if (is.numeric(df[[col]])) {
          df[[col]][na_positions[[col]]] <- -Inf
        } else {
          df[[col]][na_positions[[col]]] <- ""
        }
      }
    }
    
    # Arrange the data
    result <- dplyr::arrange(df, ...)
    
    # Restore NA values
    for (col in names(na_positions)) {
      result[[col]][result[[col]] == -Inf | result[[col]] == ""] <- NA
    }
    
    return(result)
  }
}
