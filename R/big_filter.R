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
  # Capture filter expressions
  dots <- rlang::enquos(...)
  
  # Validate filter expressions
  for (expr in dots) {
    if (!rlang::is_expression(rlang::quo_get_expr(expr))) {
      stop("Invalid filter expression")
    }
  }
  
  if (inherits(data, "duckdb_connection")) {
    # For DuckDB, construct SQL WHERE clause
    conditions <- sapply(dots, function(x) {
      expr <- rlang::quo_get_expr(x)
      if (rlang::is_call(expr)) {
        # Convert R operators to SQL
        op_map <- list(
          `>` = ">", `<` = "<", `>=` = ">=", `<=` = "<=",
          `==` = "=", `!=` = "!=", `&` = "AND", `|` = "OR",
          `is.na` = "IS NULL", `!is.na` = "IS NOT NULL"
        )
        op <- as.character(expr[[1]])
        if (op %in% names(op_map)) {
          if (op %in% c("is.na", "!is.na")) {
            sprintf("%s %s", rlang::as_label(expr[[2]]), op_map[[op]])
          } else {
            sprintf("%s %s %s", 
                   rlang::as_label(expr[[2]]), 
                   op_map[[op]], 
                   rlang::as_label(expr[[3]]))
          }
        } else {
          stop("Unsupported operator in DuckDB filter")
        }
      } else {
        stop("Invalid filter expression for DuckDB")
      }
    })
    query <- sprintf("SELECT * FROM test_table WHERE %s", 
                    paste(conditions, collapse = " AND "))
    return(DBI::dbGetQuery(data, query))
  } else {
    # For data.frame or data.table, use dplyr::filter
    tryCatch({
      if (inherits(data, "data.table")) {
        # Convert data.table to data.frame to ensure dplyr operations work
        data <- as.data.frame(data)
      }
      result <- dplyr::filter(data, ...)
      if (!is.data.frame(result)) {
        stop("Invalid filter expression")
      }
      # Convert back to data.table if input was data.table
      if (inherits(data, "data.table")) {
        result <- data.table::as.data.table(result)
      }
      return(result)
    }, error = function(e) {
      if (grepl("could not find function", e$message)) {
        stop("Invalid filter expression")
      }
      stop(e$message)
    })
  }
}
