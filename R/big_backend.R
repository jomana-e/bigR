#' Set or get the preferred backend
#'
#' @param backend Set the backend ("data.table", "arrow", "duckdb")
#' @return The current backend
#' @examples
#' \dontrun{
#' # Get current backend
#' current <- big_backend()
#' print(current)
#'
#' # Set backend to data.table
#' big_backend("data.table")
#'
#' # Set backend to DuckDB for large datasets
#' big_backend("duckdb")
#'
#' # Set backend to Arrow for Parquet operations
#' big_backend("arrow")
#'
#' # Reset to automatic backend selection
#' big_backend("auto")
#' }
#' @export
big_backend <- function(backend = NULL) {
  valid_backends <- c("auto", "data.table", "arrow", "duckdb")
  
  if (!is.null(backend)) {
    if (!is.character(backend) || length(backend) != 1) {
      stop("Backend must be a single character string")
    }
    if (!backend %in% valid_backends) {
      stop(sprintf("Invalid backend. Must be one of: %s", paste(valid_backends, collapse = ", ")))
    }
    options(bigR.backend = backend)
  } else if (!missing(backend)) {
    stop("Backend cannot be NULL")
  }
  
  current <- getOption("bigR.backend", "auto")
  if (!current %in% valid_backends) {
    warning(sprintf("Current backend '%s' is invalid. Resetting to 'auto'", current))
    options(bigR.backend = "auto")
    current <- "auto"
  }
  
  return(current)
}

#' Collect results from a lazy operation
#'
#' @param x A lazy operation result
#' @return A materialized data frame
#' @export
collect <- function(x) {
  UseMethod("collect")
}

#' @export
collect.default <- function(x) {
  return(x)
}

#' @export
collect.tbl_lazy <- function(x) {
  dplyr::collect(x)
}

#' @export
collect.ArrowObject <- function(x) {
  arrow::as_data_frame(x)
}

#' @export
collect.data.table <- function(x) {
  as.data.frame(x)
}
