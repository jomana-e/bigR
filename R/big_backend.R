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
  if (!is.null(backend)) {
    options(bigR.backend = backend)
  }
  return(getOption("bigR.backend", "auto"))
}
