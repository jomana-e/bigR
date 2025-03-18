#' Set or get the preferred backend
#'
#' @param backend Set the backend ("data.table", "arrow", "duckdb")
#' @return The current backend
#' @export
big_backend <- function(backend = NULL) {
  if (!is.null(backend)) {
    options(bigR.backend = backend)
  }
  return(getOption("bigR.backend", "auto"))
}
