#' Calculate memory usage of a dataset
#'
#' @importFrom utils object.size
#'
#' @param data A dataframe
#' @return Memory usage in MB
#' @export
big_memory_usage <- function(data) {
  return(object.size(data) / (1024^2)) # Convert bytes to MB
}
