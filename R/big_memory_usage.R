#' Calculate memory usage of a dataset
#'
#' @importFrom utils object.size
#'
#' @param data A dataframe
#' @return Memory usage in MB
#' @examples
#' \dontrun{
#' # Check memory usage of a dataframe
#' df <- data.frame(
#'   x = rnorm(1000000),
#'   y = runif(1000000)
#' )
#' mem_usage <- big_memory_usage(df)
#' print(paste("Memory usage:", round(mem_usage, 2), "MB"))
#'
#' # Check memory after transformation
#' df_transformed <- df %>%
#'   dplyr::mutate(z = x * y)
#' new_mem_usage <- big_memory_usage(df_transformed)
#' print(paste("New memory usage:", round(new_mem_usage, 2), "MB"))
#' }
#' @export
big_memory_usage <- function(data) {
  return(object.size(data) / (1024^2)) # Convert bytes to MB
}
