#' Apply a function to a dataset using parallel computing
#'
#' @param data A dataframe
#' @param FUN Function to apply
#' @param n_cores Number of CPU cores to use
#' @return Processed dataframe
#' @examples
#' \dontrun{
#' # Define a computationally expensive function
#' expensive_fun <- function(df) {
#'   df$result <- sapply(df$value, function(x) {
#'     Sys.sleep(0.1)  # Simulate expensive computation
#'     return(x^2)
#'   })
#'   return(df)
#' }
#'
#' # Apply the function in parallel
#' result <- big_parallel_apply(data, expensive_fun)
#'
#' # Specify number of cores explicitly
#' result <- big_parallel_apply(data, expensive_fun, n_cores = 4)
#'
#' # Complex data transformation
#' transform_fun <- function(df) {
#'   df %>%
#'     dplyr::mutate(
#'       scaled = scale(value),
#'       category = cut(value, breaks = 10)
#'     )
#' }
#' result <- big_parallel_apply(data, transform_fun)
#' }
#' @export
big_parallel_apply <- function(data, FUN, n_cores = parallel::detectCores()) {
  cl <- parallel::makeCluster(n_cores)
  parallel::clusterExport(cl, varlist = c("data", "FUN"), envir = environment())
  results <- parallel::parLapply(cl, split(data, seq(n_cores)), FUN)
  parallel::stopCluster(cl)
  return(dplyr::bind_rows(results))
}
