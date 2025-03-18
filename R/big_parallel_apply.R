#' Apply a function to a dataset using parallel computing
#'
#' @param data A dataframe
#' @param FUN Function to apply
#' @param n_cores Number of CPU cores to use
#' @return Processed dataframe
#' @export
big_parallel_apply <- function(data, FUN, n_cores = parallel::detectCores()) {
  cl <- parallel::makeCluster(n_cores)
  parallel::clusterExport(cl, varlist = c("data", "FUN"), envir = environment())
  results <- parallel::parLapply(cl, split(data, seq(n_cores)), FUN)
  parallel::stopCluster(cl)
  return(dplyr::bind_rows(results))
}
