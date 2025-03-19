#' Map a function over dataset chunks
#'
#' @param data A dataframe or path to a CSV file
#' @param FUN Function to apply to each chunk
#' @param chunk_size Number of rows per chunk
#' @param progress Whether to show a progress bar
#' @param parallel Whether to process chunks in parallel
#' @param output_file Optional path to save the transformed data
#' @return The transformed dataset
#' @examples
#' \dontrun{
#' # Process data frame in chunks
#' result <- big_chunk_map(df, function(chunk) {
#'   chunk$new_col <- chunk$value * 2
#'   return(chunk)
#' })
#'
#' # Process CSV file
#' result <- big_chunk_map("large_data.csv", function(chunk) {
#'   chunk$new_col <- chunk$value * 2
#'   return(chunk)
#' })
#'
#' # Use parallel processing
#' result <- big_chunk_map(df, 
#'   function(chunk) {
#'     chunk$new_col <- chunk$value * 2
#'     return(chunk)
#'   },
#'   parallel = TRUE
#' )
#'
#' # Show progress bar
#' result <- big_chunk_map(df,
#'   function(chunk) {
#'     chunk$new_col <- chunk$value * 2
#'     return(chunk)
#'   },
#'   progress = TRUE
#' )
#'
#' # Save results to file
#' big_chunk_map(df,
#'   function(chunk) {
#'     chunk$new_col <- chunk$value * 2
#'     return(chunk)
#'   },
#'   output_file = "transformed.csv"
#' )
#' }
#' @export
big_chunk_map <- function(data, FUN, chunk_size = 10000, progress = FALSE, parallel = FALSE, output_file = NULL) {
  # Handle input data
  if (is.character(data)) {
    df <- readr::read_csv(data, col_names = TRUE, progress = FALSE)
  } else {
    df <- data
  }
  
  # Validate inputs
  if (!is.data.frame(df)) {
    stop("Input must be a data frame or path to a CSV file")
  }
  if (nrow(df) == 0) {
    stop("Input data frame is empty")
  }
  if (chunk_size <= 0) {
    stop("Chunk size must be positive")
  }
  if (!is.function(FUN)) {
    stop("FUN must be a function")
  }
  
  # Create progress bar if requested
  if (isTRUE(progress)) {
    pb <- progress::progress_bar$new(
      total = ceiling(nrow(df) / chunk_size),
      format = "Processing [:bar] :percent eta: :eta"
    )
  }
  
  # Process chunks
  if (parallel) {
    # Parallel processing
    n_cores <- parallel::detectCores()
    cl <- parallel::makeCluster(n_cores)
    parallel::clusterExport(cl, c("FUN"), envir = environment())
    
    chunks <- split(df, ceiling(seq_len(nrow(df)) / chunk_size))
    results <- parallel::parLapply(cl, chunks, FUN)
    
    parallel::stopCluster(cl)
  } else {
    # Sequential processing
    results <- list()
    for (i in seq(1, nrow(df), by = chunk_size)) {
      chunk <- df[i:min(i + chunk_size - 1, nrow(df)), , drop = FALSE]
      processed <- FUN(chunk)
      results <- append(results, list(processed))
      
      if (isTRUE(progress)) pb$tick()
    }
  }
  
  # Handle different return types
  first_result <- results[[1]]
  if (is.data.frame(first_result)) {
    result <- dplyr::bind_rows(results)
  } else if (is.atomic(first_result)) {
    result <- unlist(results)
  } else {
    stop("Function must return a data frame or atomic vector")
  }
  
  # Write to file if requested
  if (!is.null(output_file)) {
    readr::write_csv(result, output_file)
  }
  
  return(result)
}
