# bigR: Efficient Handling of Large Datasets in R

<!-- badges: start -->
[![R-CMD-check](https://github.com/jomanaabdelrahman/bigR/workflows/R-CMD-check/badge.svg)](https://github.com/jomanaabdelrahman/bigR/actions)
[![CRAN status](https://www.r-pkg.org/badges/version/bigR)](https://CRAN.R-project.org/package=bigR)
<!-- badges: end -->

## Overview

`bigR` is an R package designed to simplify working with large datasets by automatically selecting the most efficient backend for data processing. It provides a seamless interface that maintains `tidyverse`-friendly syntax while leveraging powerful out-of-core processing and parallel computation capabilities.

## Features

- **Automatic Backend Selection**: Intelligently chooses between different backends (`data.table`, `Arrow`, `DuckDB`) based on data size and operation type
- **Out-of-Core Processing**: Efficiently handles datasets larger than available RAM
- **Parallel Computation**: Automatic parallelization of supported operations
- **`tidyverse`-Compatible**: Uses familiar `dplyr`-style syntax
- **Optimized I/O Operations**: Fast reading and writing of CSV and Parquet files
- **Memory-Efficient**: Monitors and optimizes memory usage during operations

## Installation

You can install the released version of bigR from CRAN with:

```r
install.packages("bigR")
```

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("jomanaabdelrahman/bigR")
```

## Usage

### Basic Example

```r
library(bigR)

# Read a large CSV file efficiently
data <- big_read_csv("large_dataset.csv")

# Perform operations with automatic optimization
result <- data %>%
  big_select(col1, col2) %>%
  big_filter(col1 > 100) %>%
  big_mutate(new_col = col1 * col2) %>%
  big_summarize(mean = mean(new_col))
```

### Parallel Processing

```r
# Automatic parallel operations
parallel_result <- data %>%
  big_parallel_mutate(complex_calculation = expensive_function(col1)) %>%
  big_parallel_summarize(total = sum(complex_calculation))
```

### Chunked Operations

```r
# Process data in chunks
chunked_result <- data %>%
  big_chunk_apply(function(chunk) {
    # Custom operations on each chunk
    process_chunk(chunk)
  })
```

## Main Functions

- `big_read_csv()`, `big_write_csv()`: Efficient CSV I/O operations
- `big_read_parquet()`, `big_write_parquet()`: Fast Parquet file handling
- `big_select()`, `big_filter()`, `big_mutate()`: Data manipulation
- `big_group_by()`, `big_summarize()`: Data aggregation
- `big_parallel_mutate()`, `big_parallel_summarize()`: Parallel operations
- `big_chunk_apply()`, `big_chunk_map()`: Chunk-wise processing
- `big_optimize()`: Performance optimization utilities
- `big_memory_usage()`: Memory monitoring

## Performance Tips

1. Use Parquet format for persistent storage when possible
2. Leverage chunk-wise processing for memory-intensive operations
3. Enable parallel processing for computationally expensive tasks
4. Monitor memory usage with `big_memory_usage()`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use `bigR` in your research, please cite it as:

```r
citation("bigR")
```

## Authors

- **Jomana Abdelrahman** - Creator and maintainer of **bigR**    
  GitHub: [@jomanaabdelrahman](https://github.com/jomanaabdelrahman)
