% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/big_select.R
\name{big_select}
\alias{big_select}
\title{Select columns from a large dataset efficiently}
\usage{
big_select(data, ...)
}
\arguments{
\item{data}{A dataframe or a database connection}

\item{...}{Columns to select}
}
\value{
The selected columns
}
\description{
Select columns from a large dataset efficiently
}
\examples{
\dontrun{
# Basic column selection
result <- big_select(data, col1, col2, col3)

# Using column helpers
result <- big_select(data, starts_with("var_"))

# Renaming columns during selection
result <- big_select(data,
  new_name1 = old_name1,
  new_name2 = old_name2
)

# Combining with other bigR functions
result <- data \%>\%
  big_select(date, value, category) \%>\%
  big_filter(value > 0) \%>\%
  big_group_by(category)

# Using with DuckDB connection
con <- DBI::dbConnect(duckdb::duckdb())
result <- big_select(con, "date", "SUM(value) as total")
}
}
