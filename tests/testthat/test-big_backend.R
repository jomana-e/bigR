test_that("big_backend sets and gets the correct backend", {
  big_backend("data.table")
  expect_equal(big_backend(), "data.table")

  big_backend("duckdb")
  expect_equal(big_backend(), "duckdb")
})
