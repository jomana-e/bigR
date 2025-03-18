test_that("big_memory_usage reports correct memory size", {
  df <- mtcars
  mem_size <- big_memory_usage(df)

  expect_true(is.numeric(mem_size))
  expect_true(mem_size > 0)
})
