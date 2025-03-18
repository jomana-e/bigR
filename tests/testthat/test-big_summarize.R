test_that("big_summarize calculates aggregations correctly", {
  df <- big_group_by(mtcars, cyl) %>% big_summarize(mean_mpg = mean(mpg))
  expect_true("mean_mpg" %in% names(df))
  expect_equal(nrow(df), length(unique(mtcars$cyl)))
})
