#Tests for summary stats baseline
library(stringr)
source('module.R')

test_that('input_data_sizes_correct', {
  expect_s3_class(input_table, "data.frame")
  expect_true(nrow(input_table)>0)
  expect_s3_class(PIVOT, "character")
})

test_that('expected_columns_present', {
  expect_true('DAY' %in% colnames(input_table))
  expect_true('INSTANCE_ID' %in% colnames(input_table))
  expect_true('IMPORTANT' %in% colnames(input_table))
  expect_true('HITS' %in% colnames(input_table))
})

test_that('day is convertable to date', {
  expect_silent(as.Date(input_table$DAY,"%Y-%m-%d", tz="GMT")) 
})

test_that('Pivots all exist', {
  expect_true(all(strsplit(gsub(', ', ',',PIVOT), ',')[[1]] %in% colnames(input_table)))
})

test_that('Hits is numeric', {
  expect_true(all(is.numeric(input_table$HITS)))
})