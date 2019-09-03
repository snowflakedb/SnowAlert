#Tests for closeout prediction baseline
library(stringr)
source('module.R')


test_that('input_data_sizes_correct', {
  expect_s3_class(input_table, "data.frame")
  expect_true(nrow(input_table) > 0)
})

test_that('expected_columns_present', {
  expect_true('CURRENT_DAY' %in% colnames(input_table))
  expect_true('NEW' %in% colnames(input_table))
  expect_true('FINAL' %in% colnames(input_table))
  expect_true('PROD' %in% colnames(input_table))
  expect_true('QUERY_ID' %in% colnames(input_table))
  expect_true('UNIQUE_KEYS' %in% colnames(input_table))
  expect_true('TITLE' %in% colnames(input_table))
})

test_that('No null values in Query Id, current_day and unique_keys',{
  expect_false(any(is.na(input_table$QUERY_ID)))
  expect_false(any(is.na(input_table$UNIQUE_KEYS)))
})

test_that('input convertable to appropriate data types', {
  expect_silent(as.Date(input_table$CURRENT_DAY,"%Y-%m-%d", tz="GMT"))
  expect_silent(as.character(input_table$UNIQUE_KEYS)) 
  expect_silent(as.logical(input_table$NEW))
  expect_silent(as.logical(input_table$FINAL))
  expect_silent(as.logical(input_table$PROD))
})

test_that('There is more than 10 days of data and at least 10 lines of entries', {
  expect_gte(nrow(input_table), 10)
  expect_gte(as.numeric(latest - oldest), 10)
})