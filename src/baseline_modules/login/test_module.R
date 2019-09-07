#Tests for login baseline
library(stringr)
source('module.R')

test_that('input_data_sizes_correct', {
  expect_s3_class(input_table, "data.frame")
  expect_true(nrow(input_table)>0)
})

test_that('expected_columns_present', {
  expect_true('EVENT_TIME' %in% colnames(input_table))
  expect_true('DAY' %in% colnames(input_table))
  expect_true('USER_ID' %in% colnames(input_table))
  expect_true('LOGIN_STATUS' %in% colnames(input_table))
})

test_that('day and event time is convertable to date/time', {
  expect_silent(as.Date(input_table$DAY,"%Y-%m-%d", tz="GMT"))
  expect_silent(as.POSIXct(input_table$EVENT_TIME)) 
})

test_that('Pivots all exist', {
  expect_true(all(strsplit(gsub(', ', ',',pivot_holder), ',')[[1]] %in% colnames(input_table)))
  expect_true(str_trim(pivot_holder) == pivot_holder)
  expect_true(gsub('^,', '', pivot_holder)==pivot_holder) #no first commas
  expect_true(gsub(',$', '', pivot_holder) == pivot_holder) #no last commas
  })

test_that('There is more than 10 days of data and at least 10 lines of entries', {
  expect_gte(nrow(input_table), 10)
  expect_gte(as.numeric(num_days_total), 10)
})



