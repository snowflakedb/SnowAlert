source('module.R')


test_that('input_data_sizes_correct', {
  expect_s3_class(input_table, "data.frame")
  expect_true(nrow(input_table)>0)
})

test_that('expected_columns_present', {
  expect_true('EVENT_TIME' %in% colnames(input_table) || 'DAY' %in% colnames(input_table))
  expect_true('PIVOT' %in% colnames(input_table))
})

test_that('day is convertable to date', {
  skip_if(!('DAY' %in% colnames(input_table)))
  expect_silent(as.Date(input_table$DAY,"%Y-%m-%d", tz="GMT")) 
})

test_that('event_time is convertable to date', {
  skip_if(!('EVENT_TIME' %in% colnames(input_table)))
  expect_silent(as.POSIXct(input_table$EVENT_TIME) ) 
})

test_that('num events is convertable to numeric', {
  skip_if(!('NUM_EVENTS' %in% colnames(input_table)))
  expect_silent(as.integer(input_table$NUM_EVENTS)) 
})

