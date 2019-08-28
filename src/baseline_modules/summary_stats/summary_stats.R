library(testthat)
#Runs test which stop script if failure
test_dir(path = '.',  reporter="minimal", stop_on_failure=TRUE)
source('module.R')
return_value
