#Regression for Violations
#Expected:
# input = "SELECT QUERY_ID, sample_result:TITLE as TITLE, coalesce(sample_result:\"ENVIRONMENT\":\"fix_parameters\":\"deployment\",sample_result:\"ENVIRONMENT\":\"deployment\")='Prod' as prod,* FROM SNOWALERT.DATA.VIOLATIONS_ACTIVE_DAYS_BREAKDOWN_CALCULATION WHERE current_day >='2019-06-10'"
#FINAL
#NEW
#CURRENT_DAY
#PROD
#QUERY_ID
#UNIQUE_KEYS
#CREATE TABLE SNOWALERT.DATA.PREDICT_VIOLATION_QUERY_ID_BASELINE (QUERY_ID VARCHAR, TITLE VARCHAR, CURRENT_DAY VARCHAR, ACTUAL_COUNTS VARCHAR, FITTED_COUNTS VARCHAR, FIT VARCHAR, MODEL VARCHAR) comment = '---
# log source: SNOWALERT.DATA.VIOLATIONS_PREDICTION_SUBSET
# required values:
  # NEW: NEW
  # FINAL: FINAL
  # CURRENT_DAY: CURRENT_DAY
  # PROD: PROD
  # QUERY_ID: QUERY_ID
  # UNIQUE_KEYS: UNIQUE_KEYS
  # TITLE: TITLE
# module name: violations_linear_prediction
# history: CURRENT_DAY
# filter: 30
# '

require(dplyr)
require(broom)
require(MASS)
require(tidyr)
require(purrr)

a <- input_table
a$CURRENT_DAY <- as.Date(a$CURRENT_DAY, format='%Y-%m-%d')
oldest <- min(a$CURRENT_DAY)
latest <- max(a$CURRENT_DAY)
a$FINAL <- as.logical(a$FINAL)
a$NEW <- as.logical(a$NEW)
a$PROD <- as.logical(a$PROD)
colnames(a) <- make.unique(names(a))
#Group for counts
b <- a %>% group_by(QUERY_ID, CURRENT_DAY) %>% dplyr::summarize(counts=n_distinct(UNIQUE_KEYS))
namessss <- a %>% group_by(QUERY_ID) %>% dplyr::summarize(TITLE=first(TITLE))

rm(a)
#Complete the missing values with zero -> no violations
c <- b %>% 
  tidyr::complete(CURRENT_DAY=seq.Date(min(b$CURRENT_DAY), max(b$CURRENT_DAY), by="day"),QUERY_ID, fill=list(counts = 0))
c$age = as.integer(Sys.Date() - c$CURRENT_DAY+2)
rm(b)

#Group for name
c <- base::merge(c, namessss, by = "QUERY_ID", all.x=TRUE)

#Do the prediction analysis
model <- c %>% tidyr::nest(-QUERY_ID) %>% 
  mutate(
    fit=map(data, ~ rlm(counts ~ CURRENT_DAY, conf.int=TRUE,  weights=1/age^2, data = ., na.action = 'na.omit', maxit=100)) ) 

e <- c %>% 
  tidyr::complete(CURRENT_DAY=seq.Date(min(c$CURRENT_DAY), max(c$CURRENT_DAY)+100, by="day"),QUERY_ID)
e$age = as.integer(max(e$CURRENT_DAY) - e$CURRENT_DAY+1)
nested <- e %>% tidyr::nest(-QUERY_ID)

prediction <- 
  model %>%
  inner_join(nested, by = "QUERY_ID") %>% 
  mutate(results=map2(.x = model$fit, .y = nested$data, .f = ~augment(.x, newdata = .y), .id=.x), model2=model$fit) %>% 
  unnest(c(results))
prediction$confidence_low <- prediction$.fitted-prediction$.se.fit
prediction$confidence_high <- prediction$.fitted+prediction$.se.fit
prediction <- base::merge(prediction, namessss, by = "QUERY_ID", all.x=TRUE)
prediction <- base::merge(prediction, dplyr::select(model, QUERY_ID, fit), by = "QUERY_ID", all.x=TRUE)

prediction$fit <- as.character(prediction$fit)


return_value <- dplyr::select(prediction, QUERY_ID, TITLE.y, CURRENT_DAY, counts, .fitted, .se.fit, fit, confidence_low, confidence_high)
return_value$CURRENT_DAY <- as.character(return_value$CURRENT_DAY)
return_value <- return_value %>% replace(., is.na(.), "")
colnames(return_value) <- c('QUERY_ID', 'TITLE', 'CURRENT_DAY', 'COUNTS', 'FITTED', 'SEFIT', 'FIT', 'CONFIDENCE_LOW', 'CONFIDENCE_HIGH')

return_value
#END
