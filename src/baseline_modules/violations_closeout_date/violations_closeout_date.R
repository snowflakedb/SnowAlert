#Regression for Violations
#Expected:
# input = "SELECT QUERY_ID, sample_result:TITLE as TITLE, coalesce(sample_result:\"ENVIRONMENT\":\"fix_parameters\":\"deployment\",sample_result:\"ENVIRONMENT\":\"deployment\")='Prod' as prod,* FROM SNOWALERT.DATA.VIOLATIONS_ACTIVE_DAYS_BREAKDOWN_CALCULATION WHERE current_day >='2019-06-10'"
#FINAL
#NEW
#CURRENT_DAY
#PROD
#QUERY_ID
#UNIQUE_KEYS
#CREATE TABLE SNOWALERT.DATA.closeout_VIOLATION_QUERY_ID_BASELINE (QUERY_ID VARCHAR, TITLE VARCHAR, UNKNOWN_END VARCHAR, CLOSEOUT_DATE VARCHAR) comment = '---
# log source: SNOWALERT.data.HOLDER_FOR_PREDICTION
# required values:
  # NEW: NEW
  # FINAL: FINAL
  # CURRENT_DAY: CURRENT_DAY
  # PROD: PROD
  # QUERY_ID: QUERY_ID
  # UNIQUE_KEYS: UNIQUE_KEYS
  # TITLE: TITLE
# module name: violations_closeout_date
# history: CURRENT_DAY
# filter: 30
# '


    require(dplyr)
    require(tidyverse)
    require(broom)
    require(MASS)

a <- input_table
rm(input_table)

print('a')
a$CURRENT_DAY <- a$CURRENT_DAY <- as.Date(as.POSIXct(a$CURRENT_DAY), format='%Y-%m-%d')
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
print(c)
#Group for name
c <- base::merge(c, namessss, by = "QUERY_ID", all.x=TRUE)
print(unique(c$QUERY_ID))
print(unique(c$CURRENT_DAY))
print(unique(c$age))
print(unique(c$counts))
#Do the prediction analysis
model <- c %>% nest(-QUERY_ID) %>% 
  mutate(
    fit=map(data, ~ rlm(counts ~ CURRENT_DAY, weights=1/age^2, data = ., na.action = 'na.omit', maxit=100)) ) 
print('model_complete')
e <- c %>% 
  tidyr::complete(CURRENT_DAY=seq.Date(min(c$CURRENT_DAY), max(c$CURRENT_DAY)+100, by="day"),QUERY_ID)
e$age = as.integer(max(e$CURRENT_DAY) - e$CURRENT_DAY+1)
nested <- e %>% nest(-QUERY_ID)


prediction <- 
  model %>%
  inner_join(nested, by = "QUERY_ID") %>% 
  mutate(results=map2(.x = model$fit, .y = nested$data, .f = ~augment(.x, newdata = .y), .id=.x), model2=model$fit) %>% 
  unnest(c(results))

prediction <- base::merge(prediction, namessss, by = "QUERY_ID", all.x=TRUE)

prediction <- base::merge(prediction, dplyr::select(model, QUERY_ID, fit), by = "QUERY_ID", all.x=TRUE)
prediction$near_zero <- abs(prediction$.fitted)

return_value <- prediction %>% group_by(QUERY_ID) %>% summarise(last_day=max(CURRENT_DAY), x_intercept=as.character(CURRENT_DAY[which.min(near_zero)]) , unknown=as.character(x_intercept==last_day), value=min(near_zero), TITLE=first(TITLE.y)) %>% dplyr::select(QUERY_ID, TITLE, unknown, x_intercept) 

