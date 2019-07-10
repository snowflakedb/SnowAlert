#Regression for Violations
#Expected:
# input = "SELECT QUERY_ID, sample_result:TITLE as TITLE, coalesce(sample_result:\"ENVIRONMENT\":\"fix_parameters\":\"deployment\",sample_result:\"ENVIRONMENT\":\"deployment\")='Prod' as prod,* FROM SNOWALERT.DATA.VIOLATIONS_ACTIVE_DAYS_BREAKDOWN_CALCULATION WHERE current_day >='2019-06-10'"
#FINAL
#NEW
#CURRENT_DAY
#PROD
#QUERY_ID
#UNIQUE_KEYS

suppressWarnings(
  suppressMessages(
    c(
      require(dplyr),
      require(tidyverse),
      require(broom),
      require(MASS)
    )
  )
)

a <- input

#Cleaning
a$CURRENT_DAY <- a$CURRENT_DAY <- as.Date(as.POSIXct(a$CURRENT_DAY), format='%Y-%m-%d')
a$FINAL <- as.logical(a$FINAL)
a$NEW <- as.logical(a$NEW)
a$PROD <- as.logical(a$PROD)
colnames(a) <- make.unique(names(a))

#Group for counts
b <- a %>% group_by(QUERY_ID, CURRENT_DAY) %>%
  dplyr::summarize(counts=n_distinct(UNIQUE_KEYS))

#Complete the missing values with zero -> no violations
c <- b %>% 
  tidyr::complete(CURRENT_DAY=seq.Date(min(b$CURRENT_DAY), max(b$CURRENT_DAY), by="day"),QUERY_ID, fill=list(counts = 0))
c$age = as.integer(Sys.Date() - c$CURRENT_DAY+1)

#Group for name
names <- a %>% group_by(QUERY_ID) %>% dplyr::summarise(TITLE=first(TITLE))
c <- base::merge(c, names, by = "QUERY_ID", all.x=TRUE)

#Do the prediction analysis
model <- c %>% nest(-QUERY_ID) %>% 
  mutate(
    fit=map(data, ~ rlm(counts ~ CURRENT_DAY, weights=1/age^2, data = ., na.action = 'na.omit', maxit=100)) ) 

e <- c %>% 
  tidyr::complete(CURRENT_DAY=seq.Date(min(c$CURRENT_DAY), max(c$CURRENT_DAY)+100, by="day"),QUERY_ID)
e$age = as.integer(max(e$CURRENT_DAY) - e$CURRENT_DAY+1)
nested <- e %>% nest(-QUERY_ID)


prediction <- 
  model %>%
  inner_join(nested, by = "QUERY_ID") %>% 
  mutate(results=map2(.x = model$fit, .y = nested$data, .f = ~augment(.x, newdata = .y), .id=.x), model2=model$fit) %>% 
  unnest(c(results))

prediction <- base::merge(prediction, names, by = "QUERY_ID", all.x=TRUE)
prediction <- base::merge(prediction, dplyr::select(model, QUERY_ID, fit), by = "QUERY_ID", all.x=TRUE)

prediction$near_zero <- abs(prediction$.fitted)

stuff <- prediction %>% group_by(QUERY_ID) %>% summarise(last_day=max(CURRENT_DAY), res=CURRENT_DAY[which.min(near_zero)] , unknown=res==last_day, value=min(near_zero), TITLE=first(TITLE.y))

#END
