require('dplyr')
require(tidyr)
require(purrr)
#input_table
#EVENT_TIME OR DAY
#NUM_EVENTS -> OPTIONAL 
#PIVOT
#ID

get_percentiles <- function(dataframe, column_name, exit_name, column_names_summarize_by ){
  p <- c(.025,.05,.1, .25,.75,.95,.975,.5)
  p_names <- map_chr(p, ~paste0(ifelse(floor(.x*100)<.x*100, gsub(".*\\.","",toString(.x)),.x*100), exit_name))
  p_funs <- map(p, ~partial(quantile, probs=.x, na.rm=TRUE)) %>%
  set_names(nm=p_names)
  avg_funs <- map(1, ~partial(mean, probs=.x, na.rm=TRUE)) %>% set_names(nm=paste0('avg_', exit_name))
  number_of <- map(1, ~partial(sum, probs=.x, na.rm=TRUE)) %>% set_names(nm=paste0('_', exit_name))
  max_funs <- map(1, ~partial(max, probs=.x, na.rm=TRUE)) %>% set_names(nm=paste0('max_', exit_name))
  min_funs <- map(1, ~partial(min, probs=.x, na.rm=TRUE)) %>% set_names(nm=paste0('min_', exit_name))
  full_funs <- c(p_funs, avg_funs,number_of, max_funs, min_funs)
  
  return(dataframe %>% group_by_(column_name) %>% summarize_at(vars(column_names_summarize_by), full_funs))
}

results <- input_table
results$PIVOT = results$LANDING_TABLE
print(colnames(results))
if('EVENT_TIME' %in% colnames(results)) {
	print('Event time case triggered')
  results$EVENT_TIME <- as.POSIXct(results$EVENT_TIME) 
  results$DAY <-  as.Date(results$EVENT_TIME, na.rm=TRUE)
}else{
   print('No event time going to day')
  results$EVENT_TIME <- NA
  results$DAY <-  as.Date(INPUT_TABLE$DAY,"%Y-%m-%d", tz="GMT")

}

if(!('ID' %in% colnames(results))){
	print('No id in input')
  results$ID <- NA
}

if('NUM_EVENTS' %in% colnames(results)){
	print('Num events found')
  results$NUM_EVENTS <- as.integer(results$NUM_EVENTS)
  by_day_when_present <- results %>% 
    group_by(PIVOT, DAY)%>% 
    summarise(num_events=sum(NUM_EVENTS), num_ids=sum(ID))
}else{
	print('num events not found')
by_day_when_present <- results %>% 
  group_by(PIVOT, DAY) %>%
  summarise(num_events=n(),
            num_ids=length(unique(ID))
  )
}

print(by_day_when_present)
earliest_time <- min(results$DAY, na.rm=TRUE)
latest_time <- max(results$DAY, na.rm=TRUE)
num_days <- latest_time - earliest_time
expand_days <- by_day_when_present %>% tidyr::complete(DAY=seq.Date(min(DAY, na.rm=TRUE), max(DAY, na.rm=TRUE), by="day"), PIVOT, fill = list(num_events = 0, if('ID' %in% colnames(results)) num_ids=0 else num_ids=NA))

when_present_numeric<- get_percentiles(by_day_when_present, 'PIVOT', 'when_present', c('num_ids', 'num_events'))
expand_days_numeric <- get_percentiles(expand_days, 'PIVOT', 'overall', c('num_ids', 'num_events'))
when_present_date <- by_day_when_present %>%group_by(PIVOT)%>%summarise(earliest_when_present=min(DAY, na.rm=TRUE), latest_when_present=max(DAY, na.rm=TRUE), num_days=length(unique(DAY)))
expand_days_date <- expand_days %>%group_by(PIVOT)%>%summarise(earliest=min(DAY), latest=max(DAY), num_days_overall=length(unique(DAY)))
numerics <- merge(when_present_numeric, expand_days_numeric, by='PIVOT', all.x=TRUE, all.y=TRUE)
dates <- merge(when_present_date, expand_days_date, by='PIVOT', all.x=TRUE, all.y=TRUE)
full <- cbind(numerics, dates)
full$earliest_when_present <- as.character(full$earliest_when_present)
full$latest_when_present <- as.character(full$latest_when_present)
return_value <- full[c('PIVOT', 
                       'num_ids_025when_present','num_events_025when_present',
                       'num_ids_5when_present','num_events_5when_present',
                       'num_ids_10when_present','num_events_10when_present',
                       'num_ids_25when_present', 'num_events_25when_present', 
                       'num_ids_75when_present', 'num_events_75when_present',
                       'num_ids_95when_present', 'num_events_95when_present',
                       'num_ids_975when_present', 'num_events_975when_present',
                       'num_ids_50when_present', 'num_events_50when_present', 
                       'num_ids_avg_when_present', 'num_events_avg_when_present',
                       'num_ids__when_present', 'num_events__when_present',
                       'num_ids_max_when_present', 'num_events_max_when_present',
                       'num_ids_min_when_present', 'num_events_min_when_present',
                       'num_ids_025overall','num_events_025overall',
                       'num_ids_5overall','num_events_5overall',
                       'num_ids_10overall','num_events_10overall',
                       'num_ids_25overall', 'num_events_25overall',
                       'num_ids_75overall', 'num_events_75overall',
                       'num_ids_95overall', 'num_events_95overall',
                       'num_ids_975overall', 'num_events_975overall',
                       'num_ids__overall', 'num_events__overall',
                       'num_ids_max_overall', 'num_events_max_overall',
                       'num_ids_min_overall', 'num_events_min_overall',
                       'num_ids_50overall', 'num_events_50overall',
                       'num_ids_avg_overall', 'num_events_avg_overall',
                       'earliest_when_present', 'latest_when_present',
                       'num_days', 'num_days_overall'
)
]
