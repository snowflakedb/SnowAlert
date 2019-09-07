require('dplyr')
pivot_holder <- "PIVOT"
num_days_total <- length(unique(input_table$DAY))
grouped <- input_table %>% 
  group_by(PIVOT) %>% 
  summarize(num_logins=length(EVENT_TIME), num_unique_users=length(unique(USER_ID)), num_successful_logins=length(which(LOGIN_STATUS=='Success')), num_days=length(unique(DAY)), percent_of_days=num_days/num_days_total)
grouped$average_per_day_when_active = grouped$num_successful_logins/grouped$num_days
grouped$average_per_day_overall = grouped$num_successful_logins/num_days_total
return_value <- grouped
