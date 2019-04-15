require('dplyr')
results <- input_table
num_days_total <- length(unique(results$day))
grouped <- results %>% group_by(pivot) %>% summarize(num_logins=length(event_time), num_unique_users=length(unique(user_id)), num_successful_logins=length(which(login_status=='Success')), num_days=length(unique(day)), percent_of_days=num_days/num_days_total)
grouped$average_per_day_when_active = grouped$num_successful_logins/grouped$num_days
grouped$average_per_day_overall = grouped$num_successful_logins/num_days_total
return_value <- grouped
