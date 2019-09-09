require('dplyr')

#input_table <- dataframe, input
#DAY <- columname with date, should be string type
#PIVOT <- pivot column name
#INSTANCE_ID <- column name with instance id
#IMPORTANT <- column name with flag whether an instance is important
#Important_Flag <- value inside IMPORTANT column that whether an instance is important
#random comment
input_table$DAY <- as.Date(input_table$DAY,"%Y-%m-%d", tz="GMT")
pivot_holder <- 'PIVOT'
pivot_cols <- strsplit(gsub(', ', ',',pivot_holder), ',')[[1]]
input_table[,pivot_cols] <- as.data.frame(sapply(input_table[,pivot_cols], function(y) gsub('"', '', y)))


earliest_time <- min(input_table$DAY, na.rm=TRUE)

latest_time <- max(input_table$DAY, na.rm=TRUE)

instances <- length(unique(input_table$INSTANCE_ID))

important_subset <- input_table %>% filter(IMPORTANT=='Important_Flag')

valuable_instances <- length(unique(important_subset$INSTANCE_ID))

matrix_size <- as.numeric(latest_time - earliest_time)*(valuable_instances)

all_matrix_size <- as.numeric(latest_time - earliest_time)*(instances)



overall_length <- input_table %>%
  group_by(PIVOT) %>%
  summarize(
    hits_overall=sum(as.numeric(HITS)),
    num_days_overall = length(unique(DAY)),
    num_instances_overall=length(unique(INSTANCE_ID))
  )


important_length <- important_subset %>%
  group_by(PIVOT)%>%
  summarize(
    hits = sum(as.numeric(HITS)),
    num_days = length(unique(DAY)),
    num_instances = length(unique(INSTANCE_ID))
  )

full_length = merge(important_length, overall_length, by=c(pivot_cols), all.x=TRUE, all.y=TRUE)
full_length$hits = coalesce(as.numeric(full_length$hits), as.numeric(0))
full_length$hits_overall = coalesce(as.numeric(full_length$hits_overall), as.numeric(0))
full_length$num_days = coalesce(as.integer(full_length$num_days), as.integer(0))
full_length$num_days_overall = coalesce(as.integer(full_length$num_days_overall),as.integer(0))
full_length$num_instances = coalesce(as.integer(full_length$num_instances), as.integer(0))
full_length$num_instances_overall = coalesce(as.integer(full_length$num_instances_overall), as.integer(0))
full_length$percentage_of_hits = full_length$hits_overall/all_matrix_size
full_length$percentage_of_important_hits = full_length$hits/matrix_size
full_length$matrix_size = matrix_size
full_length$overall_matrix_size = all_matrix_size
return_value <- full_length[c(pivot_cols, 'hits', 'hits_overall', 'num_days', 'num_days_overall', 'num_instances', 'num_instances_overall', 'percentage_of_hits','percentage_of_important_hits', 'matrix_size', 'overall_matrix_size')]


