require('dplyr')

#input_table <- dataframe, input
#DAY <- columname with date, should be string type
#PIVOT <- pivot column name
#INSTANCE_ID <- column name with instance id
#IMPORTANT <- column name with flag whether an instance is important
#Important_Flag <- value inside IMPORTANT column that whether an instance is important
#random comment

INPUT_TABLE <- input_table
print(paste('Length of input: ',length(input_table)))
print(paste('Type of input_table: ', typeof(input_table)))

INPUT_TABLE$DAY <- as.Date(INPUT_TABLE$DAY,"%Y-%m-%d", tz="GMT")

pivot_cols <- strsplit(gsub(', ', ',','PIVOT'), ',')[[1]]
INPUT_TABLE[,pivot_cols] <- lapply(INPUT_TABLE[,pivot_cols], function(y) gsub('"', '', y))


earliest_time <- min(INPUT_TABLE$DAY, na.rm=TRUE)
latest_time <- max(INPUT_TABLE$DAY, na.rm=TRUE)
instances <- length(unique(INPUT_TABLE$INSTANCE_ID))
                    
important_subset <- INPUT_TABLE %>% filter(IMPORTANT=='Important_Flag')
valuable_instances <- length(unique(important_subset$INSTANCE_ID))
matrix_size <- as.numeric(latest_time - earliest_time)*(valuable_instances)
all_matrix_size <- as.numeric(latest_time - earliest_time)*(instances)



overall_length <- INPUT_TABLE %>%
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
full_length$hits = coalesce(full_length$hits, as.numeric(0))
full_length$hits_overall = coalesce(full_length$hits_overall, as.numeric(0))
full_length$num_days = coalesce(full_length$num_days, as.numeric(0))
full_length$num_days_overall = coalesce(full_length$num_days_overall,as.numeric(0))
full_length$num_instances = coalesce(full_length$num_instances, as.numeric(0))
full_length$num_instances_overall = coalesce(full_length$num_instances_overall, as.numeric(0))
full_length$percentage_of_hits = full_length$hits_overall/all_matrix_size
full_length$percentage_of_important_hits = full_length$hits/matrix_size
full_length$matrix_size = matrix_size
full_length$overall_matrix_size = all_matrix_size

return_value <- full_length


