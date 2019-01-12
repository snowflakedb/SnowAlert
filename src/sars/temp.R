#!/usr/bin/Rscript
require('dplyr')
results <- read.csv(file("stdin"),stringsAsFactors=FALSE)
aws_inventory <- read.csv(file="./aws_inventory.csv", header=TRUE, sep=",", stringsAsFactors=FALSE)
merged <- merge(results, aws_inventory, by='INSTANCE_ID', all.x=TRUE)
merged$DAY <- as.Date(merged$DAY,"%Y-%m-%d", tz="GMT")
merged$PROCESS <- gsub('"','',merged$PROCESS)
earliest_time <- min(merged$DAY, na.rm=TRUE)
latest_time <- max(merged$DAY, na.rm=TRUE)
instances <- length(unique(merged$INSTANCE_ID))
temp <- merged %>% filter(ROLE=='"nginx_fe"' | ROLE=='"GS"'|ROLE=='nginx_fe' | ROLE=='GS')
valuable_instances <- length(unique(temp$INSTANCE_ID))
matrix_size <- as.integer(latest_time - earliest_time)*(valuable_instances)
all_matrix_size <- as.integer(latest_time - earliest_time)*(instances)



overall_length <- merged %>% 
  group_by(PROCESS) %>%
  summarize(
    hits_overall=sum(HITS), 
    num_days_overall = length(unique(DAY)),
    num_instances_overall=length(unique(INSTANCE_ID))
    )

important_length <- merged %>%
  group_by(PROCESS)%>%
  summarize(
    hits = sum(HITS),
    num_days = length(unique(DAY)),
    num_instances = length(unique(INSTANCE_ID))
  )

full_length = merge(important_length, overall_length, by='PROCESS', all.x=TRUE, all.y=TRUE)
full_length$hits = coalesce(full_length$hits, as.integer(0))
full_length$hits_overall = coalesce(full_length$hits_overall, as.integer(0))
full_length$num_days = coalesce(full_length$num_days, as.integer(0))
full_length$num_days_overall = coalesce(full_length$num_days_overall,as.integer(0))
full_length$num_instances = coalesce(full_length$num_instances, as.integer(0))
full_length$num_instances_overall = coalesce(full_length$num_instances_overall, as.integer(0))
full_length$percentage_of_hits = full_length$hits_overall/all_matrix_size
full_length$percentage_of_important_hits = full_length$hits/matrix_size
full_length$matrix_size = matrix_size
full_length$overall_matrix_size = all_matrix_size
write.csv(full_length)


