require('RJDBC')
require('DBI')
require('config')

config <- config::get(file="dependencies_config.yml")

#Connection
.jinit()
conn <- dbConnect(config$jdbcDriver, config$connection_string)

#Search regex
grep_string <- readline(prompt="Enter search regex: ")
with_schema <- NULL
#Base data views that are affected
base_data <- dbGetQuery(conn, paste("SHOW VIEWS IN", config$base_data_location, sep=' '))
affected_base_data <- subset(base_data, grepl(grep_string, tolower(base_data$name)))
with_schema <- union(grep_string, tolower(paste(affected_base_data$database_name, affected_base_data$schema,affected_base_data$name, sep='.')))
#Data views
data <- dbGetQuery(conn, paste("SHOW VIEWS IN", config$data_location, sep=' '))
affected_data <- subset(data, grepl(paste('.*(',paste(with_schema, collapse='|'), ').*', sep=''), tolower(data$text)))
with_schema <- union(with_schema, paste(tolower(paste(affected_data$database_name, affected_data$schema,affected_data$name, sep='.'))))
with_schema <- with_schema[-1]
recursive_view_search <- function(with_schema, views){
  a <- subset(views, grepl(paste('.*(',paste(with_schema,collapse='|') ,').*', sep = ''), tolower(views$text)))
  new_schema = tolower(paste(a$database_name, a$schema, a$name, sep='.'))
  if(nrow(a) == length(with_schema)){
    return(with_schema)
  }
  else{
    return(union(with_schema,recursive_view_search(new_schema, views)))
    
  }
  
  
}
with_schema <- recursive_view_search(with_schema, data)

#Rules views
rules <- dbGetQuery(conn, paste("SHOW VIEWS IN", config$rules_location, sep=' '))
affected_rules <- subset(rules, grepl(paste('.*(',paste(with_schema,collapse='|') ,').*', sep = ''), tolower(rules$text)))
with_schema <- union(with_schema, tolower(paste(affected_rules$database_name, affected_rules$schema,affected_rules$name, sep='.')))

#Shares
schemas <- dbGetQuery(conn, paste("SHOW SCHEMAS IN", config$shares_location))
shares<-subset(schemas,grepl(config$share_identifier, tolower(schemas$name)))
views_in_shares <- do.call("rbind", lapply(paste("SHOW VIEWS IN", tolower(paste(shares$database,shares$name, sep='.')), sep=' '), function(x) dbGetQuery(conn, x)))
those_affected <- subset(views_in_shares, grepl(paste('.*(',paste(with_schema,collapse='|') ,').*', sep = ''), tolower(views_in_shares$text)))
with_schema <- union(with_schema, tolower(paste(those_affected$database_name, those_affected$schema, those_affected$name, sep='.')))

#Baselines
tables <- dbGetQuery(conn, paste("SHOW TABLES IN", config$tables_location))
baselines <- subset(tables, grepl(config$baseline_identifier, tolower(tables$name)))
those_affected <- subset(baselines, grepl(paste('.*(',paste(with_schema,collapse='|') ,')(.*|\\n)', sep = ''), tolower(baselines$comment)))
with_schema <- union(with_schema, tolower(paste(those_affected$database_name, those_affected$schema, those_affected$name, sep='.')))



#Pretty output
cat(paste(with_schema, collapse= ',\n'))


