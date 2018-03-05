############################################################################-
############################################################################-
##
## NYC Subway GTFS Real Time Archive - Combining all monthly databases ----
##
############################################################################-
############################################################################-

#=========================#
#### Loading packages ####
#=========================#


library(magrittr)
library(dplyr)
library(purrr)
library(lubridate)
library(stringr)
library(iterators)
library(readr)
library(DBI)
library(dbplyr)
library(glue)

# setwd("D:/Files/BASP/R analyses/NYC/MTA/Subway/Subway Time/GTFS Files/2016-2017")


#=========================#
#### Setting up ####
#=========================#


subway_gtfs_2017_05 <- dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2017_05_db.sqlite3")


trip_updates_template <- 
    subway_gtfs_2017_05 %>% 
    tbl("trip_updates") %>% 
    head(0) %>% 
    collect()


vehicle_position_template <- 
    subway_gtfs_2017_05 %>% 
    tbl("vehicle_position") %>% 
    head(0) %>% 
    collect()

dbDisconnect(subway_gtfs_2017_05)

#==========================#
#### Folder structures ####
#==========================#

# base <- "D:/Files/BASP/R analyses/NYC/MTA/Subway/Subway Time/GTFS Files/2016-2017"
base <- "./2016-2017/"

file_names <- 
    list.files(
        base, 
        full.names = FALSE, 
        recursive = FALSE
    ) %>% 
    str_subset(".sqlite3")


#==========================#
#### Looping the loop ####
#==========================#


subway_gtfs_2016_2017 <- dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_db.sqlite3")


for (i in 1:length(file_names)) {
# for (i in 1:2) {
    
    i_trip_updates_res <- icount()
    
    subway_gtfs_res_db <- dbConnect(RSQLite::SQLite(), glue("./2016-2017/{file_names[i]}"))
    
    
    trip_updates_res <-
        dbSendQuery(
            conn = subway_gtfs_res_db, 
            statement = "SELECT * FROM trip_updates")
    
    
    while (!dbHasCompleted(trip_updates_res)) {
        
        cat("trip_updates_res: ", nextElem(i_trip_updates_res), ", ", sep = "")
        
        trip_updates_chunk <- dbFetch(trip_updates_res, n = 100000)
        
        dbWriteTable(
            subway_gtfs_2016_2017,
            "trip_updates",
            value = trip_updates_chunk,
            append = TRUE,
            temporary = FALSE
        )
    }
    
    cat("\n")
    cat("\n")
    
    i_vehicle_position_res <- icount()
    
    vehicle_position_res <-
        dbSendQuery(
            conn = subway_gtfs_res_db, 
            statement = "SELECT * FROM vehicle_position")
        
    cat("\n-------------------------------\n")
    
    while (!dbHasCompleted(vehicle_position_res)) {
        
        cat("vehicle_position_res: ", nextElem(i_trip_updates_res), ", ", sep = "")
        
        vehicle_position_chunk <- dbFetch(vehicle_position_res, n = 100000)
        
        dbWriteTable(
            subway_gtfs_2016_2017,
            "vehicle_position",
            value = vehicle_position_chunk,
            append = TRUE,
            temporary = FALSE
        )
        
    }
    
    cat("\n-------------------------------\n")
    
    cat("\n")
    
    # rm(trip_updates_chunk)
    # rm(vehicle_position_chunk)
    gc()
    
    dbDisconnect(subway_gtfs_res_db)
    
}

#==========================#
#### Creating indexes 
#==========================#

db_create_index(subway_gtfs_2016_2017, "trip_updates", "trip_id")
db_create_index(subway_gtfs_2016_2017, "trip_updates", "route_id")
db_create_index(subway_gtfs_2016_2017, "trip_updates", "stop_sequence")
db_create_index(subway_gtfs_2016_2017, "trip_updates", "stop_id")

db_create_index(subway_gtfs_2016_2017, "vehicle_position", "trip_id")
db_create_index(subway_gtfs_2016_2017, "vehicle_position", "route_id")
db_create_index(subway_gtfs_2016_2017, "vehicle_position", "vehicle_id")

###########################################################################################
###########################################################################################

n <- 10000000

# arrival_delay <-
#     tbl(subway_gtfs_2016_2017, "trip_updates") %>% head(n) %>% pull(arrival_delay)
# 
# start_date <-
#     tbl(subway_gtfs_2016_2017, "trip_updates") %>% head(n) %>% pull(start_date)

random_1 <- runif(n, 0, 9) %>% round() #%>% as.integer()
random_2 <- runif(n, 1000000, 9999999) %>% round() #%>% as.integer()

###########################################################################################
###########################################################################################

gdata::object.size(random_1)
gdata::object.size(random_2)

###########################################################################################
###########################################################################################

rows <- integer(0L)

trip_updates_res <-
    dbSendQuery(
        conn = subway_gtfs_2016_2017, 
        statement = "SELECT arrival_delay FROM trip_updates")


i_trip_updates_res <- icount()

while (!dbHasCompleted(trip_updates_res)) {

    cat(nextElem(i_trip_updates_res), ", ", sep = "")
    
    trip_updates_chunk <- dbFetch(trip_updates_res, n = 1e+6)
    
    rows <- append(rows, nrow(trip_updates_chunk))
    
    gc()

}

# write_lines(nrow(trip_updates_chunk), "rows.csv", append = TRUE)


dbClearResult(trip_updates_res)

###########################################################################################
###########################################################################################

n <- 1e6

trip_updates_1e6 <- 
    tbl(subway_gtfs_2016_2017, "trip_updates") %>% 
    head(n) %>% 
    collect()

trip_updates_1e6_2 <- distinct(trip_updates_1e6)

###########################################################################################
###########################################################################################


subway_gtfs_2016_2017 <-
    dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_db.sqlite3")


subway_gtfs_2016_2017_distinct <-
    dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_distinct_db.sqlite3")


###########################################################################################
###########################################################################################


trip_updates_res <-
    dbSendQuery(
        conn = subway_gtfs_2016_2017,
        statement = "SELECT * FROM trip_updates")

rows          <- integer(0L)
rows_distinct <- integer(0L)

i_trip_updates_res <- icount()

###----###----###----###----###----###----###----###----###----###----###


while (!dbHasCompleted(trip_updates_res)) {
    
    cat(nextElem(i_trip_updates_res), ", ", sep = "")
    
    
    ###----###----###----###----###----###----###----###----###----###----###
    
    trip_updates_chunk <- dbFetch(trip_updates_res, n = 1e6)
    
    trip_updates_chunk_distinct <- distinct(trip_updates_chunk)
    
    rows          <- append(rows,          nrow(trip_updates_chunk))
    rows_distinct <- append(rows_distinct, nrow(trip_updates_chunk_distinct))

    ###----###----###----###----###----###----###----###----###----###----###
    
    
    dbWriteTable(
        subway_gtfs_2016_2017_distinct,
        "trip_updates",
        value = trip_updates_chunk_distinct,
        append = TRUE,
        temporary = FALSE
    )
    
    gc()
}


write_csv(data_frame(rows = rows, rows_distinct = rows_distinct), "rows.csv", append = FALSE)


###########################################################################################
###########################################################################################

dbClearResult(trip_updates_res)
dbDisconnect(subway_gtfs_2016_2017)
dbDisconnect(subway_gtfs_2016_2017_distinct)

###########################################################################################
###########################################################################################

subway_gtfs_2016_2017_distinct <-
    dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_distinct_db.sqlite3")


db_create_index(subway_gtfs_2016_2017_distinct, "trip_updates", "trip_id")
db_create_index(subway_gtfs_2016_2017_distinct, "trip_updates", "route_id")
db_create_index(subway_gtfs_2016_2017_distinct, "trip_updates", "stop_sequence")
db_create_index(subway_gtfs_2016_2017_distinct, "trip_updates", "stop_id")

###########################################################################################
###########################################################################################

subway_gtfs_2016_2017_distinct <-
    dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_distinct_db.sqlite3")

###########################################################################################
###########################################################################################

vehicle_position_res <-
    dbSendQuery(
        conn = subway_gtfs_2016_2017,
        statement = "SELECT * FROM vehicle_position")

rows          <- integer(0L)
rows_distinct <- integer(0L)

i_vehicle_position_res <- icount()

###----###----###----###----###----###----###----###----###----###----###


while (!dbHasCompleted(vehicle_position_res)) {
    
    cat(nextElem(i_vehicle_position_res), ", ", sep = "")
    
    
    ###----###----###----###----###----###----###----###----###----###----###
    
    vehicle_position_chunk <- dbFetch(vehicle_position_res, n = 1e6)
    
    vehicle_position_chunk_distinct <- distinct(vehicle_position_chunk)
    
    rows          <- append(rows,          nrow(vehicle_position_chunk))
    rows_distinct <- append(rows_distinct, nrow(vehicle_position_chunk_distinct))

    ###----###----###----###----###----###----###----###----###----###----###
    
    
    dbWriteTable(
        subway_gtfs_2016_2017_distinct,
        "vehicle_position",
        value = vehicle_position_chunk_distinct,
        append = TRUE,
        temporary = FALSE
    )
    
    gc()
}


write_csv(data_frame(rows = rows, rows_distinct = rows_distinct), "rows2.csv", append = FALSE)


###########################################################################################
###########################################################################################

dbClearResult(vehicle_position_res)
dbDisconnect(subway_gtfs_2016_2017_distinct)

###########################################################################################
###########################################################################################

subway_gtfs_2016_2017_distinct <-
    dbConnect(RSQLite::SQLite(), "./2016-2017/subway_gtfs_2016_2017_distinct_db.sqlite3")

db_create_index(subway_gtfs_2016_2017, "vehicle_position", "trip_id")
db_create_index(subway_gtfs_2016_2017, "vehicle_position", "route_id")
db_create_index(subway_gtfs_2016_2017, "vehicle_position", "vehicle_id")

###########################################################################################
###########################################################################################

tbl(subway_gtfs_2016_2017_distinct, "trip_updates") %>% 
    pull(arrival_time) %>% 
    length()

