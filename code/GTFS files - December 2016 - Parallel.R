####################################################-
####################################################-
##
## NYC Subway GTFS Real Time Archive - December ----
##
####################################################-
####################################################-

#=========================#
#### Loading packages ####
#=========================#


library(gtfsr)
library(magrittr)
library(dplyr)
library(gtfsway)
library(httr)
library(xml2)
library(rvest)
library(purrr)
library(RProtoBuf)
library(lubridate)
library(stringr)
library(iterators)
library(readr)
library(parallel)
library(foreach)
library(doSNOW)
library(DBI)
library(dbplyr)


setwd("D:/Files/BASP/R analyses/NYC/MTA/Subway/Subway Time/GTFS Files/2016-2017")

source("gtfs_tripUpdates_2.R")
source("gtfs_vehiclePosition_2.R")


#==========================#
#### Folder structures ####
#==========================#

base <- "D:/Files/BASP/R analyses/NYC/MTA/Subway/Subway Time/GTFS Files/2016-2017"

paths <- 
    list.files(
        base, 
        full.names = FALSE, 
        recursive = TRUE
    ) %>% 
    str_subset(".xz") %>% 
    str_subset("subway_time_201612")

exdir <- 
    paths %>% 
    str_replace(".tar.xz", "")

paths_df <- 
    paths %>% 
    str_split("/", simplify = TRUE) %>% 
    as_tibble()

file_names <- 
    paths_df %>% 
    select(V3)


dt_trip_info_NA <- 
    tibble(
        trip_id = NA_character_,
        start_time = NA_character_,
        start_date = NA_character_,
        route_id = NA_character_
    )

dt_trip_info_0 <- dt_trip_info_NA %>% head(0)

dt_stop_time_update_NA <- 
    tibble(
        stop_sequence = NA_real_,
        stop_id = NA_character_,
        arrival_time = NA_real_,
        arrival_delay = NA_integer_,
        departure_time = NA_real_,
        departure_delay = NA_integer_
    )

dt_stop_time_update_0 <- dt_stop_time_update_NA %>% head(0)

trip_updates_0 <- 
    data.frame(dt_trip_info_NA, dt_stop_time_update_NA) %>%
    as_tibble() %>% 
    head(0)


vehicle_position_NA <- 
    tibble(
        trip_id = NA_character_,
        route_id = NA_character_,
        lat = NA_real_,
        lon = NA_real_,
        current_status = NA_integer_,
        timestamp = NA_real_,
        vehicle_id = NA_character_
    )

vehicle_position_0 <- vehicle_position_NA %>% head(0)

message_lst <- list()


#==========================#
#### Looping the loop ####
#==========================#


subway_gtfs_2016_12_db <- dbConnect(RSQLite::SQLite(), "subway_gtfs_2016_12_db.sqlite3")

cl <- makeSOCKcluster(4)
registerDoSNOW(cl)


###===###===###===###===###===###===###===###===###===###===###===###===###


for (i in 1:length(paths)) {
    
    
    start_time <- now()
    
    
    cat("===============================\n")
    cat(now() %>% as.character(), "\n")
    cat("-------------------------------\n")
    
    cat("Loop ", i, "/", 
        length(paths), ': "', 
        file_names[[i,1]], '"',
        "\n", sep = "")
    
    cat("-------------------------------\n")
    cat("===============================\n")   
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    cat("1. Untarring `.tar.xz` files\n")
    
    files_no_si <-
        untar(tarfile =
                  xzfile(paste0(base, "/", paths[i]),
                         open = "rb"),
              list = TRUE) %>%
        .[!str_detect(., "gtfs-si")]
    
    
    untar(
        tarfile =
            xzfile(
                paste0(base, "/", paths[i]),
                open = "rb"),
        files = files_no_si,
        exdir = paste0(base, "/", exdir[i])
    )
    
    gc()
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    untarred_files <- list.files(paste0(base, "/", exdir[i]))
    
    a <- seq(from = 1,   to = 6000 - 199, by = 200)
    b <- seq(from = 200, to = 6000,       by = 200)
    
    
    untarred_files_chunked_0 <- list()
    
    for (j in 1:length(a)) {
        
        untarred_files_chunked_0[[j]] <- untarred_files[a[j]:b[j]]
        
        }
    
    
    untarred_files_chunked_1 <-
        
        lapply(untarred_files_chunked_0, function(x) {
            
            y <- x[complete.cases(x)]
            
        })
    
    untarred_files_chunked <- 
        untarred_files_chunked_1[(which(
            sapply(
                untarred_files_chunked_1, 
                function(x) {length(x) != 0}), 
            arr.ind = TRUE)
        )]
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###

    
    cat("2. Trip updates (in parallel)\n")
    
    trip_updates_df_1 <-
        
        foreach(
            k = 1:length(untarred_files_chunked),
            # .export = c(
            #     "dt_trip_info_0",
            #     "dt_stop_time_update_NA",
            #     "dt_stop_time_update_0",
            #     "gtfs_tripUpdates_2",
            #     "untarred_files_chunked",
            #     "base",
            #     "exdir"
            # ),
            .packages = c(
                "dplyr", 
                "tibble", 
                "plyr", 
                "RProtoBuf", 
                "iterators", 
                "gtfsway"
            ), 
            # .combine = "bind_rows",
            # .multicombine = TRUE,
            .errorhandling = "pass",
            .verbose = TRUE
            
        ) %dopar%
        
        {
            raw_data <-
                lapply(
                    X = untarred_files_chunked[[k]],
                    FUN = function(x) {
                        
                        readBin(
                            con = paste0(base, "/", exdir[i], "/", x),
                            what = "raw",
                            n = 2000000
                        )
                    }
                )
            
            feed_message <-
                lapply(
                    X = raw_data,
                    FUN = function(x) {
                        RProtoBuf::read(x, descriptor = transit_realtime.FeedMessage)
                        
                    }
                )
            
            # trip_updates_n <- icount()
            
            trip_updates_df <-
                
                lapply(
                    X = feed_message,
                    FUN = function(x) {
                        
                        # print(paste0(nextElem(trip_updates_n), "-", sep = ""))
                        
                        lapply(
                            X = gtfs_tripUpdates_2(x),
                            FUN = function(y) {
                                    
                                
                                data.frame(
                                    
                                    if (nrow(y$dt_trip_info) != 0) {
                                        
                                        bind_rows(
                                            dt_trip_info_0,
                                            y$dt_trip_info
                                        )
                                        
                                    } else {
                                        
                                        dt_stop_time_update_NA
                                        
                                    },
                                    
                                    if (nrow(y$dt_stop_time_update) != 0) {
                                        
                                        y$dt_stop_time_update
                                        
                                        bind_rows(
                                            dt_stop_time_update_0,
                                            y$dt_stop_time_update
                                        )
                                        
                                    } else {
                                        
                                        dt_stop_time_update_NA
                                    }
                                )
                            }
                            
                        ) %>%
                            bind_rows() %>%
                            bind_rows(trip_updates_0, .) %>%
                            mutate(
                                start_date = as.integer(start_date),
                                stop_sequence = as.integer(stop_sequence),
                                arrival_time = as.integer(arrival_time),
                                departure_time = as.integer(departure_time)
                            ) %>%
                            mutate(start_time = na_if(start_time, "")) %>%
                            as_tibble()
                    }
                ) %>% bind_rows()
        }
    
    gc()
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    cat("3. Writing `trip_updates` to database\n")
    
    trip_updates_df_2 <- bind_rows(trip_updates_df_1)
    
    dbWriteTable(
        conn = subway_gtfs_2016_12_db,
        name = "trip_updates",
        value = trip_updates_df_2,
        append = TRUE,
        temporary = FALSE
    )

    
    trip_rows <- nrow(trip_updates_df_2)
    
    
    rm(trip_updates_df_2)
    gc()
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    cat("4. Vehicle position (in parallel)\n")
    
    vehicle_position_df_1 <-
        
        foreach(
            l = 1:length(untarred_files_chunked),
            # .export = c(
            #     "untarred_files_chunked",
            #     "vehicle_position_0",
            #     "gtfs_vehiclePosition_2",
            #     "base",
            #     "exdir"
            # ),
            .packages = c("dplyr", "tibble", "plyr", "RProtoBuf", "iterators", "gtfsway"),
            # .combine = "bind_rows",
            # .multicombine = TRUE,
            .errorhandling = "pass",
            .verbose = TRUE
            
        ) %dopar%
        
        {
            raw_data <-
                lapply(
                    X = untarred_files_chunked[[l]],
                    FUN = function(x) {
                        
                        readBin(
                            con = paste0(base, "/", exdir[i], "/", x),
                            what = "raw",
                            n = 2000000
                        )
                    }
                )
            
            feed_message <-
                lapply(
                    X = raw_data,
                    FUN = function(x) {
                        RProtoBuf::read(x, descriptor = transit_realtime.FeedMessage)
                        
                    }
                )
            
            
            # vehicle_position_n <- icount()
            
            vehicle_position_df <- 
                
                lapply(
                    X = feed_message, 
                    FUN = function(x) {
                        
                        # print(paste0(nextElem(vehicle_position_n), "-", sep = ""))
                        
                        gtfs_vehiclePosition_2(x) %>%
                            bind_rows() %>%
                            bind_rows(vehicle_position_0, .) %>%
                            mutate(
                                timestamp = as.integer(timestamp),
                                vehicle_id = na_if(vehicle_id, "")) %>%
                            as_tibble()
                    }
                ) %>% bind_rows()
            
        }
    
    gc()
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    cat("5. Writing `vehicle_position` to database\n")
    
    vehicle_position_df_2 <- bind_rows(vehicle_position_df_1)
    
    dbWriteTable(
        subway_gtfs_2016_12_db,
        "vehicle_position",
        value = vehicle_position_df_2,
        append = TRUE,
        temporary = FALSE
    )
    
    
    pos_rows <- nrow(vehicle_position_df_2)
    
    
    rm(vehicle_position_df_2)
    gc()
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    end_time <- now()
    
    message_lst[[i]] <- 
        data_frame(
            loop = i, 
            start = start_time,
            end = end_time,
            diff_time = start_time - end_time,
            file_name = file_names[[i,1]],
            trip_rows = trip_rows,
            pos_rows  = pos_rows
        )
    
    write_rds(message_lst, "message_lst_2016_12.RDS")
    
    
    ###===###===###===###===###===###===###===###===###===###===###===###===###
    
    
    cat("6. Removing untarred files\n")
    
    file.remove(paste0(base, "/", exdir[i], "/", untarred_files)) %>% table() %>% print()
    
    gc()
    
}


###===###===###===###===###===###===###===###===###===###===###===###===###===###


message_df <- 
    message_lst %>% 
    bind_rows() %>% 
    mutate(diff_time = end - start) %>% 
    select(loop:end, diff_time, file_name:pos_rows)

write_rds(message_df, "message_df_2016_12.rds")
write_csv(message_df, "message_df_2016_12.csv")


###===###===###===###===###===###===###===###===###===###===###===###===###===###


stopCluster(cl)
dbDisconnect(subway_gtfs_2016_12_db)


################################################################################

# write_rds(trip_updates_df_2, "trip_updates_df_2.rds")

# trip_updates_df_2 <- read_rds("trip_updates_df_2.rds")

################################################################################
################################################################################
################################################################################
