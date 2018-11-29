rm(list=ls())
library("readr")
library("tidyr")
library("dplyr")
library("openxlsx")
library("zoo")

# list the price data files
file_list <- list.files("data/raw/FIT_data",pattern = "csv$",
                        full.names=TRUE)

 

dfnames<-list.files("data/raw/FIT_data",pattern = "csv$",
                    full.names=FALSE)

# remove irregulars in the file names, 
dfnames<-gsub("tabula-", "ug", dfnames)
dfnames<-gsub(".csv", "", dfnames)

list2env(
  lapply(setNames(file_list, make.names(dfnames)), 
         function(i){ read_csv(i)}), envir = .GlobalEnv)
