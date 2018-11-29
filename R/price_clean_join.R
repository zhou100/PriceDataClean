##################################################################
# Author: Yujun Zhou, University of Illinois , Sep 25, 2017
# Updated: Nov. 23, 2018
# R code for automatic cleaning, processing and merging excel tables

# Goal: clean and separate raw price data by date, grain type and transpose data into the wide format 

# Purpose: merge old price data with new


# Input: 

# 1. weekly price 0815: clean, merged data of maize, rice, groundnuts, beans
# 2. malawi.price.1516: 2015-2016 raw price files with a mix of weekly raw price
# 3. malawi.price.1617: 2016-2017 raw price files with a mix of weekly raw price


# Output: 

# 1. cleaned, joined, transposed maize price 08-17
# 2. cleaned, joined, transposed rice price 08-17
# 3. cleaned, joined, transposed groundnut price 08-17
# 4. cleaned, joined, transposed beans price 08-17

#################################################################

rm(list=ls())


library("readxl")
library("tidyr")
library("dplyr")
library("openxlsx")
library("zoo")


source("R/function/reshapeDF.R")


#################################################################
### 1. read in the data and separate by commodity 
#################################################################
# list the new price data files
price.files.1516 <- list.files(path=paste("data/raw/","malawi.price.1516",sep="/"), 
                        pattern = "xls$",
                        full.names=TRUE)

price.files.1617 <- list.files("data/raw/malawi.price.1617", 
                        pattern = "xls$",
                        full.names=TRUE)

all.price.files<-append(price.files.1516,price.files.1617)

# replace file_list_all with file_list if you are only updating the new data 
file.names<- all.price.files

# remove irregulars in the file names, 
pattern<-c(getwd(),"malawi.price.","1516","1617","data/raw","MONTTHLY ","MONTHLY ","   \\(Autosaved\\)","MONTHY ","MONTH.","*.xls","prices","//")
for (i in 1:length(pattern)) {
  file.names <- gsub(pattern[i],"", file.names)
}
file.names<-gsub("/", "", file.names)
file.names<-gsub("  ", "_", file.names)
file.names<-gsub(" ", "_", file.names)

# check the names 
file.names

# check the length of tables to update 
length(file.names)

# the next line code loops over all the excel files in the raw data and read them into the memory
# ready for cleaning 

list2env(
  lapply(setNames(all.price.files, make.names(file.names)), 
         function(i){read_excel(i,sheet = "Data",na = "NA")}), envir = .GlobalEnv)



# The next chunk of code is loop through every excel file (i.e. every month)
# separate the price data of each crop by identifying the breaks between data 
# the data will be between the row number of commodities and the first break that appears 

# create a vector of length equal to the length of the commodity market

price.dfs.list = vector(mode="list", length = 22) 


for ( FileIndex in 1:length(file.names)){
  
  ###########################################################
  # Start with removing columns and rows with no information  
  ###########################################################
  
  temp <- get(file.names[FileIndex])  # get the monthly price data file stored in memory by name
  
  temp[temp=="NA"]<-NA  # replace "NA" with NA just in case..
  

  temp <- temp[,colSums(is.na(temp))<nrow(temp)] # exclude empty cells 
  
  # find the row where the first column == ADD, 
  # because that is the line of the column names  
  row_head<-which(temp[,1]=="ADD")
  
  # the line above row_head, stores the date information
  # use the date as the column names
  col_names<-as.numeric(temp[(row_head-1),])  # transform to numeric 
  col_names[which.min(col_names)]<-NA  
  col_names<-convertToDate(col_names) # format into date 

  colnames(temp)<-col_names   # use the date as the column names

  colnames(temp)[1:3]<-c("Province","DISTRICT","Market")  #rename columns
  
  # Define column end, because the information after the column with "start date", 
  # is a review of the price information in the previous year, so it's duplicated information.
  col_end<-which(temp[(row_head-1),]=="Start Date:")-2
  
  # remove the first three rows with no information and the columns after column end 
  temp<-temp[(row_head+1):nrow(temp),]
  temp<-temp[,1:col_end[1]]
  
  
  #####################################################################################################################
  # separate the information by crop commodity names  
  # This makes use of the fact that each commodity is separated by a blank line 
  # and followed a line contains the commodity name
  ###########################################################  ###########################################################
  
  row.blanks<-which(rowSums(is.na(temp))==ncol(temp))  # count the row numbers of each blank line
  #print(length(blanks))
  
  row.commodity<-which(rowSums(is.na(temp))==ncol(temp)-1 & !is.na(temp$Market) & temp$Market!="AVERAGE PRICE" & temp$Market!="the end") 
  #find the row numbers of the commodity name
  
  row.avg.price<-which(!is.na(temp$Market) & temp$Market=="AVERAGE PRICE")  #row numbers of avg_price, which is the end line for each chunk of data 

  #save chunks into separate tables with the commodity as the name 
  commodity.names<-temp$Market[row.commodity]
  
  commodity.names<-gsub(" ", "_", commodity.names, fixed = TRUE)
  commodity.names<-gsub("/", "_", commodity.names, fixed = TRUE)

  
  # the data will be between the row number of commodities and the first break that appears 
  # commodity.names
  
  
  
  #####################################################################################################################
  # store the information into dataframes by crop commodity names
  # merge the dataframe with the previous one 
  ###########################################################  ###########################################################
  
  for (j in 1:length(commodity.names)){
    #assign(paste0(commodity.names[j],i), temp[(row.commodity[j]+1):(row.avg.price[j]-1),])
    
    # make each commodity into a dataframe in memory 
    assign(commodity.names[j], temp[(row.commodity[j]+1):(row.avg.price[j]-1),])
    
    # get the dataframe by name and store them in a list 
    # use if statement to avoid error at the start
    
    if(FileIndex==1){
      temp.df = get(commodity.names[j])     

      price.dfs.list[[j]] = temp.df %>% select (-Province,-DISTRICT)
      
    } else {
      
      temp.df = get(commodity.names[j])
      temp.df = temp.df %>% select (-Province,-DISTRICT)
      
      price.dfs.list[[j]] = dplyr::left_join( price.dfs.list[[j]], temp.df , by=c("Market"))
    }
    
  }# loop over different commodity  
  
  # remove temporary dfs to save memory 
  temp  = c() 
  temp.df = c()

} # loop over different files  






# save some of the data into dataframe 
commodity.names


 
maize.df.new = price.dfs.list[[which(commodity.names=="MAIZE_GRAIN")]]
rice.df.new = price.dfs.list[[which(commodity.names=="POLISHED_RICE")]]
nuts.df.new = price.dfs.list[[which(commodity.names=="SHELLED_G_NUTS")]]
beans.df.new = price.dfs.list[[which(commodity.names=="BEANS_GENERAL")]]


# source a function that formats the df and transpose them

source("R/function/reshapeDF.R")

new.dfs.list = list(maize.df.new,rice.df.new,nuts.df.new,beans.df.new)

new.trans.dfs.list = lapply(new.dfs.list, function(x){reshapeDF(x,date.var="date",date.position="colnames")})


maize.trans.df.new = new.trans.dfs.list[[1]]
rice.trans.df.new = new.trans.dfs.list[[2]]
nuts.trans.df.new = new.trans.dfs.list[[3]]
beans.trans.df.new = new.trans.dfs.list[[4]]

#################################################################
### 3. read old data and join the new processed data, by crop 
#################################################################

# read in price data 0815, transpose them and ready them for merge with the new 

# read in the previously hand cleaned data set from 08-15

maize.0815 = read_excel("data/raw/WeeklyPricesMaize_0815.xlsx",sheet = "Maize",na = "NA")
rice.0815 = read_excel("data/raw/WeeklyPricesMaize_0815.xlsx",sheet = "Polished Rice",na = "NA")
nuts.0815 = read_excel("data/raw/WeeklyPricesMaize_0815.xlsx",sheet = "Groundnuts Shelled",na = "NA")
# beans first row is the date, don't read in column names 
beans.0815 = read_excel("data/raw/WeeklyPricesMaize_0815.xlsx",sheet = "Beans general",na = "NA",col_names = FALSE)

# need to format these into the same format as above. i.e. using date as column names 


# first find where the date variable is, formatting into date and make it the colnames 
# handle this with a if functions 
# note that this is not the general case, depend on what your data looks like. 

formatDF = function(DF){
  
  if (is.na(DF[1,1])){
    DF[1,1] = "date"
  }  

  # remove the row with the week variable in maize 
  
  if (DF[2,1] = "week"){
    DF = DF[-2,]
  }
  
    
}











# source a function that formats the df and transpose them 
source("R/function/reshapeDF.R")

# apply the reshape function on all the existing data 
price.0815.list= list(maize.0815,rice.0815,nuts.0815,beans.0815)
price.0815.pivot.list = lapply(price.0815.list, function(x){reshapeDF(x,date.var="date",date.position="row1")})

maize.pivot= price.0815.pivot.list[[1]]
dim(maize.pivot)

# show that it's the same with the hand pivoted table 
# maize.0815.pivot = read_excel("data/raw/WeeklyPricesMaize_0815.xlsx",sheet = "mkt_transpose",na = "NA")
# 
# dim(maize.0815.pivot)




maize.pivot










MAIZE_GRAIN <- read_csv("~/Box Sync/Research/Price_data_auto/merged/MAIZE_GRAIN.csv")
colnames(MAIZE_GRAIN)[1]<-"date"
colnames(MAIZE_GRAIN)[which(colnames(MAIZE_GRAIN)=="CHIKHWAWA")]<-"CHIKWAWA"
colnames(MAIZE_GRAIN)[which(colnames(MAIZE_GRAIN)=="LUNCHEZA")]<-"LUCHENZA"

 
WeeklyPricesMaize_join <- read_excel("~/Box Sync/Research/Price_data_auto/Market data/WeeklyPricesMaize_join.xlsx",sheet = "mkt_transpose",na = "empty")
WeeklyPricesMaize_join <- WeeklyPricesMaize_join[,-which(colnames(WeeklyPricesMaize_join)=="Average")]
MAIZE_GRAIN<-MAIZE_GRAIN[,-which(colnames(MAIZE_GRAIN)=="CHILINGA")]
MAIZE_GRAIN$MIGOWI<-NA

MAIZE_GRAIN[,4:ncol(MAIZE_GRAIN)]<-as.numeric(unlist(MAIZE_GRAIN[,4:ncol(MAIZE_GRAIN)]))
MAIZE_GRAIN$date<-as.character(MAIZE_GRAIN$date) 
MAIZE_GRAIN$week<-as.numeric(MAIZE_GRAIN$week) 

WeeklyPricesMaize_join[,4:ncol(WeeklyPricesMaize_join)]<-as.numeric(unlist(WeeklyPricesMaize_join[,4:ncol(WeeklyPricesMaize_join)]))                                             
WeeklyPricesMaize_join$date<-as.character(WeeklyPricesMaize_join$date)
WeeklyPricesMaize_join$week<-as.numeric(WeeklyPricesMaize_join$week)


newdata<-dplyr::setdiff(MAIZE_GRAIN,WeeklyPricesMaize_join)
maize_joined<-dplyr::union(MAIZE_GRAIN,WeeklyPricesMaize_join)


maize_joined$date<-as.Date(maize_joined$date)
maize_joined<-maize_joined[order(maize_joined$date),] 






write.csv(maize_joined,"maize_price_joined.csv") # write out in csv format 

