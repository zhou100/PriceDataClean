library(dplyr)

##################################################################
# Small function to transpose/pivot table
# and format date variable 
##################################################################
transposeDF = function(DF,date.var="date",date.position=NULL){
  
  
  temp.df = t(DF)  # transpose 
  temp.df = as.data.frame(temp.df) 
  

  colname.vector = as.character(unlist(temp.df[1,]))
  colnames(temp.df) = colname.vector # the first row with the date information will be the header
  temp.df = temp.df[-1, ]          # removing the first row.

  
  if (date.position == "colnames" ){
    
  temp.df =  tibble::rownames_to_column(temp.df,var = "date")

  date = temp.df[["date"]]
  
  temp.df= temp.df %>% dplyr::select (-date)
  
    
  } else if (date.position == "row1" ){
    
    
    rownames(temp.df)     = NULL
    
    date = as.integer( as.character(temp.df[[date.var]])) # format date to integers 
    
    date = as.Date(date,origin="1900-01-01") # format date to dates  
    
  }
  
  
  
  temp.df.numeric = as.matrix(temp.df)
  
  dim(temp.df.numeric)
  for (j in 1:ncol(temp.df.numeric)){
    for (i in 1:nrow(temp.df.numeric)){
      temp.df.numeric[i,j] = as.numeric(temp.df.numeric[i,j])
    }
  }
  dim(temp.df.numeric)
  
  trans.df = as.data.frame(temp.df.numeric)
  trans.df[date.var] = as.data.frame(date) # create date column
  
  trans.df = separate(data = trans.df,col = date.var,into = c("year","month","day"),remove = FALSE)
  
  trans.df = trans.df %>%  
    filter(!is.na(date)) %>%
    dplyr::select(date,year,month,day,everything())
  
  
  tryCatch({
    trans.df = trans.df %>% dplyr::select(-Average)
  }, error = function(e){
    
  }
    
  )
  
  trans.df[trans.df==0] = NA
  
  
  return(trans.df) 
  
  
  
}
