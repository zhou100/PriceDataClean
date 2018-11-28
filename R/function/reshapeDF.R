library(dplyr)

##################################################################
# Small function to transpose/pivot table
# and format date variable 

##################################################################
reshapeDF = function(DF, date.var="date"){
  
  if (is.na(DF[1,1])){
    DF[1,1] = "date"
  } else if(DF[1,1]=="MISUKU"){
    colnames(DF)
  } 
  
  temp.df = t(DF)  # transpose 
  temp.df = as.data.frame(temp.df) 
  rownames(temp.df)     = NULL
  
  colname.vector = as.character(unlist(temp.df[1, ]))
  colnames(temp.df) = colname.vector # the first row will be the header
  temp.df = temp.df[-1, ]          # removing the first row.
  
  date = as.integer( as.character(temp.df[[date.var]])) # format date to integers 
  
  date = as.Date(date,origin="1900-01-01") # format date to dates  
  
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
    dplyr::select(date,year,month,everything()) %>% 
    dplyr::select(-Average)
  
  trans.df[trans.df==0] = NA
  
  
  return(trans.df) 
  
  
  
}
