#' Rename the original field names to something more friendly
#' 
#' @param x vector of column names
clean_names <- function(x){
  new_names <- gsub(pattern = "[ ()/]", replacement = "_", x = x)
  new_names <- gsub(pattern = "__", replacement = "_", x = new_names)
  new_names <- gsub(pattern = "_$", replacement = "", x = new_names)
  return(new_names) 
}

#' Open a sheet from the Excel Template and return a neat dataframe
#' 
#' @param file Path to the excel template file with data 
#' @param sheet_name The excel sheet to read from.
#' 
#' @export
open_sheet <- function(file, sheet_name){
  df <-
    openxlsx::read.xlsx(xlsxFile = file, 
                        sheet = sheet_name, 
                        colNames = TRUE, 
                        startRow = 2, 
                        sep.names = '_', 
                        detectDates = TRUE) %>% as_tibble()
  names(df) <- clean_names(names(df))
  return(df)
}

#' Quickly get all unique values in a vector and print them out in a string 
#' 
#' This is for the markdown insertion report, for inline tallies of what is 
#' in a field of data 
#' 
#' @param x a vector of data
#' 
#' @export
tally_values_to_string <- function(x){
  vals <- table(x, useNA = "ifany") 
  names_w_colon <- paste0(names(vals), ":")
  vals <- paste(unname(vals), ";")
  res <- paste(c(rbind(names_w_colon, vals)), collapse = ' ')
  return(res)
}