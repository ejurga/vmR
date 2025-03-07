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
  df <- df %>% mutate(across(where(~ all(is.na(.x))), ~as.character(.x)))
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
tally_values_to_string <- function(x){
  vals <- table(x, useNA = "ifany") 
  names_w_colon <- paste0("   ", names(vals), ":")
  vals <- paste0(unname(vals), "\n")
  res <- paste0(c(rbind(names_w_colon, vals)), collapse = ' ')
  return(res)
}

#' Print out a tally of unique values across all columns in a dataframe
#' 
#' Except for ID columns!
#' 
#' @param df dataframe
#' 
#' @export
print_tallies_of_columns <- function(df){
  cols <- colnames(df)
  cols <- cols[!grepl(x=cols, "[iI][dD]$")]
  na_cols <- c()
  for (col in cols){
    # Check if date, and if so, print out a range instead
    if ( grepl(x=col, pattern = "date$") & !all(is.na(df[[col]])) ){
      dates <- as_date(df[[col]])
      cat(col, "\n")
      cat("Date range is:", paste(range(dates), collapse = " - "))
      cat(", and there are", length(unique(dates)), "unique dates \n \n")
    # Add to NA list if the column is all NA
    } else if ( all(is.na(df[[col]])) ){
      na_cols <- c(na_cols, col) 
    # Otherwise, get the tallies and print them
    } else {
      t <- table(df[[col]], useNA = "ifany")
      cat(col, "\n")
      cat(tally_values_to_string(df[col]), "\n")
    }
  }
  cat("These columns are empty:", paste(na_cols, collapse = ", \n"))
}

