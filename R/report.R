#' Rename the original field names to something more friendly
#' 
#' @param x vector of column names
clean_names <- function(x){
  new_names <- gsub(pattern = "[ ()/]", replacement = "_", x = x)
  new_names <- gsub(pattern = "__", replacement = "_", x = new_names)
  new_names <- gsub(pattern = "_$", replacement = "", x = new_names)
  new_names <- tolower(new_names)
  return(new_names)
}

read_template_from_excel <- function(file, fields = c("Samples", "Isolates", "Sequences")){
  fields <- match.arg(fields) 
  if (fields=="Samples"){
    dfs = list()
    cols_remove = c("alternative_sample_id", "isolate_id", "alternative_isolate_id")
    dfs$sam  <- open_sheet(file = file, sheet_name = "Sample Collection & Processing")
    dfs$host <- open_sheet(file, sheet_name = "Host Information")         %>% select(-any_of(cols_remove)) %>% distinct()
    dfs$env  <- open_sheet(file, sheet_name = "Environmental conditions") %>% select(-any_of(cols_remove)) %>% distinct()
    tryCatch({
      df <- dfs$sam %>% 
        left_join(dfs$host, by = "sample_collector_sample_id", relationship = "one-to-many")  %>% 
        left_join(dfs$env, by = "sample_collector_sample_id", relationship = "one-to-many")
      return(df)}, 
    finally = {return(dfs)})
    } else if (fields=="Isolates"){
      df  <- open_sheet(file = file, sheet_name = "Strain and Isolate Information")
      return(df)
    } else {
      df  <- open_sheet(file = file, sheet_name = "Sequence Information")
      return(df)
    }
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
                        detectDates = TRUE, na.strings = "") %>% as_tibble()
  # If all NA, set to character
  df <- df %>% mutate(across(where(~ all(is.na(.x))), ~as.character(.x)))
  # Set blank strings to NA, where openxlsx fails to do so: 
  df[df==""] <- NA
  # Clean the names according the them rules
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

