
#' Sort a vector of column names into categories
#'
#' This is based on regular expressions 
#'
#' @param x A vector of GRDI column names
#'
sort_by_categories <- function(x) {
  patterns <-
    c("(plan)|(project)", "geo", "food", "^env", "^ana", "^nucleic",
      "date",  "collect",  "(storage)|(volume)")
  res <- list()
  for (pattern in patterns){
    vals <- grep(x = x, , pattern = pattern, value = TRUE)
    if (length(vals) > 0) res[[pattern]] <- vals
  }
  res[["other"]] <- x[!x %in% unname(unlist(res))]
  return(res)
}

#' Return a dataframe of columns in which all the rows are the same value
#'
#' @param df any dataframe
cols_all_the_same <- function(df) {
  df <-
    select(df, where(~length(unique(.x))==1)) %>%
    distinct() %>% t() %>%
    as.data.frame() %>% rownames_to_column()
  colnames(df) <- c("Field", "Value")
  unique_df <- as_tibble(df)
  return(unique_df)
}

#' Return distinct rows by original_sample_description 
#'
#' Sorts a dataframe into distince rows based on the field original_sample_description
#'
#' @param df Any dataframe
#' @param ... unquoted field names to exclude from the distinct command
#'
get_distinct_by_sample_description <- function(df, ...) {
  sum_df <-
    df %>%
    select(-sample_collector_sample_ID, -c(...)) %>%
    group_by(across(-contains("date"))) %>%
    mutate(n_samples = n()) %>%
    group_by(n_samples, .add = TRUE) %>%
    summarise(
      across(contains("date"),
             list(range = ~paste0(range(as_date(.x)), collapse = " - "),
                  unique = ~as.character(length(unique(.x))))), .groups = "drop") %>%
  select(original_sample_description, n_samples, everything())
  return(sum_df)
}


#' Return distinct rows by original_sample_description AND fields with a single value
#'
#' @param file The path to the GRDI excel template
#'
samples_by_group <- function(file) {

  df <- grdi_excel_to_sample_metadata_df(file)

  same <- cols_all_the_same(df)
  is_na <- is.na(same$Value)
  grdi_null <- grepl(x=same$Value, "^Not ")
  same_sorted <-
    same[c(which(!(is_na | grdi_null)), which(grdi_null), which(is_na)),]
  df_rest <- df %>% select(-any_of(same_sorted$Field))
  df_groups <- get_distinct_by_sample_description(df_rest, geo_loc_name_site)
  return(list(grouped_by=df_groups, same_df=same_sorted))
}

#' Full join the Sample metadata from the Excel file template
#'
#' Join the "Sample collection & Processing", "Host Information", and "Environmental conditions" 
#' tabs from the GRDI excel template into a single dataframe, joined by sample_collector_sample_ID.
#'
#' @param file The path to the GRDI excel template
#'
grdi_excel_to_sample_metadata_df <- function(file) {
  samples <- open_sheet(file = file, sheet_name = "Sample Collection & Processing")
  host <- open_sheet(file = file, sheet_name = "Host Information")
  env_data <- open_sheet(file = file, sheet_name = "Environmental conditions")
  
  full <-
    samples %>% left_join(host, by = "sample_collector_sample_ID") %>%
    left_join(env_data, by = "sample_collector_sample_ID")

  return(full)
}

#' Format an Excel GRDI file to report on same-value columns and distinct metadata per sample description
#'
#'
#' @param file Output file
#' @param title Title of the excel workbook
#' @param same_df The same_df dataframe returned from [samples_by_group]
#' @param grouped_df The grouped_df dataframe returned from [samples_by_group]
save_report_to_excel_workbook <- function(file, title, same_df, grouped_df){
  wb <- openxlsx::createWorkbook(creator = "Emil Jurga", title = "WP3.2 Data for review")
  openxlsx::addWorksheet(wb = wb, sheetName = "Fields All Same")
  openxlsx::addWorksheet(wb = wb, sheetName = "Per sample type")
  openxlsx::writeDataTable(wb = wb, sheet = "Fields All Same", x = same_df)
  openxlsx::writeDataTable(wb = wb, sheet = "Per sample type", x = grouped_df)
  openxlsx::setColWidths(wb = wb, 
                         sheet = "Fields All Same", cols = c(1,2),
                         widths = "auto")
  openxlsx::setColWidths(wb = wb, sheet = "Per sample type", seq(1:ncol(grouped_df)),
                         widths = "auto")
  openxlsx::freezePane(wb = wb, sheet = "Per sample type", firstCol = TRUE)
  openxlsx::saveWorkbook(wb, file = file, overwrite = T)
}

