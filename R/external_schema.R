#' Get the latest GRDI schema as an R function
#' 
parse_latest_grdi_schema <- function(){
  file <- tempfile()
  grdi_yaml_url <- "https://raw.githubusercontent.com/cidgoh/pathogen-genomics-package/main/templates/grdi/schema.yaml"
  download.file(url = grdi_yaml_url, destfile = file, method = "wget", extra = "-4")
  schema <- yaml::yaml.load_file(file)
  return(schema)
}

#' Make a blank dataframe with the template column names
#' 
#' All will be NA
#' 
#' @param nrow number of rows for the dataframe
#' @return empty dataframe of length nrow
#' @export
make_blank_sample_df <- function(nrow = 1){
  schema <- vmR:::parse_latest_grdi_schema()
  x <- sapply(FUN = function(x) slot_usage[[x]]$slot_group, X = names(slot_usage))
  sample_cols <- names(x[x %in% c('Sample collection and processing', 'Environmental conditions and measurements', 'Host information', 'Risk assessment information')])
  df <- as_tibble(matrix(ncol = length(sample_cols), nrow = nrow, dimnames = list(NULL, sample_cols)))
  df <- df %>% mutate(across(everything(), ~as.character(.x)))
  return(df)
}