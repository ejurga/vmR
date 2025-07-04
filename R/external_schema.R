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
make_blank_df <- function(nrow = 1, type = c("samples", "isolates", "sequences")) {
  type <- match.arg(type)
  schema <- vmR:::parse_latest_grdi_schema()
  slot_usage <- schema$classes$GRDI$slot_usage
  x <- sapply(FUN = function(x) slot_usage[[x]]$slot_group, X = names(slot_usage))
  
  ext_fields <- c("experimental_protocol_field",
                  "experimental_specimen_role_type", 
                  "nucleic_acid_extraction_method",
                  "nucleic_acid_extraction_kit",
                  "sample_volume_measurement_value", 
                  "sample_volume_measurement_unit",
                  "residual_sample_status",
                  "sample_storage_duration_value", 
                  "sample_storage_duration_unit",
                  "nucleic_acid_storage_duration_value",
                  "nucleic_acid_storage_duration_unit")
  
  if (type=="samples"){
    sam_cols <- names(x)[x %in% c('Sample collection and processing', 'Environmental conditions and measurements', 'Host information', 'Risk assessment information')]
    cols <- sam_cols[!sam_cols %in% ext_fields]
  } else if (type=="isolates") {
    cols <- names(x[x=="Strain and isolation information"])
  } else if (type=="sequences") {
    seq_cols <- names(x[x=="Sequence information"])
    cols <- c(ext_fields,seq_cols)
  } else stop("problem!")
  
  df <- as_tibble(matrix(ncol = length(cols), nrow = nrow, dimnames = list(NULL, cols)))
  df <- df %>% mutate(across(everything(), ~as.character(.x)))
  return(df)
}