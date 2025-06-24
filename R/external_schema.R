#' Get the latest GRDI schema as an R function
#' 
parse_latest_grdi_schema <- function(){
  file <- tempfile()
  grdi_yaml_url <- "https://raw.githubusercontent.com/cidgoh/pathogen-genomics-package/main/templates/grdi/schema.yaml"
  download.file(url = grdi_yaml_url, destfile = file, method = "wget", extra = "-4")
  schema <- yaml::yaml.load_file(file)
  return(schema)
}