#' Download and load the NCBITaxon ontology
#'
download_and_load_ncbiTaxon <- function(){
  message("Downloading")   
  file <- tempfile() 
  curl::curl_download(url = "https://purl.obolibrary.org/obo/ncbitaxon.obo", destfile = file, quiet = FALSE)
  message("Loading ontology into R (takes some time), and requires > 15Gb mem")
  ncbi <- ontologyIndex::get_OBO(file, extract_tags = "everything")
  return(ncbi)
}



#' Get full NCBITaxon ontology and add bacterial genera and species to microbes table
#' 
#' @param db DBI connection to VMR
#' @param ncbi ontology loaded into R
#' @param commit Commit to the VMR?
#' 
append_ncbi_taxon_ontology_to_microbes <- function(db, ncbi, insert = FALSE){
  
  # Get Bacillati, Fusobacteriati, Pseudomonadati 
  bacteria <- ontologyIndex::get_descendants(ncbi, roots = c("NCBITaxon:1783272","NCBITaxon:3384189","NCBITaxon:3379134"), exclude_roots = TRUE)
 
  # Remove uncertain taxa
  unc <- grep(x=ncbi$name[bacteria], "unclassified")
  env <- grep(x=ncbi$name[bacteria], "environmental")
  inc_sed <- grep(x=ncbi$name[bacteria], "incertae sedis")
  uncBac <- ontologyIndex::get_descendants(ncbi, roots = ncbi$id[bacteria][c(unc, env, inc_sed)], exclude_roots = FALSE)
  
  df <- tidyr::tibble(ontology_id = bacteria, scientific_name = ncbi$name[bacteria], property = ncbi$property_value[bacteria])
  x <- ncbi$property_value[bacteria]
  x[lengths(x)==0] <- NA
  x <- gsub(x=x, "has_rank ", "")
  df$has_rank <- x
  
  df_gs <-
    df |> 
    dplyr::filter(has_rank %in% c("NCBITaxon:genus", "NCBITaxon:species", "NCBITaxon:subgenus", "NCBITaxon:subspecies")) |>
    dplyr::select(ontology_id, scientific_name) |>
    dplyr::filter(!ontology_id %in% uncBac)
  
  dbOrgs <- DBI::dbReadTable(db, "microbes")
  
  df_add <- df_gs[!df_gs$ontology_id %in% dbOrgs$ontology_id,]
  df_add$curated = FALSE
  
  message("Found ", nrow(df_add), " taxon ontologies to add")
  
  if (insert) {
    DBI::dbBegin(db)
    DBI::dbAppendTable(db, name = "microbes", value = df_add)
    message("Transaction started, call DBI::dCommit to finish")
  } else { message("commit set to FALSE, not appending") }
}
