#' Convert GRDI ontology term into VMR ontology indexes
#'
#' Ontology terms in the VMR are indexed in its ontology_terms
#' table. To insert these terms into their respective columns, they
#' must be converted into these index ids. This function handles this
#' from within R by querying the ontology_term table.
#'
#' @param db [DBI] connection to the VMR
#' @param x Vector of ontology terms in the form of "term name [ONT:00000000]"
#'
#' @export
convert_GRDI_ont_to_vmr_ids <-
  function(db, x,
           ont_table = c('ontology_terms', 
                         'countries', 
                         'state_province_regions',
                         'host_organisms', 
                         'microbes')){
  ont_table <- match.arg(ont_table)
  # Check if all is NA, and exit if so. 
  if ( all(is.na(x)) ){ 
    return(x) 
  }
  # Build the query
  get_sql <- glue::glue_sql(.con=db, "SELECT id, ontology_id FROM ", ont_table, " WHERE ontology_id = $1")
  # Extract just the ontology ids from the vectors to query
  ids <- extract_ont_id(x)
  # Conver to factor to limit query to unique values
  fac <- factor(ids)
  # Perform the query
  res_df <- dbGetQuery(db, get_sql,  list(levels(fac)))
  # Check to make sure that all terms are found in the VMR, else we will end 
  # up with silent NULLs inserted
  in_vmr <- levels(fac) %in% res_df$ontology_id
  if ( !all(in_vmr) ){
    stop("Terms not found in VMR: ", paste0(levels(fac)[!in_vmr], collapse = ", "))
  }
  # Convert the original vector into the VMR ids, and make sure its an integer.
  res <-
    factor(fac, levels = res_df$ontology_id, labels = res_df$id) |>
    as.character() |> 
    as.integer()
  # If, somehow, there are new NAs introduced during the conversion, warn about them.
  compare_nas <- is.na(x)==is.na(res)
  if ( !all(compare_nas) ) {
    message("Warning: NA's detected during ontology conversion: ", sum(!compare_nas))
  }
  return(res)
}

#' Get a table of ontology terms from a lookup table
#'
#' @export
lookup_table_terms <- function(db, lookup_table){
  SQL <-
    glue::glue_sql(.con = db,
      "SELECT ont.id, ont.ontology_id, ont.en_term
      FROM ", lookup_table, " AS lu
      LEFT JOIN ontology_terms AS ont
      ON lu.ontology_term_id = ont.id")
  lu_df <- dbGetQuery(db, SQL) %>% as_tibble()
  return(lu_df)
}

#' Search for a term in a lookup table in the VMR
#'
#' @param db Connection to vmr
#' @param lookup lookup table to search through
#' @param x pattern
#' @param form if 'term', just return the ontology string, if table, return as a datafram
#' 
#' @export
grep_ont_term <- function(db, lookup, x, form = c("term", "table")){
  form <- match.arg(form)
  df <- lookup_table_terms(db, lookup) 
  res <- df %>% filter(grepl(x=en_term, pattern = x, ignore.case = TRUE))
  if (form=='term'){
    terms <- paste0(res$en_term, ' [', res$ontology_id, ']')
    return(terms)
  } else {
    return(res)
  }
}

#' Get sample ids from the sample_collector_sample_ids
#' 
#' @param db [DBI] connection to VMR
#' @param sample_names values to query
#' 
#' @export
get_sample_ids <- function(db, sample_names){
  x <- dbGetQuery(db, "SELECT $1 AS query, id FROM samples WHERE sample_collector_sample_id = $1", list(sample_names))
  ids <- x$id[match(sample_names, x$query)]
  if (any(is.na((ids)))) warning("Some sample IDs NOT FOUND in VMR")
  return(ids)
}

#' Get isolate ids from the sample_collector_sample_ids
#' 
#' @param db [DBI] connection to VMR
#' @param sample_names values to query
#' 
#' @export
get_isolate_ids <- function(db, isolate_ids){
  x <- dbGetQuery(db, "SELECT $1 AS query, id FROM isolates WHERE isolate id = $1", list(isolate_ids))
  ids <- x$id[match(isolate_ids, x$query)]
  if (any(is.na((ids)))) warning("Some isolates IDs NOT FOUND in VMR")
  return(ids)
}

#' Set user isolate IDs to VMR ids from all possible alternative ids
#'
#' Attempt to use the possible_isolate_names view to set the user 
#' isolate IDs to VMR isolate_IDs
#'
#' @param db [DBI] connection
#' @param x The vector to attempt to convert to VMR IDs
#' @export
#'
set_vmr_isolate_id_from_alternates <- function(db, x){
  vmrPos <- dbReadTable(db, "possible_isolate_names") %>% as_tibble()
  m <- match(x, vmrPos$isolate_collector_id)
  res <- vmrPos$isolate_id[m]
  if (anyNA(res)) message("NAs detected, total: ", sum(is.na(res)))
  return(res)
}
