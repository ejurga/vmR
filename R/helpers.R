#' amr_regexes
#'
#' Rerturn a vector to filter out AMR columns
#'
#' @export
amr_regexes <-function(){
    c("_resistance_phenotype$",
      "_measurement(_units|_sign){0,1}$",
      "_laboratory_typing_[a-z_]+$",
      "_vendor_name$",
      "_testing_standard[a-z_]{0,}$",
      "_breakpoint$")
}

#' A little helper function in case ontology terms are not udpated
#'
#' @param x vector of ontology terms to update
#' @export
add_braces_to_ontology_terms <- function(x){
  res <-gsub(x = x, pattern = ':\\s([A-Z]+[_:][0-9]+)', ' [\\1]')
  return(res)
}

#' Extract Ontology ID from GRDI term
#'
#' @param x Vector of GRDI terms in the format "Term name \[ONTOLOGY:0000000\]"
#'
#' @export
extract_ont_id <- function(x){
  sub(x = x, "^.{0,}\\[([A-Za-z_]+)[:_]([A-Z0-9]+)\\]", "\\1:\\2")
}

#' Match lookup terms
#'
#' @export
amatch_term <- function(db, x, lookup_table){
  df <- lookup_table_terms(vmr, lookup_table)
  res <- stringdist::afind(pattern=x, x=df$en_term)
  res_df <- df[max.col(t(-res$distance)),]
  for (i in seq(length(x))) message(x[i], " --> ", res_df$en_term[i])
  return(res_df)
}

is_dataframe_all_empty <- function(df, cols){
  all(sapply(X = cols, FUN = function(x) all(is.na(df[x]))))
}

#' Replace GRDI NULLs with NA 
set_grdi_nulls_to_NA <- function(x){
  x[grepl(x=x, "^Not ")] <- NA
  return(x)
}