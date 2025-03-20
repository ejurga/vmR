#' Take the common and scientific name columns and convert to a single ID 
#' 
#' Also checks to make sure that those two columns were the same ontology 
#' term, otherwise, it is probably an error. 
#' 
#' @inheritParams get_sample_ids
#' @param common_name host_common_name field 
#' @param scientific_name host_scientific_name field
#' 
#' @export
host_organisms_to_ids <- function(db, common_name, scientific_name){
 
  # Remove grdi NULLs and set to NA 
  common_name <- set_grdi_nulls_to_NA(x = common_name)
  scientific_name <- set_grdi_nulls_to_NA(x = scientific_name)
  # Get only the ontology ids 
  common_ids <- extract_ont_id(x = common_name)
  science_ids <- extract_ont_id(x = scientific_name)
  # Are they equal?
  is.eq <- common_ids==science_ids
  if (!all(is.eq, na.rm = TRUE)){
    stop("common name and scientific name ontologies mismatched")
  }
  # Replace any NAs in one vector with the value from the other
  ontology_terms <- ifelse(is.na(common_name), scientific_name, common_name)
  # Get the Ids
  host_org_ids <- convert_GRDI_ont_to_vmr_ids(db, ontology_terms, ont_table = "host_organisms")
  return(host_org_ids)
}

#' Replace GRDI NULLs with NA 
set_grdi_nulls_to_NA <- function(x){
  x[grepl(x=x, "^Not ")] <- NA
  return(x)
}

#' Insert values into one of the common-format multi-choice metadata tables 
#' 
#' Currently, the GRDI schema allows some fields to be populated with multiple
#' values. These fields are implemented in the database as their own tables.
#' These tables are all similar in their structure, so this convenience
#' function quickly populates these tables given the values in the GRDI fields.
#' The function assumes that the values are separated wither with
#' "|", or a ";"
#'
#' @param db [DBI] connection to VMR
#' @param ids a vector of the foreign-key ids of the table that the multi-choice 
#'            table is related by.
#' @param vals a vector of values, of same length as ids, that are to be inserted 
#'             as values into the multi-choice table
#' @param table The name of the destination multi-choice table
#' @param is_ontology Are the values GRDI ontology terms?
#' @export
insert_into_multi_choice_table <- function(db, ids, vals, table, is_ontology = FALSE){

  df_long <- 
    tibble(id = ids, terms = vals) %>%
    separate_longer_delim(cols = terms, delim = stringr::regex("\\s{0,1}[|;,]\\s{0,1}")) %>%
    filter(!is.na(terms))
  
  if (nrow(df_long)==0){ 
    message("No values for table ", table)
  } else { 
    if (is_ontology==TRUE){
      message("Inserting into ", table)
      df_long$ont_ids <- convert_GRDI_ont_to_vmr_ids(db, df_long$terms)
    }
    insert_sql <- glue::glue_sql("INSERT INTO", table, "VALUES ($1, $2)", .sep = " ")
    res <- dbExecute(db, insert_sql, list(df_long$id, df_long$ont_ids))
    message("Inserted ", res, " record into multi-choice table ", table)
  }
}

#' Convert a function's arguments into a list of vectors of same length
#' 
#' Input the `environment()` of a function to return the parameters as a 
#' list of equal length vectors, without the db argument that is common to
#' all the insert functions.
#' 
#' @param env the [environment] call of a function
#' @return a list of the functions arguments, minus the DBI connection argument
#' 
sql_args_to_uniform_list <- function(env){
  argg <- c(as.list(env))
  argg <- argg[!names(argg)=="db"]
  argg <- if_len_one_rep(argg)
  return(argg)
}

#' Convert the vectors of ontology terms into their corresponding VMR ids
#' 
#' Given a list of vectors that are to be input into the VMR, convert the 
#' desired columns containing ontology terms into their respective VMR IDs
#' 
#' @param ontology_columns A vector of the list names that are to be 
#'   converted into VMR ids 
#' @param sql_arguments A names list of vectors, which contain the vectors 
#'   of ontology IDs to be converted into VMR ids.
#' @return A named list of vector of same length as `sql_arguments`
#' 
sql_args_to_ontology_ids <- function(db, ontology_columns, sql_arguments){
  
  x <- which(names(sql_arguments) %in% ontology_columns) 
  sql_arguments[x] <-
    lapply(FUN = convert_GRDI_ont_to_vmr_ids, 
           X = sql_arguments[x],  
           db = db) 
  return(sql_arguments)
}

#' Repeat value of length 1 to max length of an argument list
#' 
#' If any in a list of arguments has length 1, then repeat this 
#' value to the same length as the maxium length in the list. Required 
#' because binding a list of parameters to an SQL query requires that 
#' they all be the same length 
#' 
if_len_one_rep <- function(x){
  for (i in which(lengths(x)<=1)){
    if (is.null(x[[i]])) x[[i]] <- NA
    x[[i]] <- rep(x[[i]], max(lengths(x))) 
  }
  return(x)
}

#' Create a generic SQL insert statement 
#' 
#' Makes a generate SQL insert statement from a table name and a list 
#' of parameters, which should correspond to that table's fields.
#' 
make_insert_sql <- function(table_name, field_names){
  cols_sql <- glue::glue_sql_collapse(field_names, sep = ", ")
  insert_sql <-
    glue::glue_sql("INSERT INTO", table_name, "(", cols_sql, ") VALUES (", make_sql_params(length(field_names)), ")",
                   .sep = " ")
  return(insert_sql)
}

#' Return an SQL formatted list of args to pass to an insert statement
#' 
make_sql_params <- function(n){
  sprintf('$%i', 1:n) |> glue::glue_sql_collapse(sep = ", ")
}

