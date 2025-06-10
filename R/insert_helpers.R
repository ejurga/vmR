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
    separate_longer_delim(cols = terms, delim = stringr::regex("\\s{0,1}[|;]\\s{0,1}")) %>%
    filter(!is.na(terms))
  
  if (nrow(df_long)==0){ 
    message("No values for table ", table)
  } else { 
    if (is_ontology==TRUE){
      message("Inserting into ", table)
      df_long$ont_ids <- convert_GRDI_ont_to_vmr_ids(db, df_long$terms)
    }
    insert_sql <- glue::glue_sql("INSERT INTO", table, "(sample_id, term_id) VALUES ($1, $2)", .sep = " ")
    res <- dbExecute(db, insert_sql, list(df_long$id, df_long$ont_ids))
    message("Inserted ", res, " record into multi-choice table ", table)
  }
}

#' Convert the vectors of ontology terms into their corresponding VMR ids
#' 
#' Given a list of vectors that are to be input into the VMR, convert the 
#' desired columns containing ontology terms into their respective VMR IDs
#' 
#' @param ontology_columns A vector of the list names that are to be 
#'   converted into VMR ids 
#' @param df The dataframe with the columns to convert
#' @return dataframe
#' 
columns_to_ontology_ids <- function(db, df, ontology_columns, ont_table = 'ontology_terms'){
 
  # make sure that the ontology colums are actually all in the df
  in_df <- ontology_columns %in% colnames(df)
  if ( !all(in_df) ) {
    stop("These columns were called to be converted into VMR ids, but are missing: ",
         paste0(ontology_columns[!in_df], collapse = ", "))
  }
   
  for (i in ontology_columns){
    print(paste("converting ontology col", i, "to vmr ids"))
    df[[i]] <- convert_GRDI_ont_to_vmr_ids(db = db, x = df[[i]], ont_table = ont_table)
  }
  return(df)
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
#' @return sql
#' 
make_insert_sql <- function(table_name, field_names){
  cols_sql <- glue::glue_sql_collapse(field_names, sep = ", ")
  insert_sql <-
    glue::glue_sql("INSERT INTO", table_name, "(", cols_sql, ") VALUES (", make_sql_params(length(field_names)), ") RETURNING id",
                   .sep = " ")
  return(insert_sql)
}

#' Return an SQL formatted list of args to pass to an insert statement
#' 
make_sql_params <- function(n){
  sprintf('$%i', 1:n) |> glue::glue_sql_collapse(sep = ", ")
}

