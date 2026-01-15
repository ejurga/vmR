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
    warning("These columns were called to be converted into VMR ids, but are missing: ",
         paste0(ontology_columns[!in_df], collapse = ", "))
  }
   
  for (i in ontology_columns[in_df]){
    print(paste("converting ontology col", i, "to vmr ids"))
    df[[i]] <- convert_GRDI_ont_to_vmr_ids(db = db, x = df[[i]], ont_table = ont_table)
  }
  return(df)
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

  if (is.null(vals)){
    message("There are no defined values for inputs into table ", table, " skipping")
  } else {
    
    df_long <- 
      tibble(id = ids, terms = vals) %>%
      separate_longer_delim(cols = terms, delim = stringr::regex("\\s{0,1}[|;]\\s{0,1}")) %>%
      filter(!is.na(terms))
  
    if (nrow(df_long)==0){ 
      message("No values for table ", table)
    } else { 
      if (is_ontology){
        message("Inserting into ", table)
        df_long$ont_ids <- convert_GRDI_ont_to_vmr_ids(db, df_long$terms)
      } else { df_long$ont_ids <- df_long$terms }
      insert_sql <- glue::glue_sql("INSERT INTO", table, "(sample_id, term_id) VALUES ($1, $2)", .sep = " ")
      res <- dbExecute(db, insert_sql, list(df_long$id, df_long$ont_ids))
      message("Inserted ", res, " record into multi-choice table ", table)
    }
  }
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
  sub(x = x, "^.{0,}\\[([A-Za-z_]+)[:_]([A-Z0-9a-z]+)\\]", "\\1:\\2")
}

#' Match lookup terms
#'
#' @export
amatch_term <- function(db, x, lookup_table){
  df <- lookup_table_terms(vmr, lookup_table)
  res <- stringdist::afind(pattern=x, x=df$en_term)
  res_df <- df[max.col(t(-res$distance)),]
  res_df$query <- x
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

#' Create a generic SQL insert statement 
#' 
#' Makes a generate SQL insert statement from a table name and a list 
#' of parameters, which should correspond to that table's fields.
#'
#' @param table_name The table name of the VMR table to make an insert for
#' @param field_names The column names to insert
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

#' Return an SQL formatted list of postgres placeholders to pass to an insert statement
#' 
#' @param n number of placeholders
make_sql_params <- function(n){
  sprintf('$%i', 1:n) |> glue::glue_sql_collapse(sep = ", ")
}

#' From a DF, select only those cols relevant to the table insert.
#' 
#' Also: warn if there are columns in the database that are missing in the 
#' input dataframe, but do not throw an error.
#' 
select_fields <- function(db, db_table, df_cols){ 
  # Get fields for database table
  dbcols <- dbListFields(db, db_table)
  # Remove DB specific cols.
  dbcols <- dbcols[!(dbcols %in% c('id', 'inserted_by', 'inserted_at', 'was_updated'))]
  # Warn if fields not present
  in_df <- dbcols %in% df_cols
  if (any(!in_df)){
    message("warning: These fields are missing from input df: ", paste0(dbcols[!in_df], collapse = ', '))
  }
  dbcols <- dbcols[in_df]
  return(dbcols)
  }
