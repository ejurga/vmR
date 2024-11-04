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
    tibble(id = ids, vals = vals) %>%
    separate_longer_delim(vals, regex("\\s{0,1}[|;]\\s{0,1}"))

  if (is_ontology==TRUE){
    df_long$vals <- convert_GRDI_ont_to_vmr_ids(db, df_long$vals)
  }

  insert_sql <- glue::glue_sql("INSERT INTO", table, "VALUES ($1, $2)", .sep = " ")
  
  res <- dbExecute(db, insert_sql, list(df_long$id, df_long$vals))
  message("Inserted ", res, " record into multi-choice table ", table)

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
  for (i in which(lengths(x)==1)){
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

