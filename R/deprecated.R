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

#' Get a list of multi choice tables associated with the VMR table
#'
#' @export
multi_choice_tables <- function(db, table){

  fk <- dbReadTable(vmr, "foreign_keys") %>% as_tibble()

  foreign_tabs <-
    fk %>%
    filter(foreign_table_name==vmr_tab) %>%
    pull(table_name)

  map <- get_template_map(vmr)

  res <-
    map %>%
    filter(vmr_table %in% foreign_tabs) %>%
    filter(is_multi_choice==TRUE) %>%
    pull(grdi_field)

  return(res)
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

