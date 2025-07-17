
#' Get a list of multi choice tables associated with the VMR table
#'
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


#' Renaming a GRDI formatted dataframe to a VMR table
#'
#' @param db A DBI connection to the VMR
#' @param df Dataframe, formatted according to the GRDI spec
#' @param table The table in the VMR database to convert into
#'
#' @return A dataframe with columns that match the fields of the selected
#'  table in the VMR
#'
rename_grdi_cols_to_vmr <- function(db, df, table){
  map_tab <-
    get_template_map(db) %>%
    filter(vmr_table == table) %>%
    filter(grdi_field != vmr_field & is_multi_choice==FALSE)

  x <- colnames(df) %in% map_tab$grdi_field
  for (col in colnames(df)[x]){
    new_col <- map_tab$vmr_field[map_tab$grdi_field==col]
    message("Replacing ", col, " --> ", new_col)
    colnames(df)[colnames(df)==col] <- new_col
  }

  return(df)
}

#' Select fields related to VMR table
#'
select_vmr_fields <- function(db, df, table){

  tab_fields <- dbListFields(db, table)
  tab_fields <- tab_fields[tab_fields!="id"]

  df.s <- df %>% select(any_of(tab_fields))

  x <- !tab_fields %in% colnames(df.s)
  if (any(x)) message("Missing vmr_field ", paste(tab_fields[x], collapse = ", "))

  return(df.s)

}

#' Separate a GRDI multi choice column into a long-form dataframe
#'
seperate_column_into_longform <- function(df, col){
  df.long <-
    df %>%
    select(id, all_of(col) ) %>%
    separate_longer_delim(all_of(col), regex("\\s{0,1}[|;]\\s{0,1}"))
  return(df.long)
}

get_template_map <- function(db){
  df <- dbReadTable(db, "template_mapping") %>% as_tibble()
  return(df)
}

#' Get the vmr's mapping row for the GRDI column
#'
get_mapping_for_one_col <- function(db, grdi_col){
  map <-
    get_template_map(db) %>%
    filter(grdi_field == grdi_col)
  if(nrow(map)>1) stop("Returned mapping is greater than 1")
  return(map)
}

#' Convert GRDI columns to VMR IDs.
#'
#' Convert GRDI columns that take GRDI ontologies and convert them to VMR
#' integer IDs.
#'
#' @param db Connection to the VMR
#' @param df A dataframe formatted to match VMR column names
#' @param vmr_table The target VMR table.
#'
all_table_ontology_columns_to_vmr_ids <- function(db, df, vmr_table){

  # Get columns that are ontology fields
  fk <- dbReadTable(db, "foreign_keys") %>% as_tibble()
  ontology_columns <-
    fk %>%
    filter(table_name == vmr_table,
           foreign_column_name == "ontology_term_id") %>%
    pull(column_name)
  # Only use columns actually in dataset
  in.df <- ontology_columns %in% colnames(df)

  for (col in ontology_columns[in.df]){
    message("Setting field ", col, " to VMR ontology IDs")
    df[[col]] <- convert_GRDI_ont_to_vmr_ids(db, df[[col]])
  }

  return(df)
}
