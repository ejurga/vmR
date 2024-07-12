PLEASE

#' Extract Ontology ID from GRDI term
#'
#' @param x Vector of GRDI terms in the format "Term name \[ONTOLOGY:0000000\]"
#'
#' @export
extract_ont_id <- function(x){
  sub(x = x, "^.+\\[([A-Za-z_]+)[:_]([A-Z0-9]+)\\]", "\\1:\\2")
}

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
           ont_table = c('ontology_terms', 'countries', 'state_province_regions',
                         'host_organisms', 'microbes')){
  ont_table <- match.arg(ont_table)

  get_sql <- glue::glue_sql(.con=db, "SELECT id, ontology_id FROM ", ont_table,
                             " WHERE ontology_id = $1")

  ids <- extract_ont_id(x)
  fac <- factor(ids)
  levels(fac)

  res_df <- dbGetQuery(db, get_sql,  list(levels(fac)))

  res <-
    factor(fac, levels = res_df$ontology_id, labels = res_df$id) |>
    as.character() |> as.integer()

  return(res)

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

#' Separate a GRDI multi choice column into a long-form dataframe
#'
seperate_column_into_longform <- function(df, col){
  df.long <-
    df %>%
    select(id, all_of(col) ) %>%
    separate_longer_delim(all_of(col), regex("\\s{0,1}[|;]\\s{0,1}"))
  return(df.long)
}

#' Insert df into one of the multi choice tables
#'
insert_into_multi_choice_table <- function(db, df, table){
    df <- df %>% select(id, everything())
    if (length(colnames(df))!=2) stop("df not 2 columns")
    new_cols <- dbListFields(db, table)
    colnames(df) <- new_cols
    dbAppendTable(db, name = table, value = df)
}

#' Populate a "multi-choice" table in the VMR
#'
#' Currently, the GRDI schema allows some fields to be populated with multiple
#' values. These fields are implemented in the database as their own tables.
#' These tables are all similar in their structure, so this convenience
#' function quickly populates these tables given the values in the GRDI fields.
#' The function assumes that the values are separated wither with
#' "|", or a ";"
#'
#' @param db [DBI] connection to VMR
#' @param df The working dataframe that at least contains the GRDI column, as
#'  well as the id field that the multi-choice table will use as a foreign key.
#' @param column str, the name of the GRDI column that can take on multiple
#'  values
#'
#'  @export
transform_and_insert_multi_choice_column <- function(db, df, column){

  map <- get_mapping_for_one_col(vmr, column)

  if (map$is_multi_choice){
    df <- seperate_column_into_longform(df, column)
  } else {
    stop("selected column is not multi-choice according to mapping")
  }

  if (map$is_lookup==TRUE){
    df[[column]] <- convert_GRDI_ont_to_vmr_ids(vmr, df[[column]])
  }
  message("Inserting into ", map$vmr_table)
  insert_into_multi_choice_table(vmr, df, table = map$vmr_table)
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
#' @export
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

#' Match lookup terms
#'
amatch_term <- function(db, x, lookup_table, maxDist=30){
  df <- lookup_table_terms(vmr, lookup_table)
  res <- stringdist::amatch(x=x, table=df$en_term, maxDist = maxDist)
  res_df <- df[res,]
  for (i in seq(length(x))) message(x[i], " --> ", res_df$en_term[i])
  return(res_df)
}
