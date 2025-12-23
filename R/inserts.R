#' Insert new data into isolate table of the VMR
#' 
#' @export
#' 
insert_isolate_data <-  function(db, df){
  
  df$contact_information <- get_contact_information_id(db,  
                                                       lab = df$isolated_by_laboratory_name, 
                                                       email = df$isolated_by_contact_email, 
                                                       name = df$isolated_by_contact_name)
  df <- rename(df, irida_sample_id = irida_isolate_id)
  # Get fields for sample table
  fields <- select_fields(db, "isolates", colnames(df))
  isolates <- df[,fields]

  # Ontology columns
  ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'isolates'")
  isolates <- columns_to_ontology_ids(db, isolates, ont_cols$column_name)
  isolates$organism <- convert_GRDI_ont_to_vmr_ids(db, isolates$organism, ont_table = "microbes")
  
  # Append to table
  n <- dbAppendTable(db, name = "isolates", value = isolates)
  message("Inserted ", n, " records into the isolate table")
}

#' Convenience function to update alternative isolate IDS with notes
#'
#' @inheritParams get_sample_ids
#' @param iso_ids user defined user ids
#' @param alt_ids alternative isolate id to add
#' @param notes notes for the alternative id
#' 
#' @export
insert_alternative_isolate_ids <- function(db, iso_ids, alt_ids, notes){

  if (length(notes)==1) notes <- rep(notes, length(iso_ids)) 

  dbExecute(
    db,
    "INSERT INTO alternative_isolate_ids (
      isolate_id, alternative_isolate_id, note
    ) VALUES (
      (SELECT id FROM isolates WHERE isolate_id = $1),
      $2, $3)
    ON CONFLICT ON CONSTRAINT alternative_isolate_ids_pkey DO NOTHING",
    list(iso_ids, alt_ids, notes))
}

#' Insert an extraction record
#' 
#' @export
new_extraction <- 
  function(db, df){
    
    # Get fields for sample table
    fields <- select_fields(db, "extractions", colnames(df))
    ext <- df[,fields]

    ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'extractions'")
    ext <- columns_to_ontology_ids(db, ext, ont_cols$column_name)
    
    insert_sql <- make_insert_sql(table_name = "extractions", field_names = fields)
     
    res <- dbGetQuery(db, insert_sql, unname(as.list(ext)))
    message("inserted ", length(res$id), " into extractions table")
    return(res$id)
}

#' Insert a sequence record
#' 
#' @export
new_sequence <- function(db, df){
    
    df$contact_information <- get_contact_information_id(db,   
                                                         lab = df$sequenced_by_laboratory_name,
                                                         email = df$sequenced_by_contact_email, 
                                                         name = df$sequenced_by_contact_name)
   
    fields <- select_fields(db, "sequencing", colnames(df))
    seq <- df[,fields]
    
    # Ontologu cols
    ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'sequencing'")
    seq <- columns_to_ontology_ids(db, seq, ont_cols$column_name)
    
    insert_sql <- make_insert_sql(table_name = "sequencing", field_names = fields)
    res <- dbGetQuery(db, insert_sql, unname(as.list(seq)))
    message("Inserted ", length(res$id), " records into sequencing")
    return(res$id)
}

#' Insert a WGS record, tying extraction to isolate
#' 
#' @export
insert_wgs_seq<- function(db, df){
  df$extraction_id <- new_extraction(vmr, df)
  df$sequencing_id <- new_sequence(vmr, df)
  insert_wgs_sql <- SQL("INSERT INTO wgs_extractions (isolate_id, extraction_id) VALUES ($1, $2)")
  res <- dbExecute(db, insert_wgs_sql, list(df$isolate_id, df$extraction_id))
  message("Inserted ", res, " records into WGS table")
}

#' Insert a MTG record, tying extraction to sample
#' 
#' @export
insert_mtg_seq<- function(db, df){
  df$extraction_id <- new_extraction(vmr, df)
  df$sequencing_id <- new_sequence(vmr, df)
  insert_mtg_sql <- SQL("INSERT INTO metagenomic_extractions (sample_id, extraction_id) VALUES ($1, $2)")
  res <- dbExecute(db, insert_mtg_sql, list(df$sample_id, df$extraction_id))
  message("Inserted ", res, " records into MTG table")
}
