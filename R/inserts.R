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
  x <- dbListFields(db, "isolates")
  fields <- x[! (x %in% c('id', 'inserted_by', 'inserted_at', 'was_updated')) ]
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
    x <- dbListFields(db, "extractions")
    fields <- x[! (x %in% c('id', 'inserted_by', 'inserted_at', 'was_updated')) ]
    ext <- df[,fields]

    ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'extractions'")
    ext <- columns_to_ontology_ids(db, ext, ont_cols$column_name)
    
    insert_sql <- make_insert_sql(table_name = "extractions", field_names = fields)
    res <- dbGetQuery(db, insert_sql, unname(params))
    return(res$id)
  }

#' Insert a sequence record
#' 
#' @export
new_sequence <- 
  function(db, 
           extraction_id = NA,
           library_id = NA,
           contact_information = NA,
           sequenced_by = NA, 
           sequencing_date = NA,
           sequencing_project_name = NA,
           sequencing_platform = NA,
           sequencing_instrument = NA,
           sequencing_assay_type = NA,
           dna_fragment_length = NA,
           genomic_target_enrichment_method = NA,
           genomic_target_enrichment_method_details = NA,
           amplicon_pcr_primer_scheme = NA,
           amplicon_size = NA,
           sequencing_flow_cell_version = NA,
           library_preparation_kit = NA,
           sequencing_protocol = NA,
           r1_fastq_filename = NA,
           r2_fastq_filename = NA,
           fast5_filename = NA,
           r1_irida_id = NA,
           r2_irida_id = NA){
    
    sql_args <- sql_args_to_uniform_list(environment())
    insert_sql <- make_insert_sql(table_name = "sequencing", field_names = names(sql_args))
    params <- sql_args_to_ontology_ids(db = db, 
                                       sql_arguments = sql_args, 
                                       ontology_columns = c("sequenced_by", 
                                                            "sequencing_platform", 
                                                            "sequencing_instrument", 
                                                            "sequencing_assay_type", 
                                                            "genomic_target_enrichment_method"))
    res <- dbGetQuery(db, insert_sql, unname(params))
    message("Inserted ", length(res$id), " records into sequencing")
    return(res$id)
  }

#' Insert a WGS record, tying extraction to isolate
#' 
#' @export
insert_wgs_ext <- function(db, extraction_id, isolate_id){
  insert_wgs_sql <- SQL("INSERT INTO wgs_extractions (isolate_id, extraction_id) VALUES ($1, $2)")
  res <- dbExecute(db, insert_wgs_sql, list(isolate_id, extraction_id))
  message("Inserted ", res, " records into WGS table")
}
