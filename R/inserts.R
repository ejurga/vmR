#' Insert new project into the VMR database
#' 
#' @inheritParams get_sample_ids
#' @param sample_plan_id VMR sample plan id
#' @param sample_plan_name The sample plan name
#' @param project_name The project name
#' @param description Description of the project
#' 
#' @export
new_project <- function(db, sample_plan_id, sample_plan_name, project_name = NA, 
                        description = NA){
  
  sql <- DBI::SQL(
    "INSERT INTO projects 
    (sample_plan_id, sample_plan_name, project_name, description)
    VALUES 
    ($1, $2, $3, $4)"
  )
  
  x <- dbExecute(db, sql, list(sample_plan_id,  
                               sample_plan_name, 
                               project_name,  
                               description))  
  
  return(paste("Inserted", x, "value into DB"))
  
}

#' Insert new samples into the samples table
#' 
#' @inheritParams get_sample_ids
#' @param sample_names sample name to insert
#' @param project_id VMR project ID associated with the sample 
#' 
#' @export
new_samples <- function(db, sample_names, project_id){
  
  sql <- DBI::SQL(
    "INSERT INTO samples 
    (sample_collector_sample_id, project_id)
    VALUES 
    ($1, $2)"
  )
  
  x <- dbExecute(db, sql, list(sample_names, project_id))
  
  return(paste("Inserted", x, "samples into DB")) 
  
}

#' Insert alternative sample ids
#' 
#' @inheritParams get_sample_ids
#' @param vmr_sample_id vector of VMR sample_ids 
#' @param alt_id The alternative ids of the samples
#' @param note A note describing the alternative ID
#' @export
insert_alternative_sample_ids <- function(db, vmr_sample_id, alt_id, note = NA){
  
  if (is.na(note)) note = rep(NA, length(vmr_sample_id))
  
  sql <- SQL(
    "INSERT INTO alternative_sample_ids  
    (sample_id, alternative_sample_id, note)
    VALUES 
    ($1, $2, $3)"
  )
  
  x <- dbExecute(db, sql, list(vmr_sample_id, alt_id, note))
  return(paste("Inserted", x, "alternative sample ids to DB")) 
}

#' Get id for contact information, inserting into the DB if necessary
#'
#' @inheritParams get_sample_ids
#' @param lab Laboratory name, passed onto collection_information.laboratory_name
#' @param name Contact name, passed onto collection_information.contact_name
#' @param email Contact email, passed onto collection_information.contact_email
#' 
#' @export
return_or_insert_contact_information <- function(db, lab="Not Provided [GENEPIO:0001668]",
                                                     name="Not Provided [GENEPIO:0001668]",
                                                     email="Not Provided [GENEPIO:0001668]", 
                                                     note=NA){

  id <- dbGetQuery(db,
                   "SELECT id 
                    FROM contact_information 
                    WHERE laboratory_name = $1 
                          AND
                          contact_name = $2 
                          AND
                          contact_email = $3
                          AND 
                          note = $4", 
                   list(lab, name, email, note)) %>% as_tibble()

  if (nrow(id)==0){
    message("Contact info not found, adding")
    id <- dbGetQuery(db,
               "INSERT INTO contact_information 
               (laboratory_name, contact_name, contact_email, note)
               VALUES 
               ($1, $2, $3, $4) 
               RETURNING id",
               list(lab, name, email, note)) %>% as_tibble()
    return_id <- id$id
  } else {
    return_id <- id$id
  }
}

#' Insert data into collection_information and associated tables
#'
#' Inserts data into the table "collection_information", 
#' 
#' @inheritParams get_sample_ids
#'
#' @export
insert_collection_information <- 
  function(db, 
           sample_id, 
           sample_collected_by = NA, 
           contact_information = NA, 
           sample_collection_date = NA,
           sample_collection_date_precision = NA, 
           presampling_activity_details = NA, 
           sample_received_date = NA, 
           original_sample_description = NA, 
           specimen_processing = NA, 
           sample_storage_method = NA, 
           sample_storage_medium = NA, 
           collection_device = NA, 
           collection_method = NA){
    
    sql_args <- sql_args_to_uniform_list(environment())
    insert_sql <- make_insert_sql(table_name = "collection_information", field_names = names(sql_args))
    params <- sql_args_to_ontology_ids(db = db,  
                                       sql_arguments = sql_args, 
                                       ontology_columns = c("sample_collected_by",  
                                                            "sample_collection_date_precision",  
                                                            "specimen_processing", 
                                                            "collection_device", 
                                                            "collection_method"))
    res <- dbExecute(db, insert_sql, unname(params))
    message("Inserted ", res, " records into collection_information")
}


#' Insert data into geo_loc and associated tables
#'
#' Inserts data into the table "geo_loc". 
#'
#' @inheritParams get_sample_ids
#' @param country As ontology terms with IDS
#' @param state_province_region As ontology terms with IDS
#' @param site as VMR site ID from 
#' @param latitude as point
#' @param lontitude as point
#' 
#' @export
insert_geo_loc <- function(db, sample_id, 
                           country = NA, 
                           state_province_region = NA, 
                           site = NA, 
                           latitude = NA, 
                           longitude = NA){
  
    sql_args <- sql_args_to_uniform_list(environment())
    
    insert_sql <- make_insert_sql(table_name = "geo_loc", field_names = names(sql_args))
   
    sql_args$country <- 
      convert_GRDI_ont_to_vmr_ids(db, sql_args$country, ont_table = "countries")
    sql_args$state_province_region <-
      convert_GRDI_ont_to_vmr_ids(db, sql_args$state_province_region, ont_table = "state_province_region")
    
    res <- dbExecute(db, insert_sql, unname(sql_args))
    message("Inserted ", res, " records into geo_loc table")
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
  dbExecute(
    db,
    "INSERT INTO alternative_isolate_ids (
      isolate_id, alternative_isolate_id, note
    ) VALUES (
      (SELECT id FROM isolates WHERE isolate_id = $1),
      $2, $3)
    ON CONFLICT ON CONSTRAINT alternative_isolate_ids_alternative_isolate_id_key DO NOTHING",
    list(iso_ids, alt_ids, notes))
}
