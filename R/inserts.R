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
                          contact_email = $3", 
                   list(lab, name, email)) %>% as_tibble()

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

#' Insert data into the food data table 
#' 
#' @export
insert_food_data <- 
  function(db, 
           sample_id, 
           food_product_production_stream = NA,
           food_product_origin_country = NA, 
           food_packaging_date = NA, 
           food_quality_date = NA){
    
    sql_args <- sql_args_to_uniform_list(environment())
    
    insert_sql <- make_insert_sql(table_name = "food_data", field_names = names(sql_args))
    
    sql_args$food_product_production_stream <- 
      convert_GRDI_ont_to_vmr_ids(db, sql_args$food_product_production_stream)
    sql_args$food_product_origin_country <- 
      convert_GRDI_ont_to_vmr_ids(db, sql_args$food_product_origin_country, ont_table = "countries")
    
    res <- dbExecute(db, insert_sql, unname(sql_args))
    message("Inserted ", res, " records into collection_information")
  }

#' Insert data into the environmental data table
#' 
#' @export
insert_env_data <- function(db, 
                            sample_id,
                            air_temperature = NA,
                            air_temperature_units = NA,
                            water_temperature = NA,
                            water_temperature_units = NA,
                            sediment_depth = NA,
                            sediment_depth_units = NA,
                            water_depth = NA,
                            water_depth_units = NA,
                            available_data_type_details = NA){
   
    sql_args <- sql_args_to_uniform_list(environment())

    insert_sql <- make_insert_sql(table_name = "environmental_data", field_names = names(sql_args))
    
    params <- sql_args_to_ontology_ids(db = db,  
                                       sql_arguments = sql_args, 
                                       ontology_columns = names(sql_args)[grepl(x = names(sql_args), "units")])
    
    res <- dbExecute(db, insert_sql, unname(params))
    message("Inserted ", res, " records into environmental_data table")
}

#' Insert into the multi-choice tables for the environmental data 
#' 
#' @export
insert_env_multi_choices <- 
  function(env_data_id,
           animal_or_plant_population = NA, 
           available_data_types = NA,
           environmental_material_constituent = NA,
           environmental_material = NA, 
           environmental_site = NA, 
           weather_type = NA){
 
   argg <- c(as.list(environment()))
   argg <- argg[!names(argg)=="env_data_id"] 
    # Mapping                  
   table_map <- list(
     animal_or_plant_population =  list(vmr_table = "environmental_data_animal_plant",   is_ontology = TRUE), 
     available_data_types = list(vmr_table = "environmental_data_available_data_type", is_ontology = TRUE),
     environmental_material_constituent =  list(vmr_table = "environmental_data_material_constituents", is_ontology = FALSE),
     environmental_material =  list(vmr_table = "environmental_data_material", is_ontology = TRUE),
     environmental_site = list(vmr_table = "environmental_data_site", is_ontology = TRUE),
     weather_type = list(vmr_table = "environmental_data_weather_type", is_ontology = TRUE) )

    # Insert into all the tables
    for (col in names(argg)){
     insert_into_multi_choice_table(vmr, 
                                     ids = env_data_id, 
                                     table = table_map[[col]]$vmr_table, 
                                     vals = argg[[col]],
                                     is_ontology = table_map[[col]]$is_ontology)
    }
  }

#' Insert data into the hosts table 
#' 
#' @export
insert_host_data <- 
  function(db,
           sample_id, 
           host_organism = NA,
           host_ecotype = NA,
           host_breed = NA,
           host_food_production_name = NA,
           host_disease = NA,
           host_age_bin = NA,
           host_origin_geo_loc_name_country = NA){
    sql_args <- sql_args_to_uniform_list(environment())
    insert_sql <- make_insert_sql(table_name = "hosts", field_names = names(sql_args))
    params <- sql_args_to_ontology_ids(db = db,  
                                       sql_arguments = sql_args, 
                                       ontology_columns = c("host_food_production_name", 
                                                            "host_age_bin"))
    params$host_origin_geo_loc_name_country <- 
      convert_GRDI_ont_to_vmr_ids(db, params$host_origin_geo_loc_name_country, ont_table = "countries")
                                          
    res <- dbExecute(db, insert_sql, unname(params))
    message("Inserted ", res, " records into hosts table")
  }

#' Insert data into the anatomical data table 
#' 
#' @inheritParams get_sample_ids 
#' 
#' @export 
insert_anatomical_data <- 
  function(sample_id,
           anatomical_region = NA){
  
    sql_args <- sql_args_to_uniform_list(environment())

    insert_sql <- make_insert_sql(table_name = "anatomical_data", field_names = names(sql_args))
    sql_args$anatomical_region <- convert_GRDI_ont_to_vmr_ids(db, sql_args$anatomical_region)
                                          
    res <- dbExecute(db, insert_sql, unname(sql_args))
    message("Inserted ", res, " records into anatomical data table")
}

#' Insert into the multi-choice tables for the anatomical data
#' 
#' @export
insert_anatomical_multi_choices <- 
  function(anatomy_ids, 
           body_product = NA, 
           anatomical_material = NA, 
           anatomical_part = NA){
           
   argg <- c(as.list(environment()))
   argg <- argg[!names(argg)=="anatomy_ids"] 
    # Mapping                  
   table_map <- list(
     body_product =  list(vmr_table = "anatomical_data_body",   is_ontology = TRUE), 
     anatomical_material = list(vmr_table = "anatomical_data_material", is_ontology = TRUE),
     anatomical_part =  list(vmr_table = "anatomical_data_part", is_ontology = TRUE))

    # Insert into all the tables
    for (col in names(argg)){
     insert_into_multi_choice_table(vmr, 
                                     ids = anatomy_ids, 
                                     table = table_map[[col]]$vmr_table, 
                                     vals = argg[[col]],
                                     is_ontology = table_map[[col]]$is_ontology)
    }
  }

#' Insert new data into isolate table of the VMR
#' 
#' @export
#' 
insert_isolate_data <- 
  function(db, 
           sample_id,  
           isolate_id, 
           organism = NA, 
           strain = NA,
           microbiological_method = NA, 
           progeny_isolate_id = NA, 
           isolated_by = NA, 
           contact_information = NA, 
           isolation_date = NA, 
           isolate_received_date = NA, 
           taxonomic_identification_process = NA, 
           taxonomic_identification_process_details = NA, 
           serovar = NA, 
           serotyping_method = NA, 
           phagetype = NA, 
           irida_sample_id = NA, 
           irida_project_id = NA, 
           biosample_id = NA, 
           bioproject_id = NA){
  
    sql_args <- sql_args_to_uniform_list(environment())

    insert_sql <- make_insert_sql(table_name = "isolates", field_names = names(sql_args))
    
    params <- sql_args_to_ontology_ids(db = db,  
                                       sql_arguments = sql_args, 
                                       ontology_columns = c("isolated_by",  
                                                            "taxonomic_identification_process"))
    
    params$organism <- convert_GRDI_ont_to_vmr_ids(db, params$organism, "microbes")                 

    res <- dbExecute(db, insert_sql, unname(params))
    message("Inserted ", res, " records into isolates data table")
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

new_extraction <- 
  function(db, 
           experimental_protocol_field = NA,
           experimental_specimen_role_type = NA,
           nucleic_acid_extraction_method = NA,
           nucleic_acid_extraction_kit = NA,
           sample_volume_measurement_value = NA,
           sample_volume_measurement_unit = NA,
           residual_sample_status = NA,
           sample_storage_duration_value = NA,
           sample_storage_duration_unit = NA,
           nucleic_acid_storage_duration_value = NA,
           nucleic_acid_storage_duration_unit = NA){
    
    sql_args <- sql_args_to_uniform_list(environment())

    insert_sql <- make_insert_sql(table_name = "extractions", field_names = names(sql_args))
    insert_sql <- glue::glue_sql(insert_sql, "RETURNING id", .sep = ' ')
    params <- sql_args_to_ontology_ids(db = db, 
                                       sql_arguments = sql_args, 
                                       ontology_columns = c("experimental_specimen_role_type",  
                                                            "sample_volume_measurement_unit",
                                                            "sample_storage_duration_unit", 
                                                            "nucleic_acid_storage_duration_unit"))
    res <- dbGetQuery(db, insert_sql, unname(params))
    return(res$id)
  }

new_sequence <- 
  function(db, 
           extraction_id = NA,
           contact_information = NA,
           sequenced_by = NA, 
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
           assembly_filename = NA,
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
    res <- dbExecute(db, insert_sql, unname(params))
    message("Inserted ", res, " records into sequencing")
  }

insert_wgs_ext <- function(db, extraction_id, isolate_id){
  insert_wgs_sql <- SQL("INSERT INTO wgs_extractions (isolate_id, extraction_id) VALUES ($1, $2)")
  res <- dbExecute(db, insert_wgs_sql, list(isolate_id, extraction_id))
  message("Inserted ", res, " records into WGS table")
}