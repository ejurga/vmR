#' Take the common and scientific name columns and convert to a single ID 
#' 
#' Also checks to make sure that those two columns were the same ontology 
#' term, otherwise, it is probably an error. 
#' 
#' @inheritParams get_sample_ids
#' @param common_name host_common_name field 
#' @param scientific_name host_scientific_name field
#' 
#' @export
host_organisms_to_ids <- function(db, common_name, scientific_name){
 
  # Remove grdi NULLs and set to NA 
  common_name <- set_grdi_nulls_to_NA(x = common_name)
  scientific_name <- set_grdi_nulls_to_NA(x = scientific_name)
  # Get only the ontology ids 
  common_ids <- extract_ont_id(x = common_name)
  science_ids <- extract_ont_id(x = scientific_name)
  # Are they equal?
  is.eq <- common_ids==science_ids
  if (!all(is.eq, na.rm = TRUE)){
    stop("common name and scientific name ontologies mismatched")
  }
  # Replace any NAs in one vector with the value from the other
  ontology_terms <- ifelse(is.na(common_name), scientific_name, common_name)
  # Get the Ids
  host_org_ids <- convert_GRDI_ont_to_vmr_ids(db, ontology_terms, ont_table = "host_organisms")
  return(host_org_ids)
}

#' Get id for contact information, inserting into the DB if necessary
#'
#' @inheritParams get_sample_ids
#' @param lab Laboratory name, passed onto collection_information.laboratory_name
#' @param name Contact name, passed onto collection_information.contact_name
#' @param email Contact email, passed onto collection_information.contact_email
#' 
#' @export
get_contact_information_id <- function(db, lab, name, email){

  lab[is.na(lab)]     <- "Not Provided [GENEPIO:0001668]"
  email[is.na(email)] <- "Not Provided [GENEPIO:0001668]"
  name[is.na(name)] <- "Not Provided [GENEPIO:0001668]"
   
  x <- 
    dbGetQuery(vmr, 
      "WITH sel AS (
        SELECT id, 
               laboratory_name,
               contact_name,
               contact_email
          FROM contact_information
         WHERE     laboratory_name = $1
               AND contact_name = $2
               AND contact_email = $3
      ),   ins AS ( 
        INSERT INTO contact_information (laboratory_name, contact_name, contact_email)
        SELECT $1, $2, $3
        WHERE NOT EXISTS (select id FROM sel)
        RETURNING id, laboratory_name, contact_name, contact_email
      )
      SELECT id FROM sel
      UNION 
      select id FROM ins",
      list(lab, name, email))
  return(x$id)
}

#' Insert alternative sample ids
#' 
#' @inheritParams get_sample_ids
#' @param vmr_sample_id vector of VMR sample_ids 
#' @param alt_id The alternative ids of the samples
#' @param note A note describing the alternative ID
#' @export
insert_alternative_sample_ids <- function(db, vmr_sample_id, alt_id, note = NA){
  
  if (length(note)==1) note = rep(note, length(vmr_sample_id))
  
  sql <- SQL(
    "INSERT INTO alternative_sample_ids  
    (sample_id, alternative_sample_id, note)
    VALUES 
    ($1, $2, $3)"
  )
  
  x <- dbExecute(db, sql, list(vmr_sample_id, alt_id, note))
  return(x) 
}

#' Insert sample metadata
#' 
#' @inheritParams get_sample_ids
#' @param df dataframe with sample metadata columns to insert
#' 
#' @export
insert_sample_metadata <- function(db, df){

  df <- df %>% select(-any_of(c("sample_collection_project_name", "sample_plan_id", "sample_plan_name")))
  
  # Deal with contact information
  df$contact_information <- get_contact_information_id(db, 
                                                       lab = df$sample_collected_by_laboratory_name,  
                                                       email = df$sample_collector_contact_email,  
                                                       name = df$sample_collector_contact_name)
  df <- select(df, -sample_collector_contact_name, -sample_collector_contact_email, -sample_collected_by_laboratory_name)
  
  # Deal with host organism
  df$host_organism <- host_organisms_to_ids(db = db, common_name = df$host_common_name, df$host_scientific_name)
  df <- select(df, -host_common_name, -host_scientific_name)
  
  # Get fields for sample table
  x <- dbListFields(db, "samples")
  fields <- x[!(x %in% c('id', 'inserted_by', 'inserted_at', 'was_updated'))]
  samples <- df[,fields]

  # Ontology columns
  ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'samples'")
  cnt_cols <- dbGetQuery(db, "SELECT column_name FROM country_cols")
  samples <- columns_to_ontology_ids(db, samples, ont_cols$column_name)
  samples <- columns_to_ontology_ids(db, samples, cnt_cols$column_name, ont_table = "countries")
  samples$geo_loc_name_state_province_region <- 
    convert_GRDI_ont_to_vmr_ids(db, x = samples$geo_loc_name_state_province_region, ont_table = "state_province_region")
  
  # Append to table
  n <- dbAppendTable(db, name = "samples", value = samples)
  message("Inserted ", n, " records into the sample table")

  # get sample ids
  df$sample_id <- get_sample_ids(db, samples$sample_collector_sample_id)
  # alt ids
  insert_alternative_sample_ids(db = db, vmr_sample_id = df$sample_id, alt_id = df$alternative_sample_ids)
  df <- select(df, -alternative_sample_ids)

  # Finally, multi choice tables
  multi_choice_tables <-
    c("presampling_activity",                           "purpose_of_sampling",                              "body_product",
      "anatomical_material",                            "anatomical_part",                                  "label_claim",
      "food_packaging",                                 "food_product",                                     "food_product_properties",
      "animal_source_of_food",                          "available_data_types",                             "animal_or_plant_population",
      "environmental_material",                         "environmental_material_constituent",               "environmental_site",
      "sampling_weather_conditions",                    "presampling_weather_conditions",                   "experimental_intervention")
  
  db_tables <- 
    c("sample_activity",                                "sample_purposes",                                   "anatomical_data_body",
      "anatomical_data_material",                       "anatomical_data_part",                              "food_data_label_claims", 
      "food_data_packaging",                            "food_data_product",                                 "food_data_product_property",
      "food_data_source",                               "environmental_data_available_data_type",            "environmental_data_animal_plant", 
      "environmental_data_material",                    "environmental_data_material_constituents",          "environmental_data_site",
      "environmental_data_sampling_weather_conditions", "environmental_data_presampling_weather_conditions", "risk_activity")
  
  is_ont <- rep(TRUE,length(db_tables))
  is_ont[is_ont=="environmental_data_material_constituents"] <- FALSE
  
  for ( i in seq(length(multi_choice_tables)) ){
    insert_into_multi_choice_table(db,
                                   ids = df$sample_id,
                                   vals = df[[multi_choice_tables[i]]],
                                   table = db_tables[i], 
                                   is_ontology = is_ont[i])
  }

  # Check for extra columns not inserted.
  remaining_cols <- colnames(df)[!colnames(df) %in% c(fields, multi_choice_tables, 'sample_id')]
  if (length(remaining_cols)!=0) message("Warning: extra cols not dealt with:", paste0(remaining_cols, collapse = ", "))

  message("Insertions complete")
}