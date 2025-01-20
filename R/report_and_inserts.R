
#' Report and Insert project information
#' 
#' @param sample_df samples
#' 
#' @export
repIns_project <- function(db, sample_df){

  pro <-
    sample_df %>% 
    select(sample_collection_project_name, sample_plan_name, sample_plan_ID) %>% 
    distinct()

  cat("Given Project Information:\n")
  print(pro)

  x <- new_project(db, 
                   sample_plan_id = pro$sample_plan_ID,  
                   sample_plan_name = pro$sample_plan_name,  
                   project_name = pro$sample_collection_project_name)
  
  print(paste("Inserted", x, "record into Projects"))

  pro <- dbGetQuery(db, "SELECT id FROM projects WHERE sample_plan_name = $1",  
                       list(pro$sample_plan_name))
  
  return(pro$id)
} 

#' Report and insert collection info
#' 
#' @param db
#' @param sample_df 
#' @param contact_id
#' 
#' @export
repIns_collection_info <- function(db, sample_df, contact_id){

  col_columns <- c(
    "sample_collected_by", "sample_collection_date", 
    "sample_collection_date_precision",  "presampling_activity_details",  
    "sample_received_date",  "specimen_processing",  "sample_storage_method", 
    "sample_storage_medium",  "collection_device",  "collection_method", 
    "purpose_of_sampling", "presampling_activity")

  col_info <- 
    sample_df %>% 
    select(sample_id, sample_collector_sample_ID, all_of(col_columns))
  
  print_tallies_of_columns(col_info)

  col_info$contact_information <- contact_id

  x <- 
    insert_collection_information(
      db = db, 
      sample_id = col_info$sample_id,
      sample_collected_by = col_info$sample_collected_by, 
      contact_information = col_info$contact_information, 
      sample_collection_date = col_info$sample_collection_date, 
      sample_collection_date_precision = col_info$sample_collection_date_precision, 
      presampling_activity_details = col_info$presampling_activity_details, 
      sample_received_date = col_info$sample_received_date, 
      specimen_processing = col_info$specimen_processing, 
      sample_storage_method = col_info$sample_storage_method, 
      sample_storage_medium = col_info$sample_storage_medium, 
      collection_device = col_info$collection_device, 
      collection_method = col_info$collection_method)
  
  print(paste("Inserted", x, "records into collection information table"))

  collection_ids <- 
    dbGetQuery(db, "SELECT id FROM collection_information WHERE sample_id = $1",  
               list(col_info$sample_id)) %>%
    pull(id)
  
  insert_into_multi_choice_table(db, 
                                 ids = collection_ids, 
                                 vals = samples$purpose_of_sampling, 
                                 table = "sample_purposes", 
                                 is_ontology = TRUE)

  insert_into_multi_choice_table(db, 
                                 ids = collection_ids, 
                                 vals = samples$presampling_activity, 
                                 table = "sample_activity", 
                                 is_ontology = TRUE)
}

#' Report and insert geo data
#' 
#' @export
repIns_geo_data <- function(db, sample_df){
  geo <- sample_df %>% select(sample_id, matches("^geo"))
  print_tallies_of_columns(geo)
  geo$site_ids <- insert_or_return_geo_site(db, geo$geo_loc_name_site)
  x <- insert_geo_loc(db, 
                      sample_id = geo$sample_id,
                      country = geo$geo_loc_name_country,
                      state_province_region = geo$geo_loc_name_state_province_region,
                      site = geo$site_ids,
                      latitude = geo$geo_loc_latitude,
                      longitude = geo$geo_loc_longitude)
  print(paste("Inserted", x, "records into geo table"))
}

#' Report and insert food data
#' 
#' @export
repIns_food_data <- function(db, sample_df){

  food <-
    sample_df %>% 
    select(sample_id, matches("food")) %>%
    mutate(across(matches("food"), 
                  ~replace(.x, which(.x %in% grdiamR::get_null_value()), NA))) %>%
    filter(!if_all(matches("food"), ~is.na(.x)))
  
  print_tallies_of_columns(food)

  x <- 
    insert_food_data(
      db,  
      sample_id = food$sample_id, 
      food_product_production_stream = food$food_product_production_stream,
      food_product_origin_country = food$food_product_origin_geo_loc_country,
      food_packaging_date = food$food_packaging_date,
      food_quality_date = food$food_quality_date)
  
  print(paste("Inserted", x, "records into food data table"))
  
  food$id <- 
    dbGetQuery(db, "SELECT id FROM food_data WHERE sample_id = $1", 
               list(food$sample_id)) %>% pull(id)
  
  vals <-  list(food$food_product,    food$food_product_properties,  
                food$food_packaging,  food$animal_source_of_food)
  
  table_names <-  c("food_data_product",   "food_data_product_property",  
                    "food_data_packaging",   "food_data_source")
  
  mapply(FUN = insert_into_multi_choice_table, 
         vals = vals, 
         table = table_names,
         MoreArgs = list(db = db, ids = food$id, is_ontology = TRUE))
}

#' Report and insert host data
#' 
#' @export
repIns_host <- function(db, sample_df, host_df){
  
  host <-
    sample_df %>% 
    select(sample_collector_sample_ID, sample_id, matches("host")) %>% 
    left_join(host_df, by = "sample_collector_sample_ID") %>%
    mutate(across(everything(), 
                  ~replace(.x, which(.x %in% grdiamR::get_null_value()), NA))) %>%
    filter(!if_all(everything(), ~is.na(.x)))
  
  host$host_organism_ids <- 
    host_organisms_to_ids(db, 
                          common_name = host$host_common_name,  
                          scientific_name = host$host_scientific_name)
  
  print_tallies_of_columns(host)
  
  x <- 
    insert_host_data(
      db,
      sample_id = host$sample_id,
      host_organism = host$host_organism_ids,
      host_ecotype = host$host_ecotype,
      host_breed = host$host_breed,
      host_food_production_name = host$host_food_production_name,  
      host_disease = host$host_disease,
      host_age_bin = host$host_age_bin,
      host_origin_geo_loc_name_country = host$host_origin_geo_loc_country)
  
  print(paste("Inserted", x, "records into food data table"))
    
}

#' Report and insert environmental data
#' 
#' @export
repIns_env <- function(db, sample_df, env_df){
  env_df$sample_id <- get_sample_ids(db, env_data$sample_collector_sample_ID)

  env <- 
    sample_df %>% 
    select(sample_id, environmental_material, environmental_site, 
           animal_or_plant_population, available_data_types) %>%
    left_join(env_df, by = "sample_id", relationship = "one-to-one") %>%
    mutate(across(everything(), 
                  ~replace(.x, which(.x %in% grdiamR::get_null_value()), NA))) %>%
    filter(!if_all(everything(), ~is.na(.x)))

  print_tallies_of_columns(env)

  x <- insert_env_data(db, 
                       sample_id = env$sample_id,
                       air_temperature = env$air_temperature,
                       air_temperature_units = env$air_temperature_units,
                       water_temperature = env$water_temperature,
                       water_temperature_units = env$water_temperature_units,
                       sediment_depth = env$sediment_depth,
                       sediment_depth_units = env$sediment_depth_units,
                       water_depth = env$water_depth,
                       water_depth_units = env$water_depth_units,
                       available_data_type_details = env$available_data_type_details)
  print(paste("Inserted", x, "into environmental data table"))
  
  env$id <- 
    dbGetQuery(db, "SELECT id FROM environmental_data WHERE sample_id = $1", 
               list(env$sample_id)) %>% 
    pull(id)
 
  insert_env_multi_choices(db,
                           env_data_id = env$id, 
                           animal_or_plant_population = env$animal_or_plant_population, 
                           available_data_types = env$available_data_types, 
                           environmental_material = env$environmental_material, 
                           environmental_site = env$environmental_site, 
                           weather_type = env$weather_type)
}

#' Report and insert anatomical data 
#' 
#' @export
repIns_ana <- function(db, sample_df){
  
  ana <-
    sample_df %>% 
    select(sample_id, anatomical_region, body_product, anatomical_material, 
           anatomical_part) %>% 
    mutate(across(everything(), 
                  ~replace(.x, which(.x %in% grdiamR::get_null_value()), NA))) %>%
    filter(!if_all(-sample_id, ~is.na(.x)))

  print_tallies_of_columns(ana)

  x <- insert_anatomical_data(db,
                              sample_id = ana$sample_id,
                              anatomical_region = ana$anatomical_region)
  print(paste("Inserted", x, "into anatomy data"))

  ana$ids <- dbGetQuery(db, "SELECT id FROM anatomical_data WHERE sample_id = $1", 
                        list(ana$sample_id)) %>% pull(id)

  insert_anatomical_multi_choices(db,
                                  anatomy_ids = ana$ids, 
                                  body_product = ana$body_product, 
                                  anatomical_material = ana$anatomical_material, 
                                  anatomical_part = ana$anatomical_part)
}