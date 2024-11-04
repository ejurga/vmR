#' Insert new project into the VMR database
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
#' @param db [DBI] connection
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

#' Set user isolate IDs to VMR ids from all possible alternative ids
#'
#' Attempt to use the possible_isolate_names view to set the user 
#' isolate IDs to VMR isolate_IDs
#'
#' @param db [DBI] connection
#' @param x The vector to attempt to convert to VMR IDs
#' @export
#'
set_vmr_isolate_id_from_alternates <- function(db, x){
  vmrPos <- dbReadTable(db, "possible_isolate_names") %>% as_tibble()
  m <- match(x, vmrPos$isolate_collector_id)
  res <- vmrPos$isolate_id[m]
  if (anyNA(res)) message("NAs detected, total: ", sum(is.na(res)))
  return(res)  
}


#' Insert data into collection_information and associated tables
#'
#' Inserts data into the table "collection_information", and the
#' associated multi-choice tables "sample_purpose" and "sample_activity".
#'
#' @param db a [DBI] connection to the VMR
#' @param df a dataframe of the relevant GRDI fields.
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
#' Inserts data into the table "geo_loc". Will check for existing entry
#'
#'
#' @param db a [DBI] connection to the VMR
#' @param df a dataframe of the relevant GRDI fields.
#'
#' @export
insert_geo_loc <- function(db, df){

  sql_str <-
    glue::glue_sql(.con = db,
      "INSERT INTO geo_loc (
        sample_id,
        geo_loc_name_country,
        geo_loc_name_state_province_region,
        geo_loc_name_site,
        geo_loc_latitude,
        geo_loc_longitude
      ) VALUES (
        $1,
        (select (id) from ontology_terms where ontology_id = $2),
        (select (id) from ontology_terms where ontology_id = $3),
        (select (id) from ontology_terms where ontology_id = $4),
        $5, $6)
      RETURNING id, sample_id")

  params <- list()
  params[[1]] <- df$sample_id
  params[[2]] <- df$`geo_loc_name (country)` |> extract_ont_id()
  params[[3]] <- df$`geo_loc_name (state/province/region)` |> extract_ont_id()
  params[[4]] <- check_for_existing_geo_loc_site(db, df$`geo_loc_name (site)`)
  params[[5]] <- df$`geo_loc latitude`
  params[[6]] <- df$`geo_loc longitude`

  params_no_null <- replace_null_params_with_na(params)

  ids <- sendBindFetch(db = db, sql = sql_str, params = params_no_null,
                       verbose = T)
}

#' Checks for entry in geo_loc_site and adds a new one if it doesn't exist
#'
#' @param db connection to VRM
#' @param x Vector of GRDI column geo_loc_name (site)
#'
#' @export
check_for_existing_geo_loc_site <- function(db, x){

  fac <- factor(x)

  vals_in_db <- character(0)

  while (!all(levels(fac) %in% vals_in_db)){

    res <-
      sendBindFetch(db,
                    sql = "SELECT * FROM geo_loc_name_sites WHERE geo_loc_name_site = $1",
                    params = list(levels(fac)))

    vals_in_db <- res$geo_loc_name_site

    vals_to_add <- levels(fac)[!levels(fac) %in% vals_in_db]
    if (length(vals_to_add)>0){
      message("Values not found, Adding to geo_loc_name_sites: ",
              paste(vals_to_add, collapse = ", "))
      insertBind(db,
                 sql = "INSERT INTO geo_loc_name_sites (geo_loc_name_site) VALUES ($1)",
                 params = list(vals_to_add))
    }
  }

  fixed <-
    factor(x, levels = res$geo_loc_name_site, labels = res$id) |>
    as.character() |> as.integer()

  return(fixed)
}

#' Convenience function to update alternative isolate IDS with notes
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
