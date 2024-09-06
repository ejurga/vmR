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
                                                     email="Not Provided [GENEPIO:0001668]"){

  id <- dbGetQuery(db,
                   "SELECT id from contact_information WHERE
                   laboratory_name = $1 AND
                   contact_name = $2 AND
                   contact_email = $3",
                   list(lab, name, email)) %>% as_tibble()

  if (nrow(id)==0){
    message("Contact info not found, adding")
    id <- dbGetQuery(db,
               "INSERT INTO contact_information (laboratory_name, contact_name, contact_email)
               VALUES ($1, $2, $3) RETURNING id",
               list(lab, name, email)) %>% as_tibble()
    return_id <- id$id
  } else {
    return_id <- id$id
  }
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
insert_collection_information <- function(db, df){
  message("Inserting into collection_information")
  insert_sql <-
    "INSERT INTO collection_information (
      sample_id,
      sample_collected_by,
      contact_information,
      sample_collection_project_name,
      sample_collection_date,
      sample_collection_date_precision,
      presampling_activity_details,
      sample_received_date,
      original_sample_description,
      specimen_processing,
      sample_storage_method,
      sample_storage_medium,
      collection_device,
      collection_method,
      sample_plan
      ) VALUES (
      $1,
      (select (id) from ontology_terms where ontology_id = $2),
      $3,
      $4,
      $5,
      (select (id) from ontology_terms where ontology_id = $6),
      $7,
      $8,
      $9,
      (select (id) from ontology_terms where ontology_id = $10),
      $11,
      $12,
      (select (id) from ontology_terms where ontology_id = $13),
      (select (id) from ontology_terms where ontology_id = $14),
      $15) RETURNING id, sample_id"

    params <- list()
    params[[1]] <- df$sample_id
    params[[2]] <- df$sample_collected_by |> extract_ont_id()
    params[[3]] <- df$contact_information
    params[[4]] <- df$sample_collection_project_name
    params[[5]] <- df$sample_collection_date
    params[[6]] <- df$sample_collection_date_precision |> extract_ont_id()
    params[[7]] <- df$presampling_activity_details
    params[[8]] <- df$sample_received_date
    params[[9]] <- df$original_sample_description
    params[[10]] <- df$specimen_processing |> extract_ont_id()
    params[[11]] <- df$sample_storage_method
    params[[12]] <- df$sample_storage_medium
    params[[13]] <- df$collection_device |> extract_ont_id()
    params[[14]] <- df$collection_method |> extract_ont_id()
    params[[15]] <- df$sample_plan

    params_no_null <- replace_null_params_with_na(params)

    ids <- sendBindFetch(db, sql = insert_sql, params = params_no_null)

    df <- left_join(ids, df, by = "sample_id")

    populate_multi_choice_table(db = db, df = df,
                                main_table = "collection_information",
                                grdi_col = "purpose_of_sampling",
                                col_table = "sample_purpose")

    populate_multi_choice_table(db = db, df = df,
                                main_table = "collection_information",
                                grdi_col = "presampling_activity",
                                col_table = "sample_activity")

    message("Done, call dbCommit or dbRollback")
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
insert_alternative_isolate_ids <- function(db, sample_ids, iso_ids, alt_ids, notes){
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
