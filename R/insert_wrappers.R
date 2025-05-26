#' Insert and reporting function for sample metadata 
#' 
#' @export
report_and_insert_projects_samples_metadata <- function(db, df, commit = FALSE){
  if (!is.logical(commit)) stop('commit must be one of TRUE or FALSE')
  tryCatch(
    {
      dbBegin(db)
      
      pro_id <- repIns_project(db = db, df = df)

      sample_ids <- new_samples(db = db, 
                                sample_names = df$sample_collector_sample_ID, 
                                project_id = rep(pro_id, nrow(df)))
      print(paste("Inserted", length(sample_ids), "records into samples table"))

      df$sample_id <- sample_ids

      any_alts <- any(!is.na(df$alternative_sample_ID))

      alt_sam <- df %>% filter(!is.na(alternative_sample_ID))
      n_alt_sam <- 
        insert_alternative_sample_ids(db, 
                                      vmr_sample_id = alt_sam$sample_id,
                                      alt_id = alt_sam$alternative_sample_ID, 
                                      note = alt_sam$alternative_sample_id_note)
      print(paste("Inserted", n_alt_sam, "into alternative sample IDs table"))                                   
      contact <-
        df %>% 
        select(sample_collected_by_laboratory_name, sample_collector_contact_name, 
               sample_collector_contact_email) %>%
        distinct()
      
      con_id <-
        return_or_insert_contact_information(
          db, 
          lab = contact$sample_collected_by_laboratory_name, 
          email = contact$sample_collector_contact_email, 
          name = contact$sample_collector_contact_name)
      
      repIns_collection_info(db, df = df, contact_id = con_id)
      repIns_geo_data(db, df)
      repIns_food_data(db, df)
      repIns_host(db, df)
      repIns_env(db, df)
      repIns_ana(db, df) 
      if (commit) {
        message("Insertions complete, commiting") 
        dbCommit(db) 
      } else {
        message("Argument 'commit' set to FALSE - rolling back") 
        dbRollback(db) 
      }
    }, 
    error = function(cond){
      message("Error encountered, rolling back transaction")
      message(conditionMessage(cond))
      dbRollback(db) 
    }
  )
}