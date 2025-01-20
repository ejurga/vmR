#' Insert and reporting function for sample metadata 
#' 
#' @export
report_and_insert_projects_samples_metadata <- function(db, sample_df, host_df, env_df){
  tryCatch(
    {
      dbBegin(db)
      
      pro_id <- repIns_project(db = vmr, sample_df = sample_df)

      n_sam <- new_samples(vmr, 
                           sample_df$sample_collector_sample_ID,
                           project_id = rep(pro_id, nrow(samples)))
      print(paste("Inserted", n_sam, "records into samples table"))

      sample_df$sample_id <-
        get_sample_ids(vmr, sample_names = sample_df$sample_collector_sample_ID)  

      any_alts <- any(!is.na(sample_df$alternative_sample_ID))

      alt_sam <- sample_df %>% filter(!is.na(alternative_sample_ID))
      n_alt_sam <- 
        insert_alternative_sample_ids(db, vmr_sample_id = alt_sam$sample_id,
                                      alt_id = alt_sam$alternative_sample_ID)
      print(paste("Inserted", n_alt_sam, "into alternative sample IDs table"))                                   
      contact <-
        sample_df %>% 
        select(sample_collected_by_laboratory_name, sample_collector_contact_name, 
               sample_collector_contact_email) %>%
        distinct()
      
      con_id <-
        return_or_insert_contact_information(
          db, 
          lab = contact$sample_collected_by_laboratory_name, 
          email = contact$sample_collector_contact_email, 
          name = contact$sample_collector_contact_name)
      
      repIns_collection_info(db, sample_df = sample_df, contact_id = con_id)
      repIns_geo_data(db, sample_df = sample_df)
      repIns_food_data(db, sample_df = sample_df)
      repIns_host(db, sample_df = sample_df, host_df = host)
      repIns_env(db, sample_df = sample_df, env_df = env_data)
      repIns_ana(db, sample_df = sample_df)
      message("Insertions complete, commiting") 
      dbCommit(db)
    }, 
    error = function(cond){
      message("Error encountered, rolling back transaction")
      message(conditionMessage(cond))
      dbRollback(db) 
    }
  )
}