get_random_samples <- function(db, percent=1){
  df <- 
    dbGetQuery(db, 
               "SELECT * FROM full_sample_metadata WHERE sample_id IN (SELECT id FROM samples TABLESAMPLE SYSTEM ($1))", 
               list(percent)) |> as_tibble()
  return(df)
}

make_test_data <- function(db, percent=1){
  df <- get_random_samples(db, percent=percent)
  test_df <- dplyr::select(df, -sample_id, -project_description)
  test_df$alternative_sample_ids <- test_df$sample_collector_sample_id
  test_df$sample_collector_sample_id <- make_new_ids(test_df$sample_collector_sample_id, prepend = "test_sample-")
  test_df$sample_collection_project_name <- "Test data"
  test_df$original_sample_description <- "For testing inserts"
  test_df$sample_plan_id <- NA
  test_df$sample_plan_name <- NA
  return(test_df)
}