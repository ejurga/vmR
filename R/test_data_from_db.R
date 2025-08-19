get_random_samples <- function(db, percent=1){
  df <- 
    dbGetQuery(db, 
               "SELECT * FROM full_sample_metadata WHERE sample_id IN (SELECT id FROM samples TABLESAMPLE SYSTEM ($1))", 
               list(percent)) |> as_tibble()
  return(df)
}

make_new_ids <- function(n, prepend, width=4){
  n_pad <- stringr::str_pad(n, width = width, pad = "0")
  new_ids <- paste0(prepend, n_pad) 
  return(new_ids)
}

make_test_data <- function(db, percent=1){
  df <- get_random_samples(db, percent=percent)
  test_df <- dplyr::select(df, -sample_id, -project_description)
  test_df$alternative_sample_id <- test_df$sample_collector_sample_id
  test_df$sample_collector_sample_id <- make_new_ids(n = seq(1, nrow(test_df)), prepend = "test_sample-")
  test_df$sample_collection_project_name <- "Test data"
  test_df$original_sample_description <- "For testing inserts"
  test_df$sample_plan_id <- NA
  test_df$sample_plan_name <- NA
  test_df$sample_collector_contact_name[is.na(test_df$sample_collector_contact_name)] <- "Mr. Test"
  return(test_df)
}

get_random_isolates <- function(db, percent=1){
  df <- 
    dbGetQuery(db, 
               "SELECT * FROM isolates_wide WHERE isolate_id IN (SELECT id FROM isolates TABLESAMPLE SYSTEM ($1))", 
               list(percent)) |> as_tibble()
  df$alternative_isolate_ids <- df$user_isolate_id
  df <- df %>% select(-user_isolate_id)
  df$isolate_id <- make_new_ids(n = seq(1, nrow(df)), prepend = "test_isolate-") 
  return(df)
}