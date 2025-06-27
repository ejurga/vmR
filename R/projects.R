#' Insert new project into the VMR database
#' 
#' @inheritParams get_sample_ids
#' @param df the dataframe with the project columns sample plan id
#' @param description Description of the project
#' 
#' @export
new_project <- function(db, df, description = NA){

  pro <- get_project_fields(df) %>% distinct()
  pro$description <- description
  
  sql <- DBI::SQL(
    "INSERT INTO projects
    (sample_plan_id, sample_plan_name, project_name, description)
    VALUES
    ($1, $2, $3, $4)
    RETURNING id"
  )

  x <- dbGetQuery(db, sql, list(pro$sample_plan_id,
                                pro$sample_plan_name,
                                pro$sample_collection_project_name, 
                                pro$description))
  
  message("inserted ", length(x$id), " into projects table")
  return(x$id)
}

#' Report and Insert project information
#' 
#' @export
report_projects <- function(df){

  pro <- get_project_fields(df) %>% distinct()

  cat("Given Project Information:\n")
  print(pro)
  cat("\n")
}

get_project_fields <- function(df){
  project_fields <- c("sample_collection_project_name", "sample_plan_name", "sample_plan_id")
  sel <- select(df, any_of(project_fields))
  return(sel)
}
  


