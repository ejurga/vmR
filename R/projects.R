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
    ($1, $2, $3, $4)
    RETURNING id"
  )

  x <- dbGetQuery(db, sql, list(sample_plan_id,
                                sample_plan_name,
                                project_name,
                                description))
  return(x$id)
}

#' Report and Insert project information
#' 
#' @export
repIns_project <- function(db, df){

  pro <-
    df %>%
    select(sample_collection_project_name, sample_plan_name, sample_plan_ID) %>%
    distinct()

  cat("Given Project Information:\n")
  print(pro)

  id <- new_project(db, 
                    sample_plan_id = pro$sample_plan_ID, 
                    sample_plan_name = pro$sample_plan_name, 
                    project_name = pro$sample_collection_project_name)
  
  print(paste("Inserted", length(id), "record into Projects"))

  return(id)
}