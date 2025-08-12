#' amr_regexes
#'
#' Rerturn a vector to filter out AMR columns
#'
#' @export
amr_regexes <-function(){
    c("_resistance_phenotype$",
      "_measurement(_units|_sign){0,1}$",
      "_laboratory_typing_[a-z_]+$",
      "_vendor_name$",
      "_testing_standard(_(version|details)){0,1}$",
      "_(resistant|intermediate|susceptible)_breakpoint$")
}

#' Insert AST data into the database based on the wide form template
#' 
#' @export
insert_amr_from_wide_form_template <- function(db,df){  
  df$amr_test_id <- new_amr_test(db, df) 
  message("Inserted ", length(df$amr_test_id), " tests into db")
  long <- long_form_antibiotics(df)
  ont <- vmR::amatch_term(db, x=unique(long$antibiotic), lookup_table = 'antimicrobial_agents')
  long$antibiotic <- ont$ontology_id[match(long$antibiotic, ont$query)]
  x <- append_to_amr_table(db, long)
  message("Appended ", x, " records into amr_antibiotics_profile table")
}

#' Insert an isolates AMR test information 
#' 
new_amr_test <- function(db, df){
  df$amr_test_contact_id  <- get_contact_information_id(db, 
                                                        lab = df$amr_testing_by_laboratory_name, 
                                                      email = df$amr_testing_by_contact_email,  
                                                       name = df$amr_testing_by_contact_name)
  df$amr_testing_by <- vmR:::convert_GRDI_ont_to_vmr_ids(db, x = df$amr_testing_by)

  amr_test_id <- dbGetQuery(vmr, 
                              "INSERT into am_susceptibility_tests (isolate_id, amr_testing_by, testing_date, contact_information)
                               VALUES ($1, $2, $3, $4) RETURNING id", 
                              list(df$isolate_id, df$amr_testing_by, df$amr_testing_date, df$amr_test_contact_id))$id
  return(amr_test_id)
}

#' From a dataframe, extract the AMR columns and return the unique antibiotics 
#' 
get_unique_antibiotics <- function(df){
  ab <- df %>% select(matches(amr_regexes()))
  rgx <- amr_regexes()[1]
  x <- grep(x=names(ab), rgx)
  antibiotics <- gsub(x=names(ab)[x], rgx, '')
  return(antibiotics)
}

#' Select the amr fields based of a single antibiotic
#' 
select_by_antibiotic <- function(df, antibiotic){
  df_sel <- df %>% select(amr_test_id, matches(paste0('^', antibiotic, '_')))
  df_sel$antibiotic <- antibiotic
  names(df_sel) <- gsub(x=names(df_sel), paste0(antibiotic, '_'), '', fixed = TRUE)
  return(df_sel)
}

#' From a wide-form grdi spec df, return the AMR fields as a long form df
#' 
#' @export
long_form_antibiotics <- function(df){
  ab <- get_unique_antibiotics(df)
  long_df <- bind_rows(lapply(FUN = select_by_antibiotic, X=ab, df=df))
  long_df$resistance_phenotype <- set_grdi_nulls_to_NA(long_df$resistance_phenotype)
  filter_df <-
    long_df %>% 
    filter(!is.na(resistance_phenotype))
  return(filter_df)
}

#' Append long-from amr profile dataframe to VMR database
#' 
append_to_amr_table <- function(db, df){
  ins <- 
    df %>% 
    rename(test_id = amr_test_id, 
           antimicrobial_agent = antibiotic, 
           antimicrobial_phenotype = resistance_phenotype, 
           testing_susceptible_breakpoint  = susceptible_breakpoint,
           testing_intermediate_breakpoint = intermediate_breakpoint,
           testing_resistance_breakpoint = resistant_breakpoint) %>%
    mutate(across(contains('breakpoint'), ~set_grdi_nulls_to_NA(.x))) %>%
    mutate(measurement = set_grdi_nulls_to_NA(measurement))
  
  ins <- vmR:::columns_to_ontology_ids(db, ins, 
                                       ontology_columns = c('antimicrobial_agent', 
                                                             'antimicrobial_phenotype', 
                                                             'measurement_units', 
                                                             'measurement_sign', 
                                                             'laboratory_typing_method', 
                                                             'laboratory_typing_platform', 
                                                             'testing_standard',
                                                             'vendor_name'))
  dbAppendTable(vmr, name = "amr_antibiotics_profile", value = ins)
}
