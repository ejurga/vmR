#' Import sensititre data, add relevant testing information, and insert to database
#' 
#' This function attempts to be a complete wrapper for the insertion of MIC
#' data from a sensititre raw data file into the VMR. 
#' 
#' This function, as is, makes several assumptions about the breakpoints 
#' and testing information. Breakpoints are currently an internal function 
#' with breakpoints received from the lab of Audrey Charlebois. It will not 
#' be suitable for data from other labs that may use different breakpoints. 
#' 
#' Testing contact information and the date testing can be supplied manually 
#' 
#' @param db database connection to the vmr
#' @param file path to the raw file 
#' @param sheet if the path is to an excel file, provide the sheet 
#' @param testing_date provide a date that the samples were tested, if no column is available in the datafile
#' @param lab_name Name of the testing lab
#' @param name name of the contact who did the testing
#' @param email the email of the contact
#' @param agency the agency that did the testing.
#' 
#' @returns nothing, but inserts into the database without commiting it.
#' @export
import_and_insert_sensititre <- function(db, file, sheet = NA, testing_date = NA, 
                                         lab_name = 'NML St-Hyacinthe', 
                                             name = 'Audrey Charlebois',
                                            email = 'audrey.charlebois@phac-aspc.gc.ca', 
                                           agency = 'Public Health Agency of Canada (PHAC) [GENEPIO:0100551]'){
  dbBegin(db)
  # Open the sensititre sheet
  raw   <- open_sensititre(file, sheet = sheet)
  # figure out if any col is named "date" or something like that
  is_date_col <- grepl(x=names(raw), "date", ignore.case = T)
  if (!any(is_date_col) & is.na(testing_date)){
    stop("AMR test date neither detected nor supplied")
  } else if (any(is_date_col) & is.na(testing_date)) {
    dates <- raw[[names(raw)[is_date_col]]]
  } else if (!any(is_date_col) & !is.na(testing_date)){
    dates <- rep(x=testing_date, times = nrow(raw))
  } else {
    stop("Testing date in data and also manually supplied, exiting")
  }
  # create df for test table
  tests <- add_amr_test_info(isolate_ids = raw$id, 
                            testing_date = dates, 
                                       n = raw$n,
                                lab_name = lab_name, 
                                   email = email, 
                                  agency = agency, 
                                    name = name)
  tests$isolate_id <- get_isolate_ids(db, tests$user_isolate_id)
  # Stop if there is a missing isolate.
  is_na <- is.na(tests$isolate_id)
  if ( any(is_na) ) stop("Isolate(s) not found in VMR: ", paste0(tests$user_isolate_id[is_na], collapse = "; "))
  # INSERTION
  tests$test_id <- new_amr_test(vmr, tests)
  # Format to long form
  df <- open_and_format_sensitre_raw_data(raw)
  # Use a left join to add the correct test_id 
  mdf <- left_join(df, 
                   select(tests, user_isolate_id, n, test_id), 
                   by = c("id"="user_isolate_id", "n"="n"), 
                   relationship = "many-to-one")
  # Convert to VMR ids
  ont_cols <- dbGetQuery(db, "SELECT column_name FROM ontology_columns WHERE table_name = 'amr_antibiotics_profile'")
  df <- columns_to_ontology_ids(vmr, mdf, ont_cols$column_name)
  # Remove unneeded columns
  ins <- select(df, -id, -n, -panel, -am, -interpretation, -sign, -mic)
  # Finally, append to MIC table
  x <- dbAppendTable(db, name = "amr_antibiotics_profile", value = ins)
  message("inserted ", x, " MIC measurements. Commit transaction if ready.")
}

#' Open a sensititre file as a df
open_sensititre <- function(file, sheet=NA){

  bname <- basename(file)
  if (grepl(x=bname, ".xls[xm]$")){
    sheets <- openxlsx::getSheetNames(file)
    if (is.na(sheet)){
      stop("Excel file has multiple sheets, specify one of the sheets: ", paste0(sheets, collapse = ", "))
    } else {
      df <- openxlsx::read.xlsx(xlsxFile = file, 
                                sheet = sheet, 
                                na.strings = " ") 
      df <- suppressMessages(dplyr::as_tibble(df, .name_repair = "unique"))
    } 
  } else if (grepl(x=bname, ".csv")){
    df <- readr::read_csv(file = file, na = " ", trim_ws = TRUE, name_repair = "unique")
  } else {stop("unhandled file format")}
 
  # Assume first column is id 
  names(df)[1] <- "id"
  # Find the column that is the "n" column, but searching if every value is between 1 and 10.
  is_n_col <- sapply(df, function(x) all(x %in% c(1:10)))
  if (!any(is_n_col)){
    warning("Unable to detect the N column, which disambiguates duplicates! Creating one.")  
    df <- df %>% group_by(id) %>% mutate(n = row_number(), .after = id) %>% ungroup()
  } else {
    names(df)[is_n_col] <- "n"
  }
  # figure out if any col is named "date" or something like that
  is_date_col <- grepl(x=names(df), "date", ignore.case = T)
  
  if (!any(is_date_col)){
    warning("No date column detected!")  
  } else {
    names(df)[is_date_col] <- 'date'
  }
  
  # A quick check for duplicates amongst the first 3 columns. 
  if (!nrow(unique(df[,1:3])) == nrow(unique(df))) stop("duplicates detected, check file")
  
  return(df)
}

open_and_format_sensitre_raw_data <- function(df){
  df <- sensititre_to_long_form(df)
  # Convert columns to ontologized values
  df$antimicrobial_agent        <- am_to_ontology(df$am)
  df$measurement                <- df$mic
  df$measurement_sign           <- signs_to_ontology(df$sign)
  df$antimicrobial_phenotype    <- interpretations_to_ontology(df$interpretation)
  # Add constant values
  df$measurement_units          <- 'ug/mL [UO:0000274]'
  df$laboratory_typing_method   <- 'Micro Broth Dilution Method [ARO:3004410]'
  df$laboratory_typing_platform <- 'Sensititre [ARO:3004402]' 
  # Add data from breakpoints
  bp <- internal_breakpoints()
  x <- match(df$antimicrobial_agent, bp$am)
  df$testing_susceptible_breakpoint  <- bp$susceptible[x]
  df$testing_intermediate_breakpoint <- bp$intermediate[x]
  df$testing_resistance_breakpoint   <- bp$resistant[x]
  df$testing_standard                <- bp$standard[x] 
  df$testing_standard_version        <- bp$version[x]
  df$testing_standard_details        <- bp$details[x]
  return(df)
}

add_amr_test_info <- function(isolate_ids, n, testing_date, lab_name,  name, email, agency){
  df <- tibble(.rows = length(isolate_ids))
  df$user_isolate_id <- isolate_ids
  df$n <- n
  df$amr_testing_by_laboratory_name <- lab_name
  df$amr_testing_by_contact_email   <- email
  df$amr_testing_by_contact_name    <- name
  df$amr_testing_by                 <- agency
  df$amr_testing_date               <- testing_date
  return(df)
}

sensititre_to_long_form <- function(df){
  # Rename the first 3 columns, which are empty
  colnames(df)[1:3] <- c('id', "n", "panel")
  # Get the columns that indicate the antimicrobial
  AM.idx <- grep(x=colnames(df), "[A-Z]{3,}")
  # Here, take the AM column and the two columns after it, which are the mic and interpretation columns
  x <- sapply(X = AM.idx, 
            FUN = function(x) {ast <- df[c(1,2,3,x, x+1, x+2)]
                               colnames(ast)[c(4,5,6)] <- c("am", "cmi", "interpretation")
                               return(ast)}, simplify = FALSE)
  # Bind into a single dataframe
  long <- bind_rows(x)
  # the cmi column contains the sign and mic in the same column. Split this into its own column
  cmi_split <- stringr::str_split(string=trimws(long$cmi), pattern = " {1,}")
  if (!all(lengths(cmi_split)==2)) stop("cmi splitting failed -> n values != 2 for all values")
  long$sign <- sapply(X=cmi_split, FUN=function(x) x[1])
  long$mic  <- sapply(X=cmi_split, FUN=function(x) x[2])
  long$mic  <- as.numeric(long$mic)
  long <- dplyr::select(long, -cmi)
  return(long)
}

get_grdi_antimicrobials <- function(){
  grdi <- DHRTools::load_schema(DHRTools:::download_schema(DHRTools:::schema_urls()$grdi))
  grdi.slots <- DHRTools:::slot_names(grdi)
  pattern <- DHRTools:::amr_regexes()[1]
  x <- grep(x=grdi.slots, pattern = pattern)
  ams <- gsub(x=grdi.slots[x], pattern = pattern, replacement = "")
  return(ams)
}

am_to_ontology <- function(x){
  abbr_to_ont <- 
    c("AMOCLA" = "Amoxicillin-clavulanic [ARO:3003997]",
      "AMPICI" = "Ampicillin [CHEBI:28971]",
      "AZITHR" = "Azithromycin [CHEBI:2955]",
      "CEFOXI" = "Cefotaxime [CHEBI:204928]",
      "CEFTRI" = "Ceftriaxone [CHEBI:29007]",
      "CHLORA" = "Chloramphenicol [CHEBI:17698]",
      "CIPROF" = "Ciprofloxacin [CHEBI:100241]",
      "COLIST" = "Colistin [ARO:0000067]",
      "GENTAM" = "Gentamicin [CHEBI:17833]",
      "MEROPE" = "Meropenem [CHEBI:43968]",
      "NALAC"  = "Nalidixic acid [CHEBI:100147]",
      "SULFIZ" = "Sulfisoxazole [CHEBI:102484]",
      "TETRA"  = "Tetracycline [CHEBI:27902]",
      "TRISUL" = "Trimethoprim-sulfamethoxazole [ARO:3004024]")
  y <- match(x, names(abbr_to_ont))
  if (any(is.na(y))) warning("Match not found, NAs coerced for values: ", paste0(unique(x[is.na(y)]), collapse = ", "))
  return(unname(abbr_to_ont[y]))
}

signs_to_ontology <- function(x){
  signs <- 
    c("<"  = "less than (<) [GENEPIO:0001002])", 
      "<=" = "less than or equal to (<=) [GENEPIO:0001003]",
      "="  = "equal to (==) [GENEPIO:0001004]",
      ">"  = "greater than (>) [GENEPIO:0001006]",
      ">=" = "greater than or equal to (>=) [GENEPIO:0001005]")
  y <- match(x, names(signs))
  if (any(is.na(y))) warning("Match not found, NAs coerced for values: ", paste0(unique(x[is.na(y)]), collapse = ", "))
  return(unname(signs[y]))
}  

interpretations_to_ontology <- function(x){
  inters <- c(
    "INTER"  = "Intermediate antimicrobial phenotype [ARO:3004300]",
    "RESIST" = "Resistant antimicrobial phenotype [ARO:3004301]",
    "SUSC"   = "Susceptible antimicrobial phenotype [ARO:3004302]")
  y <- match(x, names(inters))
  if (any(is.na(y))) warning("Match not found, NAs coerced for values: ", paste0(unique(x[is.na(y)]), collapse = ", "))
  return(unname(inters[y]))
}  


#' A set of internal breakpoints to assign to the data
#' 
#' These breakpoints are based off of reports from the lab of Audrey Charlebois.
internal_breakpoints <- function(x){
  df <- tidyr::tibble(.rows = 14) 
  df$am <- c( 
   "Amoxicillin-clavulanic [ARO:3003997]",
   "Ampicillin [CHEBI:28971]",
   "Azithromycin [CHEBI:2955]",
   "Cefotaxime [CHEBI:204928]",
   "Ceftriaxone [CHEBI:29007]",
   "Chloramphenicol [CHEBI:17698]",
   "Ciprofloxacin [CHEBI:100241]",
   "Colistin [ARO:0000067]",
   "Gentamicin [CHEBI:17833]",
   "Meropenem [CHEBI:43968]",
   "Nalidixic acid [CHEBI:100147]",
   "Sulfisoxazole [CHEBI:102484]",
   "Tetracycline [CHEBI:27902]",
   "Trimethoprim-sulfamethoxazole [ARO:3004024]")
  df$units <-  ('ug/ml')
  df$susceptible  <- c( 8, 8,16, 8, 1, 8, 0.06     ,   NA, 2, 1,16,256,  4, 2)
  df$intermediate <- c(16,16,NA,16, 2,16,'0.12-0.5','<=2', 4, 2,NA, NA,  8,NA)
  df$resistant    <- c(32,32,32,32, 4,32,         1,    4, 8, 4,32, 512,16, 4)
  df$standard    <- rep('Clinical Laboratory and Standards Institute (CLSI) [ARO:3004366]', nrow(df))
  df$standard[3] <-'National Antimicrobial Resistance Monitoring System (NARMS) [ARO:3007195]'
  df$version     <- rep('M100, 35th ed. Wayne, PA', nrow(df))
  df$version[3]  <- NA
  df$details     <- rep(NA, nrow(df))
  df$details[c(1,4,8,9,11,12)] <- 'For Salmonella: MICs were harmonized with NARMS.'
  return(df)
}