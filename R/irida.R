
#' Get isolates that have an IRIDA id but are missing corresponding IRIDA sequence information
#' 
#' @inheritParams get_sample_ids
#' 
#' @return df with irida_id, sequence_id, library_id, and the sequencing_platform
isolates_with_no_irida_sequencing_ids <- function(db){
  q <- "SELECT iso.irida_sample_id::text,
               seq.id          AS sequencing_id,
               seq.library_id,
               ont.en_term     AS sequencing_platform
          FROM sequencing           AS seq
          LEFT JOIN wgs_extractions AS ext ON       ext.extraction_id = seq.extraction_id
          LEFT JOIN isolates        AS iso ON                  iso.id = ext.isolate_id
          LEFT JOIN ontology_terms  AS ont ON seq.sequencing_platform = ont.id
         WHERE iso.irida_sample_id IS NOT NULL 
           AND seq.r1_irida_id     IS NULL 
           AND seq.r2_irida_id     IS NULL"
  df <- dbGetQuery(vmr, q) %>% as_tibble()
  return(df)
}

#' Get paired-end reads from IRIDA
#' 
#' Uses [RiRida::get_sequences] to query IRIDA for all paired-end reads for the 
#' supplied samples, and returns a formatted DF.
#' 
#' @param samples a vector of irida sample ID.
#' 
#' @return dataframe of paired-end reads assocaited with each sample
#' @export
query_pairs_from_irida <- function(samples){

  x <- duplicated(samples)
  if (any(x)) warning("duplicated irida sample ids passed -> querying only once")
  dedup <- samples[!x]
  
  seqs <- RiRida::get_sequences(samples = dedup, type = "pairs")

  df <- 
    seqs |>
    select(id, file, createdDate,fileName, direction, pair_id, identifier) |>
    pivot_wider(id_cols = c(id, pair_id), names_from = direction, values_from = c(fileName, file, identifier, createdDate)) |>
    rename(sample_id = id)

  dups <- duplicated(df$sample_id)
  dupped_samples <- df$sample_id[dups]
  if (any(dups)){
    warning("Warning: There are multiple sequences associated with irida samples: ", paste0(unique(dupped_samples), collapse = ", "))
  }

  return(df)
}

#' Update VMR with irida sequence information
#' 
#' @inheritParams get_sample_ids
#' 
#' @export
update_sequences_with_irida <- function(db, seq_id, f_file, r_file, f_id, r_id){

  query <- "UPDATE sequencing SET r1_fastq_filename = $1,
                                  r2_fastq_filename = $2,
                                        r1_irida_id = $3,
                                        r2_irida_id = $4 
                       WHERE id = $5"
 
  n <- dbExecute(db, query, list(f_file, r_file, f_id, r_id, seq_id))
  message("updated ", n, " records with irida sequencing info")
}
