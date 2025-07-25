% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{columns_to_ontology_ids}
\alias{columns_to_ontology_ids}
\title{Convert the vectors of ontology terms into their corresponding VMR ids}
\usage{
columns_to_ontology_ids(db, df, ontology_columns, ont_table = "ontology_terms")
}
\arguments{
\item{df}{The dataframe with the columns to convert}

\item{ontology_columns}{A vector of the list names that are to be
converted into VMR ids}
}
\value{
dataframe
}
\description{
Given a list of vectors that are to be input into the VMR, convert the
desired columns containing ontology terms into their respective VMR IDs
}
