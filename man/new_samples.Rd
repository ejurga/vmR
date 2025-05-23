% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/inserts.R
\name{new_samples}
\alias{new_samples}
\title{Insert new samples into the samples table}
\usage{
new_samples(db, sample_names, project_id)
}
\arguments{
\item{db}{\link{DBI} connection to VMR}

\item{sample_names}{sample name to insert}

\item{project_id}{VMR project ID associated with the sample}
}
\description{
Insert new samples into the samples table
}
