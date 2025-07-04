% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{make_insert_sql}
\alias{make_insert_sql}
\title{Create a generic SQL insert statement}
\usage{
make_insert_sql(table_name, field_names)
}
\arguments{
\item{table_name}{The table name of the VMR table to make an insert for}

\item{field_names}{The column names to insert}
}
\value{
sql
}
\description{
Makes a generate SQL insert statement from a table name and a list
of parameters, which should correspond to that table's fields.
}
