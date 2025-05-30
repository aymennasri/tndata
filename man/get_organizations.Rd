% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_data.R
\name{get_organizations}
\alias{get_organizations}
\title{List Organizations}
\usage{
get_organizations(min_count = 1)
}
\arguments{
\item{min_count}{Integer. Minimum number of datasets an organization must
have to be included in results. Default is 1, meaning only organizations
with at least one dataset are returned.}
}
\value{
A tibble (data frame) with the following columns:
\describe{
\item{name}{Character. Machine-readable name/identifier of the organization.}
\item{display_name}{Character. Human-readable name of the organization.}
\item{dataset_count}{Integer. Number of datasets published by the organization.}
}
}
\description{
Retrieves organizations data from the Tunisian data catalog API
(data.gov.tn) using faceted search. This function returns organizations
that have published datasets.
}
\examples{
try({
  # Get all organizations with at least 5 datasets
  orgs <- get_organizations(min_count = 5)
  head(orgs)
})

}
