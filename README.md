
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tndata <img src="inst/figures/tndata_logo.png" align="right" height="138"/>

<!-- badges: start -->

[![](https://cranlogs.r-pkg.org/badges/tndata)](https://cran.r-project.org/package=tndata)
<!-- badges: end -->

The tndata package simplifies access to Tunisian government open data
with R. It queries datasets by theme, author, or keywords, retrieves
metadata, and gets structured results ready for analysis; all through
the official [data.gov.tn](data.gov.tn) CKAN API.

## Installation

You can install the development version of tndata from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("aymennasri/tndata")
```

## Example

``` r
library(tndata)

download_dataset("Ressources en eau- Gouvernorat de Kasserine", format = "xls")
```
