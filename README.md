
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tndata <a href="http://aymennasri.me/tndata/"><img src="man/figures/logo.png" align="right" height="139" alt="tndata website" /></a>

<!-- badges: start -->
<!-- [![](https://cranlogs.r-pkg.org/badges/grand-total/tndata)](https://cran.r-project.org/package=tndata) -->
<!-- badges: end -->

## Overview

The tndata package simplifies access to Tunisian government open data
with R. It queries datasets by theme, author, or keywords, retrieves
metadata, and gets structured results ready for analysis; all through
the official [data.gov.tn](https://data.gov.tn/fr/) CKAN API.

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
