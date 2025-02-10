#' Summarize Dataset Themes
#'
#' Fetches and summarizes themes (groups) alongside the number of datasets in each theme
#' from the Tunisian data catalog API (data.gov.tn). The results are sorted by dataset count
#' in descending order and exclude themes with zero datasets.
#'
#' @return A tibble (data frame) with two columns:
#' \describe{
#'   \item{theme}{Character. Name of the theme/group.}
#'   \item{dataset_count}{Numeric. Number of datasets in the theme.}
#' }
#'
#' @examples
#' \donttest{
#' try({
#'   themes_summary <- get_themes_summary()
#'   head(themes_summary)
#' })
#' }
#'
#' @export
#' @importFrom httr GET status_code
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr filter arrange desc tibble

get_themes_summary <- function() {
  # Get groups/themes
  response <- httr::GET("https://catalog.data.gov.tn/api/3/action/group_list",
                  query = list(all_fields = TRUE))

  if (httr::status_code(response) == 200) {
    content <- jsonlite::fromJSON(rawToChar(response$content))
    groups <- content$result

    results <- data.frame(
      theme = character(),
      dataset_count = numeric(),
      stringsAsFactors = FALSE
    )

    for (i in 1:nrow(groups)) {
      response <- httr::GET("https://catalog.data.gov.tn/api/3/action/group_package_show",
                      query = list(id = groups$name[i]))

      if (httr::status_code(response) == 200) {
        datasets <- jsonlite::fromJSON(rawToChar(response$content))

        # Add to results
        results <- rbind(results, data.frame(
          theme = groups$display_name[i],
          dataset_count = groups$package_count[i],
          stringsAsFactors = FALSE
        )) |>
          dplyr::filter(dataset_count != 0) |>
          dplyr::arrange(dplyr::desc(dataset_count)) |>
          dplyr::tibble()
      }
    }

    return(results)
  } else {
    stop("API request failed with status code: ", httr::status_code(response))
  }
}
