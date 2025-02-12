#' Summarize Dataset Themes
#'
#' Fetches and summarizes themes (groups) alongside the number of datasets in each theme
#' from the Tunisian data catalog API (data.gov.tn).
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

#' List Available Datasets
#'
#' Fetches datasets by keyword
#'
#' @param keyword Character. Keyword to search for in dataset titles.
#' @param max_results Numeric. Maximum number of datasets to return, defaults to 100.
#'
#' @return A tibble (data frame) with the following columns:
#' \describe{
#'  \item{title}{Character. Title of the dataset.}
#'  \item{id}{Character. ID of the dataset.}
#'  \item{resources}{List. A list of tibbles with the following columns:
#'  \describe{
#'  \item{name}{Character. Name of the dataset.}
#'  \item{format}{Character. Format of the dataset.}
#'  \item{url}{Character. URL of the dataset.}
#'  }
#'  }
#'  \item{created}{Date. Date the dataset was created.}
#' }
#'
#' @examples
#' \donttest{
#' try({
#'  datasets <- get_datasets("agriculture")
#'  head(datasets)
#' })
#' }
#'
#' @export
#' @importFrom httr RETRY stop_for_status content
#' @importFrom dplyr tibble
#' @importFrom purrr map_df
#' @importFrom logger log_info log_warn log_error
#' @importFrom glue glue

get_datasets <- function(keyword, max_results = 100) {
  api_url = "https://catalog.data.gov.tn/fr/api/3/action/package_search"

  logger::log_info("Searching for datasets with keyword: {keyword}")

  tryCatch({
    response <- httr::RETRY(
      "GET",
      api_url,
      query = list(q = keyword, rows = max_results),
      times = 3
    )
    httr::stop_for_status(response)

    content <- httr::content(response, "parsed")
    if (is.null(content$result$results)) {
      logger::log_warn("No results found")
      return(dplyr::tibble())
    }

    datasets <- purrr::map_df(content$result$results, ~{
      dplyr::tibble(
        title = .x$title %||% NA_character_,
        id = .x$id %||% NA_character_,
        resources = list(
          purrr::map_df(.x$resources, ~{
            dplyr::tibble(
              name = .x$name %||% NA_character_,
              format = .x$format %||% NA_character_,
              url = .x$url %||% NA_character_
            )
          })
        ),
        created = as.POSIXct(.x$metadata_created) %||% NA
      )
    })

    logger::log_info("Found {nrow(datasets)} datasets")
    datasets

  }, error = function(e) {
    logger::log_error("Error searching datasets: {conditionMessage(e)}")
    stop(glue::glue("Failed to search datasets: {conditionMessage(e)}"))
  })
}

#' Download Dataset
#'
#' Downloads a dataset from the Tunisian data catalog API (data.gov.tn).
#'
#' @param title Character. Title of the dataset to download.
#' @param download_dir Character. Directory to save the downloaded dataset at, defaults to "datasets".
#' @param format Character. Format of the dataset to download.
#'
#' @return The demanded dataset in the demanded path.
#'
#' @examples
#' \donttest{
#' try({
#'  download_dataset("DONNÉES CLIMATIQUES STATION Tozeur AVFA", format = "csv")
#' })
#' }
#'
#'
#' @export
#' @importFrom httr GET content stop_for_status write_disk
#' @importFrom dplyr filter mutate
#' @importFrom purrr pmap compact
#' @importFrom fs dir_delete
#' @importFrom stringr str_replace_all str_to_lower
#' @importFrom utils URLencode
#' @importFrom logger log_info log_error log_success
#' @importFrom glue glue
#' @importFrom fs dir_create

download_dataset <- function(title, download_dir = "datasets", format = NULL) {
  api_url = "https://catalog.data.gov.tn/fr/api/3/action/package_search"
  logger::log_info("Starting download for dataset: {title}")

  response <- httr::RETRY(
    "GET",
    api_url,
    query = list(q = title, rows = 1),
    times = 3
  )
  httr::stop_for_status(response)

  content <- httr::content(response, "parsed")
  ds <- content$result$results[[1]]

  if(is.null(ds)) {
    logger::log_error("Dataset not found: {title}")
    stop("Dataset not found")
  }

  resources <- purrr::map_df(ds$resources, ~{
    dplyr::tibble(
      name = .x$name %||% NA_character_,
      format = .x$format %||% NA_character_,
      url = .x$url %||% NA_character_
    )
  }) |>
    dplyr::mutate(encoded_url = purrr::map_chr(url, ~utils::URLencode(.x)))

  if (!is.null(format)) {
    logger::log_info("Filtering for format: {format}")
    resources <- resources |>
      dplyr::filter(toupper(format) == toupper(!!format))

    if(nrow(resources) == 0) {
      logger::log_error("No resources found with format: {format}")
      stop("No matching resources found")
    }
  }

  download_resource <- function(url, name, format) {
    tryCatch({
      filename <- paste0(
        stringr::str_replace_all(name, "\\s+", "_"),
        ".", stringr::str_to_lower(format)
      )
      dest <- file.path(download_dir, filename)

      logger::log_info("Downloading {filename}")
      httr::GET(url, httr::write_disk(dest, overwrite = TRUE)) |>
        httr::stop_for_status()

      logger::log_success("Successfully downloaded: {filename}")
      dest
    }, error = function(e) {
      logger::log_error("Failed to download {name}: {conditionMessage(e)}")
      NULL
    })
  }

  dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)
  fs::dir_delete(download_dir)
  dir.create(download_dir)

  logger::log_info("Starting downloads to directory: {download_dir}")

  results <- resources |>
    purrr::pmap(~download_resource(..3, ..1, ..2)) |>
    purrr::compact()

  logger::log_info("Download complete: {length(results)} files downloaded")
  invisible(results)
}
