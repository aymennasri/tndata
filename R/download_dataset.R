#' Download Dataset
#'
#' Downloads a dataset from the Tunisian data catalog API (data.gov.tn).
#'
#' @param title Character. Display name of the dataset to download.
#' @param download_dir Character. Directory to save the downloaded dataset at,
#'   defaults to "datasets".
#' @param format Character. Format of the dataset to download.
#'
#' @return The demanded dataset in the demanded path.
#'
#' @examples
#' \donttest{
#' try({
#'   download_dataset(
#'     "Ressources en eau- Gouvernorat de Kasserine",
#'     format = "xls",
#'     download_dir = tempdir()
#'  )
#' })
#' }
#'
#' @export
#' @importFrom httr2 request req_url_query req_retry req_perform resp_body_json
#' @importFrom dplyr filter mutate
#' @importFrom purrr pmap compact
#' @importFrom fs dir_delete
#' @importFrom stringr str_replace_all str_to_lower
#' @importFrom utils URLencode
#' @importFrom logger log_info log_error log_success
#' @importFrom glue glue
#' @importFrom fs dir_create

download_dataset <- function(title, download_dir = "datasets", format = NULL) {
  api_url <- "https://catalog.data.gov.tn/fr/api/3/action/package_search"
  logger::log_info("Starting download for dataset: {title}")

  response <- httr2::request(api_url) |>
    httr2::req_url_query(q = title, rows = 1) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform()

  content <- httr2::resp_body_json(response)
  ds <- content$result$results[[1]]

  if (is.null(ds)) {
    logger::log_error("Dataset not found: {title}")
    stop("Dataset not found")
  }

  resources <- purrr::map_df(
    ds$resources,
    ~ {
      dplyr::tibble(
        name = .x$name %||% NA_character_,
        format = .x$format %||% NA_character_,
        url = .x$url %||% NA_character_
      )
    }
  ) |>
    dplyr::mutate(encoded_url = purrr::map_chr(url, ~ utils::URLencode(.x)))

  if (!is.null(format)) {
    logger::log_info("Filtering for format: {format}")
    resources <- resources |>
      dplyr::filter(toupper(format) == toupper(!!format))

    if (nrow(resources) == 0) {
      logger::log_error("No resources found with format: {format}")
      stop("No matching resources found")
    }
  }

  fs::dir_create(download_dir, recurse = TRUE)

  logger::log_info("Starting downloads to directory: {download_dir}")

  results <- resources |>
    purrr::pmap(~ .download_resource(..3, ..1, ..2, download_dir)) |>
    purrr::compact()

  logger::log_info("Download complete: {length(results)} files downloaded")
  invisible(results)
}
