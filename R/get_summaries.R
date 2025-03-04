#' Summarize Dataset Themes
#'
#' Fetches and summarizes themes (groups) alongside the number of datasets in
#' each theme from the Tunisian data catalog API (data.gov.tn).
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
#'   themes_summary <- get_themes()
#'   head(themes_summary)
#' })
#' }
#'
#' @export
#' @importFrom httr2 request req_url_query req_perform resp_status resp_body_raw
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr filter arrange desc tibble

get_themes <- function() {
  response <- httr2::request(
    "https://catalog.data.gov.tn/api/3/action/group_list"
  ) |>
    httr2::req_url_query(all_fields = TRUE) |>
    httr2::req_perform()

  if (httr2::resp_status(response) == 200) {
    content <- jsonlite::fromJSON(rawToChar(httr2::resp_body_raw(response)))
    groups <- content$result

    results <- data.frame(
      theme = groups$display_name,
      dataset_count = groups$package_count,
      stringsAsFactors = FALSE
    ) |>
      dplyr::filter(.data$dataset_count != 0) |>
      dplyr::arrange(dplyr::desc(.data$dataset_count)) |>
      dplyr::tibble()

    return(results)
  } else {
    stop("API request failed with status code: ", httr2::resp_status(response))
  }
}

#' List Available Datasets
#'
#' Fetches datasets by keyword and/or author
#'
#' @param keyword Character. Keyword to search for in dataset titles.
#' @param author Character. Author name to filter datasets by.
#' @param max_results Numeric. Maximum number of datasets to return, defaults
#'   to 100.
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
#'   datasets <- get_datasets("agriculture")
#'   head(datasets)
#' })
#' }
#'
#' @export
#' @importFrom httr2 request req_url_query req_perform req_retry
#' @importFrom httr2 resp_status resp_body_json resp_check_status
#' @importFrom dplyr tibble
#' @importFrom purrr map_df
#' @importFrom logger log_info log_warn log_error
#' @importFrom glue glue
#' @importFrom lubridate ymd_hms

get_datasets <- function(keyword = NULL, author = NULL, max_results = 100) {
  api_url <- "https://catalog.data.gov.tn/fr/api/3/action/package_search"

  # Handle author filtering
  if (!is.null(author)) {
    fq_author <- paste0("author:", author)
    logger::log_info("Adding author filter: {author}")
  } else {
    fq_author <- NULL
    logger::log_info("No author filter specified")
  }

  if (!is.null(keyword)) {
    query_parts <- c(query_parts, keyword)
    logger::log_info("Adding keyword filter: {keyword}")
  } else {
    logger::log_info("No keyword filter specified")
  }

  logger::log_info("Searching for datasets...")
  tryCatch(
    {
      req <- httr2::request(api_url) |>
        httr2::req_url_query(
          q = query_string,
          rows = max_results,
          fq = fq_author,
          include_private = FALSE,
          include_drafts = FALSE
        ) |>
        httr2::req_retry(max_tries = 3)

      response <- httr2::req_perform(req)
      httr2::resp_check_status(response)
      content <- httr2::resp_body_json(response)

      total_count <- content$result$count %||% 0

      if (is.null(content$result$results)) {
        logger::log_warn("No results found")
        return(dplyr::tibble())
      }
      datasets <- purrr::map_df(
        content$result$results,
        ~ {
          # Format dates
          created <- if (!is.null(.x$metadata_created)) {
            dt <- lubridate::ymd_hms(.x$metadata_created, tz = "UTC")
            format(dt, "%Y-%m-%d %H:%M:%S")
          } else {
            NA_character_
          }

          modified <- if (!is.null(.x$metadata_modified)) {
            dt <- lubridate::ymd_hms(.x$metadata_modified, tz = "UTC")
            format(dt, "%Y-%m-%d %H:%M:%S")
          } else {
            NA_character_
          }

          dplyr::tibble(
            id = .x$id %||% NA_character_,
            title = .x$title %||% NA_character_,
            name = .x$name %||% NA_character_,
            author = .x$author %||% NA_character_,
            notes = .x$notes %||% NA_character_,
            organization = if (!is.null(.x$organization))
              .x$organization$name else NA_character_,
            created = created,
            modified = modified,
            resources = list(
              purrr::map_df(
                .x$resources,
                ~ {
                  dplyr::tibble(
                    name = .x$name %||% NA_character_,
                    format = .x$format %||% NA_character_,
                    url = .x$url %||% NA_character_
                  )
                }
              )
            )
          )
        }
      )
      logger::log_info(
        "Displaying {nrow(datasets)} out of {total_count} total datasets"
      )
      datasets
    },
    error = function(e) {
      logger::log_error("Error searching datasets: {conditionMessage(e)}")
      stop(glue::glue("Failed to search datasets: {conditionMessage(e)}"))
    }
  )
}

#' List Authors/Organizations
#'
#' Retrieves organizations data from the Tunisian data catalog API
#' (data.gov.tn) using faceted search. This function returns organizations
#' that have published datasets.
#'
#' @param min_count Integer. Minimum number of datasets an organization must
#'   have to be included in results. Default is 1, meaning only organizations
#'   with at least one dataset are returned.
#'
#' @return A tibble (data frame) with the following columns:
#' \describe{
#'   \item{name}{Character. Machine-readable name/identifier of the organization.}
#'   \item{display_name}{Character. Human-readable name of the organization.}
#'   \item{dataset_count}{Integer. Number of datasets published by the organization.}
#' }
#'
#' @examples
#' \donttest{
#' try({
#'   # Get all organizations with at least 5 datasets
#'   orgs <- get_organizations(min_count = 5)
#'   head(orgs)
#' })
#' }
#'
#' @export
#' @importFrom httr2 request req_url_query req_perform resp_status
#' @importFrom httr2 resp_body_json
#' @importFrom dplyr arrange desc filter tibble
#' @importFrom logger log_info log_warn log_error log_success
#' @importFrom purrr map_df

get_organizations <- function(min_count = 1) {
  logger::log_info("Retrieving organizations using faceted search")

  # Use faceted search to get all organizations with datasets
  response <- httr2::request(
    "https://catalog.data.gov.tn/fr/api/3/action/package_search"
  ) |>
    httr2::req_url_query(
      q = "*:*",
      rows = 0,
      facet = "true",
      `facet.field` = '["organization"]',
      `facet.limit` = -1, # Unlimited results
      `facet.mincount` = min_count # Only include orgs with at least this many datasets
    ) |>
    httr2::req_perform()

  if (httr2::resp_status(response) != 200) {
    logger::log_error(
      "API request failed with status: {httr2::resp_status(response)}"
    )
    return(dplyr::tibble())
  }

  content <- httr2::resp_body_json(response)

  # Extract organizations from facets
  if (!is.null(content$result$search_facets$organization)) {
    orgs_from_facets <- purrr::map_df(
      content$result$search_facets$organization$items,
      function(item) {
        dplyr::tibble(
          name = item$name,
          display_name = item$display_name,
          dataset_count = item$count
        )
      }
    ) |>
      dplyr::arrange(dplyr::desc(.data$dataset_count))

    logger::log_success(
      "Found {nrow(orgs_from_facets)} organizations via faceted search"
    )
    return(orgs_from_facets)
  }

  logger::log_warn("No organization facets found in the API response")
  return(dplyr::tibble())
}

#' List Dataset Keywords/Tags
#'
#' Retrieves a list of unique keywords/tags from the Tunisian data catalog API.
#'
#' @param limit Integer. Maximum number of tags to return (default: 10).
#' @param query Character. Optional search string to filter tags.
#'
#' @return A data frame of keywords/tags with counts.
#'
#' @examples
#' \donttest{
#' keywords <- get_keywords(limit = 10)
#' }
#'
#' @export
#' @importFrom httr2 request req_url_query req_perform
#' @importFrom httr2 req_retry resp_body_json resp_check_status
#' @importFrom dplyr arrange desc
#' @importFrom purrr map_df
#' @importFrom logger log_info log_error

get_keywords <- function(limit = 10, query = NULL) {
  facet_url <- "https://catalog.data.gov.tn/fr/api/3/action/package_search"

  logger::log_info("Retrieving dataset keywords")

  query_params <- list(
    facet = "true",
    facet.field = '["tags"]',
    facet.limit = limit
  )

  if (!is.null(query)) {
    query_params$q <- query
  }

  req <- httr2::request(facet_url) |>
    httr2::req_url_query(!!!query_params) |>
    httr2::req_retry(max_tries = 3)

  response <- httr2::req_perform(req)
  httr2::resp_check_status(response)
  content <- httr2::resp_body_json(response)

  if (
    is.null(content$result$search_facets$tags) ||
      length(content$result$search_facets$tags$items) == 0
  ) {
    logger::log_error("No keywords found")
    return(data.frame(
      keyword = character(0),
      count = integer(0),
      stringsAsFactors = FALSE
    ))
  }

  keywords <- purrr::map_df(
    content$result$search_facets$tags$items,
    function(item) {
      data.frame(
        keyword = item$name,
        count = item$count,
        stringsAsFactors = FALSE
      )
    }
  )

  keywords <- keywords |>
    dplyr::arrange(dplyr::desc(.data$count))

  logger::log_info("Retrieved {nrow(keywords)} keywords")
  return(keywords)
}
