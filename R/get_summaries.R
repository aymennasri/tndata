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
#'   themes_summary <- get_themes()
#'   head(themes_summary)
#' })
#' }
#'
#' @export
#' @importFrom httr GET status_code
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr filter arrange desc tibble

get_themes <- function() {
  response <- httr::GET(
    "https://catalog.data.gov.tn/api/3/action/group_list",
    query = list(all_fields = TRUE)
  )

  if (httr::status_code(response) == 200) {
    content <- jsonlite::fromJSON(rawToChar(response$content))
    groups <- content$result

    results <- data.frame(
      theme = character(),
      dataset_count = numeric(),
      stringsAsFactors = FALSE
    )

    for (i in seq_len(nrow(groups))) {
      response <- httr::GET(
        "https://catalog.data.gov.tn/api/3/action/group_package_show",
        query = list(id = groups$name[i])
      )

      if (httr::status_code(response) == 200) {
        results <- rbind(
          results,
          data.frame(
            theme = groups$display_name[i],
            dataset_count = groups$package_count[i],
            stringsAsFactors = FALSE
          )
        ) |>
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
#'   datasets <- get_datasets("agriculture")
#'   head(datasets)
#' })
#' }
#'
#' @export
#' @importFrom httr RETRY stop_for_status content
#' @importFrom dplyr tibble
#' @importFrom purrr map_df
#' @importFrom logger log_info log_warn log_error
#' @importFrom glue glue
#' @importFrom lubridate ymd_hms

get_datasets <- function(keyword = NULL, author = NULL, max_results = 100) {
  api_url <- "https://catalog.data.gov.tn/fr/api/3/action/package_search"

  # Build query parameters
  query_parts <- c()
  if (!is.null(keyword)) query_parts <- c(query_parts, keyword)
  if (!is.null(author)) query_parts <- c(query_parts, paste0("author:", author))
  query_string <- paste(query_parts, collapse = " ")

  logger::log_info("Searching for datasets with query: {query_string}")
  tryCatch(
    {
      response <- httr::RETRY(
        "GET",
        api_url,
        query = list(q = query_string, rows = max_results),
        times = 3
      )
      httr::stop_for_status(response)
      content <- httr::content(response, "parsed")

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
#' Retrieves organizations data from the Tunisian data catalog API (data.gov.tn).
#' (Caution - this function may be slow if you choose to retrieve all organizations)
#'
#' @param limit Integer. Maximum number of organizations to return, defaults to 10.
#'
#' @return A data frame with columns for id, name, dataset_count, and description.
#'
#' @export
#' @importFrom httr GET content
#' @importFrom dplyr arrange desc filter tibble
#' @importFrom logger log_info log_warn log_error log_success

get_authors <- function(limit = 20) {
  logger::log_info("Retrieving top {limit} organizations by dataset count")

  response <- httr::GET(
    "https://catalog.data.gov.tn/fr/api/3/action/organization_list",
    query = list(
      all_fields = TRUE,
      include_dataset_count = TRUE,
      sort = "package_count desc",
      limit = limit
    )
  )

  if (httr::status_code(response) != 200) {
    logger::log_error("API request failed: {httr::status_code(response)}")
    return(dplyr::tibble())
  }

  content <- httr::content(response, "parsed")

  authors <- purrr::map_df(content$result, function(org) {
    dplyr::tibble(
      id = org$id,
      name = org$display_name %||% org$title %||% org$name,
      dataset_count = org$package_count %||% 0,
      description = org$description %||% NA_character_,
      image_url = org$image_url %||% NA_character_
    )
  }) |> dplyr::filter(dataset_count > 0)  |>
    dplyr::arrange(dplyr::desc(dataset_count))

  logger::log_success("Retrieved {nrow(authors)} organizations")
  return(authors)
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
#' @importFrom httr GET content stop_for_status
#' @importFrom dplyr arrange desc
#' @importFrom purrr map_df
#' @importFrom logger log_info log_error

get_keywords <- function(limit = 10, query = NULL) {
  # # Use the tag_list action for more accurate results
  # api_url <- "https://catalog.data.gov.tn/fr/api/3/action/tag_list"

  # For complete tag details with counts, use package_search facets
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

  response <- httr::RETRY(
    "GET",
    facet_url,
    query = query_params,
    times = 3
  )

  httr::stop_for_status(response)
  content <- httr::content(response, "parsed")

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
    dplyr::arrange(dplyr::desc(count))

  logger::log_info("Retrieved {nrow(keywords)} keywords")
  return(keywords)
}
