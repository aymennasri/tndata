#' Download a resource from a URL
#'
#' @param url Character. URL of the resource to download.
#' @param name Character. Name of the resource.
#' @param format Character. Format of the resource.
#' @param download_dir Character. Directory to save the downloaded resource.
#'
#' @return Path to the downloaded file or NULL if download failed.
#'
#' @keywords internal
#' @noRd

.download_resource <- function(url, name, format, download_dir = "datasets") {
  tryCatch(
    {
      filename <- paste0(
        stringr::str_replace_all(name, "\\s+", "_"),
        ".",
        stringr::str_to_lower(format)
      )
      dest <- file.path(download_dir, filename)

      if (file.exists(dest)) {
        logger::log_info("File already exists: {filename}")
        return(dest)
      }

      logger::log_info("Downloading {filename}")
      httr::GET(url, httr::write_disk(dest, overwrite = TRUE)) |>
        httr::stop_for_status()

      logger::log_success("Successfully downloaded: {filename}")
      dest
    },
    error = function(e) {
      logger::log_error("Failed to download {name}: {conditionMessage(e)}")
      NULL
    }
  )
}
