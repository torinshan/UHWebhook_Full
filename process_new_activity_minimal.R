################################################################################
#                   WEBHOOK-TRIGGERED ACTIVITY PROCESSOR                      #
#   Uses only: data.table, httr, jsonlite, plumber (no tidyverse, catapultR)  #
################################################################################

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(jsonlite)
  library(plumber)
})

message("=== WEBHOOK PROCESSOR LOADED ===")

#—— CONFIGURATION ——————————————————————————————————————————————————————————————
cfg <- list(
  dirs = list(
    # Local Windows path for log and data
    base         = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test/Merged Files",
    raw          = file.path(base, "CSVs_Raw"),
    clean        = file.path(base, "CSVs_Clean"),
    merged       = file.path(base, "Merged Files"),
    webhook_conn = file.path(base, "Webhook - Connected")
  ),
  files = list(
    activities_csv = file.path(dirs$merged, "df_activities.csv"),
    log_csv        = file.path(dirs$webhook_conn, "webhook_processing_log.csv")
  ),
  api = list(
    base_url    = "https://connect-us.catapultsports.com/api/v6",
    token       = Sys.getenv("CATAPULT_AUTH_TOKEN"),
    retry       = 2L,
    backoff_sec = 1
  )
)

# Ensure output directories exist
lapply(cfg$dirs, function(d) dir.create(d, recursive = TRUE, showWarnings = FALSE))

# Pre-built headers
api_headers <- c(
  Authorization = sprintf("Bearer %s", cfg$api$token),
  Accept        = "application/json"
)

#—— UTILITY ——————————————————————————————————————————————————————————————————
fetch_json <- function(endpoint) {
  url <- paste0(cfg$api$base_url, endpoint)
  for (i in seq_len(cfg$api$retry)) {
    resp <- try(GET(url, add_headers(.headers = api_headers)), silent = TRUE)
    if (inherits(resp, "response") && status_code(resp) == 200L) {
      content_txt <- content(resp, as = "text", encoding = "UTF-8")
      return(if (nzchar(content_txt)) fromJSON(content_txt, flatten = TRUE) else NULL)
    }
    Sys.sleep(cfg$api$backoff_sec)
  }
  warning("Failed to GET ", url)
  NULL
}

parse_catapult_data <- function(data_list) {
  if (is.null(data_list) || length(data_list) == 0) return(data.table())
  DT <- setDT(data_list)
  for (col in names(DT)[sapply(DT, is.list)]) {
    DT[, (col) := vapply(get(col), function(x) {
      if (is.null(x) || length(x) == 0) return(NA_character_)
      paste0(unlist(x), collapse = ", ")
    }, FUN.VALUE = "")]
  }
  DT[, names(DT) := lapply(.SD, as.character)]
  DT
}

append_csv_unique <- function(DT, file, id_col = "activity_id") {
  if (nrow(DT) == 0) {
    message("ℹ No new rows to append to ", basename(file)); return(FALSE)
  }
  if (file.exists(file)) {
    existing <- fread(file)
    combined <- rbind(existing, DT, fill = TRUE)
    setkeyv(combined, id_col)
    combined <- unique(combined)
  } else {
    combined <- DT
  }
  fwrite(combined, file)
  message("✓ Wrote ", nrow(DT), " rows to ", basename(file))
  TRUE
}

#—— CORE PROCESSING ————————————————————————————————————————————————————————————————
process_new_activity <- function(activity_id, event_type) {
  start_time <- Sys.time()
  message(sprintf(">> Starting process for %s (%s)", activity_id, event_type))

  raw <- fetch_json(sprintf("/activities/%s", activity_id))
  if (is.null(raw)) {
    msg <- "Fetch failed"; success <- FALSE
  } else {
    DT <- parse_catapult_data(raw)
    if ("id" %in% names(DT)) setnames(DT, "id", "activity_id")
    if (append_csv_unique(DT, cfg$files$activities_csv)) {
      msg <- "Processed successfully"; success <- TRUE
    } else {
      msg <- "Save failed"; success <- FALSE
    }
  }

  log_entry <- data.table(
    timestamp   = format(start_time, "%Y-%m-%d %H:%M:%S"),
    activity_id = activity_id,
    event_type  = event_type,
    success     = success,
    message     = msg
  )
  append_csv_unique(log_entry, cfg$files$log_csv, id_col = "timestamp")

  message(sprintf("<< Finished: %s", msg))
  list(activity_id = activity_id, event_type = event_type,
       success = success, message = msg)
}

# Entrypoint for the webhook
t##* @post /openfield-webhook
##* @serializer json
function(req, res) {
  payload <- tryCatch(
    if (is.character(req$postBody)) fromJSON(req$postBody) else req$postBody,
    error = function(e) NULL
  )
  if (is.null(payload)) {
    return(list(success=FALSE, message="Invalid JSON payload"))
  }

  # Directly use trigger$id as the Catapult activity_id
  activity_id <- payload$trigger$id
  event_type  <- payload$action

  if (event_type %in% c("created", "updated")) {
    return(process_new_activity(activity_id, event_type))
  }

  list(activity_id = activity_id, event_type = event_type,
       success = TRUE, message = "Event ignored")
}
