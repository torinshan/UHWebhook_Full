################################################################################
#                   WEBHOOK-TRIGGERED ACTIVITY PROCESSOR                      #
#   Uses only: data.table, httr, jsonlite, plumber (no tidyverse, catapultR)  #
################################################################################

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(jsonlite)
})

message("=== WEBHOOK PROCESSOR LOADED ===")

#—— CONFIGURATION ——————————————————————————————————————————————————————————————
cfg <- list(
  dirs = list(
    base       = "/app/data",
    raw        = "/app/data/CSVs_Raw",
    clean      = "/app/data/CSVs_Clean",
    merged     = "/app/data/Merged Files",
    foundational = "/app/Foundational_CSVs"
  ),
  files = list(
    activities_csv = "/app/data/Merged Files/df_activities.csv",
    log_csv        = "/app/data/Merged Files/webhook_processing_log.csv"
  ),
  api = list(
    base_url    = "https://connect-us.catapultsports.com/api/v6",
    token       = Sys.getenv("CATAPULT_AUTH_TOKEN"),
    retry       = 2L,
    backoff_sec = 1
  )
)

# Ensure output directories exist
dir.create(cfg$dirs$merged, recursive = TRUE, showWarnings = FALSE)

# Pre‐built headers
api_headers <- c(
  Authorization = sprintf("Bearer %s", cfg$api$token),
  Accept        = "application/json"
)


#—— UTILITY ————————————————————————————————————————————————————————————————

# 1) Safe API GET with retries and backoff
fetch_json <- function(endpoint) {
  url <- paste0(cfg$api$base_url, endpoint)
  for (i in seq_len(cfg$api$retry)) {
    resp <- try(GET(url, add_headers(.headers = api_headers)), silent = TRUE)
    if (inherits(resp, "response") && status_code(resp) == 200L) {
      content_txt <- content(resp, as = "text", encoding = "UTF-8")
      if (nzchar(content_txt)) return(fromJSON(content_txt, flatten = TRUE))
      return(NULL)
    }
    Sys.sleep(cfg$api$backoff_sec)
  }
  warning("Failed to GET ", url)
  NULL
}

# 2) Convert nested lists → flat data.table of characters
parse_catapult_data <- function(data_list) {
  if (is.null(data_list) || length(data_list) == 0) return(data.table())

  DT <- setDT(data_list)
  # Flatten any list‐columns by pasting elements
  for (col in names(DT)[sapply(DT, is.list)]) {
    DT[, (col) := vapply(get(col), function(x) {
      if (is.null(x) || length(x) == 0) return(NA_character_)
      paste0(unlist(x), collapse = ", ")
    }, FUN.VALUE = "")]
  }
  # Force every column to character
  DT[, names(DT) := lapply(.SD, as.character)]
  DT
}

# 3) Append to CSV safely, deduplicating by activity_id
append_csv_unique <- function(DT, file, id_col = "activity_id") {
  if (nrow(DT) == 0) {
    message("ℹ No new rows to append to ", basename(file))
    return(FALSE)
  }
  if (file.exists(file)) {
    existing <- fread(file)
    combined <- rbind(existing, DT, fill = TRUE)
    # remove duplicates
    setkeyv(combined, id_col)
    combined <- unique(combined)
  } else {
    combined <- DT
  }
  fwrite(combined, file)
  message("✓ Wrote ", nrow(DT), " rows to ", basename(file))
  TRUE
}

#—— CORE PROCESSING —————————————————————————————————————————————————————————

# Fetch, parse, and save a single activity
process_new_activity <- function(activity_id, event_type) {
  start_time <- Sys.time()
  message(sprintf(">> Starting process for %s (%s)", activity_id, event_type))

  raw <- fetch_json(sprintf("/activities/%s", activity_id))
  if (is.null(raw)) {
    msg <- "Fetch failed"; success <- FALSE
  } else {
    DT <- parse_catapult_data(raw)
    # Rename "id" → "activity_id"
    if ("id" %in% names(DT)) setnames(DT, "id", "activity_id")
    if (append_csv_unique(DT, cfg$files$activities_csv)) {
      msg <- "Processed successfully"; success <- TRUE
    } else {
      msg <- "Save failed"; success <- FALSE
    }
  }

  # Log
  log_entry <- data.table(
    timestamp    = format(start_time, "%Y-%m-%d %H:%M:%S"),
    activity_id  = activity_id,
    event_type   = event_type,
    success      = success,
    message      = msg
  )
  append_csv_unique(log_entry, cfg$files$log_csv, id_col = "timestamp")  

  message(sprintf("<< Finished: %s", msg))
  list(activity_id = activity_id, event_type = event_type,
       success = success, message = msg)
}

# Entrypoint for the webhook
process_webhook_activity <- function(raw_payload) {
  payload <- tryCatch(
    if (is.character(raw_payload)) fromJSON(raw_payload) else raw_payload,
    error = function(e) NULL
  )
  if (is.null(payload)) {
    return(list(success = FALSE, message = "Invalid JSON payload"))
  }

  evt  <- payload$action
  aid  <- payload$trigger$id
  if (is.null(aid)) {
    return(list(success = FALSE, message = "No activity ID in payload"))
  }
  if (evt %in% c("created", "updated")) {
    return(process_new_activity(aid, evt))
  }

  # ignore other events
  list(activity_id = aid, event_type = evt,
       success = TRUE, message = "Event ignored")
}

message("Available functions: process_new_activity(), process_webhook_activity()")
