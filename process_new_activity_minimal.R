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
    base         = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test/Merged Files",
    raw          = file.path(cfg$dirs$base, "CSVs_Raw"),
    clean        = file.path(cfg$dirs$base, "CSVs_Clean"),
    merged       = file.path(cfg$dirs$base, "Merged Files"),
    webhook_conn = file.path(cfg$dirs$base, "Webhook - Connected")
  ),
  files = list(
    activities_csv = file.path(cfg$dirs$merged, "df_activities.csv"),
    log_csv        = file.path(cfg$dirs$webhook_conn, "webhook_processing_log.csv")
  ),
  api = list(
    base_url    = "https://connect-us.catapultsports.com/api/v6",
    token       = Sys.getenv("CATAPULT_AUTH_TOKEN"),
    retry       = 2L,
    backoff_sec = 1
  )
)
# Ensure directories exist
lapply(cfg$dirs, function(d) dir.create(d, recursive=TRUE, showWarnings=FALSE))

# Pre-built headers
auth_headers <- c(
  Authorization = sprintf("Bearer %s", cfg$api$token),
  Accept        = "application/json"
)

#—— UTILITY ——————————————————————————————————————————————————————————————————
fetch_json <- function(endpoint) {
  url <- paste0(cfg$api$base_url, endpoint)
  for (i in seq_len(cfg$api$retry)) {
    resp <- try(GET(url, add_headers(.headers = auth_headers)), silent = TRUE)
    if (inherits(resp, "response") && status_code(resp)==200L) {
      txt <- content(resp, as="text", encoding="UTF-8")
      return(if (nzchar(txt)) fromJSON(txt, flatten=TRUE) else NULL)
    }
    Sys.sleep(cfg$api$backoff_sec)
  }
  warning("Failed to GET ", url)
  NULL
}

parse_catapult_data <- function(data_list) {
  if (is.null(data_list) || length(data_list)==0) return(data.table())
  DT <- setDT(data_list)
  for (col in names(DT)[sapply(DT, is.list)]) {
    DT[, (col):= vapply(get(col), function(x) {
      if (is.null(x) || length(x)==0) return(NA_character_)
      paste0(unlist(x), collapse=", ")
    }, FUN.VALUE="")]
  }
  DT[, names(DT):= lapply(.SD, as.character)]
  DT
}

append_csv_unique <- function(DT, file, id_col="activity_id") {
  if (nrow(DT)==0) return(FALSE)
  if (file.exists(file)) {
    existing <- fread(file)
    combined <- unique(rbind(existing, DT, fill=TRUE), by=id_col)
  } else {
    combined <- DT
  }
  fwrite(combined, file)
  TRUE
}

#—— CORE PROCESSING ————————————————————————————————————————————————————————————————
process_new_activity <- function(activity_id, event_type) {
  start_time <- Sys.time()
  message(sprintf(">> Starting process for %s (%s)", activity_id, event_type))

  raw <- fetch_json(sprintf("/activities/%s", activity_id))
  if (is.null(raw)) {
    status <- list(success=FALSE, message="Fetch failed")
  } else {
    DT <- parse_catapult_data(raw)
    if ("id" %in% names(DT)) setnames(DT, "id", "activity_id")
    ok <- append_csv_unique(DT, cfg$files$activities_csv)
    status <- if (ok) list(success=TRUE, message="Processed successfully") else list(success=FALSE, message="Save failed")
  }

  # Log entry
  log_dt <- data.table(
    timestamp   = format(start_time, "%Y-%m-%d %H:%M:%S"),
    activity_id = activity_id,
    event_type  = event_type,
    success     = status$success,
    message     = status$message
  )
  append_csv_unique(log_dt, cfg$files$log_csv, id_col="timestamp")

  message(sprintf("<< Finished: %s", status$message))
  c(list(activity_id=activity_id, event_type=event_type), status)
}

# Wrapper for webhook payload
process_webhook_activity <- function(raw_body) {
  payload <- tryCatch(if(is.character(raw_body)) fromJSON(raw_body) else raw_body,
                      error=function(e) NULL)
  if (is.null(payload)) stop("Invalid JSON payload")
  aid <- payload$trigger$id
  evt <- payload$action
  if (evt %in% c("created","updated")) {
    return(process_new_activity(aid, evt))
  }
  list(activity_id=aid, event_type=evt, success=TRUE, message="Event ignored")
}

#—— PLUMBER ENDPOINT —————————————————————————————————————————————————————————————
#* @post /openfield-webhook
#* @serializer json
function(req, res) {
  tryCatch({
    process_webhook_activity(req$postBody)
  }, error=function(e){
    message("Error processing activity: ", e$message)
    res$status <- 500
    list(success=FALSE, message=e$message)
  })
}
