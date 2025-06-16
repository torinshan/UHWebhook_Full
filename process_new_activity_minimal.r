################################################################################
#                    MINIMAL WEBHOOK-TRIGGERED ACTIVITY PROCESSOR             #
################################################################################
# Simplified version using only basic packages available in container
# Uses: data.table, httr, jsonlite (no tidyverse, catapultR, etc.)
################################################################################

# Load only the packages that are installed
suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(jsonlite)
})

cat("=== MINIMAL WEBHOOK-TRIGGERED ACTIVITY PROCESSOR STARTED ===\n")

################################################################################
#                           CONFIGURATION                                     #
################################################################################

CONFIG <- list(
  # Directory configuration (Docker-friendly paths)
  base_dir = "/app/data",
  raw_dir = "/app/data/CSVs_Raw",
  clean_dir = "/app/data/CSVs_Clean",
  output_dir = "/app/data/Merged Files",
  foundational_dir = "/app/Foundational_CSVs",
  
  # API configuration
  auth_token = Sys.getenv("CATAPULT_AUTH_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6IjEzMWY5NGIxOTg3ZGY4NzcxNTljOGQ2MTAzMTIzNDNjIn0.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJlNDQ1Nzc0NDZlNmUyNzIxNTZkMDMwOTY4ZmY1MjFlMDNmMzkyNzhlMTM1NDEzMDY4OTg1YTU0OGNmODQ1OGZkMGVkZjBmZGU0ZjY1OWJmMSIsImlhdCI6MTc0OTg0ODI3NC43OTI1NzMsIm5iZiI6MTc0OTg0ODI3NC43OTI1NzcsImV4cCI6NDkwMzQ0ODI3NC43Nzk5ODcsInN1YiI6IjdmYjIxMWE4LTgwODMtNGFiZS04NWI5LTU5YjQyNzZjNTdhMSIsInNjb3BlcyI6WyJjb25uZWN0Il0sImlzcyI6Imh0dHBzOi8vYmFja2VuZC11cy5vcGVuZmllbGQuY2F0YXB1bHRzcG9ydHMuY29tIiwiY29tLmNhdGFwdWx0c3BvcnRzIjp7Im9wZW5maWVsZCI6eyJjdXN0b21lcnMiOlt7InJlbGF0aW9uIjoiYXV0aCIsImlkIjo2ODd9XX19fQ.aBCJ333ycM0_crBs_OhJOhcYRuF_bCH1RdYGK1FQUckXQmSLnbypdyodn0buY9xFBtVfCcvnyAWuM9NbtlnoJ8zKxWRP03sY5iAY3y2fCYfVubO5AK-6VPPAiNZzYU1pu7AK4tDr0YH2G7Cdic5vPdIp4CGmU3p-3IM7sZ73fJSxJNCaaddZyjxjruruWd5fBNG-0IsFOrcZflFuCVXk855rrzCa2ZhB-IDBxipemD6AEvleyiFondpnbEhb3s7k9Hu8wgawqUccq6QvBqsvQwFcWyNDrVIRnqUPC8-3vnaZKud8QG-t48aKkPMFLdiOqhYuS27qeZqn_uWtRl3kKp878iotYGcaTqFfOXd_WRNmXRLSHKHfoocCS93imwnTEUvlT-c5hMMd9UVEUQ6Ht5bZb6BWRflE7PwHPufKkssMZcRf-iAAiZbsUOlFKwzI-2L1pK_xrDLXMqDRlaa5459QuFJf2lgkrFS4akJnnD28TXhTJUFZQ2M9Vy2Tg3IICGx5PpWMnIjWswsEw2zFqwiLnBHJVaXG_6GEuKfoBkxyU2_zRmYrw0r-g_Jmj9noypw2Ok2P57x22w9NroKyg14vUtMxfYh7nHzWeGM_NGlREmld8ptnKaa5X6eOCDAXv2M6twdkvqIi2ZDO9bKIqLV6o2gwtq6qvyHQPjXBSiQ"),
  
  # Processing configuration
  api_timeout_retry = 2,
  api_delay_seconds = 1
)

# Create directories
dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

################################################################################
#                           UTILITY FUNCTIONS                                 #
################################################################################

# API headers
api_headers <- c(
  "Authorization" = paste("Bearer", CONFIG$auth_token),
  "Accept" = "application/json"
)

# Simple API fetch function
fetch_api_data <- function(url, retries = CONFIG$api_timeout_retry) {
  for (attempt in 1:retries) {
    tryCatch({
      response <- GET(url, add_headers(.headers = api_headers))
      
      if (status_code(response) == 200) {
        raw_content <- content(response, "text", encoding = "UTF-8")
        if (nchar(raw_content) > 0) {
          return(fromJSON(raw_content, flatten = TRUE))
        }
      }
      
      if (attempt == retries) {
        warning(sprintf("Failed to fetch data from %s after %d attempts", url, retries))
        return(NULL)
      }
      
      Sys.sleep(CONFIG$api_delay_seconds)
    }, error = function(e) {
      if (attempt == retries) {
        warning(sprintf("Error fetching from %s: %s", url, e$message))
        return(NULL)
      }
      Sys.sleep(CONFIG$api_delay_seconds)
    })
  }
}

# Simple data parsing
parse_catapult_data <- function(data) {
  if (is.null(data) || length(data) == 0) return(data.frame())
  
  dt <- as.data.table(data)
  
  # Convert list columns to character
  list_cols <- names(dt)[sapply(dt, is.list)]
  if (length(list_cols) > 0) {
    for (col in list_cols) {
      dt[, (col) := sapply(get(col), function(item) {
        if (is.null(item) || length(item) == 0) {
          return(NA_character_)
        }
        if (length(item) > 1) {
          return(paste(as.character(item), collapse = ", "))
        }
        return(as.character(item)[1])
      })]
    }
  }
  
  # Ensure all columns are character
  dt[, names(dt) := lapply(.SD, as.character)]
  
  return(as.data.frame(dt))
}

# Safe CSV append function
append_to_csv_safe <- function(new_data, file_path, description) {
  if (nrow(new_data) == 0) {
    cat(sprintf("ℹ No %s to append\n", description))
    return(FALSE)
  }
  
  tryCatch({
    if (file.exists(file_path)) {
      existing_data <- fread(file_path)
      
      # Simple append
      combined_data <- rbind(existing_data, new_data, fill = TRUE)
      
      # Remove duplicates if ID column exists
      if ("activity_id" %in% names(combined_data)) {
        combined_data <- combined_data[!duplicated(activity_id)]
      }
      
      fwrite(combined_data, file_path)
      cat(sprintf("✓ Appended %s: %d new rows\n", description, nrow(new_data)))
    } else {
      fwrite(new_data, file_path)
      cat(sprintf("✓ Created new %s file: %d rows\n", description, nrow(new_data)))
    }
    return(TRUE)
  }, error = function(e) {
    cat(sprintf("✗ Error appending %s: %s\n", description, e$message))
    return(FALSE)
  })
}

################################################################################
#                      MAIN PROCESSING FUNCTION                               #
################################################################################

process_new_activity <- function(activity_id, event_type = "created") {
  cat(sprintf("\n=== PROCESSING ACTIVITY: %s (Event: %s) ===\n", activity_id, event_type))
  
  processing_stats <- list(
    activity_id = activity_id,
    event_type = event_type,
    start_time = Sys.time(),
    success = FALSE,
    message = ""
  )
  
  # Fetch activity details
  cat("Fetching activity details...\n")
  activity_url <- sprintf("https://connect-us.catapultsports.com/api/v6/activities/%s", activity_id)
  activity_data <- fetch_api_data(activity_url)
  
  if (is.null(activity_data)) {
    processing_stats$message <- "Failed to fetch activity data"
    return(processing_stats)
  }
  
  # Parse activity data
  new_activity <- parse_catapult_data(activity_data)
  
  if (nrow(new_activity) == 0) {
    processing_stats$message <- "No activity data returned"
    return(processing_stats)
  }
  
  # Standardize activity ID column
  if ("id" %in% names(new_activity)) {
    names(new_activity)[names(new_activity) == "id"] <- "activity_id"
  }
  
  cat(sprintf("✓ Activity details fetched: %d row\n", nrow(new_activity)))
  
  # Save activity data
  activity_file <- file.path(CONFIG$output_dir, "df_activities.csv")
  if (append_to_csv_safe(as.data.table(new_activity), activity_file, "activities")) {
    processing_stats$success <- TRUE
    processing_stats$message <- "Activity processed successfully"
  } else {
    processing_stats$message <- "Failed to save activity data"
  }
  
  processing_stats$end_time <- Sys.time()
  
  # Log processing
  log_entry <- data.frame(
    timestamp = format(processing_stats$start_time, "%Y-%m-%d %H:%M:%S"),
    activity_id = activity_id,
    event_type = event_type,
    success = processing_stats$success,
    message = processing_stats$message,
    stringsAsFactors = FALSE
  )
  
  log_file <- file.path(CONFIG$output_dir, "webhook_processing_log.csv")
  tryCatch({
    if (file.exists(log_file)) {
      existing_log <- fread(log_file)
      combined_log <- rbind(existing_log, log_entry)
      fwrite(combined_log, log_file)
    } else {
      fwrite(log_entry, log_file)
    }
  }, error = function(e) {
    cat(sprintf("⚠ Warning: Could not save processing log: %s\n", e$message))
  })
  
  cat(sprintf("✓ Processing completed: %s\n", processing_stats$message))
  return(processing_stats)
}

################################################################################
#                        WEBHOOK INTEGRATION FUNCTION                         #
################################################################################

process_webhook_activity <- function(webhook_payload) {
  cat("=== WEBHOOK ACTIVITY PROCESSOR ===\n")
  
  # Parse webhook payload
  if (is.character(webhook_payload)) {
    payload <- tryCatch({
      fromJSON(webhook_payload)
    }, error = function(e) {
      cat(sprintf("✗ Error parsing webhook payload: %s\n", e$message))
      return(NULL)
    })
  } else {
    payload <- webhook_payload
  }
  
  if (is.null(payload)) {
    return(list(success = FALSE, message = "Invalid webhook payload"))
  }
  
  # Extract activity information
  event_type <- payload$action
  activity_id <- payload$trigger$id
  
  if (is.null(activity_id)) {
    return(list(success = FALSE, message = "No activity ID found"))
  }
  
  cat(sprintf("Processing webhook: %s for activity %s\n", event_type, activity_id))
  
  # Process the activity
  if (event_type %in% c("created", "updated")) {
    result <- process_new_activity(activity_id, event_type)
    return(result)
  } else {
    return(list(
      activity_id = activity_id, 
      event_type = event_type, 
      success = TRUE,
      message = "Event logged but not processed"
    ))
  }
}

cat("=== MINIMAL WEBHOOK PROCESSOR LOADED ===\n")
cat("Available functions:\n")
cat("  - process_new_activity(activity_id, event_type)\n")
cat("  - process_webhook_activity(webhook_payload)\n")
