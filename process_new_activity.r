################################################################################
#                    WEBHOOK-TRIGGERED ACTIVITY PROCESSOR                     #
################################################################################
# Processes new activities based on webhook notifications
# Designed for real-time processing of individual activities
# 
# Author: Automated Pipeline Generator
# Created: 2025
# Target: Real-time processing (<5 minutes per activity)
################################################################################

# Load required libraries
suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(httr)
  library(jsonlite)
  library(catapultR)
  library(lubridate)
  library(openxlsx)
})

cat("=== WEBHOOK-TRIGGERED ACTIVITY PROCESSOR STARTED ===\n")
start_time <- Sys.time()

################################################################################
#                           CONFIGURATION                                     #
################################################################################

CONFIG <- list(
  # Directory configuration
  base_dir = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test",
  raw_dir = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test/CSVs_Raw",
  clean_dir = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test/CSVs_Clean",
  output_dir = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Houston Test/Merged Files",
  foundational_dir = "C:/Users/Torin/Documents/Strength and Conditioning - Jobs/API Test/Foundational_CSVs",
  
  # API configuration
  auth_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6IjEzMWY5NGIxOTg3ZGY4NzcxNTljOGQ2MTAzMTIzNDNjIn0.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJlNDQ1Nzc0NDZlNmUyNzIxNTZkMDMwOTY4ZmY1MjFlMDNmMzkyNzhlMTM1NDEzMDY4OTg1YTU0OGNmODQ1OGZkMGVkZjBmZGU0ZjY1OWJmMSIsImlhdCI6MTc0OTg0ODI3NC43OTI1NzMsIm5iZiI6MTc0OTg0ODI3NC43OTI1NzcsImV4cCI6NDkwMzQ0ODI3NC43Nzk5ODcsInN1YiI6IjdmYjIxMWE4LTgwODMtNGFiZS04NWI5LTU5YjQyNzZjNTdhMSIsInNjb3BlcyI6WyJjb25uZWN0Il0sImlzcyI6Imh0dHBzOi8vYmFja2VuZC11cy5vcGVuZmllbGQuY2F0YXB1bHRzcG9ydHMuY29tIiwiY29tLmNhdGFwdWx0c3BvcnRzIjp7Im9wZW5maWVsZCI6eyJjdXN0b21lcnMiOlt7InJlbGF0aW9uIjoiYXV0aCIsImlkIjo2ODd9XX19fQ.aBCJ333ycM0_crBs_OhJOhcYRuF_bCH1RdYGK1FQUckXQmSLnbypdyodn0buY9xFBtVfCcvnyAWuM9NbtlnoJ8zKxWRP03sY5iAY3y2fCYfVubO5AK-6VPPAiNZzYU1pu7AK4tDr0YH2G7Cdic5vPdIp4CGmU3p-3IM7sZ73fJSxJNCaaddZyjxjruruWd5fBNG-0IsFOrcZflFuCVXk855rrzCa2ZhB-IDBxipemD6AEvleyiFondpnbEhb3s7k9Hu8wgawqUccq6QvBqsvQwFcWyNDrVIRnqUPC8-3vnaZKud8QG-t48aKkPMFLdiOqhYuS27qeZqn_uWtRl3kKp878iotYGcaTqFfOXd_WRNmXRLSHKHfoocCS93imwnTEUvlT-c5hMMd9UVEUQ6Ht5bZb6BWRflE7PwHPufKkssMZcRf-iAAiZbsUOlFKwzI-2L1pK_xrDLXMqDRlaa5459QuFJf2lgkrFS4akJnnD28TXhTJUFZQ2M9Vy2Tg3IICGx5PpWMnIjWswsEw2zFqwiLnBHJVaXG_6GEuKfoBkxyU2_zRmYrw0r-g_Jmj9noypw2Ok2P57x22w9NroKyg14vUtMxfYh7nHzWeGM_NGlREmld8ptnKaa5X6eOCDAXv2M6twdkvqIi2ZDO9bKIqLV6o2gwtq6qvyHQPjXBSiQ",
  
  # Processing configuration (no roster filtering for webhook processing)
  enable_roster_filtering = FALSE,
  enable_outlier_detection = TRUE,
  enable_excel_export = FALSE,  # Disabled for speed in real-time processing
  
  # API timeouts and retries for real-time processing
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

# CatapultR token
catapultr_token <- ofCloudCreateToken(sToken = CONFIG$auth_token, sRegion = "America")

# Enhanced API fetch with error handling and retries
fetch_api_data <- function(url, retries = CONFIG$api_timeout_retry) {
  for (attempt in 1:retries) {
    tryCatch({
      response <- VERB("GET", url, add_headers(.headers = api_headers), content_type("application/octet-stream"))
      
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

# Enhanced data parsing
parse_catapult_data <- function(data) {
  if (is.null(data) || length(data) == 0) return(data.frame())
  
  dt <- as.data.table(data)
  
  # Process list columns efficiently
  list_cols <- names(dt)[sapply(dt, is.list)]
  if (length(list_cols) > 0) {
    for (col in list_cols) {
      dt[, (col) := sapply(get(col), function(item) {
        tryCatch({
          if (is.null(item) || length(item) == 0) {
            return(NA_character_)
          }
          if (is.data.frame(item)) {
            return(jsonlite::toJSON(item, auto_unbox = TRUE, null = "null"))
          }
          if (is.list(item)) {
            return(paste(unlist(item, use.names = FALSE), collapse = ", "))
          }
          if (length(item) > 1) {
            return(paste(as.character(item), collapse = ", "))
          }
          return(as.character(item)[1])
        }, error = function(e) {
          return(paste("ERROR:", e$message))
        })
      })]
    }
  }
  
  # Ensure all columns are character for consistency
  dt[, names(dt) := lapply(.SD, as.character)]
  
  result <- as.data.frame(dt)
  rm(dt)
  return(result)
}

# Safe CSV reading function
read_existing_csv_safe <- function(file_path, description) {
  if (file.exists(file_path)) {
    tryCatch({
      data <- fread(file_path)
      cat(sprintf("✓ Read existing %s: %d rows\n", description, nrow(data)))
      return(data)
    }, error = function(e) {
      cat(sprintf("⚠ Error reading %s: %s\n", description, e$message))
      return(data.table())
    })
  } else {
    cat(sprintf("ℹ %s not found - will create new file\n", description))
    return(data.table())
  }
}

# Safe CSV append function
append_to_csv_safe <- function(new_data, file_path, description) {
  if (nrow(new_data) == 0) {
    cat(sprintf("ℹ No %s to append\n", description))
    return(FALSE)
  }
  
  tryCatch({
    if (file.exists(file_path)) {
      # Read existing data
      existing_data <- fread(file_path)
      
      # Ensure column compatibility
      common_cols <- intersect(names(existing_data), names(new_data))
      if (length(common_cols) == 0) {
        cat(sprintf("⚠ No common columns between existing and new %s\n", description))
        return(FALSE)
      }
      
      # Align columns and combine
      existing_subset <- existing_data[, ..common_cols]
      new_subset <- new_data[, ..common_cols]
      
      # Ensure consistent data types
      existing_subset[] <- lapply(existing_subset, as.character)
      new_subset[] <- lapply(new_subset, as.character)
      
      combined_data <- rbind(existing_subset, new_subset)
      
      # Remove duplicates if ID column exists
      id_cols <- c("activity_id", "period_id", "athlete_id", "id")
      existing_id_col <- intersect(id_cols, names(combined_data))
      if (length(existing_id_col) > 0) {
        original_rows <- nrow(combined_data)
        combined_data <- combined_data[!duplicated(get(existing_id_col[1]))]
        if (nrow(combined_data) < original_rows) {
          cat(sprintf("✓ Removed %d duplicate %s records\n", original_rows - nrow(combined_data), description))
        }
      }
      
      fwrite(combined_data, file_path)
      cat(sprintf("✓ Appended %s: %d new rows, %d total rows\n", description, nrow(new_subset), nrow(combined_data)))
    } else {
      # Create new file
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
  cat(sprintf("\n=== PROCESSING NEW ACTIVITY: %s (Event: %s) ===\n", activity_id, event_type))
  
  processing_stats <- list(
    activity_id = activity_id,
    event_type = event_type,
    start_time = Sys.time(),
    api_requests = 0,
    records_processed = 0,
    files_updated = 0,
    errors = 0
  )
  
  ################################################################################
  #                        FETCH ACTIVITY DETAILS                               #
  ################################################################################
  
  cat("Fetching activity details...\n")
  
  # Fetch specific activity
  activity_url <- sprintf("https://connect-us.catapultsports.com/api/v6/activities/%s", activity_id)
  activity_data <- fetch_api_data(activity_url)
  processing_stats$api_requests <- processing_stats$api_requests + 1
  
  if (is.null(activity_data)) {
    cat(sprintf("✗ Failed to fetch activity %s\n", activity_id))
    processing_stats$errors <- processing_stats$errors + 1
    return(processing_stats)
  }
  
  # Parse activity data
  new_activity <- parse_catapult_data(activity_data)
  rm(activity_data)
  
  if (nrow(new_activity) == 0) {
    cat(sprintf("✗ No activity data returned for %s\n", activity_id))
    processing_stats$errors <- processing_stats$errors + 1
    return(processing_stats)
  }
  
  # Standardize activity ID column
  if ("id" %in% names(new_activity)) {
    names(new_activity)[names(new_activity) == "id"] <- "activity_id"
  }
  
  cat(sprintf("✓ Activity details fetched: %d row\n", nrow(new_activity)))
  processing_stats$records_processed <- processing_stats$records_processed + nrow(new_activity)
  
  ################################################################################
  #                        PROCESS ACTIVITY DATA                                #
  ################################################################################
  
  # Convert to data.table for processing
  dt_activity <- as.data.table(new_activity)
  
  # Enhanced activity processing
  if ("activity_athletes" %in% names(dt_activity)) {
    dt_activity[, activity_athletes := map_chr(activity_athletes, function(x) {
      if (is.list(x)) paste(map_chr(x, ~ paste(unlist(.), collapse = ":")), collapse = ", ")
      else as.character(x)
    })]
  }
  
  # Time processing with robust format handling
  tryCatch({
    dt_activity[, `:=`(
      start_time = tryCatch({
        as.POSIXct(as.numeric(start_time), origin = "1970-01-01", tz = "UTC")
      }, error = function(e) {
        as.POSIXct(start_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      }),
      end_time = tryCatch({
        as.POSIXct(as.numeric(end_time), origin = "1970-01-01", tz = "UTC")
      }, error = function(e) {
        as.POSIXct(end_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      })
    )]
    
    dt_activity[, `:=`(
      start_date = as.Date(start_time),
      end_date = as.Date(end_time),
      activity_date = as.Date(start_time)
    )]
  }, error = function(e) {
    cat(sprintf("⚠ Warning: Time parsing failed: %s\n", e$message))
    dt_activity[, activity_date := Sys.Date()]
  })
  
  # Session classification
  if ("athlete_count" %in% names(dt_activity)) {
    # Simple classification for single activity
    athlete_count <- as.numeric(dt_activity$athlete_count[1])
    dt_activity[, session_class := fcase(
      athlete_count <= 5, "Small Group/RTP",
      athlete_count >= 15, "Team",
      default = "Medium Group"
    )]
  }
  
  # Clean tags
  if ("tags" %in% names(dt_activity)) {
    dt_activity[, tags := gsub("GPS,\\s*|,\\s*GPS|^GPS$", "", tags)]
    dt_activity[tags == "", tags := NA]
  }
  
  new_activity <- as.data.frame(dt_activity)
  rm(dt_activity)
  
  ################################################################################
  #                        FETCH RELATED PERIODS                                #
  ################################################################################
  
  cat("Fetching periods for activity...\n")
  
  # Fetch periods for this activity
  periods_url <- sprintf("https://connect-us.catapultsports.com/api/v6/periods?activity_id=%s", activity_id)
  periods_data <- fetch_api_data(periods_url)
  processing_stats$api_requests <- processing_stats$api_requests + 1
  
  new_periods <- data.frame()
  if (!is.null(periods_data)) {
    new_periods <- parse_catapult_data(periods_data)
    rm(periods_data)
    
    if (nrow(new_periods) > 0) {
      # Standardize period ID column
      if ("id" %in% names(new_periods)) {
        names(new_periods)[names(new_periods) == "id"] <- "period_id"
      }
      
      # Process periods
      dt_periods <- as.data.table(new_periods)
      
      # Time processing
      tryCatch({
        dt_periods[, `:=`(
          start_date_time = tryCatch({
            as.POSIXct(as.numeric(start_time), origin = "1970-01-01", tz = "UTC")
          }, error = function(e) {
            as.POSIXct(start_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
          }),
          end_date_time = tryCatch({
            as.POSIXct(as.numeric(end_time), origin = "1970-01-01", tz = "UTC")
          }, error = function(e) {
            as.POSIXct(end_time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
          })
        )]
        
        dt_periods[, `:=`(
          start_date = as.Date(start_date_time),
          start_time = format(start_date_time, "%H:%M:%S"),
          end_date = as.Date(end_date_time),
          end_time = format(end_date_time, "%H:%M:%S"),
          activity_date = as.Date(start_date_time)
        )]
      }, error = function(e) {
        cat(sprintf("⚠ Warning: Period time parsing failed: %s\n", e$message))
        dt_periods[, activity_date := Sys.Date()]
      })
      
      # Calculate period duration
      dt_periods[, period_duration := as.numeric(difftime(end_date_time, start_date_time, units = "secs"))]
      
      # Clean tags
      if ("tags" %in% names(dt_periods)) {
        dt_periods[, tags := gsub("GPS,\\s*|,\\s*GPS|^GPS$", "", tags)]
        dt_periods[, tags := gsub("0e81fa0a-e08e-4975-a6c6-f1240aa6337c,\\s*|,\\s*0e81fa0a-e08e-4975-a6c6-f1240aa6337c", "", tags)]
      }
      
      new_periods <- as.data.frame(dt_periods)
      rm(dt_periods)
      
      cat(sprintf("✓ Periods fetched: %d rows\n", nrow(new_periods)))
      processing_stats$records_processed <- processing_stats$records_processed + nrow(new_periods)
    }
  }
  
  ################################################################################
  #                       FETCH AGGREGATED STATISTICS                           #
  ################################################################################
  
  cat("Fetching aggregated statistics...\n")
  
  # Load parameter slugs
  activity_slugs <- character(0)
  period_slugs <- character(0)
  
  tryCatch({
    activity_slugs_file <- file.path(CONFIG$foundational_dir, "slugs_activities.csv")
    if (file.exists(activity_slugs_file)) {
      activity_slugs <- as.character(read.csv(activity_slugs_file, header = FALSE)[,1])
    }
    
    period_slugs_file <- file.path(CONFIG$foundational_dir, "slugs_periods.csv")
    if (file.exists(period_slugs_file)) {
      period_slugs <- as.character(read.csv(period_slugs_file, header = FALSE)[,1])
    }
  }, error = function(e) {
    cat(sprintf("⚠ Warning loading parameter slugs: %s\n", e$message))
  })
  
  # Fetch aggregated activity stats
  new_agg_activity <- data.frame()
  if (length(activity_slugs) > 0) {
    tryCatch({
      agg_data <- ofCloudGetStatistics(
        catapultr_token,
        params = activity_slugs,
        groupby = c("athlete", "period", "activity"),
        filters = list(
          name = "activity_id",
          comparison = "=",
          values = activity_id
        )
      )
      processing_stats$api_requests <- processing_stats$api_requests + 1
      
      if (!is.null(agg_data) && nrow(agg_data) > 0) {
        new_agg_activity <- parse_catapult_data(agg_data)
        
        # Ensure activity_date column exists
        if (!"activity_date" %in% names(new_agg_activity)) {
          date_cols <- names(new_agg_activity)[grepl("date", names(new_agg_activity), ignore.case = TRUE)]
          if (length(date_cols) > 0) {
            new_agg_activity$activity_date <- as.Date(new_agg_activity[[date_cols[1]]], format = "%m/%d/%Y")
          } else {
            new_agg_activity$activity_date <- Sys.Date()
          }
        }
        
        cat(sprintf("✓ Aggregated activity stats: %d rows\n", nrow(new_agg_activity)))
        processing_stats$records_processed <- processing_stats$records_processed + nrow(new_agg_activity)
      }
    }, error = function(e) {
      cat(sprintf("⚠ Error fetching aggregated activity stats: %s\n", e$message))
      processing_stats$errors <- processing_stats$errors + 1
    })
  }
  
  # Fetch aggregated period stats
  new_agg_period <- data.frame()
  if (nrow(new_periods) > 0 && length(period_slugs) > 0) {
    period_ids <- new_periods$period_id[!is.na(new_periods$period_id)]
    
    if (length(period_ids) > 0) {
      tryCatch({
        agg_data <- ofCloudGetStatistics(
          catapultr_token,
          params = period_slugs,
          groupby = c("athlete", "period", "activity"),
          filters = list(
            name = "period_id",
            comparison = "=",
            values = period_ids
          )
        )
        processing_stats$api_requests <- processing_stats$api_requests + 1
        
        if (!is.null(agg_data) && nrow(agg_data) > 0) {
          new_agg_period <- parse_catapult_data(agg_data)
          
          # Ensure activity_date column exists
          if (!"activity_date" %in% names(new_agg_period)) {
            date_cols <- names(new_agg_period)[grepl("date", names(new_agg_period), ignore.case = TRUE)]
            if (length(date_cols) > 0) {
              new_agg_period$activity_date <- as.Date(new_agg_period[[date_cols[1]]], format = "%m/%d/%Y")
            } else {
              new_agg_period$activity_date <- Sys.Date()
            }
          }
          
          cat(sprintf("✓ Aggregated period stats: %d rows\n", nrow(new_agg_period)))
          processing_stats$records_processed <- processing_stats$records_processed + nrow(new_agg_period)
        }
      }, error = function(e) {
        cat(sprintf("⚠ Error fetching aggregated period stats: %s\n", e$message))
        processing_stats$errors <- processing_stats$errors + 1
      })
    }
  }
  
  ################################################################################
  #                            SAVE NEW DATA                                    #
  ################################################################################
  
  cat("Saving new data to existing files...\n")
  
  # Define file paths
  files_to_update <- list(
    list(data = new_activity, file = file.path(CONFIG$output_dir, "df_activities.csv"), desc = "activities"),
    list(data = new_periods, file = file.path(CONFIG$output_dir, "df_periods.csv"), desc = "periods"),
    list(data = new_agg_activity, file = file.path(CONFIG$output_dir, "df_agg_activity.csv"), desc = "aggregated activity stats"),
    list(data = new_agg_period, file = file.path(CONFIG$output_dir, "df_agg_period.csv"), desc = "aggregated period stats")
  )
  
  # Save/append data
  for (file_info in files_to_update) {
    if (nrow(file_info$data) > 0) {
      # Convert to data.table for append operation
      dt_data <- as.data.table(file_info$data)
      
      if (append_to_csv_safe(dt_data, file_info$file, file_info$desc)) {
        processing_stats$files_updated <- processing_stats$files_updated + 1
      }
    }
  }
  
  ################################################################################
  #                            LOG PROCESSING RESULTS                           #
  ################################################################################
  
  processing_stats$end_time <- Sys.time()
  processing_stats$total_time <- processing_stats$end_time - processing_stats$start_time
  
  # Create processing log entry
  log_entry <- data.frame(
    timestamp = format(processing_stats$start_time, "%Y-%m-%d %H:%M:%S"),
    activity_id = activity_id,
    event_type = event_type,
    processing_time_seconds = round(as.numeric(processing_stats$total_time, units = "secs"), 2),
    api_requests = processing_stats$api_requests,
    records_processed = processing_stats$records_processed,
    files_updated = processing_stats$files_updated,
    errors = processing_stats$errors,
    success = processing_stats$errors == 0,
    stringsAsFactors = FALSE
  )
  
  # Append to processing log
  log_file <- file.path(CONFIG$output_dir, "webhook_processing_log.csv")
  tryCatch({
    if (file.exists(log_file)) {
      existing_log <- fread(log_file)
      combined_log <- rbind(existing_log, log_entry)
      fwrite(combined_log, log_file)
    } else {
      fwrite(log_entry, log_file)
    }
    cat(sprintf("✓ Processing log updated: %s\n", log_file))
  }, error = function(e) {
    cat(sprintf("⚠ Warning: Could not save processing log: %s\n", e$message))
  })
  
  ################################################################################
  #                              SUMMARY                                        #
  ################################################################################
  
  cat(sprintf("\n=== PROCESSING SUMMARY FOR ACTIVITY %s ===\n", activity_id))
  cat(sprintf("Event Type: %s\n", event_type))
  cat(sprintf("Processing Time: %.2f seconds\n", as.numeric(processing_stats$total_time, units = "secs")))
  cat(sprintf("API Requests: %d\n", processing_stats$api_requests))
  cat(sprintf("Records Processed: %d\n", processing_stats$records_processed))
  cat(sprintf("Files Updated: %d\n", processing_stats$files_updated))
  
  if (processing_stats$errors > 0) {
    cat(sprintf("⚠ Errors: %d\n", processing_stats$errors))
  } else {
    cat("✓ Processing completed successfully\n")
  }
  
  return(processing_stats)
}

################################################################################
#                        WEBHOOK INTEGRATION FUNCTION                         #
################################################################################

# Function to be called by the webhook handler
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
    cat("✗ Invalid webhook payload\n")
    return(NULL)
  }
  
  # Extract activity information
  event_type <- payload$action %||% "unknown"
  activity_id <- payload$trigger$id %||% NULL
  
  if (is.null(activity_id)) {
    cat("✗ No activity ID found in webhook payload\n")
    return(NULL)
  }
  
  cat(sprintf("Webhook received: %s for activity %s\n", event_type, activity_id))
  
  # Process based on event type
  if (event_type == "created") {
    cat("Processing new activity creation...\n")
    result <- process_new_activity(activity_id, "created")
    return(result)
  } else if (event_type == "updated") {
    cat("Processing activity update...\n")
    # For now, treat updates the same as creation (re-fetch all data)
    result <- process_new_activity(activity_id, "updated")
    return(result)
  } else if (event_type == "deleted") {
    cat("Processing activity deletion...\n")
    # TODO: Implement deletion logic
    cat("⚠ Deletion processing not yet implemented\n")
    return(list(activity_id = activity_id, event_type = "deleted", message = "Not implemented"))
  } else {
    cat(sprintf("⚠ Unknown event type: %s\n", event_type))
    return(list(activity_id = activity_id, event_type = event_type, message = "Unknown event type"))
  }
}

################################################################################
#                            COMMAND LINE INTERFACE                           #
################################################################################

# Allow script to be called directly with activity ID
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) == 0) {
    cat("Usage: Rscript process_new_activity.R <activity_id> [event_type]\n")
    cat("   or: Called by webhook with JSON payload\n")
    quit(status = 1)
  }
  
  if (length(args) == 1) {
    # Single argument - assume it's activity_id
    activity_id <- args[1]
    event_type <- "created"
    cat(sprintf("Processing activity %s (manual trigger)\n", activity_id))
    result <- process_new_activity(activity_id, event_type)
  } else if (length(args) == 2) {
    # Two arguments - activity_id and event_type
    activity_id <- args[1]
    event_type <- args[2]
    cat(sprintf("Processing activity %s with event type %s (manual trigger)\n", activity_id, event_type))
    result <- process_new_activity(activity_id, event_type)
  } else {
    cat("Too many arguments provided\n")
    quit(status = 1)
  }
  
  # Exit with appropriate status
  if (!is.null(result) && result$errors == 0) {
    cat("Script completed successfully\n")
    quit(status = 0)
  } else {
    cat("Script completed with errors\n")
    quit(status = 1)
  }
}

cat("=== WEBHOOK-TRIGGERED ACTIVITY PROCESSOR LOADED ===\n")
cat("Available functions:\n")
cat("  - process_new_activity(activity_id, event_type)\n")
cat("  - process_webhook_activity(webhook_payload)\n")
cat("Ready to process webhook notifications.\n")