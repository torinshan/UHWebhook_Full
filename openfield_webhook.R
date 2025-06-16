library(plumber)
library(jsonlite)
library(data.table)

# Source the activity processor
source("process_new_activity.R")

# Define file paths
csv_file <- "activity_data.csv"
processing_log_file <- "webhook_processing_log.csv"

#* @post /openfield-webhook
function(req, res) {
  start_time <- Sys.time()
  
  tryCatch({
    # Parse the incoming payload
    payload <- fromJSON(req$postBody)
    
    # Log the raw webhook data
    webhook_data <- data.frame(
      webhook_timestamp = as.character(Sys.time()),
      activity_id = payload$trigger$id %||% "unknown",
      event_type = payload$action %||% "unknown",
      raw_payload = req$postBody,
      stringsAsFactors = FALSE
    )
    
    # Append to webhook log
    if (!file.exists(csv_file)) {
      fwrite(webhook_data, csv_file)
    } else {
      fwrite(webhook_data, csv_file, append = TRUE)
    }
    
    # Extract key information
    activity_id <- payload$trigger$id
    event_type <- payload$action
    
    cat(sprintf("Webhook received at %s: %s for activity %s\n", 
                Sys.time(), event_type, activity_id))
    
    # Process the activity based on event type
    processing_result <- NULL
    
    if (!is.null(activity_id) && event_type %in% c("created", "updated")) {
      cat(sprintf("Triggering processing for activity %s...\n", activity_id))
      
      # Call the processing function
      processing_result <- tryCatch({
        process_webhook_activity(payload)
      }, error = function(e) {
        cat(sprintf("Error processing activity: %s\n", e$message))
        list(
          activity_id = activity_id,
          event_type = event_type,
          error = e$message,
          success = FALSE
        )
      })
      
      cat("Processing completed.\n")
    } else if (event_type == "deleted") {
      cat(sprintf("Activity %s deleted - logging only (deletion processing not implemented)\n", activity_id))
      processing_result <- list(
        activity_id = activity_id,
        event_type = "deleted",
        message = "Deletion logged but not processed",
        success = TRUE
      )
    } else {
      cat(sprintf("Unhandled event type: %s\n", event_type))
      processing_result <- list(
        activity_id = activity_id %||% "unknown",
        event_type = event_type,
        message = "Event type not handled",
        success = FALSE
      )
    }
    
    # Calculate processing time
    end_time <- Sys.time()
    processing_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Prepare response
    response_data <- list(
      status = "received",
      timestamp = as.character(Sys.time()),
      activity_id = activity_id,
      event_type = event_type,
      processing_time_seconds = round(processing_time, 2),
      processing_result = processing_result
    )
    
    cat(sprintf("Webhook processing completed in %.2f seconds\n", processing_time))
    
    # Return success response
    return(response_data)
    
  }, error = function(e) {
    # Log error and return error response
    cat(sprintf("Webhook error: %s\n", e$message))
    
    error_data <- data.frame(
      webhook_timestamp = as.character(Sys.time()),
      activity_id = "error",
      event_type = "error",
      raw_payload = paste("ERROR:", e$message),
      stringsAsFactors = FALSE
    )
    
    if (!file.exists(csv_file)) {
      fwrite(error_data, csv_file)
    } else {
      fwrite(error_data, csv_file, append = TRUE)
    }
    
    res$status <- 500
    return(list(
      status = "error",
      timestamp = as.character(Sys.time()),
      error = e$message
    ))
  })
}

#* @get /health
function() {
  list(
    status = "healthy",
    timestamp = as.character(Sys.time()),
    message = "Webhook server is running"
  )
}

#* @get /logs
function() {
  if (file.exists(csv_file)) {
    recent_logs <- fread(csv_file)
    if (nrow(recent_logs) > 10) {
      recent_logs <- tail(recent_logs, 10)
    }
    return(list(
      status = "success",
      timestamp = as.character(Sys.time()),
      recent_webhooks = recent_logs
    ))
  } else {
    return(list(
      status = "no_data",
      timestamp = as.character(Sys.time()),
      message = "No webhook logs found"
    ))
  }
}

#* @get /processing-logs
function() {
  if (file.exists(processing_log_file)) {
    processing_logs <- fread(processing_log_file)
    if (nrow(processing_logs) > 10) {
      processing_logs <- tail(processing_logs, 10)
    }
    return(list(
      status = "success",
      timestamp = as.character(Sys.time()),
      recent_processing = processing_logs
    ))
  } else {
    return(list(
      status = "no_data",
      timestamp = as.character(Sys.time()),
      message = "No processing logs found"
    ))
  }
}