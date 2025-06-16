# run_webhook.R
library(plumber)

cat("=== STARTING ENHANCED OPENFIELD WEBHOOK SERVER ===\n")
cat("Loading webhook handler with activity processing...\n")

# Create plumber API from the webhook file
pr <- plumb("enhanced_openfield_webhook.R")

cat("Webhook server starting on port 8000...\n")
cat("Available endpoints:\n")
cat("  POST /openfield-webhook  - Receives and processes activity webhooks\n")
cat("  GET  /health            - Health check endpoint\n")
cat("  GET  /logs              - Recent webhook logs\n")
cat("  GET  /processing-logs   - Recent processing logs\n")
cat("\n")

# Run the server
pr$run(
  host = "0.0.0.0",
  port = 8000,
  swagger = TRUE
)