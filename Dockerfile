FROM rocker/r-ver:latest

# Install system dependencies including curl for health checks
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages(c('plumber', 'jsonlite', 'data.table', 'tidyverse', 'httr', 'catapultR', 'lubridate', 'openxlsx'), repos='https://cran.rstudio.com')"

# Create app directory
WORKDIR /app

# Create all necessary directories
RUN mkdir -p /app/data/CSVs_Raw \
    /app/data/CSVs_Clean \
    "/app/data/Merged Files" \
    /app/logs \
    /app/Foundational_CSVs

# Create foundational CSV files with realistic parameter slugs
RUN echo "total_distance\ntotal_player_load\nmax_speed\naverage_speed\ntotal_duration\nhigh_speed_distance\nsprint_distance\nacceleration_count\ndeceleration_count\nmax_acceleration\nmax_deceleration" > /app/Foundational_CSVs/slugs_activities.csv && \
    echo "period_distance\nperiod_player_load\nperiod_max_speed\nperiod_average_speed\nperiod_duration\nperiod_high_speed_distance\nperiod_sprint_distance\nperiod_acceleration_count\nperiod_deceleration_count" > /app/Foundational_CSVs/slugs_periods.csv

# Copy application files (corrected filenames to match your actual files)
COPY openfield_webhook.R /app/openfield_webhook.R
COPY process_new_activity.r /app/process_new_activity.R
COPY run_webhook.R /app/run_webhook.R

# Set appropriate permissions
RUN chmod +x /app/*.R

# Set environment variables
ENV R_CONFIG_ACTIVE=production
ENV CATAPULT_AUTH_TOKEN=""

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run the webhook server
CMD ["Rscript", "run_webhook.R"]
