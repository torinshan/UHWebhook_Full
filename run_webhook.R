# Base image with R (using your working version)
FROM rocker/r-ver:4.5.1

# Install system-level libraries required by plumber, httpuv, and sodium
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libz-dev \
    libsodium-dev \
    build-essential \
    curl \
    && apt-get clean

# Install only the essential R packages that we know work
RUN R -e "install.packages(c('plumber', 'jsonlite', 'data.table', 'httr'), repos = 'https://cran.rstudio.com')"

# Set working directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/data/CSVs_Raw \
    /app/data/CSVs_Clean \
    "/app/data/Merged Files" \
    /app/logs \
    /app/Foundational_CSVs

# Create foundational CSV files
RUN echo "total_distance\ntotal_player_load\nmax_speed" > /app/Foundational_CSVs/slugs_activities.csv && \
    echo "period_distance\nperiod_player_load\nperiod_max_speed" > /app/Foundational_CSVs/slugs_periods.csv

# Copy application files
COPY openfield_webhook.R /app/openfield_webhook.R
COPY process_new_activity_minimal.R /app/process_new_activity.R

# Set environment variables
ENV CATAPULT_AUTH_TOKEN=""

# Expose the plumber API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run the plumber API (using your working pattern)
CMD ["R", "-e", "plumber::plumb('openfield_webhook.R')$run(host='0.0.0.0', port=8000)"]
