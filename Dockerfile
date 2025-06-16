FROM rocker/r-ver:latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages(c('plumber', 'jsonlite', 'data.table', 'tidyverse', 'httr', 'catapultR', 'lubridate', 'openxlsx'), repos='https://cran.rstudio.com')"

# Create app directory
WORKDIR /app

# Copy application files
COPY enhanced_openfield_webhook.R /app/enhanced_openfield_webhook.R
COPY process_new_activity.R /app/process_new_activity.R
COPY run_webhook.R /app/run_webhook.R

# Create data directories
RUN mkdir -p /app/data
RUN mkdir -p /app/logs

# Create foundational directory and add placeholder files
RUN mkdir -p /app/Foundational_CSVs
RUN echo "parameter_slug_1\nparameter_slug_2\nparameter_slug_3" > /app/Foundational_CSVs/slugs_activities.csv
RUN echo "period_parameter_1\nperiod_parameter_2\nperiod_parameter_3" > /app/Foundational_CSVs/slugs_periods.csv

# Expose port
EXPOSE 8000

# Set environment variables
ENV R_CONFIG_ACTIVE=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run the webhook server
CMD ["Rscript", "run_webhook.R"]