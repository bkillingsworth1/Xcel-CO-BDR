
# Data Processing Script for GV Model Output Data


# --------------- #
# ---- SETUP ---- #
# --------------- #

library(tidyverse)
library(devtools)
library(openxlsx)
library(lubridate)
library(jsonlite)

# Read in data sample data
sample_data <- read.csv("/home/weirth/../killingsworth/Xcel CO BDR/Data/xe_bdr_evergreen_request1_111224.csv")

# Read in GV Model Output Data (metered savings) - VARIOUS LOCATIONS OF MODEL OUTPUTS

# directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs_bigrun1" # full outputs (500)
directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs" # 12/13 outputs (special Franklin parameters)
# directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs_compare" # GV TEST 12/4
# directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs_compare/run3_snipped" # GV TEST 12/4 3 snipped
# directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs_compare/run3_12am_snipped" # GV TEST 12/4 3 12am snipped
# directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs_compare/run3_12am" # GV TEST 12/4.2 12 am

# List all .txt files in the directory - include metered_savings and hourly_model_full_output - character vector
txt_files <- list.files(directory, pattern = "\\.txt$", full.names = TRUE, recursive = TRUE)

# Check incase txt_files is empty
if (length(txt_files) == 0) {
  stop("No .txt files found. Check the directory path and file extensions.")
}

# We only need the metered savings files - filters down character vector
metered_files <- grep("metered_savings_", txt_files, value = TRUE)

# Parse and extract JSON from metered files - use lapply to apply function to every part of the character vector
metered_data_list <- lapply(metered_files, function(file) {
  tryCatch({
    # Check if the file exists - if not found will pass and return filepath string
    if (!file.exists(file)) {
      stop(paste("File not found:", file))
    }
    
    # Read and parse JSON
    json_data <- fromJSON(file, simplifyVector = TRUE) 
    
    # Optional: Perform specific transformations for your JSON structure
    # Example: if the JSON contains nested data you want to extract
    if (!is.null(json_data$data)) {
      return(json_data$data)
    } else {
      return(json_data)
    }
  }, error = function(e) {
    message(paste("Error parsing file:", file, ":", e$message))
    return(NULL)
  })
})

# Remove NULL entries if some files failed to parse
metered_data_list <- Filter(Negate(is.null), metered_data_list)

# Name meter list elements by file names
names(metered_data_list) <- basename(metered_files)

# Print summary of loaded files
cat("Metered files loaded:", length(metered_data_list), "\n")


# --------------------------------- #
# ---- CLEAN JSON TO DATAFRAME ---- #
# --------------------------------- #

# Function to process single JSON .txt file (converts TZ and filters for 7-12-2024 1-4pm MT)

process_single_file <- function(json_data, file_name) {
  # Extract 'metered_savings'
  metered_savings <- json_data$metered_savings
  
  # Replace null (NA) values with 0 - this is specifically for hour 24
  metered_savings <- lapply(metered_savings, function(value) ifelse(is.null(value), 0, value))
  
  # Extract timestamps and values
  timestamps <- names(metered_savings)
  values <- unlist(metered_savings, use.names = FALSE)
  
  # Handle mismatched lengths
  if (length(timestamps) != length(values)) {
    warning(paste("Mismatched lengths in file:", file_name))
    # Retain only matched pairs
    min_length <- min(length(timestamps), length(values))
    timestamps <- timestamps[1:min_length]
    values <- values[1:min_length]
  }
  
  # Create a data frame
  df <- data.frame(
    time = timestamps,
    savings = values,
    stringsAsFactors = FALSE
  )
  
  # Convert time from UTC to Mountain Time
  df$time <- ymd_hms(df$time, tz = "UTC") %>%
    with_tz("America/Denver")
  
  # Extract customer ID from the file name
  df$id <- gsub("metered_savings_|\\.txt", "", file_name)
  
  # Filter for July 12, 2024, 1–4 PM Mountain Time
  start_time <- ymd_hms("2024-07-12 13:00:00", tz = "America/Denver")
  end_time <- ymd_hms("2024-07-12 16:00:00", tz = "America/Denver")
  # df <- df %>%
  #   filter(time >= start_time & time <= end_time)
  
  return(df)
}


# Test on a single file - REVIEW WITH BK

# Test on the first file in metered_data_list
# json_data <- metered_data_list[[1]]       # Extract the JSON data for the first file
# file_name <- names(metered_data_list)[1]  # Extract the file name for the first file

# Call the function
# single_file_df <- process_single_file(json_data, file_name)


# Implement the function across all JSON files
process_all_files <- function(data_list) {
  bind_rows(lapply(seq_along(data_list), function(i) {
    json_data <- data_list[[i]]       # Get JSON data
    file_name <- names(data_list)[i]  # Get the file name
    process_single_file(json_data, file_name)
  }))
}

# Apply the function to all JSON files
all_metered_savings <- process_all_files(metered_data_list)


# ------------------------------------------- #
# ---- WRANGLE DATAFRAME AND JOIN SAMPLE ---- #
# ------------------------------------------- #

# Group by customer and sum savings
all_metered_savings2 <- all_metered_savings %>%
  group_by(id) %>%
  summarize(model_metered_savings = sum(savings))

# Join on sample data

# Cleaning of ID column is needed
sample_data2 <- sample_data %>%
  mutate(id = gsub("_AMI$", "", item_id)) %>% # Remove '_AMI' from 'item_id'
  rename(sample_metered_savings = metered_savings)

# Perform the join and add savings column
joined_data <- all_metered_savings2 %>%
  left_join(sample_data2, by = "id") %>%
  mutate(absolute_savings_difference = abs(model_metered_savings - sample_metered_savings)) %>%
  select(sample_id = item_id, model_id = id, franklin_estimation = sample_metered_savings, evergreen_estimation = model_metered_savings, absolute_savings_difference)


# Write out the data
# write_csv(joined_data, "/home/weirth/CO BDR/Xcel-CO-BDR/GV Data Processing/Final Clean Data/savings_processed.csv")

# Write out 500 distinct ID's to a csv ONLY RUN WHEN RUNNING FULL MODEL OUTPUTS
#joined_data %>% select(model_id) %>% write_csv("/home/weirth/CO BDR/Xcel-CO-BDR/GV Data Processing/Final Clean Data/500_sites_ids.csv")

# Print mean absolute difference
mean(joined_data$absolute_savings_difference)


# ---- Extra code and questions ---- 

# joined_data <- joined_data %>%
#   rename(franklin_estimation = sample_metered_savings, evergreen_estimation = model_metered_savings)
# 
# 
# 
# # Create a histogram of absolute_savings_difference
# ggplot(joined_data, aes(x = absolute_savings_difference)) +
#   geom_histogram(binwidth = 0.1, color = "black", fill = "cyan3", alpha = 0.7) +
#   labs(
#     title = "Distribution of Absolute Savings Difference",
#     x = "Absolute Savings Difference",
#     y = "Frequency"
#   ) +
#   theme_bw()
# 
# ggplot(joined_data, aes(x = sample_metered_savings, y = absolute_savings_difference)) +
#   geom_point(alpha = 0.7, color = "black") +  # Scatter points
#   labs(
#     title = "Absolute Savings Difference by Sample Metered Savings",
#     x = "Franklin Estimation of Savings",
#     y = "Absolute Savings Difference"
#   ) +
#   theme_bw()
# 
# ggplot(joined_data, aes(x = sample_metered_savings, y = model_metered_savings)) +
#   geom_point(alpha = 0.7, color = "black") +  # Scatter points
#   geom_smooth(method = "lm") +
#   labs(
#     title = "Evergreen vs. Franklin Estimated Savings",
#     x = "Franklin Estimation of Savings",
#     y = "Our Modeled Estimation"
#   ) +
#   theme_bw()
# 
# 
# joined_data %>%
#   rename(franklin_savings = sample_metered_savings, evergreen_savings = model_metered_savings) %>%
#   write_csv("/home/weirth/CO BDR/Xcel-CO-BDR/GV Data Processing/processed_data_12_10.csv")
