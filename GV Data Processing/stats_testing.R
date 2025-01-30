
# Testing GV model output data

# --------------- #
# ---- SETUP ---- #
# --------------- #

library(jsonlite)
library(tidyverse)

# ---- Loading in JSON Data ----

# Define the directory containing .txt files
directory <- "/home/weirth/../valle/data/xcel_co_bdr/model_outputs"

# List all .txt files in the directory (including subdirectories)
txt_files <- list.files(directory, pattern = "\\.txt$", full.names = TRUE, recursive = TRUE)

# Stop if no files are found
if (length(txt_files) == 0) {
  stop("No .txt files found. Check the directory path and file extensions.")
}

# Separate files by their naming pattern
hourly_files <- grep("hourly_model_full_output_", txt_files, value = TRUE)
metered_files <- grep("metered_savings_", txt_files, value = TRUE)

# Parse JSON from hourly files
hourly_data_list <- lapply(hourly_files, function(file) {
  tryCatch(fromJSON(file), error = function(e) {
    message(paste("Error parsing file:", file))
    return(NULL)
  })
})

# Parse JSON from metered files
metered_data_list <- lapply(metered_files, function(file) {
  tryCatch(fromJSON(file), error = function(e) {
    message(paste("Error parsing file:", file))
    return(NULL)
  })
})

# Name list elements by file names
names(hourly_data_list) <- basename(hourly_files)
names(metered_data_list) <- basename(metered_files)

# Print summary of loaded files
cat("Hourly files loaded:", length(hourly_data_list), "\n")
cat("Metered files loaded:", length(metered_data_list), "\n")




# -------------------------- #
# ---- LOOK AT THE DATA ---- #
# -------------------------- #


# Hourly model output

df <- as.data.frame(hourly_data_list[[1]])





00027c7b480ef7812586aad6259bcc4d60f4511d

1.96162841
1.983415

