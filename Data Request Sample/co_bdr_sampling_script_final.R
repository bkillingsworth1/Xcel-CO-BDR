# CO BDR Data Request Sample - Alex Weirth

# ---- Configuration ----

# Packages
library(tidyverse)
library(devtools)
library(openxlsx)

# Read in data 

# AMI Data - Reading from BK ami directory
data <- read.csv("../killingsworth/Xcel CO BDR/Data/xe_bdr_evergreen_request1_111224.csv")

# Destination filepath for sample - into my AMI directory
write_filepath <- "~/CO BDR/final_sample/cobdr_final_sample.csv"

# Function for sampling
sample_bdr_data <- function(data, filepath) {
  
  # Sample 1k with negative savings
  negative_sample <- data %>%
    filter(metered_savings < 0) %>%
    sample_n(1000)
  
  # Filter out negative savings and calculate quartiles for positive savings
  positive_data <- data %>% filter(metered_savings >= 0)
  positive_quartiles <- quantile(positive_data$metered_savings, probs = seq(0, 1, 0.25))
  
  # Sample 2,250 from each Q
  q1_sample <- positive_data %>%
    filter(metered_savings >= positive_quartiles[1] & metered_savings < positive_quartiles[2]) %>%
    sample_n(2250)
  
  q2_sample <- positive_data %>%
    filter(metered_savings >= positive_quartiles[2] & metered_savings < positive_quartiles[3]) %>%
    sample_n(2250)
  
  q3_sample <- positive_data %>%
    filter(metered_savings >= positive_quartiles[3] & metered_savings < positive_quartiles[4]) %>%
    sample_n(2250)
  
  q4_sample <- positive_data %>%
    filter(metered_savings >= positive_quartiles[4]) %>%
    sample_n(2250)
  
  # Combine samples
  sample_data <- bind_rows(negative_sample, q1_sample, q2_sample, q3_sample, q4_sample)
  
  # Print verification details
  print(paste("Number of negative savings observations:", nrow(negative_sample)))
  
  # View the quartiles
  print("Quartile value ranges:")
  print(round(positive_quartiles, 2))
  
  # Print labeled quartile distribution
  quartile_labels <- c("Q1", "Q2", "Q3", "Q4")
  print("Distribution of observations in each labeled quartile:")
  print(table(cut(sample_data$metered_savings[sample_data$metered_savings >= 0], 
                  breaks = positive_quartiles, labels = quartile_labels, include.lowest = TRUE)))
  
  sample_data <- sample_data %>% select(item_id)
  
  # Write the sample data to the specified file path
  write.csv(sample_data, file = filepath, row.names = FALSE)
  
  return(invisible(sample_data))  # Return without printing full data
  #return(sample_data)
}

sample_bdr_data(data, write_filepath)
