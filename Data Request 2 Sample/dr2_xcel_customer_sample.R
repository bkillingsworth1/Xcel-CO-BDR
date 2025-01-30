
# CO BDR Data Request 2 Sample - 14k Customers for Xcel

# ----------
# Setup
# ----------

# Packages
library(tidyverse)
library(writexl)

# AMI Data - Reading from BK ami directory
cohortA <- read.csv("/home/weirth/../killingsworth/Xcel CO BDR/Data/xe_bdr_evergreen_request1_111224.csv")

# --------------------
# Sample 14k Customers
# --------------------

# perfom the sample
set.seed(12)
sampled_customers <- slice_sample(cohortA, n = 14000)

# take only relevant column (item id)
sampled_customers_final <- sampled_customers %>%
  select(item_id)

# --------------------
# Write out
# --------------------

write_xlsx(sampled_customers_final, "~/CO BDR/Xcel-CO-BDR/Data Request 2 Sample/Xcel Sampled Treatment Group/xcel_14k_sample.xlsx")
