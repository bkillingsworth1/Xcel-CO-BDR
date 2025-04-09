
library(tidyverse)

# ==============================================================================
# Reading in Data
# ==============================================================================

# Partly metered files - some customer exclusions based on missing pretreatment data
partly_meterid_A <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/partly_meterId_cohortA.csv')
partly_meterid_B <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/party_meterId_cohortB.csv')

# Empty metered files - some customer exclusions based on missing pretreatment data
empty_meterid_A <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/empty_meterId_cohortA.csv')
empty_meterid_B <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/empty_meterId_cohortB.csv')

# Initial exports (incorrect savings values) - IGNORE FOR NOW - values did change
event1_savings_bad <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event1_initial_export_evergreen_hourly.csv')  %>% mutate(event = "Event 1")
event2_savings_bad <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event2_initial_export_evergreen_hourly.csv')  %>% mutate(event = "Event 2")
event3_savings_bad <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event3_initial_cohort_A_export_evergreen_hourly.csv')  %>% mutate(event = "Event 3")
event4_savings_bad <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event3_initial_cohort_B_export_evergreen_hourly.csv')  %>% mutate(event = "Event 4")

# Updated exports (new updated savings values)
event1_savings_new <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event1_export_evergreen_hourly.csv') %>% mutate(event = "Event 1")
event2_savings_new <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event2_export_evergreen_hourly.csv')  %>% mutate(event = "Event 2")
event3_savings_new <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event3_cohort_A_export_evergreen_hourly.csv') %>% mutate(event = "Event 3")
event4_savings_new <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/event3_cohort_B_export_evergreen_hourly.csv')  %>% mutate(event = "Event 4")

# Full customer metadata
customer_data <- read_csv('/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/From Franklin 03202025/exported_600k_list.csv')

# ==============================================================================
# Data Preparation
# ==============================================================================

# ---- Incorporate customer in info flag ----

# Take only distinct customer id and flags (this removes 18 duplicates)
customer_flags <- customer_data %>% select(customer_id, event1_in_info, event2_in_info, event3_in_info, event4_in_info) %>% distinct()

# Add on in_event_info_flag for each row (each row only represents one customer in one event)
event1_savings_new_prepped <- event1_savings_new %>%
  mutate(meter_id = sub("_AMI$", "", item_id)) %>%
  left_join(customer_flags %>% select(meter_id = customer_id, event1_in_info), by = "meter_id") %>%
  mutate(IN_EVENT_INFO = ifelse(event1_in_info == "Yes", TRUE, FALSE))

event2_savings_new_prepped <- event2_savings_new %>%
  mutate(meter_id = sub("_AMI$", "", item_id)) %>%
  left_join(customer_flags %>% select(meter_id = customer_id, event2_in_info), by = "meter_id") %>%
  mutate(IN_EVENT_INFO = ifelse(event2_in_info == "Yes", TRUE, FALSE))

event3_savings_new_prepped <- event3_savings_new %>%
  mutate(meter_id = sub("_AMI$", "", item_id)) %>%
  left_join(customer_flags %>% select(meter_id = customer_id, event3_in_info), by = "meter_id") %>%
  mutate(IN_EVENT_INFO = ifelse(event3_in_info == "Yes", TRUE, FALSE))

event4_savings_new_prepped <- event4_savings_new %>%
  mutate(meter_id = sub("_AMI$", "", item_id)) %>%
  left_join(customer_flags %>% select(meter_id = customer_id, event4_in_info), by = "meter_id") %>%
  mutate(IN_EVENT_INFO = ifelse(event4_in_info == "Yes", TRUE, FALSE))

# Bind datasets and clear up memory
# Savings
bad_savings <- bind_rows(event1_savings_bad, event2_savings_bad, event3_savings_bad, event4_savings_bad)
new_savings <- bind_rows(event1_savings_new_prepped, event2_savings_new_prepped, event3_savings_new_prepped, event4_savings_new_prepped)
# rm(event1_savings_bad, event2_savings_bad, event3_savings_bad, event4_savings_bad, event1_savings_new, event2_savings_new, event3_savings_new, event4_savings_new, event1_savings_new_prepped, event2_savings_new_prepped, event3_savings_new_prepped, event4_savings_new_prepped)

# ---- Incorporate flag for insufficient pre-period data ----

# Possible confirmation here: if a customer shows up in these lists = remove them completely from aggregations

# Bad meter customers
drop_meterid_customers <- bind_rows(partly_meterid_A, partly_meterid_B, empty_meterid_A, empty_meterid_B) %>%
  mutate(meter_id = sub("_AMI$", "", meterId)) %>%
  select(meter_id) %>% (distinct)

# # ---- Add customer active/inactive flag (based on all event hours) ----
# 
# # Determine active/inactive by event total savings
# customer_activity <- new_savings %>%
#   group_by(meter_id, event) %>%
#   summarize(
#     event_savings = sum(savings_MW, na.rm = TRUE),
#     customer_activity = ifelse(sum(savings_MW, na.rm = TRUE) > 0, "Active", "Inactive")
#   )

# ==============================================================================
# Aggregate Savings Calculations
# ==============================================================================

# ---- Filtered savings aggregations by event ----

# Some more data prep to achieve matching table
# - 1. create MW column
# - 2. convert time stamps to colorado time (times given in UTC)
# - 3. add "bad meter" flag, if a customer should be DQ'd based on lack of pre data
# - 4. create "interval" label - specifically for table aggregations
new_savings_cleaned <- new_savings %>%
  mutate(savings_MW = metered_savings / 1000,
         BAD_METER = ifelse(meter_id %in% drop_meterid_customers$meter_id, TRUE, FALSE),
         colorado_time = with_tz(timestamp, tzone = "America/Denver"),
         interval = paste0(format(colorado_time, "%H:%M:%S"), "-", format(colorado_time + hours(1), "%H:%M:%S")),
         customer_activity = ifelse(metered_savings > 0, "Active", "Inactive")) %>%
  select(meter_id, event, customer_activity, timestamp, colorado_time, interval, savings_kwh = metered_savings, savings_MW, IN_EVENT_INFO, NON_RESPONSIVE = `Non-responsive`, BAD_METER)

# ---- ADDITIONAL BK TASK ----

# Going to compare new estimates with our Dec 500 site analysis

# Write out this df
export <- new_savings_cleaned %>%
  filter(event == "Event 1" & savings_kwh > 0) %>%
  group_by(meter_id, IN_EVENT_INFO, NON_RESPONSIVE, BAD_METER) %>%
  summarize(savings_kwh = sum(savings_kwh))

write_csv(export, '/Volumes/Projects/482004 - Xcel CO BDR M&V/Data/franklin_updated_savings.csv')

# ==============================================================================
# Attempt 1 ) Apply all filters as originally understood
# ==============================================================================

# Create summary table
summary <- new_savings_cleaned %>%
  
  # First we need to drop all ineligible customers and only keep (active/inactive people)
  filter(BAD_METER == FALSE & IN_EVENT_INFO == TRUE) %>%
  
  # Now aggreagte by event and interval
  group_by(event, interval) %>%
  
  # Now create conditional counts like the franklin report table
  summarize(
    MW_Reduction_Active = round(sum(if_else(savings_MW > 0  & NON_RESPONSIVE == FALSE, savings_MW, 0), na.rm = TRUE), 2),
    N_Active = sum(NON_RESPONSIVE == FALSE & savings_MW > 0, na.rm = TRUE),
    MW_Reduction_Inactive = round(sum(if_else(savings_MW < 0 & NON_RESPONSIVE == FALSE, savings_MW, 0), na.rm = TRUE), 2),
    N_Inactive = sum(savings_MW > 0  & NON_RESPONSIVE == FALSE, na.rm = TRUE)
  ) %>%
  arrange(event, interval) %>%
  select(event, interval, MW_Reduction_Active, MW_Reduction_Inactive)

# RESULT: savings match, n's are significantly off


# ==============================================================================

# Define customer activity by event
customer_activity <- new_savings_cleaned %>%
  group_by(meter_id, event) %>%
  summarize(total_event_activity = sum(savings_MW)) %>%
  mutate(activity_label = if_else(total_event_activity > 0, "Active", "Inactive"))

# RESULT: savings match, n's are significantly off# TABLE 2 FOR EXCEL WORKBOOK
summary2 <- new_savings_cleaned %>%
  left_join(customer_activity, by = c("meter_id", "event")) %>%
  
  # First we need to drop all ineligible customers and only keep (active/inactive people)
  filter(BAD_METER == FALSE & IN_EVENT_INFO == TRUE) %>%
  
  # Now aggreagte by event and interval
  group_by(event, interval) %>%
  
  # Now create conditional counts like the franklin report table
  summarize(
    MW_Reduction_Active = round(sum(if_else(activity_label == "Active" & NON_RESPONSIVE == FALSE, savings_MW, 0), na.rm = TRUE), 2),
    MW_Reduction_Inactive = round(sum(if_else(activity_label == "Inactive" & NON_RESPONSIVE == FALSE, savings_MW, 0), na.rm = TRUE), 2)
  ) %>%
  arrange(event, interval) %>%
  select(event, interval, MW_Reduction_Active, MW_Reduction_Inactive)

# ==============================================================================

# TABLE # FOR EXCEL WORKBOOK
summary3 <- new_savings_cleaned %>%
  
  # First we need to drop all ineligible customers and only keep (active/inactive people)
  filter(BAD_METER == FALSE & IN_EVENT_INFO == TRUE) %>%
  
  # Now aggreagte by event and interval
  group_by(event) %>%
  
  # Now create conditional counts like the franklin report table
  summarize(
    MW_Reduction_Active = round(sum(savings_MW, na.rm = TRUE), 2),
    total_participants = n_distinct(meter_id)
    ) %>%
  arrange(event) %>%
  select(event, MW_Reduction_Active, total_participants)



# ==============================================================================
# Attempt 2 ) Attempt to get similar n's
# ==============================================================================

# Could the counts be off due to all flags not being implemented?
n_check <- new_savings_cleaned %>%
  filter(BAD_METER == FALSE & IN_EVENT_INFO == TRUE) %>%
  group_by(event, interval) %>%
  summarize(
    N_Active = sum(NON_RESPONSIVE == FALSE, na.rm = TRUE),
    N_Inactive = sum(NON_RESPONSIVE == TRUE, na.rm = TRUE)
  ) %>%
  arrange(event, interval)

final <- summary %>%
  left_join(n_check, by = c("event", "interval")) %>%
  select(event, interval, MW_Reduction_Active, N_Active, MW_Reduction_Inactive, N_Inactive)


agg <- summary2 %>% group_by(event) %>% summarize(total_kwh_active = sum(MW_Reduction_Active), total_kwh_inactive = sum(MW_Reduction_Inactive))




# ==============================================================================
# KP Step-by-Step Approach
# ==============================================================================

# 1.) Apply all filters - already done
n_all_filters <- summary %>%
  select(-MW_Reduction_Active, -MW_Reduction_Inactive)

# 2.) Apply only "bad meter" flag
n_badmeter <- new_savings_cleaned %>%
  filter(BAD_METER == FALSE) %>%
  group_by(event, interval) %>%
  summarize(
    N_Active = sum(customer_activity == "Active", na.rm = TRUE),
    N_Inactive = sum(customer_activity == "Inactive", na.rm = TRUE)
  ) %>%
  arrange(event, interval)

# 3.) Apply only "in_event_info" flag
n_ineventinfo <- new_savings_cleaned %>%
  filter(IN_EVENT_INFO == TRUE) %>%
  group_by(event, interval) %>%
  summarize(
    N_Active = sum(customer_activity == "Active", na.rm = TRUE),
    N_Inactive = sum(customer_activity == "Inactive", na.rm = TRUE)
  ) %>%
  arrange(event, interval)

# 4.) Apply only "non-responsive flag
n_nonresponsive <- new_savings_cleaned %>%
  filter(NON_RESPONSIVE == FALSE) %>%
  group_by(event, interval) %>%
  summarize(
    N_Active = sum(customer_activity == "Active", na.rm = TRUE),
    N_Inactive = sum(customer_activity == "Inactive", na.rm = TRUE)
  ) %>%
  arrange(event, interval)

# 5.) No filters at all 
n_nofilter <- new_savings_cleaned %>%
  group_by(event, interval) %>%
  summarize(
    N_Active = sum(customer_activity == "Active", na.rm = TRUE),
    N_Inactive = sum(customer_activity == "Inactive", na.rm = TRUE)
  ) %>%
  arrange(event, interval)

# write all of these out
write_csv(n_all_filters, '/Users/weirth/Downloads/all_filters.csv')
write_csv(n_badmeter, '/Users/weirth/Downloads/badmeter.csv')
write_csv(n_ineventinfo, '/Users/weirth/Downloads/ineventinfo.csv')
write_csv(n_nonresponsive, '/Users/weirth/Downloads/nonresponsive.csv')
write_csv(n_nofilter, '/Users/weirth/Downloads/nofilters.csv')


