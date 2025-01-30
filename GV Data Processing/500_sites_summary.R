
# 500 Sites Summary CO BDR

library(tidyverse)

# Assume this is full data from GV 10k sites
full_data <- read_csv("/home/weirth/../valle/data/xcel_co_bdr/merged_combined_data_sorted.csv")

# `Account #`                                 kWh   Hour    Day Month Year `Weather Station ID`
# <chr>                                     <dbl>  <dbl>  <dbl> <dbl> <dbl> <chr>               
# 1 00027c7b480ef7812586aad6259bcc4d60f4511d 1.05      0     1     7  2023 USW00023062         
# 2 00027c7b480ef7812586aad6259bcc4d60f4511d 1.53      1     1     7  2023 USW00023062         
# 3 00027c7b480ef7812586aad6259bcc4d60f4511d 0.346     2     1     7  2023 USW00023062         
# 4 00027c7b480ef7812586aad6259bcc4d60f4511d 0.405     3     1     7  2023 USW00023062         
# 5 00027c7b480ef7812586aad6259bcc4d60f4511d 0.53      4     1     7  2023 USW00023062         
# 6 00027c7b480ef7812586aad6259bcc4d60f4511d 0.524     5     1     7  2023 USW00023062         


# Need to filter for 500 sites in franklin's estimates export of Data Processing Script.R
sites <- read_csv("GV Data Processing/Final Clean Data/500_sites_ids.csv")

# Filter full data for 500 sites
relevant_sites <- full_data %>%
  filter(`Account #` %in% sites$model_id) %>%
  select(id = `Account #`, Year, Month, Day, Hour, kWh)

# Clear up AMI
rm(full_data)

# All sites are present in each
table(sites$model_id %in% relevant_sites$id) # all 500 site ids in full data hourly data

# Modify time column
relevant_sites2 <- relevant_sites %>%
  # Step 1: Create a timestamp in UTC
  mutate(
    timestamp_utc = make_datetime(year = Year, month = Month, day = Day, hour = Hour, tz = "UTC")
  ) %>%
  # Step 2: Adjust the timestamp to Mountain Time
  mutate(
    timestamp_mt = with_tz(timestamp_utc, "America/Denver")
  ) %>%
  # Step 3: Split the adjusted timestamp back into Year, Month, Day, Hour
  mutate(
    Year = year(timestamp_mt),
    Month = month(timestamp_mt),
    Day = day(timestamp_mt),
    Hour = hour(timestamp_mt)
  ) %>%
  # Step 4: Remove intermediate timestamp columns if not needed
  select(-timestamp_utc, -timestamp_mt)


# Create summary tables
summary_month <- relevant_sites2 %>%
  mutate(
    Year = as.numeric(Year),
    Month = as.numeric(Month)
  ) %>%
  group_by(id, Year, Month) %>%
  summarize(n_observations = n(),
            total_kwh = sum(kWh)) %>%
  arrange(Year, Month)

summary_day <- relevant_sites2 %>%
  mutate(
    Year = as.numeric(Year),
    Month = as.numeric(Month)
  ) %>%
  group_by(id, Year, Month, Day) %>%
  summarize(n_observations = n(),
            total_kwh = sum(kWh)) %>%
  arrange(Year, Month, Day)

summary_hour <- relevant_sites2 %>%
  mutate(
    Year = as.numeric(Year),
    Month = as.numeric(Month)
  ) %>%
  group_by(id, Year, Month, Day, Hour) %>%
  summarize(n_observations = n(),
            total_kwh = sum(kWh)) %>%
  arrange(Year, Month, Day, Hour)

# Write out for sharing
# write_csv(summary, "GV Data Processing/Final Clean Data/summary_of_500_updated.csv")


# ---- Look at distinct observations ----

summary2 <- relevant_sites2 %>%
  mutate(
    Year = as.numeric(Year),
    Month = as.numeric(Month),
    Day = as.numeric(Day),
    Hour = as.numeric(Hour),
    timestamp = ymd_hms(paste(Year, Month, Day, Hour, "00", "00", sep = "-"))
  ) %>%
  group_by(timestamp) %>%
  summarize(unique_kWh_readings = n_distinct(timestamp, kWh),
            unique_sites = n_distinct(id),
            kWh_readings_per_site = unique_kWh_readings/unique_sites)


ggplot(summary2, aes(x=timestamp,y=unique_kWh_readings))+
  geom_line(color = "darkred")+
  labs(
    title = "Distinct kWh Readings per Hour",
    y = "Distinct kWh Readings"
  ) +
  theme_bw() +
  scale_x_datetime(
    breaks = seq(as.POSIXct("2023-01-01"), as.POSIXct("2024-12-31"), by = "1 month"),  # Monthly breaks
    date_labels = "%b %Y"  # Format labels as "Jan 2023"
  ) +
  theme(
    panel.grid.minor.x = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    axis.title.x = element_blank()
    )

ggplot(summary2, aes(x=timestamp,y=kWh_readings_per_site))+
  geom_line(color = "darkblue")+
  labs(
    title = "Distinct kWh Readings per Hour per Site",
    y = "Distinct kWh Readings/Distinct Sites"
  ) +
  theme_bw() +
  scale_x_datetime(
    breaks = seq(as.POSIXct("2023-01-01"), as.POSIXct("2024-12-31"), by = "1 month"),  # Monthly breaks
    date_labels = "%b %Y"  # Format labels as "Jan 2023"
  ) +
  theme(
    panel.grid.minor.x = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    axis.title.x = element_blank()
  )

# ---- KP REQUEST ----

# Can you look at one day in July for three sites with the same monthly kWh for July and just share the hourly data for those three sites and one day?

id1 <- "0050656cd811c82f13f1bdb1361fe4c354229414"
id2 <- "00544e778c0e9c85b21e26a116f9f781faea0034"
id3 <- "09fa62aa2227619e39ff95bd93f12be88df5704c"

kp_q1 <- summary_hour %>%
  filter(id == id1 | id == id2 | id == id3) %>%
  filter(Month == 7 & Day == 1 & Year == 2023) 

  # group_by(id, Year, Month, Day, Hour, n_observations, total_kwh) %>%
  # summarize(n = n()) %>%
  # filter(n > 1)

write_csv(kp_q1, "GV Data Processing/Final Clean Data/july_dups_example.csv")
  
