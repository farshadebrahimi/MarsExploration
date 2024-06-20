# This script calculated all the water used in SRT tests by MARS. If no record of actual water used in RT table, sys_storagevolume_ft3 from greenit used instead.
# Author: Farshad Ebrahimi
# Date: 6/20/2024
#0.0: load libraries --------------
#shiny
library(shiny)
#pool for database connections
library(pool)
#odbc for database connections
library(odbc)
#tidyverse for data manipulations
library(tidyverse)
#shinythemes for colors
library(shinythemes)
#lubridate to work with dates
library(lubridate)
#shinyjs() to use easy java script functions
library(shinyjs)
#DT for datatables
library(DT)
#reactable
library(reactable)
#reactable for reactable tables
library(reactable)
#excel download
library(xlsx)
library(DBI)
#Not in logical
`%!in%` <- Negate(`%in%`)

#set db connection
#using a pool connection so separate connections are unified
#gets environmental variables saved in local or pwdrstudio environment
poolConn <- dbPool(odbc(), dsn = "mars14_datav2", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))

greenit <- dbGetQuery(poolConn, "SELECT * FROM external.tbl_systembdv") 

srt <- dbGetQuery(poolConn, "SELECT * FROM fieldwork.tbl_srt")

# Left-join SRT with greenit to get storage volume for those lacking srt_volume_ft3
joined_srt_greenit <- srt %>%
  left_join(greenit, by = "system_id") %>%
  mutate(total_water_used = ifelse(is.na(srt_volume_ft3), sys_storagevolume_ft3, srt_volume_ft3)) %>%
  mutate(fy = case_when(test_date >= as.Date("2017-07-01") & test_date <= as.Date("2018-06-30")  ~ 18,
                        test_date >= as.Date("2018-07-01") & test_date <= as.Date("2019-06-30")  ~ 19,
                        test_date >= as.Date("2019-07-01") & test_date <= as.Date("2020-06-30")  ~ 20,
                        test_date >= as.Date("2020-07-01") & test_date <= as.Date("2021-06-30")  ~ 21,
                        test_date >= as.Date("2021-07-01") & test_date <= as.Date("2022-06-30")  ~ 22,
                        test_date >= as.Date("2022-07-01") & test_date <= as.Date("2023-06-30")  ~ 23,
                        test_date >= as.Date("2023-07-01") & test_date <= as.Date("2024-06-30")  ~ 24)) %>%
  select(srt_uid, system_id, test_date, total_water_used, fy) %>%
  distinct() %>%
  na.omit()

# calculate total per fy
totalwater_perfy <- joined_srt_greenit %>%
  group_by(fy) %>%
  summarise(total_water_per_fy_ft3 = sum(total_water_used))

