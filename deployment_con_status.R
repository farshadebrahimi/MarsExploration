### Assigning construction status to the deployment records using SRT, CWL data, and CIPIT tables
### By: Farshad Ebrahimi
### Last modified: 3/12/2024
#load packages
library(tidyverse)
library(odbc)
library(pool)
library(DBI)


#Not in logical
`%!in%` <- Negate(`%in%`)


#con
poolConn <- dbPool(odbc(), dsn = "mars14_datav2", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))

#get tables
# current deployments_con_status table
deployment_con_status <- odbc::dbGetQuery(poolConn, paste0("SELECT * FROM fieldwork.tbl_deployments_con_status")) 

#SRT
srt <- odbc::dbGetQuery(poolConn, paste0("SELECT * FROM fieldwork.tbl_srt")) 
#Deployment
deployment <- dbGetQuery(poolConn, "SELECT *, admin.fun_smp_to_system(smp_id) as system_id FROM fieldwork.viw_deployment_full WHERE smp_id like '%-%-%'") 
# CWL data
cwl_data_list <- dbGetQuery(poolConn, "WITH cte_smp_id_ow AS (
                                                SELECT DISTINCT smp_id, admin.fun_smp_to_system(smp_id) as system_id, ow_suffix, ow_uid
                                                FROM fieldwork.tbl_ow
                                                ),
                                                cte_CWL_uid AS (
                                                SELECT DISTINCT ow_uid
                                                FROM data.tbl_ow_leveldata_raw
                                                )
                                                SELECT *
                                                FROM cte_CWL_uid AS l
                                                INNER JOIN cte_smp_id_ow AS r
                                                ON l.ow_uid = r.ow_uid
                                                WHERE system_id like '%-%'
                                                ")

external.cipit_project <- dbGetQuery(poolConn, "SELECT * FROM external.tbl_cipit_project")
external.smpbdv <- dbGetQuery(poolConn, "SELECT * FROM  external.tbl_smpbdv")
smp_milestones <- inner_join(external.cipit_project, external.smpbdv, by = "worknumber") %>%
  select(smp_id, construction_start_date, construction_complete_date, pc_ntp_date, contract_closed_date) %>%
  distinct()


### SRT table has con-status. let's join it with deployments
### SRT join with Deployment Match by date and system

srt_joined <- deployment %>%
  left_join(srt, by=c("deployment_dtime_est" = "test_date", "system_id"="system_id" )) %>%
  filter(!is.na(srt_uid)) %>%
  select(deployment_uid, con_phase_lookup_uid) %>%
  distinct

### all long term sites have post-con status
long_terms <- deployment %>%
  filter(term == "Long") %>%
  select(deployment_uid) %>%
  mutate(con_phase_lookup_uid = 2)

other_deployments <- deployment %>%
  anti_join(srt_joined, by = "deployment_uid") %>%
  anti_join(long_terms, by = "deployment_uid") %>%
  select(smp_id, deployment_uid, deployment_dtime_est) %>%
  distinct()


### run a conditional loop to assign status based on cipit smp milestones
smp_milestones <- other_deployments %>%
  inner_join(smp_milestones, by = "smp_id")

smp_milestones['con_phase_lookup_uid'] <- NA

for(i in 1:nrow(smp_milestones)) {
  
  if (!is.na(smp_milestones[i, "construction_start_date"]) & !is.na(smp_milestones[i, "construction_complete_date"]) ) {
    
    if (smp_milestones[i, "deployment_dtime_est"] >= smp_milestones[i, "construction_start_date"] & smp_milestones[i, "deployment_dtime_est"] <= smp_milestones[i, "construction_complete_date"]  ) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 1
      
    } else if (smp_milestones[i, "deployment_dtime_est"] < smp_milestones[i, "construction_start_date"]) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 3
      
    } else {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 2
      
    }
    
    
  } else if (!is.na(smp_milestones[i, "pc_ntp_date"]) & !is.na(smp_milestones[i, "construction_complete_date"] )) {
    
    if (smp_milestones[i, "deployment_dtime_est"] >= smp_milestones[i, "pc_ntp_date"] & smp_milestones[i, "deployment_dtime_est"] <= smp_milestones[i, "construction_complete_date"]  ) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 1
      
    } else if (smp_milestones[i, "deployment_dtime_est"] < smp_milestones[i, "pc_ntp_date"]) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 3
      
    } else {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 2
      
    }
    
  } else if (!is.na(smp_milestones[i, "construction_start_date"]) & !is.na(smp_milestones[i, "contract_closed_date"])) {
    
    if (smp_milestones[i, "deployment_dtime_est"] >= smp_milestones[i, "construction_start_date"] & smp_milestones[i, "deployment_dtime_est"] <= smp_milestones[i, "contract_closed_date"]  ) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 1
      
    } else if (smp_milestones[i, "deployment_dtime_est"] < smp_milestones[i, "construction_start_date"]) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 3
      
    } else {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 2
      
    }
    
    
    
  } else if (!is.na(smp_milestones[i, "pc_ntp_date"]) & !is.na(smp_milestones[i, "contract_closed_date"])) {
    
    if (smp_milestones[i, "deployment_dtime_est"] >= smp_milestones[i, "pc_ntp_date"] & smp_milestones[i, "deployment_dtime_est"] <= smp_milestones[i, "contract_closed_date"]  ) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 1
      
    } else if (smp_milestones[i, "deployment_dtime_est"] < smp_milestones[i, "pc_ntp_date"]) {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 3
      
    } else {
      
      smp_milestones[i, "con_phase_lookup_uid"] <- 2
      
    }
    
    
  } else { 
    
    smp_milestones[i, "con_phase_lookup_uid"] <- NA
    
    
  }
  
  
}


other_deployment_phase <- smp_milestones %>%
  select(deployment_uid, con_phase_lookup_uid) %>%
  distinct()

# all deployment phases
all_public_deployment_phase <- srt_joined %>%
  union_all(long_terms) %>%
  union_all(other_deployment_phase) %>%
  anti_join(deployment_con_status, by = "deployment_uid")

## write to DB
dbWriteTable(poolConn, Id(schema = "fieldwork", table = "tbl_deployments_con_status"), all_public_deployment_phase, append= TRUE, row.names = FALSE)

## disconnect db
pool::poolClose(poolConn)
