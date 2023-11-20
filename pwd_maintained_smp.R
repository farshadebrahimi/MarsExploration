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

#0.1: database connection and global options --------

#set default page length for datatables
options(DT.options = list(pageLength = 15))

#set db connection
#using a pool connection so separate connnections are unified
#gets environmental variables saved in local or pwdrstudio environment
poolConn <- dbPool(odbc(), dsn = "mars14_datav2", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))



listing <- dbGetQuery(poolConn, "with ppp 
as 
(SELECT tbl_gswibasin.smp_id,
    tbl_gswibasin.surface_maintenance,
    tbl_gswibasin.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswibasin
UNION
 SELECT tbl_gswiblueroof.smp_id,
    tbl_gswiblueroof.surface_maintenance,
    tbl_gswiblueroof.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswiblueroof
UNION
 SELECT tbl_gswibumpout.smp_id,
    tbl_gswibumpout.surface_maintenance,
    tbl_gswibumpout.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswibumpout
UNION
 SELECT tbl_gswicistern.smp_id,
    tbl_gswicistern.surface_maintenance,
    tbl_gswicistern.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswicistern
UNION
 SELECT tbl_gswidrainagewell.smp_id,
    tbl_gswidrainagewell.surface_maintenance,
    tbl_gswidrainagewell.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswidrainagewell
UNION
 SELECT tbl_gswigreenroof.smp_id,
    tbl_gswigreenroof.surface_maintenance,
    tbl_gswigreenroof.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswigreenroof
UNION
 SELECT tbl_gswipermeablepavement.smp_id,
    tbl_gswipermeablepavement.surface_maintenance,
    tbl_gswipermeablepavement.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswipermeablepavement
UNION
 SELECT tbl_gswiplanter.smp_id,
    tbl_gswiplanter.surface_maintenance,
    tbl_gswiplanter.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswiplanter
UNION
 SELECT tbl_gswiraingarden.smp_id,
    tbl_gswiraingarden.surface_maintenance,
    tbl_gswiraingarden.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswiraingarden
UNION
 SELECT tbl_gswiswale.smp_id,
    tbl_gswiswale.surface_maintenance,
    tbl_gswiswale.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswiswale
UNION
 SELECT tbl_gswitree.smp_id,
    tbl_gswitree.surface_maintenance,
    tbl_gswitree.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswitree
UNION
 SELECT tbl_gswitreetrench.smp_id,
    tbl_gswitreetrench.surface_maintenance,
    tbl_gswitreetrench.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswitreetrench
UNION
 SELECT tbl_gswitrench.smp_id,
    tbl_gswitrench.surface_maintenance,
    tbl_gswitrench.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswitrench
UNION
 SELECT tbl_gswiwetland.smp_id,
    tbl_gswiwetland.surface_maintenance,
    tbl_gswiwetland.subsurface_maintenance,
	lifecycle_status
   FROM external.tbl_gswiwetland),
   sss as
   
   (select * from ppp where subsurface_maintenance = 'NONE' AND surface_maintenance = 'NONE' AND lifecycle_status = 'ACT' AND smp_id like '%-%'), 
   
mmm as 
(select * from sss  inner join external.tbl_smpbdv ON sss.smp_id = external.tbl_smpbdv.smp_id)

select * from mmm  where cipit_status = 'Closed' OR  cipit_status = 'Construction-Substantially Complete'  OR  cipit_status = 'Construction-Contract Closed' ")


inlets <- dbGetQuery(poolConn, "select * from fieldwork.viw_all_inlets")

smps <- listing[,-1] %>% left_join(inlets, by = "smp_id") 

