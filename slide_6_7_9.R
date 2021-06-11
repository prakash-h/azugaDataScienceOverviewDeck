# load libraries
library(data.table)
library(lubridate)
library(DBI)
library(RMySQL)
library(dplyr)
library(reshape2)
library(xlsx)
library(ggplot2)
library(maps)
library(plotly)
library(readxl)
library(orca)
library(htmlwidgets)

options(scipen = 2)

############SLIDE6 Data as an Asset: Azuga's Commercial Fleet data ########################

# tripN1 <- read.csv("D:\\tripSummaryData.csv")
# tripN2 <- read.csv("D:\\Node2.csv")
# tripN3 <- read.csv("D:\\Node3.csv")

query1 <- ("select customerId,
                  trackeeId,
                  year(startDate) as year,
                  MONTH(startDate) as month ,
                  count(startDate) as count,
                  sum(tripTime) as tripTime ,
                  sum(distanceTravelled) as distanceTravelled,
                  sum(tripCount) as tripCount
           from azuga.tripSummary
           group by customerId,trackeeId,
           year(startDate),MONTH(startDate)")

con <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplerreadreplica.azuga.com')
tripN1 <- dbGetQuery(conn = con, query1)
dbDisconnect(con)
rm(con)

con_2 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern2rr.azuga.com')
tripN2 <- dbGetQuery(conn = con_2, query1)
dbDisconnect(con_2)
rm(con_2)


con_3 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern3rr.azuga.com')
tripN3 <- dbGetQuery(conn = con_3, query1)
dbDisconnect(con_3)
rm(con_3)


trip <- rbind(tripN1,tripN2,tripN3)

trip <- trip %>%
  filter(tripTime. != 0 & distanceTravelled.!=0.0)

trip$customerId <- as.character(trip$customerId)


trip <- trip %>%
  inner_join( revenueCustomers,by=c("customerId"="Azuga Customer ID"))

drive_days<- trip %>%
  group_by(trackeeId,year,month) %>%
  summarise(count=sum(count))

summary(drive_days$count)

test <- trip %>%
  filter(count> 31)

trips_miles<- trip %>%
  group_by(year) %>%
  summarise(trips=sum(tripCount), miles=sum(distanceTravelled)/1.609)

trip$min_day <- (trip$tripTime/60)/trip$count

trip_aggr <- trip %>%
  group_by(trackeeId) %>%
  summarise(min_median =median(min_day),days_median=median(count))

#write.csv (trips_miles,"D:\\trips_miles_year.csv")
write.xlsx(mom_comparison_2, file=paste0("D:\\Data_Science\\DataScienceOverviewDeck\\","Trackee_All_Updated.xlsx"), sheetName="Trip_Miles_Data", append=TRUE, row.names=FALSE,col.names = FALSE)

##############SLIDE 7 - Azuga's Commercial Fleet data (Heat Map of Makes) ###############

# connect to Node 1 DB (azuga schema)
conn_azuga <- DBI::dbConnect(RMySQL::MySQL(), user='azugaro', password='73f1c86914j2P1X', 
                             dbname='azuga', host='keplerreadreplica.azuga.com')

trackee <- tbl(conn_azuga, "trackee")

trackeeDetails <- trackee %>%
  filter(deleted==0) %>%
  select(customerId,trackeeId,make,model,year) %>%
  collect(n=Inf)


rm(trackee)
dbDisconnect(conn_azuga)
rm(conn_azuga)

# connect to Node 2 DB (azuga schema)
conn_azuga_2 <- DBI::dbConnect(RMySQL::MySQL(), user='azugaro', password='73f1c86914j2P1X',
                               dbname='azuga', host='keplern2rr.azuga.com')

trackee <- tbl(conn_azuga_2, "trackee")

trackeeDetails_2 <- trackee %>%
  filter(deleted==0) %>%
  select(customerId,trackeeId,make,model,year) %>%
  collect()

rm(trackee,group,user)
dbDisconnect(conn_azuga_2)
rm(conn_azuga_2)

# connect to Node 3 DB (azuga schema)
conn_azuga_3 <- DBI::dbConnect(RMySQL::MySQL(), user='azugaro', password='73f1c86914j2P1X',
                               dbname='azuga', host='keplern3rr.azuga.com')

trackee <- tbl(conn_azuga_3, "trackee")

trackeeDetails_3 <- trackee %>%
  filter(deleted==0) %>%
  select(customerId,trackeeId,make,model,year) %>%
  collect()


rm(trackee,group,user)
dbDisconnect(conn_azuga_3)
rm(conn_azuga_3)

#append all the three trackee details into one table
trackeeDetails <- rbind(trackeeDetails,trackeeDetails_2,trackeeDetails_3)

rm(trackeeDetails_2,trackeeDetails_3,group_1,group_2,group_3)


#   connect to DB (adhoc schema)
conn_adhoc <- DBI::dbConnect(RMySQL::MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='adhoc', host='keplerreadreplica.azuga.com')
salesforceAccountSync <- tbl(conn_adhoc, "salesforceAccountSync")

#   fetch list of revenue customers
revenueCustomers<- salesforceAccountSync %>%
  filter(`Account Type`=='Revenue') %>%
  select(`Azuga Customer ID`,`Account Name`, `SIC Code`, `Billing Country`) %>%
  collect()

trackeeDetails$customerId <- as.character(trackeeDetails$customerId)

trackeeDetails <-trackeeDetails %>%
  inner_join( revenueCustomers,by=c("customerId"="Azuga Customer ID"))

#write.csv(trackeeDetails,file="D:/ActiveTrackeeData.csv")
write.xlsx(mom_comparison_2, file=paste0("D:\\Data_Science\\DataScienceOverviewDeck\\","Trackee_All_Updated.xlsx"), sheetName="ActiveTrackeeData", append=TRUE, row.names=FALSE,col.names = FALSE)

##############SLIDE 7 - Azuga's Commercial Fleet data (Latest Location Store) ###############

query <- ("select customerId,
                  vehicleId,
                  lat,
                  lon,
                  time
          from azuga.latestLocationStore") 

con <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplerreadreplica.azuga.com')
lat_loca_Node1 <- dbGetQuery(conn = con, query)
dbDisconnect(con)
rm(con)

con_2 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern2rr.azuga.com')
lat_loca_Node2 <- dbGetQuery(conn = con_2, query)
dbDisconnect(con_2)
rm(con_2)


con_3 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern3rr.azuga.com')
lat_loca_Node3 <- dbGetQuery(conn = con_3, query)
dbDisconnect(con_3)
rm(con_3)

dfNew <- rbind(lat_loca_Node1,lat_loca_Node2,lat_loca_Node3)

#write.csv(lat_loca_Node_2_3,file="D\\:Node2_3.csv")


usa1 <- read.xlsx("D:\\lat_long.xlsx",sheetName = 'Sheet1')
usa <- map_data("usa") # we already did this, but we can do it again

ggplot() + geom_polygon(data = usa, aes(x=long, y = lat, group = group)) +
  coord_fixed(1.3)



#dfNew = read.csv("D:\\Al_Nodes_latest_loc.csv")

# geo styling
g <- list(
  scope = 'usa',
  projection = list(type = 'albers usa'),
  showland = TRUE,
  landcolor = toRGB("gray95"),
  subunitcolor = toRGB("gray85"),
  countrycolor = toRGB("gray85"),
  countrywidth = 0.5,
  subunitwidth = 0.5
)

fig <- plot_geo(dfNew, lat = ~lat, lon = ~lon)
fig <- fig %>% add_markers(
  color = ~customerId, symbol = I("square"), size = I(8)
)
fig <- fig %>% layout(
  title = 'Vehicle Distribution', geo = g
)

#fig
export(fig, file = "image.png")
htmlwidgets::saveWidget(fig, file = "image.html")


################SLIDE 9 --Azuga's Commercial Fleet data (DTC) #################

query2 <- ("select distinct year(Time) as year,VEHICLE_ID
            from azuga.DTC_MESSAGE")

con <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplerreadreplica.azuga.com')
DTC1 <- dbGetQuery(conn = con, query2)
dbDisconnect(con)
rm(con)

con_2 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern2rr.azuga.com')
DTC2 <- dbGetQuery(conn = con_2, query2)
dbDisconnect(con_2)
rm(con_2)


con_3 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern3rr.azuga.com')
DTC3 <- dbGetQuery(conn = con_3, query2)
dbDisconnect(con_3)
rm(con_3)


DTC <- rbind(DTC1,DTC2,DTC3)

length(unique(DTC$VEHICLE_ID))

dtc_aggr <- DTC %>%
  group_by(year) %>%
  summarise(veh_count=n_distinct(VEHICLE_ID))

write.xlsx(mom_comparison_2, file=paste0("D:\\Data_Science\\DataScienceOverviewDeck\\","Trackee_All_Updated.xlsx"), sheetName="Vehicles_With_DTCMessages", append=TRUE, row.names=FALSE,col.names = FALSE)

################SLIDE 9 --Azuga's Commercial Fleet data (Battery Events) #################

query3 <- ("select count(distinct(vehicleId)) as count from azuga.obd
            where pid=65535")

con <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplerreadreplica.azuga.com')
Battery1 <- dbGetQuery(conn = con, query3)
dbDisconnect(con)
rm(con)

con_2 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern2rr.azuga.com')
Battery2 <- dbGetQuery(conn = con_2, query3)
dbDisconnect(con_2)
rm(con_2)


con_3 <- dbConnect (MySQL(), user='azugaro', password='73f1c86914j2P1X', dbname='azuga', host='keplern3rr.azuga.com')
Battery3 <- dbGetQuery(conn = con_3, query3)
dbDisconnect(con_3)
rm(con_3)

Battery <- rbind(Battery1,Battery2,Battery3)
