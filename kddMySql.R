#################
# R source file used to create the KDD database and predictions table
# Created by Ashish Naik
# MIT License
#################

# install.packages("RMySQL")
require(RMySQL)

source("kddCommon.R")
source("kddData.R")

gMySqlCols <- c("projectid", "is_exciting", "school_state", "poverty_level", "underprivileged_students_reached")
gMySqlTable <- "predictions"

# Function: mySqlWriteGBMPredictions
# Creates the KDD database and predictions table.
mySqlWriteGBMPredictions <- function(){

    projectData <- getProjectData(gProjectsFile)
    
    if(!exists("kddGBMPredictions")){
        kddGBMPredictions <- readRDS(gSavedGbmPredictionsDF)
    }
    
    mySqlData <- merge(x = kddGBMPredictions[, names(kddGBMPredictions) %in% gMySqlCols], 
                       y = projectData[, names(projectData) %in% gMySqlCols], 
                       by.x = "projectid", 
                       by.y = "projectid", 
                       all.x=T)
    
    mydb = dbConnect(MySQL(), user=<username>, password=<password>, host='localhost')
    
    dbSendQuery(mydb, "CREATE DATABASE IF NOT EXISTS kddcupdb;")
    dbSendQuery(mydb, "USE kddcupdb")
    
    mydb = dbConnect(MySQL(), user=<username>, password=<password>, dbname='kddcupdb', host='localhost')
    
    dbSendQuery(mydb, sprintf("DROP TABLE IF EXISTS %s;", gMySqlTable))
    
    print(sprintf("Creating %s table...", gMySqlTable))
    
    dbWriteTable(mydb, 
                 name = gMySqlTable, 
                 value = mySqlData, 
                 field.types = list(projectid = "VARCHAR(32) NOT NULL", 
                                    is_exciting = "CHAR(1) NOT NULL",
                                    school_state = "CHAR(2) NOT NULL",
                                    poverty_level = "CHAR(16) NOT NULL",
                                    underprivileged_students_reached = "SMALLINT NOT NULL"),
                 row.names=FALSE)
    print(sprintf("Created %s table with columns %s", gMySqlTable, paste(dbListFields(mydb, gMySqlTable), collapse=", ")))
    
    dbDisconnect(mydb)
    
}
