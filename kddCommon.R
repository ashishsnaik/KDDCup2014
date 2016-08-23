####################
# R source file containing common definitions
# Created by Ashish Naik
# MIT License
####################

# set the working directory
setwd(<working_directory>)
# set the seed
set.seed(2015)
# disable scientific notation
options(scipen=999)

gDataPath <- "data/input"
gOutputPath <- "output"
gRDataPath <- "data/rdata"
gRModelsPath <- "data/rmodels"

gOutcomesFile <- paste(gDataPath, "outcomes.csv", sep='/')
gDonationsFile <- paste(gDataPath, "donations.csv", sep='/')
gEssaysFile <- paste(gDataPath, "essays.csv", sep='/')
gProjectsFile <- paste(gDataPath, "projects.csv", sep='/')
gResourcesFile <- paste(gDataPath, "resources.csv", sep='/')

gNaReplaceStr <- "#NA#"
gDummyOutcome <- -1

gTrainStartDate <- as.Date("2010-04-14")
gValidationStartDate <- as.Date("2013-01-01")
gTestDataStartDate <- as.Date("2014-01-01")


# GBM related definitions
gSavedGbmFitModelTune <- paste(gRModelsPath, "gbmFitModel_tune.rda", sep="/")
gSavedGbmTunedParams <- paste(gRModelsPath, "gbmTunedParams.rda", sep="/")
gSavedGbmFitTuningPlot <- paste(gOutputPath, "gbmTuningPlot.png", sep="/")
gSavedGbmROCObject <- paste(gRModelsPath, "gbmROCObject.rda", sep="/")
gSavedGbmROCPlot <- paste(gOutputPath, "gbmROCPlot.png", sep="/")
gSavedGbmFitsEnsemble <- paste(gRModelsPath, "gbmFitsEnsemble.rda", sep="/")
gSavedGbmPredictionsDF <- paste(gOutputPath, "gbmPredictionsData.rda", sep="/")
gSavedGbmSubmissionGZFile <- paste(gOutputPath, "gbmPredictions.csv.gz", sep="/")


# Data files should be present in the gDataPath directory
if(!file.exists(gDataPath)){
    stop(sprintf("Data file path %s does not exist", gDataPath))
}

# create rdata directory if it doesn't exist
if(!file.exists(gRDataPath)){
    print(sprintf("Creating %s directory...", gRDataPath))
    dir.create(file.path(getwd(), gRDataPath))
}

# create rmodels directory if it doesn't exist
if(!file.exists(gRModelsPath)){
    print(sprintf("Creating %s directory...", gRModelsPath))
    dir.create(file.path(getwd(), gRModelsPath))
}

# create output directory if it doesn't exist
if(!file.exists(gOutputPath)){
    print(sprintf("Creating %s directory...", gOutputPath))
    dir.create(file.path(getwd(), gOutputPath))
}


# Function: handleMissingValues
# Generic function that replaces missing values in a dataframe.
# Missing Numeric: replaced by 0 or median
# Missing Non-numeric: replaced by a new category defined by gNaReplaceStr, currently #NA#
handleMissingValues <- function(df, numericZeroReplace=F, nonNumericReplaceStr=gNaReplaceStr){
    
    replaceMissing <- function(colName){
        col <- df[,colName]
        na <- is.na(col)
        if(any(na)){
            if(is.numeric(col)){
                numReplace <- ifelse(numericZeroReplace==T, 0, median(col, na.rm=T))
                col[na] <- numReplace 
                print(paste("NUMERIC", colName, "MISSING RECORDS", sum(na), "REPLACED BY", 
                            ifelse(numericZeroReplace==T, 'ZERO', 'MEDIAN'), 
                            as.character(numReplace), sep=" - "))
            }else{
                col[na] <- nonNumericReplaceStr 
                print(paste("NON-NUMERIC", colName, "MISSING RECORDS", sum(na), "REPLACED BY", 
                            nonNumericReplaceStr, sep=" - "))
            }
        }
        return(col)
    }    
    
    df <- sapply(names(df), replaceMissing, simplify=F)
    
    return(as.data.frame(df, stringsAsFactors=F))
}
