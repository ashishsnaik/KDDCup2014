####################
# R source file used to read KDD data, extract and transform features
# Created by Ashish Naik
# MIT License
####################

# install.packages("qdap")
require(qdap)

# install.packages("plyr")
require(plyr)

source("kddCommon.R")


# Function: getKddData
# Returns a data frame with KDD data.
getKddData <- function(){
    
    tReadDataStart <- Sys.time()

    savedKddData <- paste(gRDataPath, "kddData.rda", sep="/")
    
    if(file.exists(savedKddData)){
        print("Reading saved KDD data... ")
        df <- readRDS(savedKddData)
        tReadDataEnd <- Sys.time()
        print(sprintf("Read Data in %s", format(tReadDataEnd-tReadDataStart)))
    
    }else{
        
        projectData <- getProjectData(gProjectsFile)
        outcomeData <- getOutcomeData(gOutcomesFile)
        essayData <- getEssayData(gEssaysFile)
        resourceData <- getResourceData(gResourcesFile)
        donationData <- getDonationsData(gDonationsFile)

        projectData$data_set <- ifelse(projectData$date_posted < 
                                           gTestDataStartDate, "eval", "test")
        projectData$data_set <- ifelse(projectData$date_posted < 
                                           gValidationStartDate, "train", projectData$data_set)

        projKeyCols <- names(projectData)[which(names(projectData) %in% 
                                                    c("projectid", "teacher_acctid", "schoolid", "date_posted", "data_set"))]
        
        mergedProjectDF <- mergeProjectData(projectData[which(projectData$data_set %in% 
                                                                  c("train", "eval")), ], donationData, outcomeData, projKeyCols)        
        
        # exclude validation data from school and teacher credibility to avoid data-leak into trained model
        schoolCredibilityData <- getSchoolCredibilityData(mergedProjectDF[which(mergedProjectDF$data_set %in% c("train")), ])
        teacherCredibilityData <- getTeacherCredibilityData(mergedProjectDF[which(mergedProjectDF$data_set %in% c("train")), ])
        
        tReadDataEnd <- Sys.time()
        print(sprintf("Read all necessary data in %s...", format(tReadDataEnd-tReadDataStart)))
        print("Making KDD data...")
        
        # independent variables (features) used in the model will be tagged with 'x_'
        colnames(projectData) <- ifelse(colnames(projectData) %in% projKeyCols, colnames(projectData), 
                                        paste("x", colnames(projectData), sep="_"))
        
        #
        # Merge the data together
        #
        outcomes <- subset(outcomeData, select=c("projectid", "is_exciting"))
        # target variable tagged with "y_"
        colnames(outcomes) <- c("projectid", "y_is_exciting")
        
        df <- merge(x=projectData, y=outcomes, by.x="projectid", by.y="projectid", all.x=T)
        df$y_is_exciting[is.na(df$y_is_exciting)] <- gDummyOutcome
        
        df <- merge(x=df, y=subset(essayData, select=grepl("^(projectid|x_.*)$", names(essayData))), 
                    by.x="projectid", by.y="projectid", all.x=T)
        
        df <- merge(x=df, y=subset(resourceData, select=grepl("^(projectid|x_.*)$", names(resourceData))), 
                    by.x="projectid", by.y="projectid", all.x=T)
        
        df <- merge(x=df, y=subset(schoolCredibilityData, select=grepl("^(schoolid|x_.*)$", names(schoolCredibilityData))), 
                    by.x="schoolid", by.y="schoolid", all.x=T)
        
        df <- merge(x=df, y=subset(teacherCredibilityData, select=grepl("^(teacher_acctid|x_.*)$", names(teacherCredibilityData))), 
                    by.x="teacher_acctid", by.y="teacher_acctid", all.x=T)
        
        # any teacher or school not found in training data 
        # will have zero credibility points
        df[is.na(df)] <- 0

        print(sprintf("Saving KDD data to %s ...", savedKddData))
        saveRDS(df, file=savedKddData)
    }
    
    tGetDataEnd <- Sys.time()
    print(sprintf("Returning Data in %s", format(tGetDataEnd-tReadDataStart)))
    
    return(df)
}

# Function: mergeProjectData
# Returns a data frame with merged project, donation, and outcome features.
mergeProjectData <- function(projectsDF, donationsDF, outcomesDF, projectColsToKeep){
    
    tStart <- Sys.time()
    savedMergedProjectData <- paste(gRDataPath, "mergedProjectData.rda", sep="/")
    
    if(file.exists(savedMergedProjectData)){
        print("Reading saved merged project data... ")
        mergedDF <- readRDS(savedMergedProjectData)
        
    }else{
        
        mergedDF <- merge(x=projectsDF[, which(names(projectsDF) %in% projectColsToKeep)], 
                          y=donationsDF,
                          by.x="projectid",
                          by.y="projectid",
                          all.x=T)
        
        # some projects data is missing from the donations data, change those NA values to 0
        # these projects are already marked 'not exciting' in the outcomes data
        mergedDF[is.na(mergedDF)] <- 0
        
        mergedDF <- merge(x=mergedDF, 
                          y=outcomesDF, 
                          by.x="projectid", 
                          by.y="projectid",
                          all.x=T)
        
        print(sprintf("Saving mergedDF to %s ...", savedMergedProjectData))
        saveRDS(mergedDF, file=savedMergedProjectData)
        
    }
    
    tEnd <- Sys.time()
    print(sprintf("Merged project data in %s", format(tEnd-tStart)))
    return(mergedDF)
}


# Function: getSchoolCredibilityData
# Returns a data frame with school credibility features.
getSchoolCredibilityData <- function(mergedProjectDF){
    
    tStart <- Sys.time()
    savedSchoolCredibilityDF <- paste(gRDataPath, "schoolCredibilityDF.rda", sep="/")
    
    if(file.exists(savedSchoolCredibilityDF)){
        print("Reading saved School Credibility data... ")
        schoolCredibilityDF <- readRDS(savedSchoolCredibilityDF)
        
    }else{

        print("Making schoolCredibilityDF... ")
        
        schoolCredibilityDF <- ddply(mergedProjectDF, 
                                      "schoolid",
                                      function(x)
                                          return(c(x_sch_projects_posted_cnt=length(x$projectid), # total projects posted by the school
                                                   x_sch_fully_funded_cnt=sum(x$fully_funded), # number of projects that were fully funded
                                                   x_sch_exciting_projects_cnt=sum(x$is_exciting), # number of projects that were exciting
                                                   x_sch_great_chat_cnt=sum(x$great_chat), # number of projects that had great chat
                                                   x_sch_thoughtful_donor_cnt=sum(x$donation_from_thoughtful_donor), # number of thoughtful donors that donated
                                                   x_sch_avg_donations_cnt=round(sum(x$donations_cnt)/length(x$projectid), 2), # average number of donations received per project
                                                   x_sch_avg_green_donations_cnt=round(sum(x$green_donations_cnt)/length(x$projectid), 2), # average green donations received per project
                                                   x_sch_avg_teacher_ref_donors_cnt=round(sum(x$teacher_ref_donor_cnt)/length(x$projectid), 2), # average teacher referred donors per project
                                                   x_sch_avg_plus_100_donor_cnt=round(sum(x$plus_100_donor_cnt)/length(x$projectid), 2), # average number of donors that donated more than $100
                                                   x_sch_avg_non_t_ref_plus_100_donor_cnt=round(sum(x$non_t_ref_plus_100_donor_cnt)/length(x$projectid), 2) # average number of non teacher referred donors that donated more than $100
                                                   )
                                                 )
                                     )
        
        print(sprintf("Saving schoolCredibilityDF to %s ...", savedSchoolCredibilityDF))
        saveRDS(schoolCredibilityDF, file=savedSchoolCredibilityDF)
        
    }
    
    tEnd <- Sys.time()
    print(sprintf("School credibility Data in %s", format(tEnd-tStart)))
    return(schoolCredibilityDF)
}


# Function: getTeacherCredibilityData
# Returns a data frame with teacher credibility features.
getTeacherCredibilityData <- function(mergedProjectDF){
    
    tStart <- Sys.time()
    savedTeacherCredibilityDF <- paste(gRDataPath, "teacherCredibilityDF.rda", sep="/")
    
    if(file.exists(savedTeacherCredibilityDF)){
        print("Reading saved Teacher Credibility data... ")
        teacherCredibilityDF <- readRDS(savedTeacherCredibilityDF)
        
    }else{

        print("Making teacherCredibilityDF... ")
        
        teacherCredibilityDF <- ddply(mergedProjectDF, 
                                      "teacher_acctid",
                                      function(x)
                                          return(c(x_tch_projects_posted_cnt=length(x$projectid), # total projects posted by the teacher
                                                   x_tch_fully_funded_cnt=sum(x$fully_funded), # number of projects that were fully funded
                                                   x_tch_exciting_projects_cnt=sum(x$is_exciting), # number of projects that were exciting
                                                   x_tch_great_chat_cnt=sum(x$great_chat), # number of projects that had great chat
                                                   x_tch_thoughtful_donor_cnt=sum(x$donation_from_thoughtful_donor), # number of thoughtful donors that donated
                                                   x_tch_avg_donations_cnt=round(sum(x$donations_cnt)/length(x$projectid), 2), # average number of donations received per project
                                                   x_tch_avg_green_donations_cnt=round(sum(x$green_donations_cnt)/length(x$projectid), 2), # average green donations received per project
                                                   x_tch_avg_teacher_ref_donors_cnt=round(sum(x$teacher_ref_donor_cnt)/length(x$projectid), 2), # average teacher referred donors per project
                                                   x_tch_avg_plus_100_donor_cnt=round(sum(x$plus_100_donor_cnt)/length(x$projectid), 2), # average number of donors that donated more than $100
                                                   x_tch_avg_non_t_ref_plus_100_donor_cnt=round(sum(x$non_t_ref_plus_100_donor_cnt)/length(x$projectid), 2) # average number of non teacher referred donors that donated more than $100
                                                   )
                                                 )
                                     )
        
        print(sprintf("Saving teacherCredibilityDF to %s ...", savedTeacherCredibilityDF))
        saveRDS(teacherCredibilityDF, file=savedTeacherCredibilityDF)
        
    }
    
    tEnd <- Sys.time()
    print(sprintf("Teacher credibility Data in %s", format(tEnd-tStart)))
    return(teacherCredibilityDF)
}


# Function: getDonationsData
# Returns data frame with donation features.
getDonationsData <- function(donationsFile=gDonationsFile){
    
    tStart <- Sys.time()
    savedDonationsDF <- paste(gRDataPath, "donationsDF.rda", sep="/")
    savedDonationFeaturesDF <- paste(gRDataPath, "donationFeaturesDF.rda", sep="/")
    
    if(file.exists(savedDonationFeaturesDF)){
        print("Reading saved Donation Features data... ")
        donationFeaturesDF <- readRDS(savedDonationFeaturesDF)
    }else{
        
        if(file.exists(savedDonationsDF)){
            print("Reading saved Donations data... ")
            donationsDF <- readRDS(savedDonationsDF)
        }else{
            
            print("Making donationsDF... ")
            print(sprintf("Reading %s ...",  donationsFile))
            
            stopifnot(file.exists(donationsFile))
            
            donationsColsToRead <- c(rep("character", 3), rep("NULL", 8), "character", "NULL", "character", 
                                     rep("NULL", 4), "character", rep("NULL", 2))
            
            donationsDF <- read.csv(donationsFile, sep=',', header=T, stringsAsFactors=F, 
                                    na.strings = c("NA", "", "NaN", "nan"), 
                                    colClass=donationsColsToRead
            )

            if(any(is.na(donationsDF))){
                print("Handling missing data...")
                donationsDF <- handleMissingValues(donationsDF, numericZeroReplace=T)
            }
            
            donationsDF$via_giving_page <- ifelse(donationsDF$via_giving_page=='t', 1, 0)
            
            donationsDF$green_donation <- ifelse(grepl('^.*(creditcard|paypal|amazon|check).*$', 
                                                       donationsDF$payment_method, ignore.case=T), 1, 0)
            
            donationsDF$plus_100_donor <- ifelse(donationsDF$dollar_amount=="100_and_up", 1, 0)
            
            donationsDF$non_t_ref_plus_100_donor <- ifelse(donationsDF$dollar_amount=="100_and_up" & 
                                                               donationsDF$via_giving_page==0, 1, 0)
            
            print(sprintf("Saving donationsDF to %s ...", savedDonationsDF))
            saveRDS(donationsDF, file=savedDonationsDF)
        }
        
        print("Creating Donation Features... ")
        donationFeaturesDF <- ddply(donationsDF, 
                                    "projectid", 
                                    function(x) 
                                        return(
                                            c(donations_cnt=length(x$donationid),
                                              green_donations_cnt=sum(x$green_donation),
                                              teacher_ref_donor_cnt=sum(x$via_giving_page),
                                              plus_100_donor_cnt=sum(x$plus_100_donor),
                                              non_t_ref_plus_100_donor_cnt=sum(x$non_t_ref_plus_100_donor))
                                        )
        )
        
        print(sprintf("Saving donationFeaturesDF to %s ...", savedDonationFeaturesDF))
        saveRDS(donationFeaturesDF, file=savedDonationFeaturesDF)
    }
    
    tEnd <- Sys.time()
    print(sprintf("Donations Data in %s", format(tEnd-tStart)))
    return(donationFeaturesDF)
}

# Function: getOutcomeData
# Returns a data frame with outcome features.
getOutcomeData <- function(outcomesFile=gOutcomesFile){
    
    tStart <- Sys.time()
    savedOutcomesDF <- paste(gRDataPath, "outcomesDF.rda", sep="/")
    
    if(file.exists(savedOutcomesDF)){
        print(sprintf("Reading saved %s... ", savedOutcomesDF))
        outcomesDF <- readRDS(savedOutcomesDF)
    }else{
        
        print("Making outcomesDF... ")
        print(sprintf("Reading %s ...",  outcomesFile))
        
        stopifnot(file.exists(outcomesFile))
        
        outcomesDF <- read.csv(outcomesFile, 
                               sep=',', 
                               header=T, 
                               stringsAsFactors=F, 
                               na.strings = c("NA", "", "NaN", "nan"), 
                               colClass=c(projectid="character"))            
        
        transformOutcomesData <- function(df){
            
            transformOCData <- function(colName){
                col <- df[,colName]
                na <- is.na(col)
                if(is.numeric(col)){
                    col[na] <- 0
                    print(sprintf("NUMERIC - %s - with %i MISSING RECORDS; REPLACED BY 0", colName, sum(na)))
                }else{
                    col <- ifelse(na | col=='f', 0, 1)
                    print(sprintf("NON-NUMERIC - %s - with %i MISSING RECORDS; REPLACED MISSING VALUES and \'f\' BY 0 and \'t\' BY 1", colName, sum(na)))
                }
                return(col)
            }
            
            df <- sapply(names(df), transformOCData, simplify=F)
            
            return(as.data.frame(df, stringsAsFactors=F))
        }
        
        print("Handling missing data and transforming...")
        outcomesDF <- cbind(projectid=outcomesDF[, names(outcomesDF) %in% c("projectid")], 
                            transformOutcomesData(outcomesDF[, !names(outcomesDF) %in% c("projectid")]))

        outcomesDF$projectid <- as.character(outcomesDF$projectid)
        
        print(sprintf("Saving outcomesDF to %s ...", savedOutcomesDF))
        saveRDS(outcomesDF, file=savedOutcomesDF)
    }
        
    tEnd <- Sys.time()
    print(sprintf("Outcomes Data in %s", format(tEnd-tStart)))
    return(outcomesDF)
}


# Function: getResourceData
# Returns a data frame with resource features.
getResourceData <- function(resourcesFile=gResourcesFile){
    
    tStart <- Sys.time()
    savedResourceFeaturesDF <- paste(gRDataPath, "resourceFeaturesDF.rda", sep="/")
    savedResourcesDF <- paste(gRDataPath, "resourcesDF.rda", sep="/")
    
    if(file.exists(savedResourceFeaturesDF)){
        print("Reading saved resources data... ")
        resourceFeaturesDF <- readRDS(savedResourceFeaturesDF)
    }else{

        if(file.exists(savedResourcesDF)){
            print(sprintf("Reading saved %s... ", savedResourcesDF))
            resourcesDF <- readRDS(savedResourcesDF)
        }else{
            
            print("Making resourcesDF... ")
            print(sprintf("Reading %s ...",  resourcesFile))
            
            stopifnot(file.exists(resourcesFile))
            
            resourcesDF <- read.csv(resourcesFile, sep=',', header=T, stringsAsFactors=F, 
                                    na.strings = c("NA", "", "NaN", "nan"), 
                                    colClass=c(resourceid="character", 
                                               projectid="character"))
            
            # change vendorid to character type (don't read it as a character as python script has 
            # written it as a floating point value,though, R read it as an integer, which is what we want.)
            resourcesDF$vendorid <- as.character(resourcesDF$vendorid)
            
            print("Handling missing data...")
            resourcesDF <- handleMissingValues(resourcesDF)

            print(sprintf("Saving resourcesDF to %s ...", savedResourcesDF))
            saveRDS(resourcesDF, file=savedResourcesDF)
        }
        
        # function extract resource features for each projectid
        extractResourceFeatures <- function(df){
            
            resourceFeaturesDF <- ddply(df, 
                                        "projectid", 
                                        function(x) 
                                            return(
                                                c(x_resource_cnt=length(x$resourceid),
                                                  x_vendor_cnt=length(unique(x$vendorid)),
                                                  x_total_units=sum(x$item_quantity),
                                                  x_total_cost=round(t(x$item_unit_price) %*% x$item_quantity, 2),
                                                  x_avg_cost_per_unit=round((t(x$item_unit_price) %*% x$item_quantity)/sum(x$item_quantity), 2))
                                            )
            )
            return(resourceFeaturesDF)
        }
        
        print("Extracting resource features...")
        resourceFeaturesDF <- extractResourceFeatures(resourcesDF)
        Sys.sleep(5)
        gc()
        
        print(sprintf("Saving extracted resource features to %s ...", savedResourceFeaturesDF))
        saveRDS(resourceFeaturesDF, file=savedResourceFeaturesDF)
    }
    
    tEnd <- Sys.time()
    print(sprintf("Resources Data in %s", format(tEnd-tStart)))
    return(resourceFeaturesDF)
}


# Function: getEssayData
# Returns a data frame with essay features.
getEssayData <- function(essaysFile=gEssaysFile){
    
    tStart <- Sys.time()
    savedEssayFeaturesDF <- paste(gRDataPath, "essayFeaturesDF.rda", sep="/")
    savedEssaysDF <- paste(gRDataPath, "essaysDF.rda", sep="/")
    
    if(file.exists(savedEssayFeaturesDF)){
        print("Reading saved essayFeaturesDF... ")
        essatFeaturesDF <- readRDS(savedEssayFeaturesDF)
    }else{

        if(file.exists(savedEssaysDF)){
            print("Reading saved essaysDF... ")
            essaysDF <- readRDS(savedEssaysDF)
        }else{
            
            stopifnot(file.exists(essaysFile))
            
            print("Making essaysDF... ")
            print(sprintf("Reading %s ...",  essaysFile))
            essaysDF <- read.csv(essaysFile, sep=',', header=T, stringsAsFactors=F, na.strings = c("NA", "", "NaN", "nan"), colClass="character")
            
            print("Handling missing data...")
            essaysDF <- handleMissingValues(essaysDF)
            
            print(sprintf("Saving essaysDF to %s ...", savedEssaysDF))
            saveRDS(essaysDF, file=savedEssaysDF)
        }
        
        # extract only word counts for now
        essaysKeyCols <- c("projectid", "teacher_acctid")
        essaysColsToWordCount <- c("title", "short_description", "need_statement", "essay")
        wordCountStr <- "wc"
        
        extractEssayFeatures <- function(df){
            for(n in names(df)){
                col <- df[,n]
                if(!is.numeric(col) & n %in% essaysColsToWordCount){
                    print(paste("Getting wordcounts for", n, sep=" - "))
                    df[,paste("x", n, wordCountStr, sep="_")] <- ifelse(col == gNaReplaceStr, 0, word_count(col, digit.remove=F))
                }
            }
            return(df)
        }
        
        print("Extracting essay features...")
        essatFeaturesDF <- extractEssayFeatures(essaysDF)
        
        essayFeatures <- append(essaysKeyCols, paste("x", essaysColsToWordCount, wordCountStr, sep="_"))
        
        essatFeaturesDF <- essatFeaturesDF[, essayFeatures]
        
        print(sprintf("Saving essatFeaturesDF to %s ...", savedEssayFeaturesDF))
        saveRDS(essatFeaturesDF, file=savedEssayFeaturesDF)
    }

    tEnd <- Sys.time()
    print(sprintf("Essays Data in %s", format(tEnd-tStart)))
    return(essatFeaturesDF)
}


# Function: getProjectData
# Returns a data frame with project features.
getProjectData <- function(projectsFile=gProjectsFile){
    
    tStart <- Sys.time()
    savedProjectsDF <- paste(gRDataPath, "projectsDF.rda", sep="/")
    
    if(file.exists(savedProjectsDF)){
        print("Reading saved projectsDF... ")
        projectsDF <- readRDS(savedProjectsDF)
    }else{
        
        stopifnot(file.exists(projectsFile))
        
        print("Making projectsDF... ")
        print(sprintf("Reading %s ...",  projectsFile))
        projectsDF <- read.csv(projectsFile, sep=',', header=T, stringsAsFactors=F, 
                               na.strings = c("NA", "", "NaN", "nan"), 
                               colClass=c(projectid="character", 
                                          teacher_acctid="character", 
                                          schoolid="character",
                                          date_posted="Date"))

        projectsDF <- projectsDF[which(projectsDF$date_posted >= gTrainStartDate),]
        
        projectsDF$wday_posted <- format(projectsDF$date_posted, "%a") # weekday
        projectsDF$month_posted <- format(projectsDF$date_posted, "%b") # month
        
        print("Handling missing NCES Ids...")
        
        # Assign missing NCESIDs (NAs), the "factor level" (integer value) of the corresponding "schoolid"
        projectsDF$school_ncesid <- apply(cbind(projectsDF$school_ncesid, xtfrm(as.factor(projectsDF$schoolid))), 1, max, na.rm=T)

        print("Handling missing Secondary Focus Subject and Area...")
        
        # replace NA secondary focus subject and area with the primary ones... 
        # (if the primary ones are also NA, that will be taken care by "handleMissingValues")
        projectsDF$secondary_focus_subject <- ifelse(is.na(projectsDF$secondary_focus_subject), 
                                                     projectsDF$primary_focus_subject, 
                                                     projectsDF$secondary_focus_subject)
        
        projectsDF$secondary_focus_area <- ifelse(is.na(projectsDF$secondary_focus_area), 
                                                  projectsDF$primary_focus_area, 
                                                  projectsDF$secondary_focus_area)
        
        print("Handling other missing values...")
        projectsDF <- handleMissingValues(projectsDF)

        # average number of underpreviliged students that could benefit from the 
        # project depends on the poverty level of the school
        avgPctUnderprivilegedStudents_HHPL <- (0.65 + (1 - 0.65) / 2)
        avgPctUnderprivilegedStudents_HPL <- (0.40 + (0.64 - 0.40) / 2)
        avgPctUnderprivilegedStudents_MPL <- (0.10 + (0.39 - 0.10) / 2)
        avgPctUnderprivilegedStudents_LPL <- (0.09/2)
        
        projectsDF$underprivileged_students_reached <- 0
        
        projectsDF$underprivileged_students_reached[which(projectsDF$poverty_level=="highest poverty")] <- 
            round(projectsDF$students_reached[which(projectsDF$poverty_level=="highest poverty")] * avgPctUnderprivilegedStudents_HHPL)
        
        projectsDF$underprivileged_students_reached[which(projectsDF$poverty_level=="high poverty")] <- 
            round(projectsDF$students_reached[which(projectsDF$poverty_level=="high poverty")] * avgPctUnderprivilegedStudents_HPL)
        
        projectsDF$underprivileged_students_reached[which(projectsDF$poverty_level=="moderate poverty")] <- 
            round(projectsDF$students_reached[which(projectsDF$poverty_level=="moderate poverty")] * avgPctUnderprivilegedStudents_MPL)
        
        projectsDF$underprivileged_students_reached[which(projectsDF$poverty_level=="low poverty")] <- 
            round(projectsDF$students_reached[which(projectsDF$poverty_level=="low poverty")] * avgPctUnderprivilegedStudents_LPL)
        
        print(sprintf("Saving projectsDF to %s ...", savedProjectsDF))
        saveRDS(projectsDF, file=savedProjectsDF)
    }
    
    tEnd <- Sys.time()
    print(sprintf("Projects Data in %s", format(tEnd-tStart)))

    return(projectsDF)
}