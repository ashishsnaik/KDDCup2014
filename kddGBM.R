####################
# R source file used to fit generalized boosted regression model and predict outcome
# Created by Ashish Naik
# MIT License
####################

source("kddCommon.R")
source("kddData.R")

# install.packages("caret")
require(caret)

# install.packages("gbm")
require(gbm)

# install.packages("pROC")
require(pROC)


# Function: gbmTransformData
# Tailors and transforms the KDD data frame columns for use by GBM model.
gbmTransformData <- function(df, featureCols, nonFeatureCols, boolFeatures){
    
    savedKddGBMData <- paste(gRDataPath, "kddGBMData.rda", sep="/")
    tGetDataStart <- Sys.time()
    
    if(file.exists(savedKddGBMData)){
        print("Reading saved data for GBM model... ")
        df <- readRDS(savedKddGBMData)
        
    }else{

        print("Transforming data for GBM model...")
        
        colsToKeep <- append(nonFeatureCols, featureCols)
        # keep only the projectid, data set (train, validation, or test), the target variable, and predictor variables
        df <- subset(df, select=colsToKeep)
        
        # change boolean 't' and 'f' values to 1 and 0...
        df[, which(names(df) %in% boolFeatures)] <- ifelse(df[, which(names(df) %in% boolFeatures)] == 't', 1, 0)
        
        xTransformData <- function(df, featCols=featureCols, nonFeatCols=nonFeatureCols){
            # except for the non-feature columns, convert all 
            # non-numeric data to numeric factor levels
            xtransform <- function(x){
                if(!is.numeric(x)){
                    x <- xtfrm(as.factor(x))
                }
                return(x)
            }
            df <- cbind(df[,names(df) %in% nonFeatCols], lapply(df[,names(df) %in% featCols], xtransform))
            return(df)
        }
        
        df <- xTransformData(df)

        print(sprintf("Saving data for GBM model to %s ...", savedKddGBMData))
        saveRDS(df, file=savedKddGBMData)
    }
    
    tGetDataEnd <- Sys.time()
    print(sprintf("GBM Data in %s", format(tGetDataEnd-tGetDataStart)))
    
    return(df)
}


# Function: runGBM
# Builds GBM model ensemble, predicts the test data, and returns the predictions in a data frame
runGBM <- function(){
    
    dfGBM <- getKddData()
    
    keyCol <- "projectid"
    datasetCol <- "data_set"
    targetCol <- names(dfGBM)[grep("^(y_.*)$", names(dfGBM))]
    featureCols <- names(dfGBM)[grep("^(x_.*)$", names(dfGBM))]
    nonFeatureCols <- append(keyCol, append(datasetCol, targetCol))
    
    boolFeatures <- paste("x",
                          c("school_charter", "school_magnet", "school_year_round", 
                            "school_nlns", "school_kipp", "school_charter_ready_promise", 
                            "teacher_teach_for_america", "teacher_ny_teaching_fellow", 
                            "eligible_double_your_impact_match", "eligible_almost_home_match"),
                          sep="_")
    
    #
    # Data preparation
    #
    dfGBM <- gbmTransformData(dfGBM, featureCols, nonFeatureCols, boolFeatures)
    
    # near-zero variance features are checked on train and remove from train+eval+test
    nzv <- nearZeroVar(dfGBM[which(dfGBM$data_set=="train"),])
    featsToRemove <- intersect(featureCols, names(dfGBM)[nzv])
    if(length(featsToRemove) > 0){
        dfGBM <- dfGBM[,!names(dfGBM) %in% featsToRemove]
    }
    featureCols <- setdiff(featureCols, featsToRemove)
    
    print("NZV features removed:")
    print(featsToRemove)
    
    kddTestData <- dfGBM[which(dfGBM$data_set=="test"),]
    nKddTest <- nrow(kddTestData)
    
    # the evaluation set will later be further divided into validation
    # and test sets to get a train-validation-test 3 way split
    trainEval <- dfGBM[which(dfGBM$data_set %in% c("train", "eval")),]
    nTrainEval <- nrow(trainEval)

    
    # there is about 12:1 imbalance between non-exciting:exciting projects in the training data,
    # so, need to use balanced training samples and build an ensemble of models
    idxTrainMajor <- which(trainEval$data_set=="train" & trainEval$y_is_exciting==0)
    idxTrainMinor <- which(trainEval$data_set=="train" & trainEval$y_is_exciting==1)
    nTrainMajor <- length(idxTrainMajor)
    nTrainMinor <- length(idxTrainMinor)
    print(sprintf("Non-exciting to Exciting projects ratio is %.2f", nTrainMajor/nTrainMinor))

    
    # 3-way data split
    # Divide the evaluation data, into two sets:
    #   1. Validation Set (VAL) for tuning model parameters 
    #   2. Test Set (TS) to check the performance of the final model 
    #   NOTE: Maintain ratio of the exciting:non-exciting projects accross the two
    idxEvalMajor <- which(trainEval$data_set=="eval" & trainEval$y_is_exciting==0)
    idxEvalMinor <- which(trainEval$data_set=="eval" & trainEval$y_is_exciting==1)
    nEvalMajor <- length(idxEvalMajor)
    nEvalMinor <- length(idxEvalMinor)
    
    
    idxVALMajor <- idxEvalMajor[createDataPartition(idxEvalMajor, p=0.5, groups=2, list=T)$Resample1]
    idxTSMajor <- setdiff(idxEvalMajor, idxVALMajor)
    idxVALMinor <- idxEvalMinor[createDataPartition(idxEvalMinor, p=0.5, groups=2, list=T)$Resample1]
    idxTSMinor <- setdiff(idxEvalMinor, idxVALMinor)
    
    idxVAL <- sample(append(idxVALMajor, idxVALMinor))
    idxTS <- sample(append(idxTSMajor, idxTSMinor))
    nVAL <- length(idxVAL)
    
    # need that target variable as a factor
    trainEval$y_is_exciting <- as.factor(trainEval$y_is_exciting)
    levels(trainEval$y_is_exciting) <- c("not_exciting", "exciting")
    
    testSet <- trainEval[idxTS, which(colnames(trainEval) %in% append(targetCol, featureCols))]
    
    #
    # 5-fold Cross-Validation setup
    #
    nResamples <- 5
    idxListTrainResamples <- list()
    idxListVAL <- list()

    # use the complete blanced training set to train the model
    # in each iteration, validate the model on a random sample from the validation set
    for(i in 1:nResamples){
        idxListTrainResamples[[i]] <- sample(append(sample(idxTrainMajor, nTrainMinor, replace=F), idxTrainMinor))
        idxListVAL[[i]] <- sample(idxVAL, round(nVAL*0.6), replace=F)
    }
    
    nBalancedTrain <- length(idxListTrainResamples[[1]])
    
    
    gbmGrid <- expand.grid(.interaction.depth = c(7, 8, 9), 
                           .n.trees = c(4:8) * 500, 
                           .shrinkage = c(0.1, 0.01),
                           .n.minobsinnode = c(10, 50))
    
    
    fitControl <- trainControl(
        method = "cv", 
        number = nResamples, # 5-fold CV
        classProbs = T,
        summaryFunction = twoClassSummary,
        index = idxListTrainResamples,
        indexOut = idxListVAL,
        verboseIter = T)
    
    tGbmStart <- Sys.time()
    gbmFit <- train(x = trainEval[, names(trainEval) %in% featureCols],
                    y = trainEval[, names(trainEval) %in% targetCol],
                    method = "gbm", 
                    metric = "ROC",
                    verbose = T, 
                    nTrain = nBalancedTrain, # round(nBalancedTrain*0.5),
                    trControl = fitControl, 
                    tuneGrid = gbmGrid
                    )
    tGbmEnd <- Sys.time()
    print(sprintf("GBM Train time: %s", format(tGbmEnd-tGbmStart)))
    
    
    tunedParams <- gbmFit$bestTune
    save(gbmFit, file=gSavedGbmFitModelTune)
    save(tunedParams, file=gSavedGbmTunedParams)
    
    png(filename = gSavedGbmFitTuningPlot, width = 1024, height = 768)
    plot(gbmFit, main="GBM Parameter Tuning (Cross Validation 3-Way Split)")
    dev.off()
    
    #
    # Model performance on Test (HOLD OUT) Set
    #
    testSetPredProb <- predict(gbmFit, testSet[,featureCols], type="prob", tunedParams)
    gbmROC <- roc(testSet[, targetCol], testSetPredProb[, "exciting"])
    print(paste("Test Set AUC", auc(gbmROC), sep=": "))
    
    save(gbmROC, file=gSavedGbmROCObject)
    
    png(filename = gSavedGbmROCPlot, width = 1024, height = 768)
    p <- plot(gbmROC, type = "S", print.thres = .5, main="GBM ROC Curve - Test Set", lwd = .5)
    dev.off()
    
    
    #
    # Final Test Data Predictions
    #
    levels(trainEval$y_is_exciting) <- c("0", "1")
    trainEval$y_is_exciting <- as.integer(as.character(trainEval$y_is_exciting))
    
    gbmFitsEnsemble <- list()
    nBalancedGBMEnsemble <- 15
    kddGBMPredictions <- data.frame("projectid" = kddTestData[, keyCol], stringsAsFactors = F)
    
    print(sprintf("Test Data Prediction using an ensemble of %i GBM Models ----------", nBalancedGBMEnsemble))
    
    tGbmPredStart <- Sys.time()
    
    for(i in 1:nBalancedGBMEnsemble){
        
        print(sprintf("Fitting GBM Model #%i...", i))
        idxBalancedTrain <- sample(append(sample(idxTrainMajor, nTrainMinor, replace = F), idxTrainMinor))
        nBalTrain <- length(idxBalancedTrain)
    
        gbmFitsEnsemble[[i]] <- gbm.fit(x=trainEval[idxBalancedTrain, names(trainEval) %in% featureCols], 
                                        y=trainEval[idxBalancedTrain, names(trainEval) %in% targetCol],
                                        distribution = "bernoulli",
                                        bag.fraction = 0.5,
                                        n.trees = tunedParams$n.trees,
                                        interaction.depth = tunedParams$interaction.depth,
                                        n.minobsinnode = tunedParams$n.minobsinnode,
                                        shrinkage = tunedParams$shrinkage,
                                        nTrain = nBalTrain,
                                        verbose=F
        )
        
        kddGBMPredictions[, paste("pred", i, sep="_")] <- predict(gbmFitsEnsemble[[i]], kddTestData[, featureCols], 
                                                                  n.trees = tunedParams$n.trees, type="response")
        
        print(sprintf("%i of %i GBM Predictions in %s", i, nBalancedGBMEnsemble, format(Sys.time()-tGbmPredStart)))
    }
    
    tGbmPredEnd <- Sys.time()
    print(sprintf("******** KDD Test Data Predictions (GBM) in %s", format(tGbmPredEnd-tGbmPredStart)))
    
    save(gbmFitsEnsemble, file=gSavedGbmFitsEnsemble)
    
    # average predictions to get final probability and outcome (cutoff = 0.5)
    predCols <- names(kddGBMPredictions)[grep("^(pred.*)$", names(kddGBMPredictions))]
    kddGBMPredictions[, "probability"] <- rowSums(kddGBMPredictions[,predCols])/length(predCols)
    kddGBMPredictions[, "is_exciting"] <- ifelse(kddGBMPredictions[, "probability"] < 0.5, 0, 1)
    
    saveRDS(kddGBMPredictions, file = gSavedGbmPredictionsDF)
    
    nExciting <- length(which(kddGBMPredictions$is_exciting == 1))
    print(sprintf("GBM Found %i of %i (%.2f%%) projects exciting", nExciting, nrow(kddGBMPredictions), nExciting/nrow(kddGBMPredictions)))
    
    print(sprintf("Writing submission file %s ...", gSavedGbmSubmissionGZFile))
    submissionDF <- data.frame("projectid" = kddGBMPredictions[, keyCol], "is_exciting" = kddGBMPredictions[, "probability"])
    write.csv(submissionDF, gzfile(gSavedGbmSubmissionGZFile), row.names=FALSE, quote=FALSE)

    print("Done")
    
    return(kddGBMPredictions)
}