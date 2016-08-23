#################
# R source file used to run the R code
# Created by Ashish Naik
# MIT License
#################

source("kddCommon.R")
source("kddGBM.R")
source("kddMySql.R")


# run the GBM model
tStart <- Sys.time()
kddGBMPredictions <- runGBM()
tEnd <- Sys.time()

print(sprintf("Total Run Time: %s", format(tEnd-tStart)))

# write predictions to sql database
mySqlWriteGBMPredictions()
