##----------------------------------------------------------------
##' Title: A_prepare_data.R
##'
##' Purpose: Prepares data for use for Aim 3
##' Gathers DEX data from Aim 2, DALY data, and Population data
##----------------------------------------------------------------

##----------------------------------------------------------------
## Clear environment and set library paths
##----------------------------------------------------------------
rm(list = ls())
pacman::p_load(data.table, arrow, tidyverse, glue)

# Set drive paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "/home/j/"
  h <- paste0("/ihme/homes/",Sys.info()[7],"/")
  l <- '/ihme/limited_use/'
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "/Volumes/snfs"
  h <- paste0("/Volumes/",Sys.info()[7],"/")
  l <- '/Volumes/limited_use'
} else {
  j <- "J:/"
  h <- "H:/"
  l <- 'L:/'
}

##----------------------------------------------------------------
## 0. Functions
##----------------------------------------------------------------
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

'%nin%' <- Negate('%in%')

##----------------------------------------------------------------
## 1. Set directories & filepaths
##----------------------------------------------------------------
# Non-age standardized data
date_non_as <- "20260108"
fp_non_as <- file.path(h, "/aim_outputs/Aim2/C_frontier_analysis/", date_non_as, "/df_non_as.csv")

# Set output directory
date_today <- format(Sys.time(), "%Y%m%d")
dir_output <- file.path(h, "/aim_outputs/Aim3/A_data_preparation/", date_today)
ensure_dir_exists(dir_output)

##----------------------------------------------------------------
## 2. Read in data
##----------------------------------------------------------------
df_non_as <- read.csv(fp_non_as)

##----------------------------------------------------------------
## 3. Create changes in spending column
##  (Spending in year 2019 - Spending in year 2010) / (DALYs in 2019 - DALY in 2010)
## TODO - do we need to age-standardize here? should we convert spending to 2019 dollars?
##----------------------------------------------------------------









