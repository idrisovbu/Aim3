##----------------------------------------------------------------
##' Title: B_daly_decomp_with_cases.R
##'
##' Purpose: 
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

source(file.path(h, "repo/Aim3/B_decomposition/decomp_function.R"))

##----------------------------------------------------------------
## 0. Functions
##----------------------------------------------------------------
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

##----------------------------------------------------------------
## 1. Set directories & filepaths
##----------------------------------------------------------------
# Non-age standardized data
date_non_as <- "20260108"
fp_non_as <- file.path(h, "/aim_outputs/Aim2/C_frontier_analysis/", date_non_as, "/df_non_as.csv")

# Set output directory
date_today <- format(Sys.time(), "%Y%m%d")
dir_output <- file.path(h, "/aim_outputs/Aim3/B_decomposition/", date_today)
ensure_dir_exists(dir_output)

fp_output <- file.path(dir_output, "daly_decomp.csv")

##----------------------------------------------------------------
## 2. Decomposition
##----------------------------------------------------------------
year_ids <- c(2010, 2019)
start_year <- 2010
end_year <- 2019
factor_names <- c("population", "age_sex_pop_proportion", "case_rate", "dalys_per_case")

# Read in data
d <- fread(fp_non_as)

# Select columns we need
d <- d %>%
  select(c("cause_id", "acause", "cause_name", "location_id", "location_name", "sex_id", "year_id", 
          "prevalence", "population", "DALYs", "age_name", "age_group_years_start"
  ))

# Rename spend_mean -> expenditure
d <- d %>%
  setnames(old = "DALYs", new = "burden") %>%
  setnames(old = "prevalence", new = "case_count")

# Create dummy draw column
d <- d %>% 
  mutate(draw = 1)

# d[, total_population := sum(population), by=.(draw, cause_id, year_id)]
d[, total_population := sum(population), by=.(acause, cause_name, location_id, location_name, year_id, draw)]

## Convert cases to per-capita rate
d[, case_rate := case_count / population]

## Convert burden to dalys per case
d[, dalys_per_case := burden / case_count]

# Age-sex population proportion
d[, age_sex_pop_proportion := population / total_population]

# Set prevalence & population to NULL
d[, c("case_count", "population") := NULL]

# Rename "total_population" -> "population
setnames(d, "total_population", "population")

# d should be a data.table with columns of population, age_sex_pop_proportion, case_rate, and dalys_per_case.
# It should have one row per draw, cause_id, age_group, sex, and year. Years should include 1996, 2006, and 2016.
d_t <- dcast(d, draw + cause_id + age_name + sex_id + location_name ~ year_id,
           value.var = c(factor_names, "burden"))
d_t <- decompose(d_t, factor_names=factor_names, start_year=start_year, end_year=end_year)
d_t[, "delta_dalys" := get(paste0("burden_", end_year)) - get(paste0("burden_", start_year))]
d_t[, c(outer(factor_names, year_ids, paste, sep="_")) := NULL]
setnames(d_t,
         c(paste0("burden_", start_year), paste0("burden_", end_year)),
         c("dalys_start", "dalys_end"))

##----------------------------------------------------------------
## 3. Save
##----------------------------------------------------------------
fwrite(d_t, fp_output)

