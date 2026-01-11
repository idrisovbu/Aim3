##----------------------------------------------------------------
##' Title: A_gupta_draw_data.R
##'
##' Purpose: Pulls draw data for population, cases (prevalence), and DALYs needed for the Gupta Analysis
##' Use: Run script from start to finish, data will save to Aim2/A_data_preparation/<current_date>
##----------------------------------------------------------------

##----------------------------------------------------------------
## 0. Clear environment and set library paths
##----------------------------------------------------------------
rm(list = ls())

pacman::p_load(arrow, data.table, dplyr)

library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))
'%nin%' <- Negate('%in%')

# Source IHME shared functions
source("/ihme/cc_resources/libraries/current/r/get_outputs.R")
source("/ihme/cc_resources/libraries/current/r/get_population.R")
source("/ihme/cc_resources/libraries/current/r/get_location_metadata.R")
source("/ihme/cc_resources/libraries/current/r/get_age_metadata.R")
source("/ihme/cc_resources/libraries/current/r/get_model_estimates.R")
source("/ihme/cc_resources/libraries/current/r/get_cause_metadata.R")
source("/ihme/cc_resources/libraries/gbd_sunset/r/get_draws.R")

df_cause <- get_cause_metadata(cause_set_id = 2, release_id = 16)

##----------------------------------------------------------------
## 0.1 Functions
##----------------------------------------------------------------
# Function to ensure filepath / folders exist
ensure_path <- function(filepath) {
  if (!dir.exists(filepath)) {
    dir.create(filepath, recursive = TRUE, showWarnings = FALSE)
  }
  return(filepath)
}

##----------------------------------------------------------------
## 0.2 Set output directory
##----------------------------------------------------------------
# Output path
date_today <- format(Sys.Date(), "%Y%m%d")
dir_out <- paste0("/ihme/homes/idrisov/aim_outputs/Aim3/A_data_preparation/", date_today)

ensure_path(dir_out)

##----------------------------------------------------------------
## 1. Set Parameters
##----------------------------------------------------------------
# Release ID, 16 for GBD 2023
rel_id <- 16

# Years
year_ids <- c(2010:2019)

# Cause_ids
cause_ids <- c(298, 973) # hiv = 298, _subs = 973

# Sex_ids
sex_ids <- c(1,2)

# Measure
# 5 = Prevalence, 2 = DALYs, 1 = Deaths, 

# Metric
# 1 = Number, 3 = Rate (per 100,000)

# Location_ids
df_loc <- get_location_metadata(location_set_id = 35,
                                release_id = 16)

df_state_loc_ids <- df_loc %>%
  filter(parent_id == 102) %>%
  select(location_name, location_id)

list_state_loc_ids <- df_state_loc_ids$location_id

# Age Group ids
df_age_groups <- get_age_metadata(release_id = 16)
list_age_group_ids <- df_age_groups$age_group_id

##----------------------------------------------------------------
## 2. Pull Draw Data
##----------------------------------------------------------------
get_draw_args = list(
                  gbd_id_type = c("cause_id", "cause_id"),
                  gbd_id = cause_ids,
                  # measure_id = None, 
                  location_id = list_state_loc_ids, 
                  year_id = year_ids, 
                  age_group_id = list_age_group_ids, 
                  sex_id = sex_ids, 
                  # metric_id = 1, # 1 = Number, 3 = rates
                  release_id = rel_id, 
                  n_draws = 250,
                  downsample = TRUE
                  )

# Pull prevalence RATES, need to convert into counts w/ population data
df_prevalence <- do.call(
  get_draws,
  c(get_draw_args, list(source = "como", metric_id = 3, measure_id = 5))
)

# Pull DALY COUNTS
df_dalys <- do.call(
  get_draws,
  c(get_draw_args, list(source = "dalynator", metric_id = 1, measure_id = 2))
)

# Pull Population draw COUNTS TBD - waiting for response on Slack
# df_dalys <- do.call(
#   get_draws,
#   c(get_draw_args, list(source = "dalynator", metric_id = 1, measure_id = 2))
# )

# Population COUNTS
df_population <- get_population(release_id = rel_id,
                                age_group_id = "all",
                                location_id = list_state_loc_ids,
                                location_set_id = 35,
                                year_id = year_ids,
                                sex_id = sex_ids
)

df_population <- left_join(
  x = df_population,
  y = df_state_loc_ids
)

##----------------------------------------------------------------
## 3. Save Draw data so we don't have to spend a million years pulling it again
##----------------------------------------------------------------
write_parquet(df_prevalence, file.path(dir_out, "draws_rates_prevalence.parquet"))
write_parquet(df_dalys, file.path(dir_out, "draws_counts_dalys.parquet"))
write_parquet(df_population, file.path(dir_out, "counts_population.parquet"))

##----------------------------------------------------------------
## 4. Convert prevalence rates to counts using population data
##----------------------------------------------------------------
# Merge prevalence rates to population counts
df_prevalence_counts <- merge(
  df_prevalence,
  df_population[, .(location_id, year_id, sex_id, age_group_id, population)],
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

# Determine draw columns
draw_cols <- grep("^draw_\\d+$", names(df_prevalence_counts), value = TRUE)

# Multiply each draw column by the respective population value (we don't divide by 100,000, it seems when we return rate it's not by 100,000?)
df_prevalence_counts <- df_prevalence_counts %>%
  mutate(
    across(
      all_of(draw_cols),
      ~ .x * population
    )
  )

# # Compare against prevalence summarized counts
# df_prevalence_sum_counts <- get_outputs(topic = "cause", 
#                              release_id=rel_id,
#                              location_id=list_state_loc_ids, 
#                              year_id=year_ids, 
#                              age_group_id="all",
#                              sex_id=sex_ids, 
#                              measure_id=5, # 5 = Prevalence
#                              metric_id=1, # 1 = Number
#                              cause_id=cause_ids, 
#                              location_set_id=35
# )
# 
# View(df_prevalence_sum_counts %>% filter(age_group_id == 10) %>% filter(location_id == 523) %>% filter(year_id == 2010) %>% filter(sex_id == 1))
# View(df_prevalence_counts %>% filter(age_group_id == 10) %>% filter(location_id == 523) %>% filter(year_id == 2010) %>% filter(sex_id == 1))

##----------------------------------------------------------------
## 5. Pivot & merge data so each draw it on it's own row
##----------------------------------------------------------------
# DALYs
df_dalys_long <- df_dalys %>%
  select(!c("version_id", "measure_id", "metric_id")) %>%
  pivot_longer(
    cols = matches("^draw_\\d+$"),
    names_to = "draw",
    names_prefix = "draw_",
    values_to = "value"
  ) %>%
  mutate(draw = as.integer(draw))

df_dalys_long <- df_dalys_long %>%
  setnames(old = "value", new = "DALYs_count")

# Prevalence
df_prevalence_long <- df_prevalence_counts %>%
  select(!c("version_id", "measure_id", "metric_id", "population")) %>%
  pivot_longer(
    cols = matches("^draw_\\d+$"),
    names_to = "draw",
    names_prefix = "draw_",
    values_to = "value"
  ) %>%
  mutate(draw = as.integer(draw))

df_prevalence_long <- df_prevalence_long %>%
  setnames(old = "value", new = "prevalence_count")

# View(head(df_dalys_long))
# View(head(df_prevalence_long))

# Merge
df <- left_join(
  x = df_dalys_long,
  y = df_prevalence_long,
  by = c("age_group_id", "cause_id", "location_id", "sex_id", "year_id", "draw")
)

# View(head(df))

############### LEFT OFF HERE, NEED TO COLLAPSE ON AGE GROUPS ############### 

##----------------------------------------------------------------
## 6. Collapse on the 0 - <1 and 1 - <5 age groups 
##----------------------------------------------------------------
# Collapse on the 0 - <1 and 1 - <5 age groups 
# Age groups in the DEX / USHD data
# c("0 - <1", "1 - <5", "10 - <15", "15 - <20", "20 - <25", "25 - <30", 
#   "30 - <35", "35 - <40", "40 - <45", "45 - <50", "5 - <10", "50 - <55", 
#   "55 - <60", "60 - <65", "65 - <70", "70 - <75", "75 - <80", "80 - <85", 
#   "85+")

# Label the 0 - <1, 1 - <5, & 85+ age groups 
df_age_collapse <- df_m %>%
  mutate(age_name_group = case_when(
    age_group_id %in% c(2, 3, 388, 389) ~ "0 - <1",
    age_group_id %in% c(238, 34) ~ "1 - <5",
    age_group_id %in% c(31, 32, 235) ~ "85+"
  ))

df_age_non_collapse <- df_age_collapse %>%
  filter(is.na(age_name_group))

# Collapse on the 0 - <1, 1 - <5, & 85+ age groups 
df_age_collapse <- df_age_collapse %>%
  filter(!is.na(age_name_group)) %>%
  group_by(age_name_group, cause_id, location_id, metric_id, sex_id, 
           year_id, acause, cause_name, location_name) %>%
  summarize(
    prevalence = sum(prevalence),
    mortality = sum(mortality, na.rm = TRUE),
    population = sum(population),
    DALYs = sum(DALYs)
  )

df_age_collapse <- df_age_collapse %>%
  rename(
    "age_group_name" = "age_name_group"
  )

# Rowbind back the df age collapse data
df_final <- rbind(df_age_collapse, df_age_non_collapse)

df_final <- df_final %>%
  select(!c("age_name_group", "age_group_id"))


##----------------------------------------------------------------
## 7. Save data
##----------------------------------------------------------------
# Prevalence, Mortality, & Population data
fn_main <- file.path(dir_out, "prev_mort_pop_data.parquet")

write_parquet(df_final, fn_main)



















