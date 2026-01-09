library(data.table)
library(argparse)
parser <- ArgumentParser()
parser$add_argument("--input_file", help="Input data file with values of population, case counts, and expenditures for each cause, age group, sex, year, and draw.",
                    type="character")
parser$add_argument("--output_file", help="Where to save results.",
                    type="character")

args <- parser$parse_args()
list2env(args, .GlobalEnv)

source("./decomp_function.R")
year_ids <- c(1996, 2016)
start_year <- 1996
end_year <- 2016
factor_names <- c("population", "age_sex_pop_proportion", "case_rate", "expenditure_per_case")

d <- fread(input_file)
d[, total_population := sum(population), by=.(draw, cause_id, year_id)]
## Convert cases to per-capita rate
d[, case_rate := case_count / population]
## convert expenditures to M$/case
d[, expenditure_per_case := expenditure / case_count]
d[, age_sex_pop_proportion := population / total_population]
d[, c("case_count", "population") := NULL]
setnames(d, "total_population", "population")

# d should be a data.table with columns of population, age_sex_pop_proportion, case_rate, and dalys_per_case.
# It should have one row per draw, cause_id, age_group, sex, and year. Years should include 1996 and 2016.
d <- dcast(d, draw + cause_id + cause_name + amenable + age_group_id + sex_id ~ year_id,
           value.var = c(factor_names, "expenditure"))
d <- decompose(d, factor_names=factor_names, start_year=start_year, end_year=end_year)
d[, "delta_expenditure" := get(paste0("expenditure_", end_year)) - get(paste0("expenditure_", start_year))]
d[, c(outer(factor_names, year_ids, paste, sep="_")) := NULL]
setnames(d,
         c(paste0("expenditure_", start_year), paste0("expenditure_", end_year)),
         c("expenditure_start", "expenditure_end"))
fwrite(d, output_file)