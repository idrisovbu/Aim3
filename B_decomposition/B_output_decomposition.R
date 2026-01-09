rm(list=ls())
library(data.table)
library(openxlsx)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--expend_dcomp_file", help="File where expenditure decompositions are saved.",
                    type="character")
parser$add_argument("--daly_dcomp_file", help="File where results of decomposing DALYs with cases are saved",
                    type="character")
parser$add_argument("--unif_res_file", help="Directory where unified results (both DALYs and expenditures) should be saved",
                    type="character")
parser$add_argument("--output_file", help="File where results are saved", type="character")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

## Define functions for getting the 2.5% and 97.5% quantiles of draws.
lower <- function(x){
  quantile(x, probs=0.025)
}
upper <- function(x){
  quantile(x, probs=0.975)
}
lower_iqr <- function(x){
  quantile(x, probs=0.25)
}
upper_iqr <- function(x){
  quantile(x, probs=0.75)
}

gdp_per_capita <- 57928.1


#################
## Read in the expenditure decompositions
expends <- fread(expend_dcomp_file)
expends[, "expenditure_end" := NULL]

## Keep track of the measure variables
exp_meas_cols <- grep("delta|effect|start", names(expends), value=TRUE)

## Convert from millions of $s to $s
expends[, (exp_meas_cols) := lapply(exp_meas_cols, function(column) 1e6*get(column))]

## Add on rows for the all-cause_aggregate
expends <- rbind(expends,
                 expends[!cause_id %in% c(366, 380, 508), lapply(.SD, sum),
                         by=setdiff(names(expends), c(exp_meas_cols, "cause_id", "cause_name")),
                         .SDcols=exp_meas_cols],
                 fill=TRUE, use.names=TRUE)
expends[is.na(cause_id), c("cause_id", "cause_name", "amenable") := .(294L, "All Causes", 2L)]

## Rename effect & percent columns to avoid ambiguity of "population_effect" column from the two data.tables
exp_meas_cols <- grep("_effect", names(expends), value=TRUE)
new_exp_meas_cols <- paste0("exp_", exp_meas_cols)
setnames(expends, exp_meas_cols, new_exp_meas_cols)
#################

dalys <- fread(daly_dcomp_file)
dalys[, dalys_end := NULL]

dalys_meas_cols <- c("delta_dalys", "dalys_start", grep("effect", names(dalys), value=TRUE))

# Add rows for the all-cause aggregate
dalys <- rbind(dalys,
               dalys[!cause_id %in% c(366, 380, 508), lapply(.SD, sum),
                     by=setdiff(names(dalys), c(dalys_meas_cols, "cause_id")), .SDcols=dalys_meas_cols],
               fill=TRUE, use.names=TRUE)
dalys[is.na(cause_id), cause_id := 294L]

daly_meas_cols <- grep("_effect", names(dalys), value=TRUE)
new_daly_meas_cols <- paste0("daly_", daly_meas_cols)
setnames(dalys, daly_meas_cols, new_daly_meas_cols)

effect_cols <- c(new_daly_meas_cols, new_exp_meas_cols)
id_cols <- c("draw", "cause_id", "age_group_id", "sex_id")

## Merge the expenditure and DALY decomposition results together, ditching DALYs for any cause-age-sex combinations where
## no expenditure estimates exist and ditching expenditures wherever DALYs don't exist.
d <- merge(expends, dalys, by=id_cols)

id_cols <- c(id_cols, "cause_name", "amenable")
meas_cols <- setdiff(names(d), id_cols)
id_cols <- setdiff(id_cols, c("age_group_id", "sex_id"))

if(!is.null(unif_res_file)){
  if(!file.exists(unif_res_file)){
    fwrite(d, unif_res_file)
  } else{
    print("Unified results file already exists.")
  }
}

## Aggregate over ages and sexes
d <- d[, lapply(.SD, sum), by=id_cols, .SDcols=meas_cols]

## Calculate percent effects
daly_effect_cols <- grep("daly", effect_cols, value=TRUE)
exp_effect_cols <- grep("exp", effect_cols, value=TRUE)

daly_percent_cols <- gsub("effect", "percent", daly_effect_cols)
daly_percent_cols <- c("delta_dalys_percent", daly_percent_cols)
exp_percent_cols <- gsub("effect", "percent", exp_effect_cols)
exp_percent_cols <- c("delta_expenditure_percent", exp_percent_cols)

d[, (daly_percent_cols) := lapply(c("delta_dalys", daly_effect_cols), function(v) 100 * get(v) / dalys_start)]
d[, (exp_percent_cols) := lapply(c("delta_expenditure", exp_effect_cols), function(v) 100 * get(v) / expenditure_start)]


meas_cols <- c("dalys_start", "delta_dalys", "expenditure_start", "delta_expenditure",
               daly_effect_cols, exp_effect_cols, daly_percent_cols, exp_percent_cols)

## DEX draws are independent of GBD draws. We can aggregate over draws for the effects exclusively on the expenditure side or
## exclusively on the DALY side without having to deal with all 1000^2 gbd_draw - dex_draw combinations, but for the ratio,
## we need to deal with all 10^6 draw-draws to calculate quantiles.
## We can't just merge the two data.tables together because 1000 draws * 1000 dex_draws * 143 causes =
## = 143 million rows - way more memory than any step in this process.
## Instead filter both data.tables by one cause, merge the filtered data.tables, calculate separately, and rbind
##
## This step is also where I handle the COL calculation, since it also involves both DEX and GBD draws.
daly_rate_effect_col_name <- "daly_dalys_per_case_effect"

cartesian_d <- rbindlist(lapply(d[, unique(cause_id)], function(cid){
  x <- merge(d[cause_id == cid,
               .(draw, cause_id, daly_dalys_per_case_effect)],
             d[cause_id == cid,
               .(dex_draw=draw, cause_id, expenditure_start,
                 exp_case_rate_effect, exp_expenditure_per_case_effect)],
             by="cause_id", allow.cartesian=TRUE)
  
  x[, c("draw", "dex_draw") := NULL]
  x[, exp_rate_effect_over_daly_rate_effect := exp_expenditure_per_case_effect / daly_dalys_per_case_effect]
  
  x[exp_expenditure_per_case_effect < 0 & daly_dalys_per_case_effect > 0,
    exp_rate_effect_over_daly_rate_effect := NA] # assign SW ratios to NA so that they are dropped.
  x[exp_expenditure_per_case_effect > 0 & daly_dalys_per_case_effect > 0,
    exp_rate_effect_over_daly_rate_effect := -Inf] # Dominated should be larger than all ICERs. Flip sign because denominator is DALYs, not DALYs averted.
  ## Note that all negative ratios (positive values of exp_rate_effect_over_daly_rate_effect) at this point must be Cost-Saving.
  ## They needn't be reordered - already in correct position relative to ICER ratios, if not to each other.
  
  qtl <- x[, rbind(.SD[exp_rate_effect_over_daly_rate_effect %in% quantile(exp_rate_effect_over_daly_rate_effect, 0.25, type=1L, na.rm=TRUE),
                       .(exp_rate_effect_over_daly_rate_effect, exp_expenditure_per_case_effect, daly_dalys_per_case_effect)][1],
                   .SD[exp_rate_effect_over_daly_rate_effect %in% quantile(exp_rate_effect_over_daly_rate_effect, 0.5, type=1L, na.rm=TRUE),
                       .(exp_rate_effect_over_daly_rate_effect, exp_expenditure_per_case_effect, daly_dalys_per_case_effect)][1],
                   .SD[exp_rate_effect_over_daly_rate_effect %in% quantile(exp_rate_effect_over_daly_rate_effect, 0.75, type=1L, na.rm=TRUE),
                       .(exp_rate_effect_over_daly_rate_effect, exp_expenditure_per_case_effect, daly_dalys_per_case_effect)][1]),
           by=cause_id]
  setorderv(qtl, c("cause_id", "exp_rate_effect_over_daly_rate_effect"))
  
  qtl[, q := c("lower", "median", "upper"), by=cause_id]
  
  setnames(qtl,
           c("exp_expenditure_per_case_effect", "daly_dalys_per_case_effect"),
           c("exp_expenditure_per_case_effect_at", "daly_dalys_per_case_effect_at"))
  qtl <- dcast(qtl, cause_id ~ q,
               value.var=c("exp_rate_effect_over_daly_rate_effect",
                           "exp_expenditure_per_case_effect_at",
                           "daly_dalys_per_case_effect_at"))
  
  ## CoL calculation
  x[, cv_3 := -3 * gdp_per_capita * daly_dalys_per_case_effect]
  x[, cv_10 := -10 * gdp_per_capita * daly_dalys_per_case_effect]
  x[, cv_100k := -1e5 * daly_dalys_per_case_effect]
  x[, cv_150k := -15e4 * daly_dalys_per_case_effect]
  x[, s1 := expenditure_start + exp_case_rate_effect + exp_expenditure_per_case_effect]
  x[, col_daly_adjusted_3 := (s1 - cv_3) / expenditure_start]
  x[, col_daly_adjusted_10 := (s1 - cv_10) / expenditure_start]
  x[, col_daly_adjusted_100k := (s1 - cv_100k) / expenditure_start]
  x[, col_daly_adjusted_150k := (s1 - cv_150k) / expenditure_start]
  x[, col := s1 / expenditure_start]
  x[, res_price_3 := s1 / (expenditure_start + cv_3)]
  x[, res_price_10 := s1 / (expenditure_start + cv_10)]
  x[, res_price_100k := s1 / (expenditure_start + cv_100k)]
  x[, res_price_150k := s1 / (expenditure_start + cv_150k)]
  
  x[, c("daly_dalys_per_case_effect", "exp_expenditure_per_case_effect", "expenditure_start") := NULL]
  
  x <- dcast(x, cause_id ~ .,
             value.var=c("col_daly_adjusted_3", "col_daly_adjusted_10", "col_daly_adjusted_100k", "col_daly_adjusted_150k",
                         "col", "res_price_3", "res_price_10", "res_price_100k", "res_price_150k"),
             fun.aggregate=list(median, lower_iqr, upper_iqr))
  x <- merge(x, qtl, by="cause_id", all=TRUE)
  return(x)
}))

setnames(cartesian_d,
         c("exp_rate_effect_over_daly_rate_effect_median",
           "exp_rate_effect_over_daly_rate_effect_lower",
           "exp_rate_effect_over_daly_rate_effect_upper",
           "exp_expenditure_per_case_effect_at_median",
           "exp_expenditure_per_case_effect_at_lower",
           "exp_expenditure_per_case_effect_at_upper",
           paste0("daly_dalys_per_case_effect", "_at_", c("median", "lower", "upper")),
           "col_lower_iqr",
           "col_upper_iqr",
           "res_price_3_lower_iqr",
           "res_price_3_upper_iqr",
           "res_price_10_lower_iqr",
           "res_price_10_upper_iqr",
           "res_price_100k_lower_iqr",
           "res_price_100k_upper_iqr",
           "res_price_150k_lower_iqr",
           "res_price_150k_upper_iqr"),
         c("exp_rate_effect_over_daly_rate_effect",
           "lower_exp_rate_effect_over_daly_rate_effect",
           "upper_exp_rate_effect_over_daly_rate_effect",
           "expenditure_rate_effect_at_median",
           "expenditure_rate_effect_at_lower",
           "expenditure_rate_effect_at_upper",
           "daly_rate_effect_at_median",
           "daly_rate_effect_at_lower",
           "daly_rate_effect_at_upper",
           "col_lower",
           "col_upper",
           "res_price_3_lower",
           "res_price_3_upper",
           "res_price_10_lower",
           "res_price_10_upper",
           "res_price_100k_lower",
           "res_price_100k_upper",
           "res_price_150k_lower",
           "res_price_150k_upper"))

## add columns for the sign of the expenditure and daly rate effects
d[, `:=`(expend_effect_positive=as.numeric(exp_expenditure_per_case_effect > 0),
         daly_effect_positive=as.numeric(get(daly_rate_effect_col_name) > 0))]

## Add column for 96 expenditure + case_rate_effect + expenditure_rate_effect
d[, exp_start_plus_rate_and_case_effects := expenditure_start + exp_case_rate_effect + exp_expenditure_per_case_effect]
meas_cols <- c(meas_cols, "exp_start_plus_rate_and_case_effects", "daly_effect_positive", "expend_effect_positive")

d <- dcast(melt(d,
                id.vars=id_cols,
                measure_vars=meas_cols),
           cause_id + cause_name + amenable ~ variable,
           fun.aggregate=list(mean, lower, upper), value.var="value")

## Only care about the mean of the means of positivity indicators, since they are binary variables that we want the proportions of 1s
d[, c(outer(c("value_upper_", "value_lower_"), c("expend_effect_positive", "daly_effect_positive"), paste0)) := NULL]

## 2x2 contingency table of quadrant counts of draw-draw pairs obeys independence, since DEX and GBD are independent.
d[, `:=`(ICER_proportion=value_mean_expend_effect_positive * (1-value_mean_daly_effect_positive),
         SW_proportion=(1-value_mean_expend_effect_positive) * value_mean_daly_effect_positive,
         Dominated_proportion=value_mean_expend_effect_positive * value_mean_daly_effect_positive,
         Cost_saving_proportion=(1-value_mean_expend_effect_positive) * (1-value_mean_daly_effect_positive))]
d[, c("value_mean_expend_effect_positive", "value_mean_daly_effect_positive") := NULL]
id_cols <- setdiff(id_cols, "draw")

setnames(d, names(d), gsub("value_|value_mean_", "", names(d)))

## Merge the data.table of estimates & IQRs of the ratio of effects to the data.table of the point & UIs of effects & percents
d <- merge(d, cartesian_d, by="cause_id")

## Convert cause_ids > 1000 back to the GBD terminology and sort by cause_id
d[cause_id > 1000, cause_id := as.integer(cause_id / 10)]
setorder(d, cause_id)

## Clean up column names
newnames <- names(d)
## Make the format of the occurrence of "DALY" consistent
newnames <- gsub("daly_daly", "daly", newnames)
newnames <- gsub("daly_", "dalys_", newnames)
newnames <- gsub("dalys_", "dalys ", newnames)
newnames <- gsub("dalys", "DALYs", newnames)
newnames <- gsub("DALYs rate", "DALY rate", newnames)
newnames <- gsub("DALYs per_capita", "DALY rate", newnames)

## Make the format of "exp"/"expenditure" consistent
newnames <- gsub("exp_exp", "exp", newnames)
newnames <- gsub("exp_", "expenditure_", newnames)
newnames <- gsub("expenditure_", "expenditure ", newnames)
newnames <- gsub("expenditure", "Expenditure", newnames)

## Capitalization and spaces
newnames <- gsub("delta_", "Delta ", newnames)
newnames <- gsub("case_rate", "Case Rate", newnames)
newnames <- gsub("population", "Population", newnames)
newnames <- gsub("age_sex_pop_proportion", "Age Structure", newnames)
newnames <- gsub("_at_median", " at median", newnames)
newnames <- gsub("_at_upper", " at upper", newnames)
newnames <- gsub("_at_lower", " at lower", newnames)
newnames <- gsub("per_case", "Per Case", newnames)
newnames <- gsub("_effect", " effect", newnames)
newnames <- gsub("_percent", " percent", newnames)
newnames <- gsub("_over_", " over ", newnames)
newnames <- gsub("col_", "COL ", newnames)
newnames <- gsub("adjusted_", "adjusted ", newnames)
newnames <- gsub("start_plus_rate_and_case effects", "start plus case rate and Expenditure per case effects", newnames)
newnames <- gsub("start", "1996", newnames)

newnames <- gsub("res_price_", "Reservation Price ", newnames)

## "Mean", "Lower", and "Upper" occur at the beginning of the string rather than the end.
lower_col_indicator <- grepl("lower_", newnames)
upper_col_indicator <- grepl("upper_", newnames)
mean_col_indicator <- grepl("mean_", newnames)
newnames[lower_col_indicator] <- paste0(gsub("lower_", "", newnames[lower_col_indicator]), " lower")
newnames[upper_col_indicator] <- paste0(gsub("upper_", "", newnames[upper_col_indicator]), " upper")
newnames[mean_col_indicator] <- paste0(gsub("mean_", "", newnames[mean_col_indicator]), " mean")

newnames <- gsub("Cost_saving", "Cost saving", newnames)
newnames <- gsub("_proportion", " proportion", newnames)

newnames <- gsub("10_", "10 ", newnames)
newnames <- gsub("3_", "3 ", newnames)
newnames <- gsub("0k_", "0k ", newnames)

setnames(d, names(d), newnames)

# Organize column order
column_order <- c("cause_id", "cause_name", "amenable")
exp_effects_order <- paste("Expenditure", c("Population", "Age Structure", "Case Rate", "Per Case"), "effect")
dalys_effects_order <- paste("DALYs", c("Population", "Age Structure", "Case Rate", "Per Case"), "effect")

cols_with_pcts <- c("Expenditure 1996", "Delta Expenditure", exp_effects_order,
                    "DALYs 1996", "Delta DALYs", dalys_effects_order,
                    "Delta Expenditure percent", gsub("effect", "percent", exp_effects_order),
                    "Delta DALYs percent",  gsub("effect", "percent", dalys_effects_order))
cols_with_lower_upper <- c(cols_with_pcts, "Expenditure rate effect over DALY rate effect")
cols_with_lower_upper <- c(t(outer(cols_with_lower_upper, c("", " lower", " upper"), paste0)))
column_order <- c(column_order, cols_with_lower_upper)

column_order <- c(column_order,
                  c(t(outer(c("Expenditure rate effect at", "DALY rate effect at"), c("median", "lower", "upper"), paste))),
                  paste0("Expenditure 1996 plus case rate and Expenditure per case effects", c("", " lower", " upper")))
col_cols <- paste("COL DALYs adjusted", c("3", "10", "100k", "150k"))
col_cols <- c(t(outer(col_cols, c("median", "iqr lower", "iqr upper"), paste)))
res_cols <- paste("Reservation Price", c("3", "10", "100k", "150k"))
res_cols <- c(t(outer(res_cols, c("median", "lower", "upper"), paste)))
quadrant_prop_cols <- paste(c("ICER", "SW", "Dominated", "Cost saving"), "proportion")
column_order <- c(column_order, col_cols, "COL median", "COL lower", "COL upper", res_cols, quadrant_prop_cols)
setcolorder(d, column_order)

if(!file.exists(output_file)){
  ## Write the big data.table with all columns
  fwrite(d, output_file)
}