# Outcome Adjusted Health Care Price Index
## Purpose
The purpose for this repository is to make the analytic code for this project available as part of [Guidelines for Accurate and Transparent Health Estimates Reporting (GATHER)](http://gather-statement.org/) compliance.

## Organization
The file decomposition_function.R contains a function, written in Rcpp, that decomposes the differences between the product of an arbitrary number of factors, according to the equations in Das Gupta (1991). It also contains a wrapper function that makes it easier to interface with the Rcpp function from a data.table in R. This is used by the files daly_decomp_with_cases.R and expenditure_decomp_with_cases.R.

Files in this repo should be run in the following order:
1. daly_decomp_with_cases.R

Takes as arguments a file path for input data and a file path for output data. Reads in the input data of DALYs, cases, and population for each cause, age-group, sex, and draw, and performs a Das Gupta decomposition of DALYs into effects of changes in population, age-structure, case rates, and DALYs per case.

2. expenditure_decomp_with_cases.R

Takes as arguments a file path for input data and a file path for output data. Reads in the input data of expenditures, cases, and population for each cause, age-group, sex, and draw, and performs a Das Gupta decomposition of expenditures into effects of changes in population, age-structure, case rates, and expenditures per case.

3. output_decomposition.R

Takes as arguments the decomposition results files that are outputs of the two decompositions, a file for output for the unified results file, and a file for output for the final summary table. Takes the output of daly_decomp_with_cases.R and expenditure_decomp_with_cases.R and produces a unified decomposition results file. Also calculates ratios of expenditure to DALYs effects, and the cost of living and reservation price indices, and produces the summary table with most of the columns in most of the tables in the manuscript.

## Inputs
Inputs required for the code to run are:
DALY decomposition input file. Must contain columns for draw, cause_id, age_group_id, sex_id, year_id, population, case_count, burden. All columns except population, case_count, and burden should be integers.
Expenditure decomposition input file. Must contain columns for cause_id, cause_name, amenable, draw, age_group_id, sex_id, year_id, population, case_count, expenditure. All columns except cause_name, population, case_count, and burden should be integers. The column cause_name should be a string, and the rest of the columns should be integers.