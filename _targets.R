# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
source(here::here('scripts/functions.R'))

options(survey.lonely.psu="adjust")

# TODO:
#' Age in years
#' <10
#' 10-19
#' 20-39
#' 40-59
#' 60-64
#' 65-74
#' 75+
#'   Unknown
#'
#' Sex
#' Female
#' Male
#' Unknown
#'
#' Insurance
#' Cash
#' Medicaid
#' 3rd party
#' Medicare Part D
#' Unknown
#'
#'
#' Copay range (in $)
#' 0
#' >0 – 10
#' >10 – 20
#' >20 - 30
#' >30 – 75
#' >75
#' Unknown

# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble",
               'MEPS',
               'survey',
               'srvyr',
               'patchwork',
               'gt',
               'JakeR',
               'surveytable',
               'magrittr',
               'assertthat',
               'ipumsr',
               'duckdb',
               'dtplyr',
               'tidyverse'), # packages that your targets need to run.
  #
  # For distributed computing in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller with 2 workers which will run as local R processes:
  #
  controller = crew::crew_controller_local(workers = 2)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package. The following
  # example is a controller for Sun Grid Engine (SGE).
  #
  #   controller = crew.cluster::crew_controller_sge(
  #     workers = 50,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.0".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
  # debug = 'aa_edu_rx_counts_2008',
  # cue = tar_cue(mode = 'never')
)

# tar_make_clustermq() is an older (pre-{crew}) way to do distributed computing
# in {targets}, and its configuration for your machine is below.
options(clustermq.scheduler = "multiprocess")

# tar_make_future() is an older (pre-{crew}) way to do distributed computing
# in {targets}, and its configuration for your machine is below.
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# source("other_functions.R") # Source other scripts as needed.
surveytable::set_count_int()



# Setup static branching --------------------------------------------------

data_years <- tibble::tibble(
  # 2000 doesn't seem too have the Multum codes, so not including
  years = 2003:2021
)

mapped_targets <- tar_map(
  values = data_years,

# Get MEPS data -----------------------------------------------------------
  tar_target(raw_meps_data,
             importMEPS(years,
                        multum_class_code = multum_class_code,
                        diabetes_drugs = diabetes_drugs,
                        manual_coded_drug_names = IPUMS_manual_coded_drug_names),
             format = 'parquet',
             deployment = 'main'),

  tar_target(
    raw_ipums_data,
    importIPUMSrx(
      ipums_year = years,
      multum_class_code = multum_class_code,
      diabetes_drugs = diabetes_drugs,
      manual_coded_drug_names = IPUMS_manual_coded_drug_names
    ),
    format = 'parquet',
    deployment = 'main'
  ),

  tar_target(recode_insulin_meps_data,
             recodeInsulin(raw_ipums_data, IPUMS_manual_coded_drug_names),
             format = 'parquet'),
  # Prescription level
  tar_target(meps_data,
             createIPUMSFactors(recode_insulin_meps_data) |>
               calculatePriceAdjustment(data = _, cpi = cpi, year = years),
             format = 'parquet'),
  tar_target(split_meps_data,
             group_split(meps_data, insulin_indicator)),

  # Aggregate to person level
  tar_target(person_level_meps,
             personLevelMEPS(meps_data),
             format = 'parquet'),

  # person level expenditures
  tar_target(expenditures_person_level,
             personLevelExpenditures(meps_data)),

  # person level expenditures
  tar_target(expenditures_person_drug_level,
             personDrugLevelExpenditures(meps_data)),

  # * RX level counts -------------------------------------------------------
  tar_target(srvy_design,
             map(
               split_meps_data,
               ~ .x |>
                 filter(third_level_category_name != 'None') |>
                 createSurveyDesign(dat = _, diabetes_drugs)
             )),
  tar_target(full_srvy_design,
             createSurveyDesign(meps_data, diabetes_drugs)),

  tar_target(expenditure_person_level_srvy,
             createSurveyDesign(expenditures_person_level, diabetes_drugs)),

  tar_target(expenditure_person_drug_level_srvy,
              createSurveyDesign(expenditures_person_drug_level, diabetes_drugs)),

# ** AA Rx counts ------------------------------------------------------

tar_target(aa_rx_counts_full,
           {
             #temp <- list()
             #for (i in 1:length(srvy_design)) {
             survey_object <- svystandardize(full_srvy_design,
                                             ~ AGE,
                                             ~ 1,
                                             population = standard_age_proportions_18,
                                             excluding.missing = ~ AGE)
             assign("survey_object", survey_object, envir = .GlobalEnv)
             set_survey('survey_object')
             set_output(max_levels = 2e5)
             surveytable::set_count_int()
             temp<- tab('third_level_category_name', drop_na = FALSE)
             temp$year <- years
             temp <- temp %>%
               filter(Level != '<N/A>') %>%
               mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                      LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                      UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
               mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ',')))

             # }
             # bind_rows(temp) %>%
             #   mutate(
             #     insulin_indicator = case_when(
             #       Level %in% c(
             #         'Basal (Analog)',
             #         'Basal (Human)',
             #         'Bolus (Analog)',
             #         'Bolus (Human)',
             #         'Insulin (Unknown)',
             #         'Pre-mixed (Analog)',
             #         'Pre-mixed (Human)'
             #       ) ~ 'Insulin',
             #       TRUE ~ 'Not Insulin'
             #     )
             #   )
           }
),
#
  tar_target(aa_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
                 survey_object <- svystandardize(srvy_design[[i]],
                                                 ~ AGE,
                                                 ~ 1,
                                                 population = standard_age_proportions_18,
                                                 excluding.missing = ~ AGE)
                 assign("survey_object", survey_object, envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <- tab('third_level_category_name', drop_na = FALSE)
                 temp[[i]]$year <- years
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>') %>%
                   mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                          LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                          UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                   mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ',')))

               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }
             ),

  tar_target(aa_sex_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
               survey_object <- svystandardize(srvy_design[[i]],
                                               ~AGE,
                                               ~SEX,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'SEX')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'SEX'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(SEX) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )

             }),

  tar_target(aa_copay_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
                 survey_object <- svystandardize(srvy_design[[i]],
                                                 ~AGE,
                                                 ~Copay,
                                                 population = standard_age_proportions_18,
                                                 excluding.missing = ~AGE)
                 assign("survey_object", survey_object, envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <- tab_cross('third_level_category_name', 'Copay')
                 temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'Copay'))
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>') %>%
                   group_by(Copay) %>%
                   mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                          SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                          LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                          UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                   mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                   ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )

             }),

  tar_target(aa_povcat_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
                 survey_object <- svystandardize(srvy_design[[i]],
                                                 ~AGE,
                                                 ~SEX,
                                                 population = standard_age_proportions_18,
                                                 excluding.missing = ~AGE)
                 assign("survey_object", survey_object, envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <- tab_cross('third_level_category_name', 'POVCAT')
                 temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'POVCAT'))
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>') %>%
                   group_by(POVCAT) %>%
                   mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                          SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                          LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                          UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                   mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                   ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )

             }),

  tar_target(age_rx_counts,
             { # Weirdly 75+ doesn't have enough RX to tabulate in 2008 so have to do a bit differently

               temp <- list()
               for (i in 1:length(srvy_design)) {
               survey_object <- svystandardize(srvy_design[[i]],
                                               ~AGE,
                                               ~1,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'AGE')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'AGE'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(AGE) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  tar_target(aa_race_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
               survey_object <- svystandardize(srvy_design[[i]],
                                               ~AGE,
                                               ~RACETHNX,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'RACETHNX')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'RACETHNX'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(RACETHNX) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  tar_target(aa_edu_rx_counts,
             {

             temp <- list()
             srvy_design1 <- map(
                split_meps_data,
                ~ .x |>
                  filter(third_level_category_name != 'None') |>
                  filter(!is.na(HIDEG)) |>
                  createSurveyDesign(dat = _, diabetes_drugs)
              )
              for (i in 1:length(srvy_design)) {
               survey_object <- svystandardize(srvy_design1[[i]],
                                               ~AGE,
                                               ~HIDEG,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'HIDEG')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'HIDEG'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(HIDEG) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
              }
              bind_rows(temp) %>%
                mutate(
                  insulin_indicator = case_when(
                    Level %in% c(
                      'Basal',
                      'Bolus',
                      'Analog',
                      'Human',
                      'Pre-mixed',
                      'Human and Analog'
                    ) ~ 'Insulin',
                    TRUE ~ 'Not Insulin'
                  )
                )
              }),

# ** Crude Rx counts ------------------------------------------------------
  tar_target(crude_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
               assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab('third_level_category_name', drop_na = TRUE)
               temp[[i]]$year <- years
               temp[[i]] <- temp[[i]] %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ',')))
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  tar_target(crude_sex_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
               assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'SEX')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'SEX'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(SEX) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )

             }),
# Suppress rules
# R - suppress all estimates
# Cx - suppress count and rate
# Px suppress percent
#
  tar_target(crude_race_rx_counts,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
               assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'RACETHNX')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'RACETHNX'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(RACETHNX) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }, deployment = 'main'),
  tar_target(crude_edu_rx_counts,
             {
               temp <- list()
               for ( i in 1:length(srvy_design)) {
               assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp[[i]] <- tab_cross('third_level_category_name', 'HIDEG')
               temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'HIDEG'))
               temp[[i]] <- temp[[i]] %>%
                 filter(Level != '<N/A>') %>%
                 group_by(HIDEG) %>%
                 mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                        SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                        LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                        UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                 mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                 ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  tar_target(crude_copay_rx_counts,
             {
               temp <- list()
               for ( i in 1:length(srvy_design)) {
                 assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <- tab_cross('third_level_category_name', 'Copay')
                 temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'Copay'))
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>') %>%
                   group_by(Copay) %>%
                   mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                          SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                          LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                          UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                   mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                   ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  tar_target(crude_povcat_rx_counts,
             {
               temp <- list()
               for ( i in 1:length(srvy_design)) {
                 assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <- tab_cross('third_level_category_name', 'POVCAT')
                 temp[[i]] <- temp[[i]] %>% separate_wider_delim(Level, ' : ', names = c('Level', 'POVCAT'))
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>') %>%
                   group_by(POVCAT) %>%
                   mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                          SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                          LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                          UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                   mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                   ungroup()
               }
               bind_rows(temp) %>%
                 mutate(
                   insulin_indicator = case_when(
                     Level %in% c(
                       'Basal',
                       'Bolus',
                       'Analog',
                       'Human',
                       'Pre-mixed',
                       'Human and Analog'
                     ) ~ 'Insulin',
                     TRUE ~ 'Not Insulin'
                   )
                 )
             }),

  # * Person level proportions ----------------------------------------------

  tar_target(person_srvy_design,
             createSurveyDesign(person_level_meps, diabetes_drugs)),

  # ** Crude person props ------------------------------------------------------


  tar_target(crude_person_prop,
             {
               assign("survey_object", person_srvy_design, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab(diabetes_drugs[i])
                   temp$year <- years
                   temp <- temp %>%
                     mutate(drug = diabetes_drugs[i],
                            across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level != 0) %>%
                     select(drug, everything()) %>%
                     select(-Level)
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
             }),
  tar_target(crude_num_diabetes,
             {
               assign("survey_object", person_srvy_design, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp <- tab('DIABDX')
               temp$year <- years
               temp
             }),




  tar_target(crude_sex_prop,
             {
               assign("survey_object", person_srvy_design, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'SEX')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'SEX'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(SEX) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),

  tar_target(crude_race_prop,
             {
               assign("survey_object", person_srvy_design, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'RACETHNX')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'RACETHNX'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(RACETHNX) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),
  tar_target(crude_edu_prop,
             {
               assign("survey_object", person_srvy_design, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'HIDEG')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'HIDEG'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(HIDEG) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),

# ** AA person level ------------------------------------------------------


  tar_target(aa_person_prop,
             {
               survey_object <- svystandardize(person_srvy_design,
                                               ~AGE,
                                               ~1,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)

               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()

               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   print(i)
                   temp <- tab(diabetes_drugs[i])
                   temp$year <- years
                   temp <- temp %>%
                     mutate(drug = diabetes_drugs[i],
                            across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     select(-Level, drug, everything())
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
             },
             deployment = 'main'),

tar_target(aa_num_diabetes,
           {survey_object <- svystandardize(person_srvy_design,
                                            ~AGE,
                                            ~1,
                                            population = standard_age_proportions_18,
                                            excluding.missing = ~AGE)
             assign("survey_object", survey_object, envir = .GlobalEnv)
             set_survey('survey_object')
             set_output(max_levels = 2e5)
             surveytable::set_count_int()
             temp <- tab('DIABDX')
             temp$year <- years
             temp
           }),


  tar_target(aa_sex_prop,
             {
               survey_object <- svystandardize(person_srvy_design,
                                               ~AGE,
                                               ~SEX,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'SEX')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'SEX'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(SEX) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),
  tar_target(age_prop,
             {
               survey_object <- svystandardize(person_srvy_design,
                                               ~AGE,
                                               ~1,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'AGE')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'AGE'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(AGE) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),
  tar_target(aa_race_prop,
             {
               survey_object <- svystandardize(person_srvy_design,
                                               ~AGE,
                                               ~RACETHNX,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'RACETHNX')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'RACETHNX'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(RACETHNX) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),
  tar_target(aa_edu_prop,
             { person_srvy_design <- createSurveyDesign(filter(person_level_meps,
                                                               !is.na(HIDEG)),
                                                        diabetes_drugs)
               survey_object <- svystandardize(person_srvy_design,
                                               ~AGE,
                                               ~HIDEG,
                                               population = standard_age_proportions_18,
                                               excluding.missing = ~AGE)
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               table_list <- list()
               for (i in 1:length(diabetes_drugs)) {
                 if (diabetes_drugs[i] %in% names(person_srvy_design$variables)) {
                   temp <- tab_cross(diabetes_drugs[i], 'HIDEG')
                   temp <- temp %>% separate_wider_delim(Level, ' : ', names = c('Level', 'HIDEG'))
                   temp$year <- years
                   temp$drug <- diabetes_drugs[i]
                   temp <- temp %>%
                     filter(Level != '<N/A>') %>%
                     group_by(HIDEG) %>%
                     mutate(Percent = round((Number / sum(Number) ) * 100, digits = 1),
                            SE.1 = round((SE / sum(Number)) * 100, digits = 1),
                            LL.1 = round((LL / sum(Number)) * 100, digits = 1),
                            UL.1 = round((UL / sum(Number)) * 100, digits = 1)) %>%
                     mutate(across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
                     filter(Level == 1) %>%
                     ungroup()
                   table_list[[i]] <- temp
                 }
               }
               table_df <- bind_rows(table_list)
               table_df <- table_df %>%
                 select(year, drug, everything(), -Level)
             }),


# * Expenditures ---------------------------------------------------------
# CPI values from https://data.bls.gov/cgi-bin/surveymost?cu
# Not sure if I want overall CPI or prescription so I grabbed both

# ** RX level expenditures --------------------------------------------------
# *** AA ---------------------------------------------------------------
# For OOP cost 30 day supply this gives the average cost for 30 days
# of the medication over a year
  tar_target(aa_rx_expenditures,
             {
               temp <- list()
               for (i in 1:length(srvy_design)) {
                 survey_object <- svystandardize(srvy_design[[i]],
                                                 ~ AGE,
                                                 ~ 1,
                                                 population = standard_age_proportions_18,
                                                 excluding.missing = ~ AGE)
                 assign("survey_object", survey_object, envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <-
                   tab_subset('RXFEXPSELF', 'third_level_category_name')
                 temp[[i]]$year <- years
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>')
               }
               bind_rows(temp) %>%
                 mutate(insulin_indicator = case_when(
                   Level %in% c(
                     'Basal/Analog',
                     'Bolus/Analog',
                     'Basal/Human',
                     'Bolus/Human',
                     'Pre-mixed',
                     'Pre-mixed/Human',
                     'Pre-mixed/Human and Analog'
                   ) ~ 'Insulin',
                   TRUE ~ 'Not Insulin'
                 )) |>
                 select(-`% known`)
             }),

  tar_target(
    insurance_aa_rx_expenditures,
    { srvy_design <- meps_data |>
      group_split(COVERTYPE) |>
      map(
        ~ .x |>
          filter(third_level_category_name != 'None') |>
          createSurveyDesign(dat = _, diabetes_drugs)
      )

      temp <- list()
      for (i in 1:length(srvy_design)) {
        survey_object <- svystandardize(
          srvy_design[[i]],
          ~ AGE,
          ~ COVERTYPE,
          population = standard_age_proportions_18,
          excluding.missing = ~ AGE
        )
        assign("survey_object", survey_object, envir = .GlobalEnv)
        set_survey('survey_object')
        set_output(max_levels = 2e5)
        surveytable::set_count_int()
        temp[[i]] <-
          tab_subset('RXFEXPSELF', 'third_level_category_name')
        temp[[i]]$year <- years
        temp[[i]] <- temp[[i]] %>%
          filter(Level != '<N/A>') |>
          mutate(group = unique(srvy_design[[i]]$variables$COVERTYPE),
                 stratifier = 'Insurance')
      }
      bind_rows(temp) %>%
        mutate(insulin_indicator = case_when(
          Level %in% c(
            'Basal/Analog',
            'Bolus/Analog',
            'Basal/Human',
            'Bolus/Human',
            'Pre-mixed',
            'Pre-mixed/Human',
            'Pre-mixed/Human and Analog'
          ) ~ 'Insulin',
          TRUE ~ 'Not Insulin'
        )) |>
        select(-`% known`)
      }
  ),


# *** Crude --------------------------------------------------------------
  tar_target(crude_rx_expenditures,
             {
               if (years %in% 2001:2009) {
                 data.frame(Level = NA, Mean = NA,
                            SEM = NA, SD = NA, year = years,
                            insulin_indicator = NA)
               } else {
               temp <- list()
               for (i in 1:length(srvy_design)) {
                 assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
                 set_survey('survey_object')
                 set_output(max_levels = 2e5)
                 surveytable::set_count_int()
                 temp[[i]] <-
                   tab_subset('oop_cost_30_day_supply', 'third_level_category_name')
                 temp[[i]]$year <- years
                 temp[[i]] <- temp[[i]] %>%
                   filter(Level != '<N/A>')
               }
               bind_rows(temp) %>%
                 mutate(insulin_indicator = case_when(
                   Level %in% c(
                     'Basal/Analog',
                     'Bolus/Analog',
                     'Basal/Human',
                     'Bolus/Human',
                     'Pre-mixed',
                     'Pre-mixed/Human',
                     'Pre-mixed/Human and Analog'
                   ) ~ 'Insulin',
                   TRUE ~ 'Not Insulin'
                 )) |>
                 select(-`% known`)
               }
             }),

  tar_target(
    insurance_crude_rx_expenditures,
    {srvy_design <- meps_data |>
      group_split(COVERTYPE) |>
      map(
        ~ .x |>
          filter(third_level_category_name != 'None') |>
          createSurveyDesign(dat = _, diabetes_drugs)
      )

      temp <- list()
      for (i in 1:length(srvy_design)) {
        assign("survey_object", srvy_design[[i]], envir = .GlobalEnv)
        set_survey('survey_object')
        set_output(max_levels = 2e5)
        surveytable::set_count_int()
        temp[[i]] <-
          tab_subset('RXFEXPSELF', 'third_level_category_name')
        temp[[i]]$year <- years
        temp[[i]] <- temp[[i]] %>%
          filter(Level != '<N/A>') |>
          mutate(group = unique(srvy_design[[i]]$variables$COVERTYPE),
                 stratifier = 'Insurance')
      }
      bind_rows(temp) %>%
        mutate(insulin_indicator = case_when(
          Level %in% c(
            'Basal/Analog',
            'Bolus/Analog',
            'Basal/Human',
            'Bolus/Human',
            'Pre-mixed',
            'Pre-mixed/Human',
            'Pre-mixed/Human and Analog'
          ) ~ 'Insulin',
          TRUE ~ 'Not Insulin'
        )) |>
        select(-`% known`)
      }
  ),

# ** Person level expenditures ----------------------------------------------
# *** Crude -------------------------------------------
# Average amount paid by individuals for 30 days of all their diabetes medication
  tar_target(crude_person_expenditures,
             {
               if (years %in% 2001:2009) {
                 data.frame(Level = NA, Mean = NA,
                            SEM = NA, SD = NA, year = years,
                            insulin_indicator = NA)
               } else {
               assign("survey_object", expenditure_person_level_srvy, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp <-
                 tab('oop_cost_30_day_supply')
               temp$year <- years
               temp <- temp |>
                 select(-`% known`)
               temp
               }
               }
  ),

  tar_target(insurance_crude_person_expenditures,
             {
               assign("survey_object", expenditure_person_level_srvy, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp <-
                 tab_subset('oop_cost_30_day_supply', 'COVERTYPE')
               temp$year <- years
               temp <- temp |>
                 select(-`% known`)
               temp
             }
             ),

# *** AA ---------------------------------------

  tar_target(aa_person_expenditures,
             {
               survey_object <- svystandardize(
                 expenditure_person_level_srvy,
                 ~ AGE,
                 ~ 1,
                 population = standard_age_proportions_18,
                 excluding.missing = ~ AGE
               )
               assign("survey_object", survey_object, envir = .GlobalEnv)
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp <-
                 tab('RXFEXPSELF')
               temp$year <- years
               temp <- temp |>
                 select(-`% known`)
               temp
             }),

  tar_target(insurance_aa_person_expenditures,
             {
               assign("survey_object", expenditure_person_level_srvy, envir = .GlobalEnv)
               survey_object <- svystandardize(
                 survey_object,
                 ~ AGE,
                 ~ COVERTYPE,
                 population = standard_age_proportions_18,
                 excluding.missing = ~ AGE
               )
               set_survey('survey_object')
               set_output(max_levels = 2e5)
               surveytable::set_count_int()
               temp <-
                 tab_subset('RXFEXPSELF', 'COVERTYPE')
               temp$year <- years
               temp <- temp |>
                 select(-`% known`)
               temp
             }),





# ** Person drug level expenditures ----------------------------------------------
# IF using OOP 30 day standard - this gives the same as the crude rx level, I think
# Not quite the same, but very close
# *** Crude -------------------------------------------
  tar_target(crude_person_drug_expenditures,
            {
              median <- medianExpenditures(expenditure_person_drug_level_srvy, category = 'drugs_only')
              median$year <- years
                if (years %in% 2001:2009) {
                  data.frame(Level = NA, Mean = NA, oop_cost_30_day_supply = NA_real_,
                             SEM = NA, SD = NA, year = years,
                             insulin_indicator = NA)
                } else {
              assign("survey_object", expenditure_person_drug_level_srvy, envir = .GlobalEnv)
              set_survey('survey_object')
              set_output(max_levels = 2e5)
              surveytable::set_count_int()
              temp <-
                tab_subset('oop_cost_30_day_supply', 'third_level_category_name')
              temp$year <- years
              temp <- temp |>
                select(-`% known`) |>
                mutate(insulin_indicator = case_when(
                  Level %in% c(
                    'Basal/Analog',
                    'Bolus/Analog',
                    'Basal/Human',
                    'Bolus/Human',
                    'Pre-mixed',
                    'Pre-mixed/Human',
                    'Pre-mixed/Human and Analog'
                  ) ~ 'Insulin',
                  TRUE ~ 'Not Insulin'
                ))
              left_join(temp, median)
                }
              }
  ),

  tar_target(insurance_crude_person_drug_expenditures,
            {
              median <- medianExpenditures(expenditure_person_drug_level_srvy, category = 'drugs_insurance')
              median$year <- years
              if (years %in% 2001:2009) {
                data.frame(Level = NA, Mean = NA, oop_cost_30_day_supply = NA_real_,
                           SEM = NA, SD = NA, year = years,
                           insulin_indicator = NA)
              } else {
              assign("survey_object", expenditure_person_drug_level_srvy, envir = .GlobalEnv)
              set_survey('survey_object')
              set_output(max_levels = 2e5)
              surveytable::set_count_int()
              temp <-
                tab_subset('RXFEXPSELF', 'drug_insurance')
              temp$year <- years
              temp <- temp |>
                select(-`% known`) |>
                mutate(insulin_indicator = case_when(
                  Level %in% c(
                    'Basal/Analog',
                    'Bolus/Analog',
                    'Basal/Human',
                    'Bolus/Human',
                    'Pre-mixed',
                    'Pre-mixed/Human',
                    'Pre-mixed/Human and Analog'
                  ) ~ 'Insulin',
                  TRUE ~ 'Not Insulin'
                ))
              temp
              left_join(temp, median)
              }
             }
  ),

# *** AA ---------------------------------------

  tar_target(aa_person_drug_expenditures,
            {
              survey_object <- svystandardize(
                expenditure_person_drug_level_srvy,
                ~ AGE,
                ~ 1,
                population = standard_age_proportions_18,
                excluding.missing = ~ AGE
              )
              medianExpenditures(survey_object, category = 'drugs_only')
            }),

  tar_target(insurance_aa_person_drug_expenditures,
             {
               survey_object <- svystandardize(
                 expenditure_person_drug_level_srvy,
                 ~ AGE,
                 ~ COVERTYPE,
                 population = standard_age_proportions_18,
                 excluding.missing = ~ AGE
               )
               medianExpenditures(survey_object, category = 'drugs_insurance')
             }
           )



  )

# Combine mapped targets --------------------------------------------------
# * Combine survey data -----------------------------------------------------
combined_raw <- tar_combine(
  combined_raw,
  mapped_targets[['raw_meps_data']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+')),
  format = 'parquet'
)

combined_raw_ipums <- tar_combine(
  combined_raw_ipums,
  mapped_targets[['raw_ipums_data']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+')),
  format = 'parquet'
)

combined_data <- tar_combine(
  combined_data,
  mapped_targets[['meps_data']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+')),
  format = 'parquet'
)

combined_person <- tar_combine(
  combined_person,
  mapped_targets[['person_level_meps']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+')),
  format = 'parquet'
)

combined_diabetes <- tar_combine(
  combined_diabetes,
  mapped_targets[['aa_num_diabetes']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') |>
    mutate(year = str_extract(year, '[0-9]+')),
  format = 'parquet'
)

combined_person_drug_exp <- tar_combine(
  combined_person_drug_exp,
  mapped_targets[['expenditures_person_drug_level']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') |>
    mutate(year = str_extract(year, '[0-9]+')) |>
    ungroup(),
  format = 'parquet'
)


# ** Combined RX level counts ----------------------------------------------
crude_rx_drug_counts <- tar_combine(
  crude_rx_drug_counts,
  mapped_targets[['crude_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           age = 'Crude',
           group = '-'),
  format = 'parquet'
)

aa_rx_drug_counts <- tar_combine(
  aa_rx_drug_counts,
  mapped_targets[['aa_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           age = 'Age-Adjusted',
           group = '-'),
  format = 'parquet'
)

aa_rx_drug_counts_full <- tar_combine(
  aa_rx_drug_counts_full,
  mapped_targets[['aa_rx_counts_full']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           age = 'Age-Adjusted',
           group = '-'),
  format = 'parquet'
)

crude_edu_rx_drug_counts <- tar_combine(
  crude_edu_rx_drug_counts,
  mapped_targets[['crude_edu_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Edu',
           age = 'Crude') %>%
    rename(group = HIDEG),
  format = 'parquet'
)

aa_edu_rx_drug_counts <- tar_combine(
  aa_edu_rx_drug_counts,
  mapped_targets[['aa_edu_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Edu',
           age = 'Age-Adjusted') %>%
    rename(group = HIDEG),
  format = 'parquet'
)

crude_copay_rx_drug_counts <- tar_combine(
  crude_copay_rx_drug_counts,
  mapped_targets[['crude_copay_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'copay',
           age = 'Crude') %>%
    rename(group = Copay),
  format = 'parquet'
)

aa_copay_rx_drug_counts <- tar_combine(
  aa_copay_rx_drug_counts,
  mapped_targets[['aa_copay_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'copay',
           age = 'Age-Adjusted') %>%
    rename(group = Copay),
  format = 'parquet'
)

crude_povcat_rx_drug_counts <- tar_combine(
  crude_povcat_rx_drug_counts,
  mapped_targets[['crude_povcat_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'povcat',
           age = 'Crude') %>%
    rename(group = POVCAT),
  format = 'parquet'
)

aa_povcat_rx_drug_counts <- tar_combine(
  aa_povcat_rx_drug_counts,
  mapped_targets[['aa_povcat_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'povcat',
           age = 'Age-Adjusted') %>%
    rename(group = POVCAT),
  format = 'parquet'
)

age_rx_drug_counts <- tar_combine(
  age_rx_drug_counts,
  mapped_targets[['age_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Age',
           age = AGE) %>%
    rename(group = AGE),
  format = 'parquet'
)

crude_race_rx_drug_counts <- tar_combine(
  crude_race_rx_drug_counts,
  mapped_targets[['crude_race_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Race',
           age = 'Crude') %>%
    rename(group = RACETHNX),
  format = 'parquet'
)


aa_race_rx_drug_counts <- tar_combine(
  aa_race_rx_drug_counts,
  mapped_targets[['aa_race_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Race',
           age = 'Age-Adjusted') %>%
    rename(group = RACETHNX),
  format = 'parquet'
)

crude_sex_rx_drug_counts <- tar_combine(
  crude_sex_rx_drug_counts,
  mapped_targets[['crude_sex_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Sex',
           age = 'Crude') %>%
    rename(group = SEX),
  format = 'parquet'
)

aa_sex_rx_drug_counts <- tar_combine(
  aa_sex_rx_drug_counts,
  mapped_targets[['aa_sex_rx_counts']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Sex',
           age = 'Age-Adjusted') %>%
    rename(group = SEX),
  format = 'parquet'
)

# ** Combine person level counts ---------------------------------------------



crude_person_proportions <- tar_combine(
  crude_person_proportions,
  mapped_targets[['crude_person_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Crude'),
  format = 'parquet'
)

aa_person_proportions <- tar_combine(
  aa_person_proportions,
  mapped_targets[['aa_person_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Age-Adjusted'),
  format = 'parquet'
)

crude_edu_person_proportions <- tar_combine(
  crude_edu_person_proportions,
  mapped_targets[['crude_edu_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Edu',
           age = 'Crude') %>%
    rename(group = HIDEG),
  format = 'parquet'
)

aa_edu_person_proportions <- tar_combine(
  aa_edu_person_proportions,
  mapped_targets[['aa_edu_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Edu',
           age = 'Age-Adjusted') %>%
    rename(group = HIDEG),
  format = 'parquet'
)

age_person_proportions <- tar_combine(
  age_person_proportions,
  mapped_targets[['age_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Age',
           age = AGE) %>%
    rename(group = AGE),
  format = 'parquet'
)

crude_race_person_proportions <- tar_combine(
  crude_race_person_proportions,
  mapped_targets[['crude_race_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Race',
           age = 'Crude') %>%
    rename(group = RACETHNX),
  format = 'parquet'
)
aa_race_person_proportions <- tar_combine(
  aa_race_person_proportions,
  mapped_targets[['aa_race_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Race',
           age = 'Age-Adjusted') %>%
    rename(group = RACETHNX),
  format = 'parquet'
)

crude_sex_person_proportions <- tar_combine(
  crude_sex_person_proportions,
  mapped_targets[['crude_sex_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Sex',
           age = 'Crude') %>%
    rename(group = SEX),
  format = 'parquet'
)

aa_sex_person_proportions <- tar_combine(
  aa_sex_person_proportions,
  mapped_targets[['aa_sex_prop']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Sex',
           age = 'Age-Adjusted') %>%
    rename(group = SEX),
  format = 'parquet'
)


# ** Combined rx level expenditures -----------------------------------
crude_rx_expenditures_total <- tar_combine(
  crude_rx_expenditures_total,
  mapped_targets[['crude_rx_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           age = 'Crude',
           group = '-'),
  format = 'parquet'
)

aa_rx_exenditures_total <- tar_combine(
  aa_rx_exenditures_total,
  mapped_targets[['aa_rx_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           age = 'Age-Adjusted',
           group = '-'),
  format = 'parquet'
)

insurance_crude_rx_expenditures_total <- tar_combine(
  insurance_crude_rx_expenditures_total,
  mapped_targets[['insurance_crude_rx_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Crude'),
  format = 'parquet'
)

insurance_aa_rx_expenditures_total <- tar_combine(
  insurance_aa_rx_expenditures_total,
  mapped_targets[['insurance_aa_rx_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Age-Adjusted'),
  format = 'parquet'
)


# ** Combine person level expenditure ---------------------------------------------



crude_person_expenditures_total <- tar_combine(
  crude_person_expenditures_total,
  mapped_targets[['crude_person_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Crude'),
  format = 'parquet'
)

aa_person_expenditures_total <- tar_combine(
  aa_person_expenditures_total,
  mapped_targets[['aa_person_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Age-Adjusted'),
  format = 'parquet'
)

insurance_crude_person_expenditures_total <- tar_combine(
  insurance_crude_person_expenditures_total,
  mapped_targets[['insurance_crude_person_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Crude') |>
    rename(group = Level),
  format = 'parquet'
)

insurance_aa_person_expenditures_total <- tar_combine(
  insurance_aa_person_expenditures_total,
  mapped_targets[['insurance_aa_person_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Age-Adjusted') |>
    rename(group = Level),
  format = 'parquet'
)

# ** Combine person drug level expenditure ---------------------------------------------

crude_person_drug_expenditures_total <- tar_combine(
  crude_person_drug_expenditures_total,
  mapped_targets[['crude_person_drug_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Crude'),
  format = 'parquet'
)

aa_person_drug_expenditures_total <- tar_combine(
  aa_person_drug_expenditures_total,
  mapped_targets[['aa_person_drug_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           stratifier = 'Overall',
           group = '-',
           age = 'Age-Adjusted'),
  format = 'parquet'
)

insurance_crude_person_drug_expenditures_total <- tar_combine(
  insurance_crude_person_drug_expenditures_total,
  mapped_targets[['insurance_crude_person_drug_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Crude') |>
    rename(group = Level),
  format = 'parquet'
)

insurance_aa_person_drug_expenditures_total <- tar_combine(
  insurance_aa_person_drug_expenditures_total,
  mapped_targets[['insurance_aa_person_drug_expenditures']],
  command = dplyr::bind_rows(!!!.x, .id = 'year') %>%
    mutate(year = str_extract(year, '[0-9]+'),
           age = 'Age-Adjusted') |>
    rename(group = Level),
  format = 'parquet'
)

# End of mapped -----------------------------------------------------------





# Replace the target list below with your own:
list(

  tar_target(rx_cpi,
             tribble(~Year,~cpi,
                     2000,285.4,
                     2001,300.9,
                     2002,316.5,
                     2003,326.3,
                     2004,337.1,
                     2005,349.0,
                     2006,363.9,
                     2007,369.157,
                     2008,378.284,
                     2009,391.055,
                     2010,407.824,
                     2011,424.981,
                     2012,440.149,
                     2013,442.580,
                     2014,458.343,
                     2015,479.315,
                     2016,502.510,
                     2017,519.618,
                     2018,528.008,
                     2019,526.785,
                     2020,532.081,
                     2021,522.392,
                     2022,533.925,
                     2023,549.458)
  ),

  tar_target(cpi,
             tribble(
               ~Year,~cpi,
               2000,172.2,
               2001,177.1,
               2002,179.9,
               2003,184.0,
               2004,188.9,
               2005,195.3,
               2006,201.6,
               2007,207.342,
               2008,215.303,
               2009,214.537,
               2010,218.056,
               2011,224.939,
               2012,229.594,
               2013,232.957,
               2014,236.736,
               2015,237.017,
               2016,240.007,
               2017,245.120,
               2018,251.107,
               2019,255.657,
               2020,258.811,
               2021,270.970,
               2022,292.655,
               2023,304.702)
  ),


  tar_target(
    unique_drug_names,
    combined_raw |>
      group_by(third_level_category_name, RXDRGNAM, RXNAME) |>
      count() |>
      rename(`Multum Lexicon Group` = third_level_category_name,
             `Generic drug name` = RXDRGNAM,
             `Pharmacy drug name` = RXNAME,
             `Number of rows` = n) |>
      write_csv('unique_drug_names.csv')
  ),
  tar_target(manual_coded_drug_names_fp,
             here::here('output/MEPS_unique_drug_names_classification.xlsx'),
             format = 'file'),
  tar_target(manual_coded_drug_names,
             readxl::read_xlsx(manual_coded_drug_names_fp) %>%
               #filter(third_level_category_name %in% c('ANTIDIABETIC COMBINATIONS', 'INSULIN')) %>%
               janitor::clean_names() %>%
               # Rename to original columns for join purposes
               rename(third_level_category_name = 1,
                      RXDRGNAM = 2,
                      RXNAME = 3,
                      drug_class_1 = 4,
                      drug_class_2 = 5)
             ),
  tar_target(IPUMS_manual_coded_drug_names_fp,
             here::here('output/IPUMS_MEPS_unique_drug_names_classification.xlsx'),
             format = 'file'),
  tar_target(IPUMS_manual_coded_drug_names,
             readxl::read_xlsx(IPUMS_manual_coded_drug_names_fp, sheet = 'Sheet1') |>
               select(-`Number of rows`) |>
               rename(third_level_category_name = 1,
                      RXDRGNAM = 2,
                      RXNAME = 3,
                      drug_class_1 = 4,
                      drug_class_2 = 5)),

  # Specify the file path for the DuckDB database
  #tar_target(db_file_path, "data/ipums_rx.db", format = 'file'),
  tar_target(diabetes_drugs,
             c('SULFONYLUREAS',
               'BIGUANIDES',
               'Bolus/Analog',
               'Basal/Analog',
               'Basal/Human',
               'Bolus/Human',
               'Pre-mixed/Human',
               'Pre-mixed/Analog',
               'Pre-mixed/Human and Analog',
               'Insulin (Unknown)',
               'Insulin',
               'Pre-mixed (Analog and Human)',
               'Basal',
               'Bolus',
               'Human',
               'Analog',
               'Human and Analog',
               'Pre-mixed',
               'Unknown',
               'Other',
               # '? (Human)',
               # '? (Analog)',
               'INSULIN',
               'ALPHA-GLUCOSIDASE INHIBITORS',
               'THIAZOLIDINEDIONES',
               'MEGLITINIDES',
               'ANTIDIABETIC COMBINATIONS',
               'DIPEPTIDYL PEPTIDASE 4 INHIBITORS',
               'AMYLIN ANALOGS',
               'None',
               'GLP-1 RECEPTOR AGONISTS',
               'SGLT-2 INHIBITORS')),

  tar_target(multum_class_code,
             read_csv('data/multum_class_code.csv')),

  mapped_targets,
  # Combined raw data -----------------------
  combined_data,
  combined_raw,
  combined_raw_ipums,
  combined_person,
  combined_diabetes,
  combined_person_drug_exp,

  tar_target(rx_data_all_years,
             {
               fp <- 'output/rx_data_all_years.csv'
               combined_data |>
                 select(
                   year,
                   MultumClass = third_level_category_name,
                   GenericDrugName = RXDRGNAM,
                   DrugName = RXNAME,
                   insulin_indicator,
                   PersonID = DUPERSID,
                   RXID = RXRECIDX,
                   UID = VARPSU,
                   SurveyWeights = DIABWF,
                   DiagnosedDiabetes = DIABDX
                 ) |>
                 write_csv(fp)
               fp
             },
             format = 'file'),
  tar_target(person_data_all_years,
             {
               fp <- 'output/person_data_all_years.csv'
               combined_person |>
                 select(
                   year,
                   `Bolus (Human)`,
                   `Bolus (Analog)`, `Basal (Human)`, `Basal (Analog)`,
                   `Pre-mixed (Human)`, `Pre-mixed (Analog)`,
                   `Insulin (Other)`,
                   `Insulin (Unknown)`,
                   BIGUANIDES, SULFONYLUREAS,
                   THIAZOLIDINEDIONES,
                   `GLP-1 RECEPTOR AGONISTS`,
                   `SGLT-2 INHIBITORS`,
                   `DIPEPTIDYL PEPTIDASE 4 INHIBITORS`,
                   `ANTIDIABETIC COMBINATIONS`,
                   `Other Oral Medications`,
                   SEX,
                   HIDEG,
                   AGE,
                   RACETHNX,
                   PersonID = DUPERSID,
                   UID = VARPSU,
                   SurveyWeights = DIABWF,
                   DiagnosedDiabetes = DIABDX,
                   VARSTR
                 ) |>
                 write_csv(fp)
               fp
             },
             format = 'file'),

  crude_person_proportions,
  crude_edu_person_proportions,
  crude_race_person_proportions,
  crude_sex_person_proportions,
  age_person_proportions,
  aa_person_proportions,
  aa_edu_person_proportions,
  aa_race_person_proportions,
  aa_sex_person_proportions,

  crude_rx_drug_counts,
  crude_edu_rx_drug_counts,
  crude_sex_rx_drug_counts,
  crude_race_rx_drug_counts,
  crude_copay_rx_drug_counts,
  crude_povcat_rx_drug_counts,
  age_rx_drug_counts,
  aa_rx_drug_counts,
  aa_rx_drug_counts_full,
  aa_edu_rx_drug_counts,
  aa_sex_rx_drug_counts,
  aa_race_rx_drug_counts,
  aa_copay_rx_drug_counts,
  aa_povcat_rx_drug_counts,

  crude_rx_expenditures_total,
  aa_rx_exenditures_total,
  insurance_crude_rx_expenditures_total,
  insurance_aa_rx_expenditures_total,

  crude_person_expenditures_total,
  aa_person_expenditures_total,
  insurance_crude_person_expenditures_total,
  insurance_aa_person_expenditures_total,

  crude_person_drug_expenditures_total,
  aa_person_drug_expenditures_total,
  insurance_crude_person_drug_expenditures_total,
  insurance_aa_person_drug_expenditures_total,

  tar_target(
    rx_expenditure_table,
    bind_rows(
      crude_rx_expenditures_total,
      aa_rx_exenditures_total,
      insurance_crude_rx_expenditures_total,
      insurance_aa_rx_expenditures_total
    ) |>
      mutate(
        drug = Level,
        drug = str_to_title(drug),
        drug = str_replace_all(drug, 'Sglt', 'SGLT'),
        drug = str_replace_all(drug, 'Glp', 'GLP'),
        insulin_indicator = case_when(
          drug %in% c(
            'Basal/Analog',
            'Basal/Human',
            'Bolus/Analog',
            'Bolus/Human',
            'Pre-mixed',
            'Pre-Mixed/Human',
            'Pre-Mixed/Analog',
            'Pre-Mixed/Human And Analog'
          ) ~ 'Insulin',
          str_detect(drug, 'Insulin') ~ 'Insulin',
          TRUE ~ 'Not Insulin'
        )
      ),
    format = 'parquet'
  ),

  tar_target(
    people_expenditure_table,
    bind_rows(
      crude_person_expenditures_total,
      aa_person_expenditures_total,
      insurance_crude_person_expenditures_total,
      insurance_aa_person_expenditures_total
    ),
    format = 'parquet'
  ),

  tar_target(
    people_drug_expenditure_table,
    bind_rows(
      crude_person_drug_expenditures_total,
      aa_person_drug_expenditures_total,
      insurance_crude_person_drug_expenditures_total |>
        mutate(stratifier = 'Insurance') |>
        separate(group, c('Level', 'group'), sep = '__'),
      insurance_aa_person_drug_expenditures_total |>
        mutate(stratifier = 'Insurance') |>
        separate(group, c('Level', 'group'), sep = '__')
    ),
    format = 'parquet'
  ),

  tar_target(
    all_rx_counts_table,
    bind_rows(
      crude_rx_drug_counts,
      crude_edu_rx_drug_counts,
      crude_sex_rx_drug_counts,
      crude_race_rx_drug_counts,
      crude_copay_rx_drug_counts,
      crude_povcat_rx_drug_counts,
      age_rx_drug_counts,
      aa_rx_drug_counts,
      aa_edu_rx_drug_counts,
      aa_sex_rx_drug_counts,
      aa_race_rx_drug_counts,
      aa_copay_rx_drug_counts,
      aa_povcat_rx_drug_counts
    ) %>%
      filter(group %!in% c('Not available', '<N/A>')) %>%
      mutate(
        drug = Level,
        drug = str_to_title(drug),
        drug = str_replace_all(drug, 'Sglt', 'SGLT'),
        drug = str_replace_all(drug, 'Glp', 'GLP'),
        insulin_indicator = case_when(
          drug %in% c(
            'Basal/Analog',
            'Basal/Human',
            'Bolus/Analog',
            'Bolus/Human',
            'Pre-mixed',
            'Pre-Mixed/Human',
            'Pre-Mixed/Analog',
            'Pre-Mixed/Human And Analog'
          ) ~ 'Insulin',
          str_detect(drug, 'Insulin') ~ 'Insulin',
          TRUE ~ 'Not Insulin'
        )
      ),
    format = 'parquet'
  ),
# *** Tables for Steve -----------------------

  tar_target(
    person_drug_expenditures_n,
    bind_rows(
      combined_person_drug_exp |>
        ungroup() |>
        count(year, third_level_category_name) |>
        mutate(COVERTYPE = '-'),
      combined_person_drug_exp |>
        ungroup() |>
        count(year, COVERTYPE, third_level_category_name)
    ) |>
      mutate(
        group = COVERTYPE,
        stratifier = case_when(group == '-' ~ 'Overall',
                               TRUE ~ 'Insurance')
      ) |>
      select(year, drug = third_level_category_name, stratifier, group, n)
    ,
    format = 'parquet'
  ),

  tar_target(
    overall_rx_counts,
    all_rx_counts_table |>
      filter(stratifier == 'Overall', age != 'Crude') |>
      select(
        drug,
        year,
        Number,
        SE,
        LL,
        UL,
        Percent,
        SE.Perc = SE.1,
        LL.Perc = LL.1,
        UL.Perc = UL.1,
        Flags
      ) |>
      mutate(Flags = case_when(Flags == "" ~ NA, TRUE ~ Flags)) |>
      select(drug, year, Number) |>
      pivot_wider(names_from = year, values_from = Number) |>
      filter(drug != 'Remove') |>
      write_csv('output/IPUMS_overall_rx_counts.csv')
  ),

  tar_target(overall_people_counts,
             all_proportions_table |>
               filter(stratifier == 'Overall', age != 'Crude') |>
               select(
               drug,
               year,
               Number,
               SE,
               LL,
               UL,
               Percent,
               SE.Perc = SE.1,
               LL.Perc = LL.1,
               UL.Perc = UL.1,
               Flags
             ) |>
               mutate(Flags = case_when(Flags == "" ~ NA, TRUE ~ Flags)) |>
               select(drug, year, Number) |>
               pivot_wider(names_from = year, values_from = Number) |>
               filter(drug != 'Remove') |>
               write_csv('output/MEPS_num_persons_prescribed_drugs.csv')),
  tar_target(person_sample_size,
             combined_person |>
               pivot_longer(names_to = 'drug', values_to = 'indicator', cols = BIGUANIDES:`SGLT-2 INHIBITORS`) |>
               filter(indicator == 1) |>
               group_by(year, drug) |>
               count() |>
               arrange(drug, year, n) |>
               write_csv('output/person_sample_size.csv')
             ),


  tar_target(all_proportions_table,
             bind_rows(crude_person_proportions,
                       crude_edu_person_proportions,
                       crude_race_person_proportions,
                       crude_sex_person_proportions,
                       age_person_proportions,
                       aa_person_proportions,
                       aa_edu_person_proportions,
                       aa_race_person_proportions,
                       aa_sex_person_proportions) %>%
               filter(group %!in% c('Not available', '<N/A>')) %>%
               mutate(drug = str_to_title(drug),
                      drug = str_replace_all(drug, 'Sglt', 'SGLT'),
                      drug = str_replace_all(drug, 'Glp', 'GLP'),
                      insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Pre-Mixed') ~ 'Insulin - Duration',
                                                    drug %in% c('Analog', 'Human', 'Human And Analog') ~ 'Insulin - Type',
                                                    TRUE ~ 'Not Insulin')),
             format = 'parquet'),

# # Pooled estimates --------------------------------------------------------
#
#
#
# ONLY NEED THE LINKAGE DAT IF I AM USING DATA FROM 2019 - 2021 AND POOLING WITH
# SOMETHING < 2019. https://meps.ahrq.gov/data_stats/download_data/pufs/h036/h36u21doc.shtml
  tar_target(linkage_dat,
             read_MEPS(type = 'Pooled linkage') |>
               select(DUPERSID, PANEL, starts_with('STRA'), starts_with('PSU')),
             format = 'parquet'),

  tar_target(pooled_dat,
             createPooledWeights(combined_data, years = as.character(2020:2021)) %>%
               mutate(across(where(is.character), ~as.factor(.x))) %>%
              filter(DIABWF > 0) |>
               # For this analysis we are combining both pre-mixed types
               mutate(third_level_category_name = case_when(
                 str_detect(third_level_category_name, 'Pre-mixed') ~ 'Pre-mixed',
                 TRUE ~ third_level_category_name
               ))
             ,
             format = 'parquet'),

  tar_target(split_pooled_dat,
             pooled_dat %>% group_split(insulin_indicator)),

  tar_target(pooled_person_dat,
             personLevelMEPS(pooled_dat, pooled = TRUE),
             format = 'parquet'),


  # * Pooled rx counts --------------------------------------------------------
  tar_target(pooled_svy,
             map(
               split_pooled_dat,
               ~ .x |>
                 filter(!is.na(third_level_category_name)) |>
                 svydesign(
                   ids = ~ VARPSU,
                   weights = ~ DIABWF,
                   strata = ~ VARSTR,
                   data = _,
                   nest = TRUE
                 ) |>
                 subset(DIABDX == '2')
             )),

  tar_target(crude_pooled_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(pooled_dat, drug_cat_insulin),
               srvyEstimatesNotInsulin(pooled_dat, drug_cat_not_insulin))),


tar_target(
  aa_pooled_rx_counts,
  srvyEstimates(
    srvyEstimatesInsulin(
      pooled_dat,
      drug_cat_insulin,
      age.adjust = FALSE,
      age.adjust.var = 1,
      age.standards = standard_age_proportions_iqvia_cats
    ),
    srvyEstimatesNotInsulin(
      pooled_dat,
      drug_cat_not_insulin,
      age.adjust = FALSE,
      age.adjust.var = 1,
      age.standards = standard_age_proportions_iqvia_cats
    )
  )
),
  tar_target(pooled_sex_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 SEX,
                 age.adjust = FALSE,
                 age.adjust.var = 'SEX',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 SEX,
                 age.adjust = FALSE,
                 age.adjust.var = 'SEX',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )
             ),
  tar_target(pooled_age_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 AGE_iqviacat_not_insulin
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 AGE_iqviacat_not_insulin
               )
             ) |>
               mutate(AGE_iqviacat = AGE_iqviacat_not_insulin)
             ),
  tar_target(pooled_race_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 RACETHNX,
                 age.adjust = FALSE,
                 age.adjust.var = 'RACETHNX',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 RACETHNX,
                 age.adjust = FALSE,
                 age.adjust.var = 'RACETHNX',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )),
  tar_target(pooled_edu_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 HIDEG,
                 age.adjust = FALSE,
                 age.adjust.var = 'HIDEG',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 HIDEG,
                 age.adjust = FALSE,
                 age.adjust.var = 'HIDEG',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )),
  tar_target(pooled_insurance_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 COVERTYPE,
                 age.adjust = FALSE,
                 age.adjust.var = 'COVERTYPE',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 COVERTYPE,
                 age.adjust = FALSE,
                 age.adjust.var = 'COVERTYPE',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )),

  tar_target(pooled_copay_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 Copay_6cat,
                 age.adjust = FALSE,
                 age.adjust.var = 'Copay_6cat',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 Copay_6cat,
                 age.adjust = FALSE,
                 age.adjust.var = 'Copay_6cat',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )),
  tar_target(pooled_povcat_rx_counts,
             srvyEstimates(
               srvyEstimatesInsulin(
                 pooled_dat,
                 drug_cat_insulin,
                 POVCAT,
                 age.adjust = FALSE,
                 age.adjust.var = 'POVCAT',
                 age.standards = standard_age_proportions_iqvia_cats
               ),
               srvyEstimatesNotInsulin(
                 pooled_dat,
                 drug_cat_not_insulin,
                 POVCAT,
                 age.adjust = FALSE,
                 age.adjust.var = 'POVCAT',
                 age.standards = standard_age_proportions_iqvia_cats
               )
             )),

  tar_target(
    pooled_data_for_graphing,
    bind_rows(
      aa_pooled_rx_counts,
      pooled_sex_rx_counts,
      pooled_race_rx_counts,
      pooled_age_rx_counts,
      filter(pooled_edu_rx_counts, HIDEG != '<N/A>'),
      pooled_povcat_rx_counts,
      pooled_insurance_rx_counts,
      pooled_copay_rx_counts,
    ) |>
      rename(AGE_7cat = AGE_iqviacat) |>
      mutate(
        group = coalesce(SEX, RACETHNX, AGE_7cat, HIDEG, COVERTYPE, POVCAT, Copay_6cat)
      ) |>
      mutate(
        group = case_when(is.na(group) ~ ' ',
                          TRUE ~ group),
        stratifier = case_when(
          group == ' ' ~ 'Overall',!is.na(SEX) ~ 'Sex',!is.na(RACETHNX) ~ 'Race',!is.na(HIDEG) ~ 'Education',!is.na(POVCAT) ~ 'Poverty',!is.na(Copay_6cat) ~ 'Copay',!is.na(AGE_7cat) ~ 'Age',!is.na(COVERTYPE) ~ 'Insurance',
          TRUE ~ NA
        ),
        Level = case_when(
          Level == 'BIGUANIDES' ~ 'Biguanides',
          Level == 'DIPEPTIDYL PEPTIDASE 4 INHIBITORS' ~ 'DP-4 Inhibitors',
          Level == 'GLP-1 RECEPTOR AGONISTS' ~ 'GLP-1 Recept Agonists',
          Level == 'SGLT-2 INHIBITORS' ~ 'SGLT-2 Inhibitors',
          Level == 'SULFONYLUREAS' ~ 'Sulfonylureas',
          Level == 'THIAZOLIDINEDIONES' ~ 'Thiazolidinediones',
          str_detect(Level, '/|Pre-mixed') ~ paste0('Insulin - ', Level),
          TRUE ~ Level
        ),
        insulin_indicator = case_when(str_detect(Level, 'Insulin') ~ 'insulin',
                                      TRUE ~ 'not_insulin')

      ) |>
      filter(
        Level != 'None',!is.na(group),!is.na(stratifier),
        group != '<N/A>'
      ) |>
      mutate(
        stratifier = factor(
          stratifier,
          levels = c(
            'Overall',
            'Sex',
            'Race',
            'Age',
            'Education',
            'Poverty',
            'Insurance',
            'Copay'
          )
        ),
        group = factor(
          group,
          levels = c(
            ' ',
            'Female',
            'Male',
            'Asian/not Hispanic',
            'Black/not Hispanic',
            'Hispanic',
            'Other Race/not Hispanic',
            'White/not Hispanic',
            '< 10',
            '10 - 19',
            '20 - 39',
            '40 - 59',
            '60 - 64',
            '65 - 74',
            '75+',
            'Less than high school',
            'High school',
            'Greater than high school',
            'Poor (<100% of poverty line)',
            'Low income (100% - <200% of poverty line)',
            'Middle income (200% - 399% of poverty line)',
            'High income (>= 400% of poverty line)',
            'Public only',
            'Any private',
            'Uninsured',
            '0',
            '>0-10',
            '>10-20',
            '>20-30',
            '>30-75',
            '>75'
          )
        )
      ) |>
      arrange(stratifier, group, insulin_indicator) |>
      select(
        -SEX,
        -RACETHNX,
        -HIDEG,
        -AGE_7cat,
        -COVERTYPE,
        -POVCAT,
        -Copay_6cat,
        -insulin_indicator,-SE.1,
        -LL.1,
        -UL.1,
        -deff,
        -df
      ) |>
      rename(
        Prescription = Level,
        `Relative CI Width` = rel_ci,
        `Effective n` = n_eff
      )
  ),
  tar_target(
    pooled_table,
    bind_rows(
      aa_pooled_rx_counts,
      pooled_sex_rx_counts,
      pooled_race_rx_counts,
      pooled_age_rx_counts,
      filter(pooled_edu_rx_counts, HIDEG != '<N/A>'),
      pooled_povcat_rx_counts,
      pooled_insurance_rx_counts,
      pooled_copay_rx_counts,
    ) |>
      rename(AGE_7cat = AGE_iqviacat) |>
      mutate(group = coalesce(SEX, RACETHNX, AGE_7cat, HIDEG, COVERTYPE, POVCAT, Copay_6cat)) |>
      mutate(group = case_when(is.na(group) ~ ' ',
                               TRUE ~ group),
             stratifier = case_when(group == ' ' ~ 'Overall',
                                    !is.na(SEX) ~ 'Sex',
                                    !is.na(RACETHNX) ~ 'Race',
                                    !is.na(HIDEG) ~ 'Education',
                                    !is.na(POVCAT) ~ 'Poverty',
                                    !is.na(Copay_6cat) ~ 'Copay',
                                    !is.na(AGE_7cat) ~ 'Age',
                                    !is.na(COVERTYPE) ~ 'Insurance',
                                    TRUE ~ NA),
             Level = case_when(Level == 'BIGUANIDES' ~ 'Biguanides',
                               Level == 'DIPEPTIDYL PEPTIDASE 4 INHIBITORS' ~ 'DPP-4i',
                               Level == 'GLP-1 RECEPTOR AGONISTS' ~ 'GLP-1RA',
                               Level == 'SGLT-2 INHIBITORS' ~ 'SGLT2i',
                               Level == 'SULFONYLUREAS' ~ 'Sulfonylureas',
                               Level == 'THIAZOLIDINEDIONES' ~ 'TZD',
                               str_detect(Level, '/|Pre-mixed') ~ paste0('Insulin - ', Level),
                               TRUE ~ Level
                               ),
             insulin_indicator = case_when(str_detect(Level, 'Insulin') ~ 'insulin',
                                           TRUE ~ 'not_insulin')

      ) |>
      # Column percentages for stratifiers
      mutate(Percent = specifyDecimal(Percent * 100, digits = 1)) |>
      # Row percentage for overall
      # Need to setup a dummy variable to make sure the right medicines are being used
      filter(Level != 'None',
             !is.na(group),
             !is.na(stratifier),
             group != '<N/A>') |>
      mutate(stratifier = factor(stratifier, levels = c('Overall', 'Sex', 'Race', 'Age', 'Education',
                                                        'Poverty', 'Insurance', 'Copay')),
             group = factor(group, levels = c(' ',
                                              'Female', 'Male',
                                              'Asian/not Hispanic',
                                              'Black/not Hispanic',
                                              'Hispanic',
                                              'Other Race/not Hispanic',
                                              'White/not Hispanic',
                                              '< 10', '10 - 19', '20 - 39', '40 - 59',
                                              '60 - 64','60 - 74', '65 - 74', '75+',
                                              'Less than high school',
                                              'High school',
                                              'Greater than high school',
                                              'Poor (<100% of poverty line)',
                                              'Low income (100% - <200% of poverty line)',
                                              'Middle income (200% - 399% of poverty line)',
                                              'High income (>= 400% of poverty line)',
                                              'Public only', 'Any private', 'Uninsured',
                                              '0', '>0-10', '>10-20', '>20-30',
                                              '>30-75', '>75'))
             ) |>
      arrange( stratifier, group, insulin_indicator) |>
      select(-SEX, -RACETHNX, -HIDEG, -AGE_7cat, -COVERTYPE, -POVCAT, -Copay_6cat, -insulin_indicator,
             -AGE_iqviacat_not_insulin,
             -SE.1, PercentLL = LL.1, PercentUL = UL.1, -deff, -df) |>
      rename(Prescription = Level, `Relative CI Width` = rel_ci, `Effective n` = n_eff) |>
      mutate(across(c(Number, SE, LL, HL), ~round(.x, digits = -3)),
             PercentLL = PercentLL * 100,
             PercentUL = PercentUL * 100) |>
      mutate(across(where(is.numeric), ~formatC(.x, format = 'd', big.mark = ','))) |>
      mutate(Number = paste0(Number, " (", Percent, ")")) |>
      select(-Percent)

  ),
  tar_target(wide_pooled_table,
             pooled_table |>
               mutate(Prescription = case_when(
                 str_detect(Prescription, 'Insulin') ~ str_remove_all(Prescription, 'Insulin - '),
                 TRUE ~ Prescription),
                 Number = case_when(!is.na(Flags) ~ paste0(Number, '*'),
                                    TRUE ~ Number)
               ) |>
               mutate(Prescription = factor(Prescription,
                                            levels = c('Bolus/Human',
                                                       'Bolus/Analog',
                                                       'Basal/Human',
                                                       'Basal/Analog',
                                                       'Pre-mixed/Human',
                                                       'Pre-mixed/Analog',
                                                       'Pre-mixed',
                                                       'Insulin/Unknown',
                                                       'All insulins',
                                                       'Biguanides',
                                                       'Sulfonylureas',
                                                       'TZD',
                                                       'DPP-4i',
                                                       'GLP-1RA',
                                                       'SGLT2i',
                                                       'Unknown'))) |>
               arrange(Prescription) |>
               filter(!is.na(Prescription)) |>
               select(Prescription, Number, stratifier, group) |>
               pivot_wider(
                 id_cols = c(stratifier, group),
                 names_from = Prescription,
                 values_from = Number
               ) |>
               arrange(group)
             ),

  tar_target(gt_pooled_table,
             gt(pooled_table,
                rowname_col = 'group',
                groupname_col = 'stratifier')

               ),
  tar_target(gt_wide_pooled_table,
            gt(wide_pooled_table,
               rowname_col = 'group',
               groupname_col = 'stratifier'
               ) |>
              tab_footnote(
                footnote = '* Value should be suppressed.'
              ) |>
              tab_options(footnotes.mark = '*')
            ),

  tar_target(save_pooled_table,
             {filepath <- here::here('output/MEPS_2020_2021_rx_counts_by_stratifier.docx')
             gtsave(gt_pooled_table, filepath)
             filepath
             }),


  tar_target(wide_insulins_table,
             wide_pooled_table |>
               select(1:9) |>
               mutate(`All insulins` = case_when(stratifier == 'Overall' ~ str_replace(`All insulins`, '\\([0-9.]+\\)', '(-)'),
                                                 TRUE ~ `All insulins`)) |>
               gt(
                  rowname_col = 'group',
                  groupname_col = 'stratifier'
               ) |>
               tab_footnote(
                 footnote = '* Value should be suppressed.'
               ) |>
               tab_options(footnotes.mark = '*')
             ),

  tar_target(wide_not_insulins_table,
             wide_pooled_table |>
               select(1,2, 9:16) |>
               gt(
                 rowname_col = 'group',
                 groupname_col = 'stratifier'
               ) |>
               tab_footnote(
                 footnote = '* Value should be suppressed.'
               ) |>
               tab_options(footnotes.mark = '*')
             ),

  tar_target(save_wide_insulins,
             {filepath <- here::here('output/MEPS_2020_2021_insulins_table.docx')
             gtsave(wide_insulins_table, filepath)
             filepath
             }
             ),

  tar_target(save_wide_not_insulins,
             {filepath <- here::here('output/MEPS_2020_2021_not_insulins_table.docx')
             gtsave(wide_not_insulins_table, filepath)
             filepath
             }
  ),

  tar_quarto(iqvia_meps_paper,
             path = here::here('manuscript/QUARTO-Manuscript.qmd')),

  tar_target(iqvia_wide,
             meps_iqvia |>
               filter(data == 'IQVIA') |>
               mutate(insulin_indicator = case_when(str_detect(Prescription, 'Insulin') ~ 'Insulin',
                                                    TRUE ~ 'Not insulin')) |>
               # Fix overall percentages
               mutate(dummy = case_when(stratifier == 'Overall' ~ 1, TRUE ~ 0),
                      dummy2 = case_when(Prescription == 'All insulins' ~ 'Not insulin',
                                         TRUE ~ insulin_indicator)) |>
               mutate(.by = c(dummy, dummy2),
                      Percent = case_when(stratifier == 'Overall' ~ specifyDecimal(Number / sum(Number, na.rm = TRUE) * 100, digits = 1),
                                          TRUE ~ specifyDecimal(Percent, digits = 1))
               ) |>
               select(-dummy, -dummy2) |>
               filter(Prescription != 'Insulin - Insulin/Unknown') |>
               mutate(Prescription = str_remove_all(Prescription, 'Insulin - '),
                      Number = formatC(Number, format = 'd', big.mark = ','),
                      Number = paste0(Number, ' (', Percent, ')')) |>
               mutate(stratifier = factor(stratifier, levels = c('Overall', 'Sex', 'Race', 'Age', 'Education',
                                                                 'Poverty', 'Insurance', 'Copay')),
                      group = factor(group, levels = c(' ',
                                                       'Female', 'Male',
                                                       'Asian/not Hispanic',
                                                       'Black/not Hispanic',
                                                       'Hispanic',
                                                       'Other Race/not Hispanic',
                                                       'White/not Hispanic',
                                                       '< 10', '10 - 19', '20 - 39', '40 - 59',
                                                       '60 - 64','60 - 74', '65 - 74', '75+',
                                                       'Less than high school',
                                                       'High school',
                                                       'Greater than high school',
                                                       'Poor (<100% of poverty line)',
                                                       'Low income (100% - <200% of poverty line)',
                                                       'Middle income (200% - 399% of poverty line)',
                                                       'High income (>= 400% of poverty line)',
                                                       'Public only', 'Any private', 'Uninsured',
                                                       '$0',
                                                       '>$0-$10',
                                                       '>$10-$20',
                                                       '>$20-$30',
                                                       '>$30-$75',
                                                       '>$75',
                                                       'Cardiology',
                                                       'Emergency Med',
                                                       'Endocrinology',
                                                       'General surgery',
                                                       'Geriatrics',
                                                       'Nephrology',
                                                       'NP/PA',
                                                       'OBGYN',
                                                       'Other',
                                                       'Primary care',
                                                       'Unknown'))
               ) |>
               mutate(Prescription = factor(
                 Prescription,
                 levels = c(
                   'Bolus/Human',
                   'Bolus/Analog',
                   'Basal/Human',
                   'Basal/Analog',
                   'Pre-mixed/Human',
                   'Pre-mixed/Analog',
                   'Pre-mixed',
                   'Insulin/Unknown',
                   'All insulins',
                   'Biguanides',
                   'Sulfonylureas',
                   'TZD',
                   'DPP-4i',
                   'GLP-1RA',
                   'SGLT2i',
                   'Unknown'
                 )
               )) |>
               arrange(Prescription) |>
               filter(!is.na(Prescription),
                      stratifier %nin% c('Education', 'Poverty', 'Race')) |>
               select(Prescription, Number, stratifier, group) |>
               pivot_wider(
                 id_cols = c(stratifier, group),
                 names_from = Prescription,
                 values_from = Number
               ) |>
               arrange(group)
             ),
  tar_target(iqvia_wide_insulins,
             iqvia_wide |>
               select(1:8) |>
               mutate(`All insulins` = case_when(stratifier == 'Overall' ~ str_replace(`All insulins`, '\\([0-9.]+\\)', '(-)'),
                                                 TRUE ~ `All insulins`)) |>
               gt(
                 rowname_col = 'group',
                 groupname_col = 'stratifier'
               )

             ),
  tar_target(iqvia_wide_not_insulins,
             iqvia_wide |>
               select(1, 2, 9:15) |>
               gt(
                 rowname_col = 'group',
                 groupname_col = 'stratifier'
               )
             ),

  # tar_target(save_iqvia_wide_insulins,
  #           {filepath <- here::here('output/iqvia_2020_2021_insulins_table.docx')
  #           gtsave(iqvia_wide_insulins, filepath)
  #           filepath
  #           }
  # ),
  #
  # tar_target(save_iqvia_wide_not_insulins,
  #           {filepath <- here::here('output/iqvia_2020_2021_not_insulins_table.docx')
  #           gtsave(iqvia_wide_not_insulins, filepath)
  #           filepath
  #           }
  # ),
# IQVIA data --------------------------------------------------------------

  tar_file_read(iqvia,
                here::here('data/iqvia.xlsx'),
                readxl::read_xlsx(!!.x,
                         col_types = c('text', 'text', 'text', 'numeric', 'numeric')) |>
                  mutate(group = if_else(stratifier == 'Overall', ' ', group)) %>%
                  bind_rows(.,
                            . |>
                              filter(str_detect(Prescription, 'Insulin')) |>
                              summarise(.by = c(stratifier, group),
                                        Number = sum(Number)) |>
                              mutate(Prescription = 'All insulins')
                  ) |>
                # collapse extra age category
                  mutate(group = case_when(group %in% c('60 - 64', '65 - 74') ~ '60 - 74',
                                           TRUE ~ group),
                         Prescription = case_when(str_detect(Prescription, 'Pre') ~ 'Insulin - Pre-mixed',
                                                  TRUE ~ Prescription)) |>
                  summarise(.by = c(stratifier, group, Prescription),
                            Number = sum(Number, na.rm = TRUE))
                  ),

  tar_target(meps_iqvia,
             bind_rows(list(MEPS = mutate(pooled_table,
                                          Number = as.numeric(str_remove_all(Number, ",|\\([0-9.]+\\)"))
                                          ),
                            IQVIA = iqvia),
                       .id = 'data') |>
               mutate(group = case_when(group == '3rd party' ~ 'Any private',
                                        group %in% c('Medicaid', 'Medicare Part D') ~ 'Public only',
                                        group == 'Cash' ~ 'Uninsured',
                                        group == '0' ~ '$0',
                                        group == ">0-10" ~ '>$0-$10',
                                        group == ">10-20" ~ ">$10-$20",
                                        group == ">20-30" ~ ">$20-$30",
                                        group == ">30-75" ~ '>$30-$75',
                                        group == ">75" ~ '>$75',
                                        TRUE ~ group)) |>
               group_by(data, Prescription, stratifier, group) |>
               summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)),
                         across(where(is.character), ~sample(.x, 1))) |>
               ungroup() |>
               group_by(data, Prescription, stratifier) |>
               filter(group %nin% c('< 10', '10 - 19')) |>
               mutate(total = sum(Number, na.rm = TRUE),
                      Percent = round((Number / total) * 100, 2)) |>
               ungroup() |>
               complete(data, Prescription, nesting(stratifier, group),
                        fill = list(Number = NA,
                                    Percent = NA))),
  # Questions: How reconcile Insurance category differences
  tar_target(meps_iqvia_plots,
             pooledGraphs(meps_iqvia)),

  tar_target(save_meps_iqvia_plots,
             ggsave(filename = 'output/meps_iqvia.png',
                    plot = meps_iqvia_plots,
                    width = 10,
                    height = 5,
                    units = 'in')),

  tar_target(comparisons_table_prep,
             prepGTMepsIqvia(meps_iqvia, appendix = FALSE)),
  tar_target(comparisons_table_appendix_prep,
             prepGTMepsIqvia(meps_iqvia, appendix = TRUE)),

  tar_target(compare_insulins_gt,
           comparisons_table_prep |>
             mutate(across(everything(), ~if_else(.x == 'N/A', NA, .x))) |>
             rename_with(~gsub('IQVIA', 'NPA', .x)) |>
             select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
             select(-contains('Unknown')) |>
             gt(
               rowname_col = 'group',
               groupname_col = 'stratifier'
             ) |>
             tab_style(
               style = list(
                 cell_text(align = 'right')
               ),
               locations = cells_body(
                 columns = 2:ncol(comparisons_table_prep |>
                                    rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                    select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
                                    select(-contains('Unknown')))
               )
             ) |>
             tab_style(
               style = cell_text(align = 'center'),
               locations = cells_column_labels(columns = 2:ncol(comparisons_table_prep |>
                                                                  rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                                                  select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
                                                                  select(-contains('Unknown')))
               )
             ) |>
             tab_spanner_delim(
               delim = '_',
               reverse = TRUE
             )|>
             tab_source_note('- represents data that has been suppressed following the National Center for Health Statistics data presentation
                              standards (15), NA cells indicate that data for that group is not available
                             from that data source') |>
             # data_color(
             #   columns = everything(), # or specify specific columns
             #   na_color = 'grey',
             #   palette = 'white'
             # ) %>%
             # # Replace NA values with an empty string
             # sub_missing(
             #   columns = everything(), # or specify specific columns
             #   missing_text = ""
             # )|>
             tab_footnote(locations = cells_stub(rows = group == 'NP/PA'),
                          footnote = 'Nurse Practitioner/Physician Assistant') |>
             tab_footnote(locations = cells_stub(rows = group == 'OBGYN'),
                          footnote = 'Obstetrics/Gynecology') |>
             tab_footnote(footnote = 'Percentages may not sum to 100 due to rounding',
                          locations = cells_row_groups(contains('%'))) |>
             tab_options(footnotes.marks = c('*', '†', '‡', '§', '||', '¶', '#', '**', '††', '‡‡'))
  ),

  tar_target(compare_not_insulins_gt,
            comparisons_table_prep |>
              mutate(across(everything(), ~if_else(.x == 'N/A', NA, .x))) |>
              rename_with(~gsub('IQVIA', 'NPA', .x)) |>
              select(stratifier, group, contains('Biguanides'),
                     contains('Sulfon'),
                     contains('TZD'),
                     contains('DPP'),
                     contains('GLP'),
                     contains('SGLT'),
                     contains('All insulin')) |>
              gt(
                rowname_col = 'group',
                groupname_col = 'stratifier'
              ) |>
              tab_spanner_delim(
                delim = '_',
                reverse = TRUE
              ) |>
              tab_style(
                style = list(
                  cell_text(align = 'right')
                ),
                locations = cells_body(
                  columns = 2:ncol( comparisons_table_prep |>
                                      rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                      select(stratifier, group, contains('Biguanides'),
                                             contains('Sulfon'),
                                             contains('TZD'),
                                             contains('DPP'),
                                             contains('GLP'),
                                             contains('SGLT'),
                                             contains('All insulin'))
                  )
                )
              ) |>
              tab_style(
                style = cell_text(align = 'center'),
                locations = cells_column_labels(columns = 2:ncol(comparisons_table_prep |>
                                                                   rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                                                   select(stratifier, group, contains('Biguanides'),
                                                                          contains('Sulfon'),
                                                                          contains('TZD'),
                                                                          contains('DPP'),
                                                                          contains('GLP'),
                                                                          contains('SGLT'),
                                                                          contains('All insulin')))
                )
              ) |>
              tab_source_note('- represents data that has been suppressed following the National Center for Health Statistics data presentation
                              standards (15), NA cells indicate that data for that group is not available
                             from that data source')|>
              # data_color(
              #   columns = everything(), # or specify specific columns
              #   na_color = 'grey',
              #   palette = 'white'
              # ) %>%
              # # Replace NA values with an empty string
              # sub_missing(
              #   columns = everything(), # or specify specific columns
              #   missing_text = ""
              # ) |>
              tab_footnote(locations = cells_stub(rows = group == 'NP/PA'),
                           footnote = 'Nurse Practitioner/Physician Assistant') |>
              tab_footnote(locations = cells_stub(rows = group == 'OBGYN'),
                           footnote = 'Obstetrics/Gynecology') |>
              tab_footnote(footnote = 'Percentages may not sum to 100 due to rounding',
                           locations = cells_row_groups(contains('%'))) |>
              tab_options(footnotes.marks = c('*', '†', '‡', '§', '||', '¶', '#', '**', '††', '‡‡'))
  ),


tar_target(compare_insulins_appendix_gt,
           comparisons_table_appendix_prep |>
             mutate(across(everything(), ~if_else(.x == 'N/A', NA, .x))) |>
             rename_with(~gsub('IQVIA', 'NPA', .x)) |>
             select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
             select(-contains('Unknown')) |>
             gt(
               rowname_col = 'group',
               groupname_col = 'stratifier'
             ) |>
             tab_style(
               style = list(
                 cell_text(align = 'right')
               ),
               locations = cells_body(
                 columns = 2:ncol(comparisons_table_prep |>
                                    rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                    select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
                                    select(-contains('Unknown')))
               )
             ) |>
             tab_style(
               style = cell_text(align = 'center'),
               locations = cells_column_labels(columns = 2:ncol(comparisons_table_prep |>
                                                                  rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                                                  select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
                                                                  select(-contains('Unknown')))
               )
             ) |>
             tab_spanner_delim(
               delim = '_',
               reverse = TRUE
             )|>
             tab_source_note('- represents data that has been suppressed following the National Center for Health Statistics data presentation
                              standards (15), NA cells indicate that data for that group is not available
                             from that data source') |>
             # data_color(
             #   columns = everything(), # or specify specific columns
             #   na_color = 'grey',
             #   palette = 'white'
             # ) %>%
             # # Replace NA values with an empty string
             # sub_missing(
             #   columns = everything(), # or specify specific columns
             #   missing_text = ""
             # )|>
             tab_footnote(locations = cells_stub(rows = group == 'NP/PA'),
                          footnote = 'Nurse Practitioner/Physician Assistant') |>
             tab_footnote(locations = cells_stub(rows = group == 'OBGYN'),
                          footnote = 'Obstetrics/Gynecology') |>
             tab_options(footnotes.marks = c('*', '†', '‡', '§', '||', '¶', '#', '**', '††', '‡‡'))
),

tar_target(compare_not_insulins_appendix_gt,
           comparisons_table_appendix_prep |>
             mutate(across(everything(), ~if_else(.x == 'N/A', NA, .x))) |>
             rename_with(~gsub('IQVIA', 'NPA', .x)) |>
             select(stratifier, group, contains('Biguanides'),
                    contains('Sulfon'),
                    contains('TZD'),
                    contains('DPP'),
                    contains('GLP'),
                    contains('SGLT'),
                    contains('All insulin')) |>
             gt(
               rowname_col = 'group',
               groupname_col = 'stratifier'
             ) |>
             tab_spanner_delim(
               delim = '_',
               reverse = TRUE
             ) |>
             tab_style(
               style = list(
                 cell_text(align = 'right')
               ),
               locations = cells_body(
                 columns = 2:ncol( comparisons_table_prep |>
                                     rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                     select(stratifier, group, contains('Biguanides'),
                                            contains('Sulfon'),
                                            contains('TZD'),
                                            contains('DPP'),
                                            contains('GLP'),
                                            contains('SGLT'),
                                            contains('All insulin'))
                 )
               )
             ) |>
             tab_style(
               style = cell_text(align = 'center'),
               locations = cells_column_labels(columns = 2:ncol(comparisons_table_prep |>
                                                                  rename_with(~gsub('IQVIA', 'NPA', .x)) |>
                                                                  select(stratifier, group, contains('Biguanides'),
                                                                         contains('Sulfon'),
                                                                         contains('TZD'),
                                                                         contains('DPP'),
                                                                         contains('GLP'),
                                                                         contains('SGLT'),
                                                                         contains('All insulin')))
               )
             ) |>
             tab_source_note('- represents data that has been suppressed following the National Center for Health Statistics data presentation
                              standards (15), NA cells indicate that data for that group is not available
                             from that data source')|>
             # data_color(
             #   columns = everything(), # or specify specific columns
             #   na_color = 'grey',
             #   palette = 'white'
             # ) %>%
             # # Replace NA values with an empty string
             # sub_missing(
             #   columns = everything(), # or specify specific columns
             #   missing_text = ""
             # ) |>
             tab_footnote(locations = cells_stub(rows = group == 'NP/PA'),
                          footnote = 'Nurse Practitioner/Physician Assistant') |>
             tab_footnote(locations = cells_stub(rows = group == 'OBGYN'),
                          footnote = 'Obstetrics/Gynecology') |>
             tab_options(footnotes.marks = c('*', '†', '‡', '§', '||', '¶', '#', '**', '††', '‡‡'))
),

  tar_target(save_compare_not_insulins,
            {filepath <- here::here('output/compare_2020_2021_not_insulins_table.docx')
            gtsave(compare_not_insulins_gt, filepath)
            filepath
            },
            format = 'file'
  ),

  tar_target(save_compare_insulins,
            {filepath <- here::here('output/compare_2020_2021_insulins_table.docx')
            gtsave(compare_insulins_gt, filepath)
            filepath
            },
            format = 'file'
  ),

tar_target(save_compare_not_insulins_appendix,
           {filepath <- here::here('output/compare_2020_2021_not_insulins_table_appendix.docx')
           gtsave(compare_not_insulins_appendix_gt, filepath)
           filepath
           },
           format = 'file'
),

tar_target(save_compare_insulins_appendix,
           {filepath <- here::here('output/compare_2020_2021_insulins_table_appendix.docx')
           gtsave(compare_insulins_appendix_gt, filepath)
           filepath
           },
           format = 'file'
),



# Dashboard data ----------------------------------------------------------

  tar_target(
    consol_dat_fp,
    "//cdc.gov/locker/CCHP_NCCD_DDT_Data1/epistat/Surveillance/Surveillance and Trends Reporting System/GRASP/NEW_Jan2018/Datasets/consol_dat_2022_1123_v2.sas7bdat",
    format = 'file'
  ),
  # Master GRASP data, but doens't have 2021
  tar_target(
    consol_dat,
    haven::read_sas(consol_dat_fp), # Don't care about state rihgt now
    format = 'parquet',
    deployment = 'main'
  ),

  tar_target(
    data_dict_fp,
    '//cdc.gov/locker/CCHP_NCCD_DDT_Data1/epistat/Surveillance/Surveillance and Trends Reporting System/GRASP/NEW_Jan2018/Lookup tables/2022_1031_master_lookup.xlsx',
    format = 'file'
  ),

  tar_target(
    data_dict,
    readxl::read_excel(data_dict_fp) %>%
      left_join(., readxl::read_excel(data_dict_fp, sheet = 2)),
    deployment = 'main'),

  tar_target(
    data_dict2,
    readxl::read_excel(data_dict_fp, sheet = 2),
    deployment = 'main'
  ),

  tar_target(
    consol_dat_dict_combined,
    joinDataWithDictionary(consol_dat, data_dict) %>% # reorder columns
      select(fipscode, Geolevel, YearID, ends_with('Name'), contains('Estimate'),
             contains('Limit'), everything()),
    format = 'parquet'

  ),

  tar_target(standard.ages,
             read_csv('https://seer.cancer.gov/stdpopulations/stdpop.singleagesthru99.txt', col_names = FALSE) %>%
               as_tibble() %>%
               slice(1:101) %>% # Want just the first 101 rows
               mutate(age = as.numeric(str_sub(X1, 4, 6)),
                      std_pop = as.numeric(str_sub(X1, 7, 14))) %>%
               select(-X1) %>%
               pivot_wider(names_from = 'age',
                             values_from = 'std_pop')),


  tar_target(
    standard_age_proportions_18,
    tibble(
      `18-44` = rowSums(standard.ages[, 19:45]) / rowSums(standard.ages[19:ncol(standard.ages)]),
      `45-64` = rowSums(standard.ages[, 46:65]) / rowSums(standard.ages[19:ncol(standard.ages)]),
      `65-74` = rowSums(standard.ages[, 66:75]) / rowSums(standard.ages[19:ncol(standard.ages)]),
      `>75`   = rowSums(standard.ages[, 75:ncol(standard.ages)]) / rowSums(standard.ages[19:ncol(standard.ages)]),
    ) %>% unlist()
  ),

  tar_target(
    standard_age_proportions_iqvia_cats,
    tibble(
      `20-39` = rowSums(standard.ages[, 21:40]) / rowSums(standard.ages[21:ncol(standard.ages)]),
      `40-59` = rowSums(standard.ages[, 41:60]) / rowSums(standard.ages[21:ncol(standard.ages)]),
      `60-74` = rowSums(standard.ages[, 61:75]) / rowSums(standard.ages[21:ncol(standard.ages)]),
      `75+` = rowSums(standard.ages[, 76:ncol(standard.ages)]) / rowSums(standard.ages[21:ncol(standard.ages)]),
    ) %>% unlist()
  ),
# Prep data for dashboard -------------------------------------------------

# * Person level data -----------------------------------------------------

# crude_person_proportions,
# crude_edu_person_proportions,
# crude_race_person_proportions,
# crude_sex_person_proportions,
# age_person_proportions,
# aa_person_proportions,
# aa_edu_person_proportions,
# aa_race_person_proportions,
# aa_sex_person_proportions,
#
# crude_rx_drug_counts,
# crude_edu_rx_drug_counts,
# crude_sex_rx_drug_counts,
# crude_race_rx_drug_counts,
# age_rx_drug_counts,
# aa_rx_drug_counts,
# aa_edu_rx_drug_counts,
# aa_sex_rx_drug_counts,
# aa_race_rx_drug_counts


  tar_target(crude_sex_person_grasp,
             formatGrasp1(crude_sex_person_proportions) %>%
               mutate(RaceName = 'Total',
                      GenderName = group,
                      EducationName = 'Total',
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
  ),

  tar_target(crude_race_person_grasp,
             formatGrasp1(crude_race_person_proportions) %>%
               mutate(RaceName = group,
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
             ),

  tar_target(crude_edu_person_grasp,
             formatGrasp1(crude_edu_person_proportions) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = group,
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
             ),

  tar_target(aa_sex_person_grasp,
             formatGrasp1(aa_sex_person_proportions) %>%
               mutate(RaceName = 'Total',
                      GenderName = group,
                      EducationName = 'Total',
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
  ),

  tar_target(aa_race_person_grasp,
             formatGrasp1(aa_race_person_proportions) %>%
               mutate(RaceName = group,
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
  ),

  tar_target(aa_edu_person_grasp,
             formatGrasp1(aa_edu_person_proportions) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = group,
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
  ),

  tar_target(age_person_grasp,
             formatGrasp1(age_person_proportions) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = group,
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = drug
               ) %>%
               formatGrasp2(., indicator.level = 'person'),
             format = 'parquet'
             ),

  tar_target(
    crude_person_grasp,
    formatGrasp1(crude_person_proportions) %>%
      mutate(RaceName = 'Total',
             GenderName = 'Total',
             EducationName = 'Total',
             AgeName = 'Crude',
             YearID = year,
             fipscode = '00000',
             SeEstimate = SE,
             LowerLimit = LL,
             UpperLimit = UL,
             IndicatorName = drug
      ) %>%
      formatGrasp2(., indicator.level = 'person'),
    format = 'parquet'
  ),
  tar_target(
    aa_person_grasp,
    formatGrasp1(aa_person_proportions) %>%
      mutate(RaceName = 'Total',
             GenderName = 'Total',
             EducationName = 'Total',
             AgeName = 'Age-Adjusted',
             YearID = year,
             fipscode = '00000',
             SeEstimate = SE,
             LowerLimit = LL,
             UpperLimit = UL,
             IndicatorName = drug
      ) %>%
      formatGrasp2(., indicator.level = 'person'),
    format = 'parquet'
  ),

# * Drug level data -------------------------------------------------------

  tar_target(
    crude_rx_grasp,
    formatGrasp1(crude_rx_drug_counts) %>%
      mutate(RaceName = 'Total',
             GenderName = 'Total',
             EducationName = 'Total',
             AgeName = 'Crude',
             YearID = year,
             fipscode = '00000',
             SeEstimate = SE,
             LowerLimit = LL,
             UpperLimit = UL,
             IndicatorName = Level
      ) %>%
      formatGrasp2(., indicator.level = 'rx'),
    format = 'parquet'
  ),

  tar_target(crude_sex_rx_grasp,
             formatGrasp1(crude_sex_rx_drug_counts) %>%
               mutate(RaceName = 'Total',
                      GenderName = group,
                      EducationName = 'Total',
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
             ),

  tar_target(crude_race_rx_grasp,
             formatGrasp1(crude_race_rx_drug_counts) %>%
               mutate(RaceName = group,
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
             ),

  tar_target(age_rx_grasp,
             formatGrasp1(age_rx_drug_counts) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = group,
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
             ),

  tar_target(crude_edu_rx_grasp,
             formatGrasp1(crude_edu_rx_drug_counts) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = group,
                      AgeName = 'Crude',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
             ),

  tar_target(
    aa_rx_grasp,
    formatGrasp1(aa_rx_drug_counts) %>%
      mutate(RaceName = 'Total',
             GenderName = 'Total',
             EducationName = 'Total',
             AgeName = 'Age-Adjusted',
             YearID = year,
             fipscode = '00000',
             SeEstimate = SE,
             LowerLimit = LL,
             UpperLimit = UL,
             IndicatorName = Level
      ) %>%
      formatGrasp2(., indicator.level = 'rx'),
    format = 'parquet'
  ),

  tar_target(aa_sex_rx_grasp,
             formatGrasp1(aa_sex_rx_drug_counts) %>%
               mutate(RaceName = 'Total',
                      GenderName = group,
                      EducationName = 'Total',
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
  ),

  tar_target(aa_race_rx_grasp,
             formatGrasp1(aa_race_rx_drug_counts) %>%
               mutate(RaceName = group,
                      GenderName = 'Total',
                      EducationName = 'Total',
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
  ),


  tar_target(aa_edu_rx_grasp,
             formatGrasp1(aa_edu_rx_drug_counts) %>%
               mutate(RaceName = 'Total',
                      GenderName = 'Total',
                      EducationName = group,
                      AgeName = 'Age-Adjusted',
                      YearID = year,
                      fipscode = '00000',
                      SeEstimate = SE,
                      LowerLimit = LL,
                      UpperLimit = UL,
                      IndicatorName = Level
               ) %>%
               formatGrasp2(., indicator.level = 'rx'),
             format = 'parquet'
  ),


# * Combine dashboard data ------------------------------------------------

  tar_target(combined_grasp,
              bind_rows(crude_rx_grasp,
                        crude_sex_rx_grasp,
                        crude_race_rx_grasp,
                        crude_edu_rx_grasp,
                        age_rx_grasp,
                        crude_person_grasp,
                        crude_sex_person_grasp,
                        crude_race_person_grasp,
                        crude_edu_person_grasp,
                        age_person_grasp,

                        aa_rx_grasp,
                        aa_sex_rx_grasp,
                        aa_race_rx_grasp,
                        aa_edu_rx_grasp,
                        aa_person_grasp,
                        aa_sex_person_grasp,
                        aa_race_person_grasp,
                        aa_edu_person_grasp
                        ),
             format = 'parquet'
              ),
#' Flags:
#' Cx: suppress count and rate
#' Px: suppress percent
#' Pc: footnote percent - compliment
#' R: Suppress ALL estimates
#' *df: Review count/percent - degrees of freedom

  tar_target(suppressed_grasp,
             suppressEstimates(combined_grasp) %>%
               mutate(DatasourceName = 'Medical Expenditure Panel Survey',
                      DatasetName = 'All Ages with Diabetes',
                      MiscName = 'No Summary/Total',
                      Geolevel = 'National',
                      IndicatorName = as.character(IndicatorName)) %>%
               select(-Flag) %>%
               select(YearID, Geolevel, fipscode, everything()),
             format = 'parquet'),

  tar_target(suppressed_grasp_csv,
             { path <- 'output/MEPS_prescription_data_2002-2021.csv'
               write_csv(suppressed_grasp, path)
               path},
             format = 'file'),

  tar_target(new_data_dict,
             tibble(
               id = 218:(217 + length(unique(suppressed_grasp$IndicatorName))), # Next available in sequence
               vid = as.character(104:(103 + length(unique(suppressed_grasp$IndicatorName)))), # Next available in sequence
               VariableTypeID = rep_len(1, length(unique(suppressed_grasp$IndicatorName))), # This is for indicators so probably won't change
               Name = sort(unique(suppressed_grasp$IndicatorName)),
               SortID = rep("" ,length(unique(suppressed_grasp$IndicatorName))), # ?
               ParentID = rep("" ,length(unique(suppressed_grasp$IndicatorName))), # ?
               HasChildren = rep(NA, length(unique(suppressed_grasp$IndicatorName))),
               DefaultValue = rep(NA, length(unique(suppressed_grasp$IndicatorName))),
               HEX = rep(NA, length(unique(suppressed_grasp$IndicatorName))), # ?
               isTotal = rep(0, length(unique(suppressed_grasp$IndicatorName))),
               include = rep(1, length(unique(suppressed_grasp$IndicatorName))),
               Note = rep(NA, length(unique(suppressed_grasp$IndicatorName))),
               VariableName = rep('Indicator', length(unique(suppressed_grasp$IndicatorName)))
             ) %>%
               bind_rows(data_dict),
             format = 'parquet'),

  tar_target(new_consol_dat,
             reverseJoinDataWithDictionary(suppressed_grasp, new_data_dict) %>%
               select(fipscode, Geolevel, YearID, RaceID, AgeID, GenderID, EducationID, IndicatorID, MiscID,
                      DataSourceID, EstimateID, SuppressID, DatasetID, Estimate, SeEstimate, LowerLimit, UpperLimit),
             format = 'parquet'),


# Plots -------------------------------------------------------------------

#' Notes on insulin: Pooled years get close for Analog, but not Human Individual
#' years are close on analog basal to IQVIA but about 50% too high on bolus
#' analog Pre-mixed Human is about 50% too high compared to IQVIA In 2021 data,
#' both basal and bolus human drop out??? Pre-mixed (Analog) is still dropping
#' from the tables. It's not present at all in the raw_meps_data_2020 or 2021
#' None of the human ones are present in raw 2021. Just total insulin numbers
#' don't really match the sums from the IQVIA tables either

#' Pooling data from just between 2019-2021 I don't need to use the linkage file
#' Ultimately this just adjust the SE estimates, not the point estimates of
#' totals Not pooled weights gives close estimates for non-insulin drugs
#' compared to IQVIA. Pooling weights makes the estimates about 1/2 what they
#' should be.
#'
#' Pre-mix use has gone down from 2009 to 2018:
#' https://journals.sagepub.com/doi/full/10.1177/19322968211028268 So it makes
#' sense maybe that use has dropped to not showing for some types Non-insulin
#' drugs %s currently seem to be ~half of what this paper is reporting:
#' https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0221174
#' This article:
#' https://www.mendeley.com/reference-manager/reader-v2/6d6f1ed0-032d-30f7-8b6b-2f2d598abb43/2b6abc73-e6e0-3d83-46f1-93fce80a440f
#' is showing similar - our non-insulin percs are about half of what they
#' present, insulin is high (but they're also presenting together) If I remove
#' insulin from % RX the non-insulin %s match a bit closer to Raval et al

  tar_target(
    crude_person_level_plot,
    all_proportions_table %>%
      mutate(year = str_sub(year, 3, 4),
             across(where(is.character), ~str_remove_all(.x,',')),
             across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
                    ~as.numeric(.x)),
             Percent = case_when(str_detect(Flags, 'Px') ~  NA,
                                 str_detect(Flags, 'Cx') ~  NA,
                                 str_detect(Flags, 'Pdf') ~ NA,
                                 str_detect(Flags, 'Cdf') ~ NA,
                                 str_detect(Flags, 'R') ~   NA,
                                 TRUE ~ Percent)) %>%
      filter(drug != 'Other Oral Medications') %>%
      group_split(stratifier) %>%
      map(
        ~filter(.x,
                age %in% c('Crude', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                group != '<N/A>') %>%
          complete(drug, year, group, fill = list(Percent = NA)) %>%
          ggplot(aes(x = year, y = Percent, color = group, group = group)) +
          #geom_point() +
          geom_line() +
          facet_wrap(~drug)+
          theme(axis.text.x = element_text(angle = 90))
      )
  ),

  tar_target(crude_rx_counts_stacked_barplot,
             all_rx_counts_table %>%
               mutate(year = str_sub(year, 3, 4),
                      across(where(is.character), ~str_remove_all(.x,',')),
                      across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
                             ~as.numeric(.x)),
                      Number = case_when(str_detect(Flags, 'Px') ~  NA,
                                          str_detect(Flags, 'Cx') ~  NA,
                                          str_detect(Flags, 'Pdf') ~ NA,
                                          str_detect(Flags, 'Cdf') ~ NA,
                                          str_detect(Flags, 'R') ~   NA,
                                          TRUE ~ Number)) %>%
               filter(drug != 'Other Oral Medications') %>%
               group_split(stratifier) %>%
               map(
                 ~filter(.x,
                         age %in% c('Crude', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                         group != '<N/A>') %>%
                   ggplot(aes(x = year, y = Number, fill = drug)) +
                   geom_bar(stat = 'identity', position = 'fill') +
                   labs(y = 'Percent') +
                   facet_wrap(~group)+
                   theme(axis.text.x = element_text(angle = 90))
               )
             ),
  tar_target(
    aa_person_level_insulin_plot,
    all_proportions_table %>%
      mutate(year = str_sub(year, 3, 4),
       across(where(is.character), ~str_remove_all(.x,',')),
       across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
              ~as.numeric(.x))) |>
       # Percent = case_when(str_detect(Flags, 'Px') ~  NA,
       #                     str_detect(Flags, 'Cx') ~  NA,
       #                     str_detect(Flags, 'Pdf') ~ NA,
       #                     str_detect(Flags, 'Cdf') ~ NA,
       #                     str_detect(Flags, 'R') ~   NA,
       #                     TRUE ~ Percent)) %>%
      mutate(insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Human',
                                                       'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                           drug %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                       'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                       'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                           str_detect(drug, 'Insulin') ~ 'Insulin',
                                           TRUE ~ 'Not Insulin')) |>
      filter(drug != 'Other Oral Medications', insulin_indicator == 'Insulin') %>%
      group_split(stratifier) %>%
      map(
  ~filter(.x,
          age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
          group != '<N/A>') %>%
    complete(drug, insulin_indicator, year, group, fill = list(Percent = NA,
                                            Stratifier = unique(.x$stratifier))) %>%
    ggplot(aes(x = year, y = Percent, color = group, group = group)) +
    #geom_point() +
    geom_line() +
    facet_wrap(~drug)+
    theme(axis.text.x = element_text(angle = 90))
      )
  ),

  tar_target(
    aa_person_level_not_insulin_plot,
    all_proportions_table %>%
      mutate(year = str_sub(year, 3, 4),
             across(where(is.character), ~str_remove_all(.x,',')),
             # Percent = case_when(str_detect(Flags, 'Px') ~  NA,
             #                     str_detect(Flags, 'Cx') ~  NA,
             #                     str_detect(Flags, 'Pdf') ~ NA,
             #                     str_detect(Flags, 'Cdf') ~ NA,
             #                     str_detect(Flags, 'R') ~   NA,
             #                     TRUE ~ Percent),
             across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
                    ~as.numeric(.x))
             ) %>%
      mutate(insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Human',
                                                       'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                           drug %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                       'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                       'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                           str_detect(drug, 'Insulin') ~ 'Insulin',
                                           TRUE ~ 'Not Insulin')) |>
      filter(drug != 'Other Oral Medications', drug != 'None', insulin_indicator != 'Insulin') %>%
      group_split(stratifier) %>%
      map(
        ~filter(.x,
                age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                group != '<N/A>') %>%
          complete(drug, insulin_indicator, year, group, fill = list(Percent = NA,
                                                                     Stratifier = unique(.x$stratifier))) %>%
          ggplot(aes(x = year, y = Percent, color = group, group = group)) +
          #geom_point() +
          geom_line() +
          facet_wrap(~drug)+
          theme(axis.text.x = element_text(angle = 90))
      )
  ),

  tar_target(
    aa_rx_counts_stacked_barplot,
    all_rx_counts_table %>%
      mutate(
        year = str_sub(year, 3, 4),
        across(where(is.character), ~ str_remove_all(.x, ',')),
        across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
               ~ as.numeric(.x)),
        Number = case_when(
          str_detect(Flags, 'Px') ~  NA,
          str_detect(Flags, 'Cx') ~  NA,
          str_detect(Flags, 'Pdf') ~ NA,
          str_detect(Flags, 'Cdf') ~ NA,
          str_detect(Flags, 'R') ~   NA,
          TRUE ~ Number
        )
      ) %>%
      filter(drug != 'Other Oral Medications') %>%
      group_split(stratifier) %>%
      map(
        ~ filter(
          .x,
          age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
          group != '<N/A>'
        ) %>%
          group_split(., insulin_indicator) |>
          lapply(
            X = _,
            FUN = function(X) {
              ggplot(X, aes(x = year, y = Number, fill = drug)) +
                geom_bar(stat = 'identity', position = 'fill') +
                labs(y = 'Percent') +
                facet_wrap(insulin_indicator ~ group) +
                guides(fill = guide_legend(nrow = 4))+
                theme(axis.text.x = element_text(angle = 90),
                      legend.position = 'bottom',
                      legend.text = element_text(size = 8))
            }
          ) |>
          patchwork::wrap_plots(nrow = 1)
      )
  ),

  tar_target(
    aa_rx_counts_stacked_lineplot,
    all_rx_counts_table %>%
      mutate(
        year = str_sub(year, 3, 4),
        across(where(is.character), ~ str_remove_all(.x, ',')),
        across(c(Number, SE, LL, UL, Percent, SE.1, LL.1, UL.1),
               ~ as.numeric(.x)),
        # Number = case_when(
        #   str_detect(Flags, 'Px') ~  NA,
        #   str_detect(Flags, 'Cx') ~  NA,
        #   str_detect(Flags, 'Pdf') ~ NA,
        #   str_detect(Flags, 'Cdf') ~ NA,
        #   str_detect(Flags, 'R') ~   NA,
        #   TRUE ~ Number
        # )
      ) %>%
      filter(drug != 'Other Oral Medications') %>%
      group_split(stratifier) %>%
      map(
        ~ filter(
          .x,
          age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
          group != '<N/A>',
          drug != 'Remove'
        ) |>
              ggplot(aes(x = year, y = Number, group = drug)) +
                geom_line() +
                labs(y = 'Number of Rxs') +
                facet_wrap(insulin_indicator ~ drug, scales = "free_y") +
                guides(fill = guide_legend(nrow = 4))+
                theme(axis.text.x = element_text(angle = 90),
                      legend.position = 'bottom',
                      legend.text = element_text(size = 8))

          )

  ),



  tar_target(aa_person_level_not_insulin_plot_png,
             map(aa_person_level_not_insulin_plot,
                 function(.x){
                   stratifier <- unique(.x$data$stratifier)%>% .[!is.na(.)]
                   stratifier <- ifelse(stratifier == '-', 'overall', stratifier)
                   ggsave(filename = glue::glue('output/{stratifier}_person_level_trends_meds_injectables.png'),
                          plot = .x,
                          width = 11,
                          height = 8,
                          units = 'in')
                 }
                 ),
             deployment = 'main'
  ),

  tar_target(aa_person_level_insulin_plot_png,
             map(aa_person_level_insulin_plot,
                 function(.x){
                   stratifier <- unique(.x$data$stratifier)%>% .[!is.na(.)]
                   stratifier <- ifelse(stratifier == '-', 'overall', stratifier)
                   ggsave(filename = glue::glue('output/{stratifier}_person_level_trends_insulin.png'),
                          plot = .x,
                          width = 11,
                          height = 8,
                          units = 'in')
                 }
             ),
             deployment = 'main'
  ),
  tar_target(aa_rx_counts_stacked_barplot_png,
             map(aa_rx_counts_stacked_barplot,
                 function(.x){
                   stratifier <- unique(.x$data$stratifier)%>% .[!is.na(.)]
                   stratifier <- ifelse(stratifier == '-', 'overall', stratifier)
                   ggsave(filename = glue::glue('output/{stratifier}_rx_percent_barplot.png'),
                          plot = .x,
                          width = 11,
                          height = 8,
                          units = 'in'
                          )
                 }
                 ),
             deployment = 'main'
  ),

# * Expenditures plots -------------------------------------

  tar_target(aa_rx_expenditures_plot,
             rx_expenditure_table %>%
               mutate(
                 year = str_sub(year, 3, 4)
               ) %>%
               mutate(insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Human',
                                                                'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                                    drug %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                                'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                                'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                                    str_detect(drug, 'Insulin') ~ 'Insulin',
                                                    TRUE ~ 'Not Insulin')) |>
               filter(drug != 'Other Oral Medications',
                      drug %!in% c('Insulin/Other', 'Unknown', 'Unkown/Human'),
                      age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                      drug != 'None') %>%
               group_split(stratifier) %>%
               map(
                 ~filter(.x,
                         !is.na(Level),
                         group != '<N/A>') %>%
                   # complete(drug, insulin_indicator, year, group, fill = list(Mean = NA,
                   #                                                            Stratifier = unique(.x$stratifier))) %>%
                   ggplot(aes(x = year, y = Mean, color = group, group = group)) +
                   #geom_point() +
                   geom_line() +
                   facet_wrap(~drug, scales = 'free_y')+
                   theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
                         plot.caption = element_text(hjust = 0)) +
                   labs(y = 'CPI Adjusted $',
                        caption = str_wrap('Average amount spent out-of-pocket on each type of medication per prescription.
                                           (Sum of total amount spent on a given medication divided by the number
                                           of prescriptions.)
                                           Dollar amounts have been adjusted to 2021 dollars. ', 80))
               )

             ),

  tar_target(aa_people_expenditures_plot,
             people_expenditure_table %>%
               mutate(
                 year = str_sub(year, 3, 4)
               ) |>
               filter(
                      age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+')
                      )%>%
               group_split(stratifier) %>%
               map(
                 ~filter(.x,

                         group != '<N/A>') %>%
                   # complete(drug, insulin_indicator, year, group, fill = list(Mean = NA,
                   #                                                            Stratifier = unique(.x$stratifier))) %>%
                   ggplot(aes(x = year, y = Mean, color = group, group = group)) +
                   #geom_point() +
                   geom_line() +
                   #facet_wrap(~drug)+
                   theme() +
                   labs(y = 'CPI Adjusted $',
                        caption = str_wrap('Average amount spent out-of-pocket per person with diabetes on diabetes medication.
                                           (Sum of total expenditures on diabetes medication divided by the number of people with diabetes.)
                                           Dollar amounts have been adjusted to 2021 dollars. ', 80))
               )
             ),

  tar_target(aa_person_rx_expenditures_plot,
             aa_person_drug_expenditures_total %>%
               mutate(
                 year = str_sub(year, 3, 4)
               ) %>%
               rename(drug = Level) |>
               mutate(insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Human',
                                                                'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                                    drug %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                                'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                                'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                                    str_detect(drug, 'Insulin') ~ 'Insulin',
                                                    TRUE ~ 'Not Insulin')) |>
               filter(drug != 'Other Oral Medications',
                      drug %!in% c('Insulin/Other', 'Unknown', 'Unkown/Human'),
                      age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                      drug != 'None') %>%
               group_split(stratifier) %>%
               map(
                 ~filter(.x,
                         !is.na(drug),
                         group != '<N/A>') %>%
                   # complete(drug, insulin_indicator, year, group, fill = list(Mean = NA,
                   #                                                            Stratifier = unique(.x$stratifier))) %>%
                   ggplot(aes(x = year, y = Mean, color = group, group = group)) +
                   #geom_point() +
                   geom_line() +
                   facet_wrap(~drug, scales = 'free_y')+
                   theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
                         plot.caption = element_text(hjust = 0)) +
                   labs(y = 'CPI Adjusted $',
                        caption = str_wrap('Average amount spent out-of-pocket on each type of medication per person.
                                             (Sum of total amount spent on a given medication divided by the number
                                             of people.)
                                             Dollar amounts have been adjusted to 2021 dollars. ', 80))
               )

  ),
  tar_target(save_raw_expenditures,
             arrow::write_parquet(
               select(ungroup(combined_person_drug_exp),
                      year, Insurance = COVERTYPE, drug = third_level_category_name, Cost = RXFEXPSELF, SurveyWeight = DIABWF),
                here::here('shiny/MEPS-Prescriptions/data', 'raw_person_drug_expenditures.parquet'))
             ),
  tar_target(save_people_drug_expenditures,
             arrow::write_parquet(people_drug_expenditure_table, here::here('shiny/MEPS-Prescriptions/data', 'person_drug_expenditures.parquet'))),
  tar_target(insurance_aa_person_rx_expenditures_plot,
             insurance_aa_person_drug_expenditures_total %>%
               separate(group, into = c('drug', 'COVERTYPE'), sep = '__') |>
               mutate(
                 year = str_sub(year, 3, 4)
               ) %>%
               mutate(insulin_indicator = case_when(drug %in% c('Basal', 'Bolus', 'Human',
                                                                'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                                    drug %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                                'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                                'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                                    str_detect(drug, 'Insulin') ~ 'Insulin',
                                                    TRUE ~ 'Not Insulin')) |>
               filter(drug != 'Other Oral Medications',
                      drug %!in% c('Insulin/Other', 'Unknown', 'Unkown/Human'),
                      age %in% c('Age-Adjusted', '< 18', '18 - 44', '45 - 64', '65 - 74', '75+'),
                      drug != 'None') %>%
               mutate(stratifier = 'Insurance') |>
               filter(!is.na(drug),
                      drug != '<N/A>') %>%
               # complete(drug, insulin_indicator, year, group, fill = list(Mean = NA,
               #                                                            Stratifier = unique(.x$stratifier))) %>%
               ggplot(aes(x = year, y = Mean, color = COVERTYPE, group = COVERTYPE)) +
               #geom_point() +
               geom_line() +
               facet_wrap(~drug, scales = 'free_y')+
               theme(axis.text.x = element_text(angle = 90, vjust = 0.5),
                     plot.caption = element_text(hjust = 0)) +
               labs(y = 'CPI Adjusted $',
                    caption = str_wrap('Average amount spent out-of-pocket on each type of medication per person.
                                               (Sum of total amount spent on a given medication divided by the number
                                               of people.)
                                               Dollar amounts have been adjusted to 2021 dollars. ', 80))

  )

)
