# Only need to run these once:
install.packages(c("foreign",
                  "devtools",
                  "tidyverse",
                  "readr",
                  "readxl",
                  "haven",
                  "survey"))
renv::install("e-mitchell/meps_r_pkg/MEPS")

# Run these every time you re-start R:
library(foreign)
library(devtools)
library(tidyverse)
library(readr)
library(readxl)
library(haven)
library(survey)
library(MEPS)
library(surveytable)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), 'data/ipums_rx.db')
on.exit(dbDisconnect(con, shutdown = TRUE))

dat <- tbl(con, 'full_data')
multum_class_code <- tbl(con, 'multum_class_code') %>%
  collect()

rx <- read_MEPS(year = '2018', type = 'RX') %>%
  mutate(RXDRGNAM = labelled::to_factor(RXDRGNAM))
rx <- rx %>%
  mutate(across(starts_with('TC'), ~str_remove(.x, '[\\-+0-9\\w+]*') %>% str_remove("\\s")))

fyc <- read_MEPS(year = '2020', type = 'FYC') %>%
  mutate(DIABDX_M18 = labelled::to_factor(DIABDX_M18))

# Where are GLP-1 receptor agonists?
diabetes_drugs <- c('SULFONYLUREAS',
                    'BIGUANIDES',
                    'INSULIN',
                    'ALPHA-GLUCOSIDASE INHIBITORS',
                    'THIAZOLIDINEDIONES',
                    'MEGLITINIDES',
                    'ANTIDIABETIC COMBINATIONS',
                    'DIPEPTIDYL PEPTIDASE 4 INHIBITORS',
                    'AMYLIN ANALOGS',
                    'GLP-1 RECEPTOR AGONISTS',
                    'SGLT-2 INHIBITORS')
rx <- rx %>%
  mutate(across(starts_with('TC'),
                ~case_when(.x == 'INAPPLICABLE' ~ NA,
                           .x == 'CANNOT BE COMPUTED' ~ NA,
                           TRUE ~ .x))) %>%
  mutate(third_level_category_name = as.factor(paste(TC1S1_1, TC1S1_2, TC1S2_1, TC1S3_1, TC2S1_1, TC2S1_2, TC3S1_1, sep = ", ")),
         second_level_category_name = paste(TC1S1, TC1S2, TC2S1, TC1S3, TC2S1, TC2S2, TC3S1, sep = ', ')) %>%
  filter(str_detect(third_level_category_name, paste(diabetes_drugs, collapse = "|"))) %>%
  mutate(third_level_category_name = str_remove(third_level_category_name, ', NA, NA, NA, NA, NA, NA'))


joined_person_rx <- full_join(rx, fyc) %>%
  filter(DIABW20F > 0)


dat_srvy <- svydesign(
  ids = ~VARPSU,
  weights = ~DIABW20F,
  strata = ~VARSTR,
  data = joined_person_rx,
  nest = TRUE
)

set_survey('dat_srvy')
set_output(max_levels = 2e5)
surveytable::set_count_int()
rx_list <- tab_subset('RXDRGNAM', 'DIABDX_M18')
rx_tab <- bind_rows(rx_list, .id = 'DCSDIABDX')

third_level_list <- tab_subset('third_level_category_name', 'DIABDX_M18')
third_level_tab <- bind_rows(third_level_list, .id = 'DCSDIABDX')



# Set options to deal with lonely psu
options(survey.lonely.psu='adjust');


# Download Stata (.dta) zip file
url = "https://meps.ahrq.gov/data_files/pufs/h192ssp.zip"
download.file(url, temp <- tempfile())

# Unzip and save to temporary folder
meps_file = unzip(temp, exdir = tempdir())

# Alternatively, this will save a permanent copy of the file to the local folder "C:/MEPS/R-downloads"
# meps_file = unzip(temp, exdir = "C:/MEPS/R-downloads")

# Read the .dta file into R
h192 = read.xport(meps_file)

# Download Stata (.dta) zip file
url = "https://meps.ahrq.gov/data_files/pufs/h188assp.zip"
download.file(url, temp <- tempfile())

# Unzip and save to temporary folder
meps_file = unzip(temp, exdir = tempdir())

# Alternatively, this will save a permanent copy of the file to the local folder "C:/MEPS/R-downloads"
# meps_file = unzip(temp, exdir = "C:/MEPS/R-downloads")

# Read the .dta file into R
h188a = read.xport(meps_file)


# Identify Narcotic analgesics or Narcotic analgesic combos using therapeutic classification (tc) codes

narc = h188a %>%
  filter(TC1S1_1 %in% c(60, 191)) %>%
  select(DUPERSID, RXRECIDX, LINKIDX, TC1S1_1, RXXP16X, RXSF16X)

head(narc)
table(narc$TC1S1_1)

# Sum data to person-level

narc_pers = narc %>%
  group_by(DUPERSID) %>%
  summarise(tot = sum(RXXP16X),
            oop = sum(RXSF16X),
            n_purchase = n()) %>%
  mutate(third_payer = tot - oop,
         any_narc = 1)

head(narc_pers)

# Merge the person-level expenditures to the FY PUF to get complete PSUs, Strata

fyc = h192 %>% select(DUPERSID, VARSTR, VARPSU, PERWT16F)

narc_fyc = full_join(narc_pers, fyc, by = "DUPERSID")

head(narc_fyc)


# Define the survey design

mepsdsgn = svydesign(
  id = ~VARPSU,
  strata = ~VARSTR,
  weights = ~PERWT16F,
  data = narc_fyc,
  nest = TRUE)

# Calculate estimates on expenditures and use

svymean(~n_purchase + tot + oop + third_payer,
        design = subset(mepsdsgn, any_narc == 1))
svymean(~n_purchase + tot + oop + third_payer,
        design = mepsdsgn, na.rm = TRUE)

svytotal(~n_purchase + tot + oop + third_payer,
         design = subset(mepsdsgn, any_narc == 1))

svytotal(~n_purchase + tot + oop + third_payer,
         design = mepsdsgn, na.rm = TRUE)
