#' Make and load a target
#'
#' @param names
#'
#' @return
#' @export
#'
#' @examples
tar_remake <- function(names) {
  tar_make( {{ names }}, reporter = 'timestamp_positives' )
  tar_load( {{ names }}, envir = .GlobalEnv )
}

`%!in%` <- Negate(`%in%`)

#' Identify columns with all NAs
#'
#' @param data
#'
#' @return
#' @export
#'
#' @examples
notNAFunc <- function(data) {
  not_na <- data %>%
    summarize(across(everything(), ~all(!is.na(.x)))) %>%
    collect()

  not_na_indices <- which(unlist(not_na) == TRUE)

  not_na_indices
}


#' Download data from MEPS
#'
#' Download RX and FYC files from MEPS, filter drugs to drugs of interest, and join to
#' FYC file
#'
#' @param year Year of data to download
#' @param multum_class_code Multum class code table for joining to data
#' @param diabetes_drugs List of drugs to match for in the RX data
#'
#' @return A prescription level data frame with person level information
#' @export
#'
#' @examples
importMEPS <- function(year, multum_class_code, diabetes_drugs,
                       manual_coded_drug_names) {

  if (year <= 2013) {
    rx <- read_MEPS(year = year, type = 'RX')
    addendum_file_num <- year - 1995
    addendum <- paste0('H68F', addendum_file_num)
    addendum_file <- read_MEPS(addendum)
    rx <- select(rx, -starts_with('TC'), -PREGCAT, -starts_with('RXDRGNAM'))
    rx <- left_join(rx, addendum_file, by = join_by(RXRECIDX, DUPERSID))

  } else {
    rx <- read_MEPS(year = year, type = 'RX')
  }

  fyc <- read_MEPS(year = year, type = 'FYC')

  fyc <- fyc %>%
    select(DUID, PID, DUPERSID,
           starts_with('PANEL'),
           starts_with('PERWT'),
           starts_with('VARSTR'),
           starts_with('VARPSU'), # Join variables
           starts_with('AGE'),
           starts_with('RACE'),
           starts_with('HISPAN'),
           starts_with('SEX'),
           starts_with('VARPSU'), # PSU
           starts_with('VARSTR'), # Strata
           starts_with('INSCOV'), # Insurance coverage variable
           starts_with('HIDEG'), # Highest degree - want only HIDEGYR and HIDEG
           starts_with('EDUYRDG'), #
           starts_with('TTLP'), # Total income
           starts_with('POVCAT'), # Categorical poverty status
           starts_with('REGION'),
           starts_with('DIAB'), # 3 different variable names for diabetes, all start with DIABDX. this should also grab the DAIBW weight vari
           starts_with('DSDIA'), # Diagnosed with diabetes by a physician
           contains('DIABDX'), # Diabetes diagnosis
           -contains('AGE31X'),
           -contains('AGE42X'),
           -contains('AGE53x'), # Remove some of the extra age variables
           -contains('DSFLNV'), # Remove the never had flu shot
           -contains('REGION31'), # Remove excess region variables
           -contains('REGION42'),
           -contains('REGION53'),
           starts_with('MCDEV'), # Medicaid/SCHIP
           starts_with('MCREV'), # Medicaire
           starts_with('PREV'),
           starts_with('TRIEV'),
           starts_with('GVAEV'),
           starts_with('GVBEV'),
           starts_with('GVCEV'),
           starts_with('RXTOT'),
           starts_with('OPAEV'),
           starts_with('OPBEV'),
           starts_with('PERWT'), # Person weight level. Not sure I need this, since just working with DCS variables
           )
  if ('TC1S3' %in% names(rx)) {
    rx <- rx %>%
      # First level
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC1' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name1 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC2' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name2 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC3' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name3 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC1S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      # Second levels
      rename(second_level_category_name1 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC1S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name2 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC1S3' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name3 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC2S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name4 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC2S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name5 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC3S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name6 = second_level_category_name) %>%
      # Third level
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S1_1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name1 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name2 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S2_1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name3 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S3_1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name4 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC2S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name5 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC2S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name6 = third_level_category_name)

  } else {
    rx <- rx %>%
      # First level
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC1' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name1 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC2' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name2 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('TC3' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name3 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC1S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      # Second levels
      rename(second_level_category_name1 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC1S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name2 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC2S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name4 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC2S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name5 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('TC3S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name6 = second_level_category_name) %>%
      # Third level
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S1_1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name1 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name2 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC1S2_1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name3 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC2S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name5 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('TC2S1_2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name6 = third_level_category_name)

  }
   # Filter down to just diabetes drugs
  rx <- rx %>%
    filter(if_any(contains('level_category'),
                  ~str_detect(.x, paste(diabetes_drugs, collapse = '|')))) %>%
    rename(third_level_category_name = third_level_category_name1) %>%
    select(-ends_with('name1'), -ends_with('name2'), -ends_with('name3'),
           -ends_with('name4'), -ends_with('name5'),
           -ends_with('name6'), -ends_with('name7'), -starts_with('PHART'))


  joined_person_rx <- full_join(rx, fyc)

  joined_person_rx <- joined_person_rx %>%
    rename_with(~str_extract(.x, 'DIABDX'),
                starts_with('DIABDX')) |>
    # If a person has no drugs listed, they get a "none"
    mutate(third_level_category_name = case_when(is.na(third_level_category_name) ~ 'None',
                                                 TRUE ~ third_level_category_name))

  names(joined_person_rx) <- str_remove(names(joined_person_rx), '[0-9]{2}')
  joined_person_rx

}

recodeInsulin <- function(raw_data, manual_coded_drug_names) {
  raw_data <- left_join(
    raw_data,
    manual_coded_drug_names,
    by = c('third_level_category_name',
           'RXDRGNAM',
           'RXNAME')
  )

  temp <- raw_data %>%
    # Comment out this mutate to switch back to type and duration for insulin separate
    mutate(drug_class_1 = case_when(third_level_category_name == 'INSULIN' ~ paste0(drug_class_1, '/', drug_class_2),
                                    TRUE ~ drug_class_1),
           drug_class_2 = case_when(third_level_category_name == 'INSULIN' ~ NA,
                                    TRUE ~ drug_class_2)) |>
    mutate(drug_class = coalesce(drug_class_1, third_level_category_name))
  antidiabetic_combos <- temp |>
    filter(third_level_category_name == 'ANTIDIABETIC COMBINATIONS') |>
    select(-drug_class) |>
    pivot_longer(c(drug_class_1, drug_class_2),
                 names_to = 'remove_me',
                 values_to = 'drug_class') |>
    select(-remove_me)

    nonantidiabetic_combos <- temp |>
      filter(third_level_category_name != 'ANTIDIABETIC COMBINATIONS')

    out <- bind_rows(antidiabetic_combos, nonantidiabetic_combos)
    out |>
    mutate(drug_class = str_replace_all(drug_class, 'Biguanide', 'BIGUANIDES')) %>%
    mutate(drug_class = str_replace_all(drug_class, 'Sulfonylureas', 'SULFONYLUREAS')) %>%
    # Uncomment this bit if you want to split type/duration
    # mutate(insulin_type = case_when(drug_class %in% c('Bolus', 'Basal', 'Pre-mixed') ~ 'Duration',
    #                                 drug_class %in% c('Human', 'Analog', 'Human and Analog') ~ 'Type',
    #                                 drug_class %in% c('Insulin', 'Unknown') ~ 'Unknown Insulin',
    #                                 TRUE ~ 'Not Insulin')) |>
    # filter(insulin_type != 'Unknown Insulin') |>
    # Uncomment to reclassify all Insulins together
    # mutate(drug_class = case_when(drug_class == 'INSULIN' ~ 'Insulin (Unknown)',
    #                               str_detect(drug_class, 'Bolus') ~ 'Insulin',
    #                               str_detect(drug_class, 'Basal') ~ 'Insulin',
    #                               str_detect(drug_class, 'Pre-mixed') ~ 'Insulin',
    #                               str_detect(drug_class, 'Insulin') ~ 'Insulin',
    #                               TRUE ~ drug_class)) %>%
    select(-third_level_category_name, -drug_class_1, -drug_class_2) %>%
    rename(third_level_category_name = drug_class) |>
    filter(third_level_category_name %!in% c('remove', 'Remove'))

}

#' Make MEPS variable!s factors
#'
#' @param meps_data
#'
#' @return
#' @export
#'
#' @examples
createFactors <- function(meps_data) {

  # Recode education
  meps_data <- meps_data %>%
    mutate(
      across(
        starts_with('HIDE'),
        ~as.factor(.x) %>%
          recode_factor(
          `-15` = 'Cannot be computed',
          `-9` = 'Not ascertained',
          `-8` = 'Do not know',
          `-7` = 'Refused',
          `-1` = 'Inapplicable',
          `1` = 'No Degree',
          `2` = 'GED',
          `3` = 'HS',
          `4` = 'Bachelor',
          `5` = 'Masters',
          `6` = 'Doctorate',
          `7` = 'Other',
          `8` = 'Under 16, inapplicable'
        ) %>%
          fct_collapse(
            `Less than high school` = c('No Degree', 'GED', 'Under 16, inapplicable'),
            `High school` = c('HS'),
            `Greater than HS` = c('Bachelor', 'Masters', 'Doctorate', 'Other'),
            `Not available` = c(
              'Not ascertained',
              'Do not know',
              'Refused',
              'Inapplicable',
              'Cannot be computed'
            )
          )
      ),
      across(
        starts_with('EDUY'),
        ~as.factor(.x) %>%
          recode_factor(
          `-15` = 'Cannot be computed',
          `-9` = 'Not ascertained',
          `-8` = 'Do not know',
          `-7` = 'Refused',
          `-1` = 'Inapplicable',
          `1` = '<= 8th grade',
          `2` = 'No High School',
          `3` = 'GED',
          `4` = 'High School',
          `5` = 'Some college',
          `6` = 'Associate Occupational/Technical',
          `7` =  'Associate Academic',
          `8` = 'Bachelor',
          `9` = 'Master, Professional, or Doctorate',
          `10` = 'Child under 5 years old'
        ) %>%
          fct_collapse(
            `Less than high school` = c('<= 8th grade', 'No High School', 'GED'),
            `High school` = c('High School', 'Some college'),
            `Greater than HS` = c(
              'Associate Occupational/Technical',
              'Associate Academic',
              'Bachelor',
              'Master, Professional, or Doctorate'
            ),
            `Not available` = c(
              'Not ascertained',
              'Do not know',
              'Refused',
              'Inapplicable',
              'Cannot be computed',
              'Child under 5 years old'
            )
          )
      )
    )

  # Recode race
  meps_data <- meps_data %>%
    mutate(
      across(
        starts_with('RACETHNX'),
        ~as.factor(.x) %>%
          recode_factor(
          `1` = 'Hispanic',
          `2` = 'Black/not Hispanic',
          `3` = 'Asian/not Hispanic',
          `4` = 'Other Race/not Hispanic',
        ) %>% fct_expand('White/not Hispanic')
      ),
      across(
        starts_with('RACETHX'),
        ~as.factor(.x) %>%
          recode_factor(
          `1` = 'Hispanic',
          `2` = 'White/not Hispanic',
          `3` = 'Black/not Hispanic',
          `4` = 'Asian/not Hispanic',
          `5` = 'Other Race/not Hispanic',
        )
      )
    )
  # Sex
  meps_data <- meps_data %>%
    mutate(SEX = recode_factor(as.factor(SEX),
                               `1` = 'Male',
                               `2` = 'Female'))

  # Insurance
  meps_data <- meps_data %>%
    mutate(
      across(
        starts_with('INSCOV'),
        ~ as.factor(.x) %>%
          recode_factor(`1` = 'Any private',
                        `2` = 'Public only',
                        `3` = 'Uninsured')
      ),

      across(
        starts_with(
          c(
            'MCDEV',
            'MCREV',
            'GVAEV',
            'GVBEV',
            'GVCEV',
            'OPAEV',
            'OPBEV',
            'PRVE',
            'TRIEV'
          )
        ),
        ~ as.factor(.x) %>%
          recode_factor(`1` = 'YES',
                        `2` = 'NO')
      )
    )

  # Diabetes
  meps_data <- meps_data %>%
    mutate(
      across(
        starts_with('DIABDX'),
        ~as.factor(.x) %>%
          recode_factor(
          `-15` = 'Cannot be computed',
          `-9` = 'Not ascertained',
          `-8` = 'Do not know',
          `-7` = 'Refused',
          `-1` = 'Inapplicable',
          `1` = 'YES',
          `2` = 'NO'
        )
      )
    )

  # Recode POVCAT
  meps_data <- meps_data %>%
    mutate(
      POVCAT =
        as.factor(POVCAT) %>%
          recode_factor(
            `1` = 'Poor (<100% of poverty line)',
            `2` = 'Near poor',
            `3` = 'Low income',
            `4` = 'Middle income',
            `5` = 'High income'
          ) |>
        fct_collapse(
          `Poor (<100% of poverty line)` = "Poor (<100% of poverty line)",
          `Low income (100% - <200% of poverty line)` = c('Near poor', 'Low income'),
          `Middle income (200% - 399% of poverty line)` = c('Middle income'),
          `High income (>= 400% of poverty line)` = 'High income'
        )
      )

  # Recode expenditures
  meps_data <- meps_data |>
    mutate(
      Copay = as.factor(
                     case_when(RXFEXPSELF == 0 ~ '0',
                               RXFEXPSELF <= 10 ~ '>0-10',
                               RXFEXPSELF <= 20 ~ '>10-20',
                               RXFEXPSELF <= 30 ~ '>20-30',
                               RXFEXPSELF <= 75 ~ '>30-75',
                               RXFEXPSELF > 75 ~ '>75')
      )
    )

  # Rename variables
  if ('HIDEG' %!in% colnames(meps_data)) {
    meps_data <- meps_data %>%
      rename_with(~str_replace(.x, 'EDUCYR', 'HIDEG'),
                  starts_with('EDUCYR')) %>%
      rename_with(~str_replace(.x, 'EDUYRDG', 'HIDEG'),
                  starts_with('EDUYRDG')) %>%
      rename_with(~str_replace(.x, 'HIDEGYR', 'HIDEG'),
                  starts_with('HIDEGY'))
  }
  # Prior to 2012, hispanic and white are coded separately for some reason
  meps_data <- meps_data %>%
    rename_with(~str_replace(.x, 'RACETHX', 'RACETHNX'),
                starts_with('RACETHX'))
  if ('HISPANX' %in% colnames(meps_data) &
      'RACEX' %in% colnames(meps_data)) {
    meps_data <- meps_data %>%
      mutate(RACETHNX = case_when(RACEX == 1 & HISPANX == 2 ~ 'White/not Hispanic',
                                  TRUE ~ RACETHNX)) %>%
      mutate(RACETHNX = as.factor(RACETHNX))
  }

  # Make sure they're factors
  meps_data <- meps_data %>%
    mutate(across(c('DIABDX', 'SEX', 'RACETHNX'), ~as.factor(.x))) %>%
    mutate(AGE = case_when(AGEX < 18 ~ '< 18',
                           AGEX < 44 ~ '18 - 44',
                           AGEX < 64 ~ '45 - 64',
                           AGEX < 75 ~ '65 - 74',
                           AGEX >= 75 ~ '75+',
                           TRUE ~ NA)) %>%
    #filter(AGE != '< 18') %>%
    mutate(AGE = as.factor(AGE))

  # Clean up some of the drug names
  meps_data <- meps_data %>%
    mutate(
      third_level_category_name = case_when(
        third_level_category_name %in% c(
          'ALPHA-GLUCOSIDASE INHIBITORS',
          'AMYLIN ANALOGS',
          'MEGLITINIDES'
        ) ~ 'Other',
        TRUE ~ third_level_category_name
      ),
      insulin_indicator = case_when(third_level_category_name %in% c('Basal', 'Bolus', 'Human',
                                                                     'Analog', 'Pre-mixed', 'Human and Analog') ~ 'Insulin',
                                    third_level_category_name %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                                     'Bolus/Human', 'Pre-Mixed/Human', 'Pre-Mixed/Analog',
                                                                     'Pre-Mixed/Human And Analog') ~ 'Insulin',
                                    str_detect(third_level_category_name, 'Insulin') ~ 'Insulin',
                                    TRUE ~ 'Not Insulin')#,
      # insulin_group = paste0(insulin_indicator, ' - ', insulin_type)
    )

}

#' Aggregate prescription level up to person level
#'
#' @param meps.data Prescription level data
#'
#' @return
#' @export
#'
#' @examples
personLevelMEPS <- function(meps.data, pooled = FALSE) {
  if (pooled == TRUE) {
    if ('PSU9621' %in% colnames(meps.data)) {
      meps.data %>%
        group_by(
          DUPERSID,
          PSU9621,
          DIABWF,
          poolwt,
          STRA9621,
          DIABDX,
          SEX,
          HIDEG,
          RACETHNX,
          AGE,
          third_level_category_name
        ) %>%
        count() %>%
        mutate(presence = 1) %>%
        pivot_wider(
          id_cols = c(
            DUPERSID,
            PSU9621,
            DIABWF,
            poolwt,
            STRA9621,
            DIABDX,
            SEX,
            HIDEG,
            RACETHNX,
            AGE
          ),
          names_from = third_level_category_name,
          values_from = presence,
          values_fill = 0
        ) %>%
        select(-starts_with('NA'))
    } else {
      meps.data %>%
        group_by(
          DUPERSID,
          VARPSU,
          DIABWF,
          poolwt,
          VARSTR,
          DIABDX,
          SEX,
          HIDEG,
          RACETHNX,
          AGE,
          third_level_category_name
        ) %>%
        count() %>%
        mutate(presence = 1) %>%
        pivot_wider(
          id_cols = c(
            DUPERSID,
            VARPSU,
            DIABWF,
            poolwt,
            VARSTR,
            DIABDX,
            SEX,
            HIDEG,
            RACETHNX,
            AGE
          ),
          names_from = third_level_category_name,
          values_from = presence,
          values_fill = 0
        ) %>%
        select(-starts_with('NA'))
    }
  } else {
  if ('PSU9621' %in% colnames(meps.data)) {
    meps.data %>%
      group_by(
        DUPERSID,
        PSU9621,
        DIABWF,
        #PERWTF,
        STRA9621,
        DIABDX,
        SEX,
        HIDEG,
        RACETHNX,
        AGE,
        third_level_category_name
      ) %>%
      count() %>%
      mutate(presence = 1) %>%
      pivot_wider(
        id_cols = c(
          DUPERSID,
          PSU9621,
          DIABWF,
          #PERWTF,
          STRA9621,
          DIABDX,
          SEX,
          HIDEG,
          RACETHNX,
          AGE
        ),
        names_from = third_level_category_name,
        values_from = presence,
        values_fill = 0
      ) %>%
      select(-starts_with('NA'))
  } else {
    meps.data %>%
      group_by(
        DUPERSID,
        VARPSU,
        DIABWF,
        #PERWTF,
        VARSTR,
        DIABDX,
        SEX,
        HIDEG,
        RACETHNX,
        AGE,
        COVERTYPE,
        third_level_category_name
      ) %>%
      count() %>%
      mutate(presence = 1) %>%
      pivot_wider(
        id_cols = c(
          DUPERSID,
          VARPSU,
          DIABWF,
          #PERWTF,
          VARSTR,
          DIABDX,
          SEX,
          HIDEG,
          COVERTYPE,
          RACETHNX,
          AGE
        ),
        names_from = third_level_category_name,
        values_from = presence,
        values_fill = 0
      ) %>%
      select(-starts_with('NA'))
  }
}

  }

#' Create survey object from data
#'
#' Read in table from db for a given year of data.
#'
#' @param db.file.path
#' @param year
#' @param survey.dat Only included to link to the survey_dat target
#'
#' @return
#' @export
#'
#' @examples
createSurveyDesign <- function(dat, diabetes_drugs) {

  dat <- dat %>%
    ungroup() %>%
    mutate(across(ends_with('name'), ~as.factor(.x)),
           across(any_of(diabetes_drugs), ~as.factor(.x)),
           across(contains('DIABDX'), ~as.factor(.x)),
           across(contains('RXNAME'), ~as.factor(.x)))
  # %>%
  #   filter(DIABWF > 0,
  #          !is.na(DIABWF))

  dat_srvy <- svydesign(
    ids = ~VARPSU,
    weights = ~DIABWF,
    strata = ~VARSTR,
    data = dat,
    nest = TRUE
  ) |>
    subset(subset = DIABDX == '2')
  dat_srvy
}

#' Create pooled year weights
#'
#'
#' @param data All years data
#' @param years Years to filter down to and pool
#'
#' @return
#' @export
#'
#' @examples
createPooledWeights <- function(data, years) {
  data <- filter(data, year %in% years)
  # of years
  num_of_years <- length(unique(data$year))
  data <- data %>%
    mutate(poolwt = DIABWF / num_of_years)
}

#' Combine dashboard data with dictionary
#'
#' @param consol_dat
#' @param data_dict
#'
#' @return
#' @export
#'
#' @examples
joinDataWithDictionary <- function(consol_dat, data_dict) {
  consol_dat %>%
    mutate(across(ends_with('ID'), ~as.character(.x))) %>%
    # Get actual names for the IndicatorID
    inner_join(data_dict %>%
                 filter(VariableTypeID == 1) %>%
                 select(Name, vid),
               by = c('IndicatorID' = 'vid')) %>%
    rename(IndicatorName = Name) %>%
    left_join(data_dict %>%  # Suppress ID join
                filter(VariableTypeID == 4) %>%
                select(Name, vid),
              by = c('SuppressID' = 'vid')) %>%
    rename(SuppressName = Name) %>%
    left_join(data_dict %>%  # Race ID join
                filter(VariableTypeID == 5) %>%
                select(Name, vid),
              by = c('RaceID' = 'vid')) %>%
    rename(RaceName = Name) %>%
    left_join(data_dict %>%  # Gender ID join
                filter(VariableTypeID == 6) %>%
                select(Name, vid),
              by = c('GenderID' = 'vid')) %>%
    rename(GenderName = Name) %>%
    left_join(data_dict %>%  # Age ID join
                filter(VariableTypeID == 7) %>%
                select(Name, vid),
              by = c('AgeID' = 'vid')) %>%
    rename(AgeName = Name)%>%
    left_join(data_dict %>%  # Data source ID join
                filter(VariableTypeID == 8) %>%
                select(Name, vid),
              by = c('DataSourceID' = 'vid')) %>%
    rename(DatasourceName = Name) %>%
    left_join(data_dict %>%  # Estimate ID join
                filter(VariableTypeID == 9) %>%
                select(Name, vid),
              by = c('EstimateID' = 'vid')) %>%
    rename(EstimateName = Name) %>%
    left_join(data_dict %>%  # EducationID join
                filter(VariableTypeID == 10) %>%
                select(Name, vid),
              by = c('EducationID' = 'vid')) %>%
    rename(EducationName = Name) %>%
    left_join(data_dict %>%  # MiscID join
                filter(VariableTypeID == 11) %>%
                select(Name, vid),
              by = c('MiscID' = 'vid')) %>%
    rename(MiscName = Name) %>%
    left_join(data_dict %>%  # Dataset ID join
                filter(VariableTypeID == 20) %>%
                select(Name, vid),
              by = c('DatasetID' = 'vid')) %>%
    rename(DatasetName = Name)
}

formatGrasp1 <- function(data) {
  data %>%
    filter(group %!in% c('Not available', '<N/A>')) %>%
    pivot_longer(c(Number, Percent),
                 names_to = 'EstimateName',
                 values_to = 'Estimate') %>%
    mutate(SE = case_when(EstimateName == 'Percent' ~ SE.1,
                          TRUE ~ SE),
           LL = case_when(EstimateName == 'Percent' ~ LL.1,
                          TRUE ~ LL),
           UL = case_when(EstimateName == 'Percent' ~ UL.1,
                          TRUE ~ UL),
           EstimateName = case_when(EstimateName == 'Number' ~ 'Count',
                                    TRUE ~ EstimateName)) %>%
    select(-LL.1, -UL.1, -SE.1)
}

formatGrasp2 <- function(data, indicator.level = c('rx', 'person')) {
  data %>%
    mutate(indicator.level = indicator.level) %>%
    mutate(across(where(is.character), ~str_remove_all(.x,','))) %>%
    mutate(across(c(Estimate, SeEstimate, LowerLimit, UpperLimit), ~as.numeric(.x))) %>%
    mutate(tempIndicatorName = str_to_sentence(IndicatorName) %>%
             str_replace(., 'Sglt', 'SGLT') %>%
             str_replace(., 'Glp', 'GLP') %>%
             str_replace(., 'Biguanides', 'Metformin')) %>%
    mutate(
      IndicatorName = case_when(
        indicator.level == 'rx' ~ glue::glue('Total prescriptions: {tempIndicatorName}'),
        indicator.level == 'person' ~ glue::glue('Number of people prescribed: {tempIndicatorName}')
      )
    ) %>%
    select(YearID, fipscode, IndicatorName,
           RaceName, GenderName, EducationName,
           AgeName, EstimateName, Estimate, SeEstimate, LowerLimit,
           UpperLimit, contains('Flag'))%>%
    rename_with(~'Flag', contains('Flag'))

}

#' Title
#'
#' Flags:
#' Cx: suppress count and rate
#' Px: suppress percent
#' Pc: footnote percent - compliment
#' R: Suppress ALL estimates
#' *df: Review count/percent - degrees of freedom
#'
#' @param data
#'
#' @return
#' @export
#'
#' @examples
suppressEstimates <- function(data) {
  data %>%
    mutate(SuppressName = case_when(EstimateName == 'Percent' & str_detect(Flag, 'Px') ~ 'Suppressed',
                                    EstimateName == 'Count' & str_detect(Flag, 'Cx') ~ 'Suppressed',
                                    EstimateName == 'Percent' & str_detect(Flag, 'Pdf') ~ 'Suppressed',
                                    EstimateName == 'Count' & str_detect(Flag, 'Cdf') ~ 'Suppressed',
                                    str_detect(Flag, 'R') ~ 'Suppressed',
                                    TRUE ~ 'Not Suppressed')
           )
}


#' Combine new dashboard data with dictionary
#'
#' @param new_consol_dat
#' @param data_dict
#'
#' @return
#' @export
#'
#' @examples
reverseJoinDataWithDictionary <- function(new_consol_dat, data_dict) {
  new_consol_dat %>%
    # Get actual names for the IndicatorID
    inner_join(data_dict %>%
                 filter(VariableName == 'Indicator') %>%
                 select(Name, vid),
               by = c('IndicatorName' = 'Name')) %>%
    rename(IndicatorID = vid) %>%
    left_join(data_dict %>%  # Suppress ID join
                filter(VariableName == 'Suppression') %>%
                select(Name, vid),
              by = c('SuppressName' = 'Name')) %>%
    rename(SuppressID = vid) %>%
    left_join(data_dict %>%  # Race ID join
                filter(VariableName == 'Race') %>%
                select(Name, vid),
              by = c('RaceName' = 'Name')) %>%
    rename(RaceID = vid) %>%
    left_join(data_dict %>%  # Gender ID join
                filter(VariableName == 'Gender') %>%
                select(Name, vid),
              by = c('GenderName' = 'Name')) %>%
    rename(GenderID = vid) %>%
    left_join(data_dict %>%  # Age ID join
                filter(VariableName == 'Age') %>%
                select(Name, vid),
              by = c('AgeName' = 'Name')) %>%
    rename(AgeID = vid) %>%
    left_join(data_dict %>%  # Data source ID join
                filter(VariableName == 'Datasource') %>%
                select(Name, vid),
              by = c('DatasourceName' = 'Name')) %>%
    rename(DataSourceID = vid) %>%
    left_join(data_dict %>%  # Estimate ID join
                filter(VariableName == 'Estimate') %>%
                select(Name, vid),
              by = c('EstimateName' = 'Name')) %>%
    rename(EstimateID = vid) %>%
    left_join(data_dict %>%  # EducationID join
                filter(VariableName == 'Education') %>%
                select(Name, vid),
              by = c('EducationName' = 'Name')) %>%
    rename(EducationID = vid) %>%
    left_join(data_dict %>%  # MiscID join
                filter(VariableName == 'Misc') %>%
                select(Name, vid),
              by = c('MiscName' = 'Name')) %>%
    rename(MiscID = vid) %>%
    left_join(data_dict %>%  # Dataset ID join
                filter(VariableName == 'Dataset') %>%
                select(Name, vid),
              by = c('DatasetName' = 'Name')) %>%
    rename(DatasetID = vid)
}

#' Read in IPUMS MEPS data
#'
#' Read in IPUMS MEPS data on RX and person level, then join them together.
#' Also add in Multim Lexicon data for drugs.
#' Split data by year and write to data.base to try and circumvent in memory issues
#'
#' @param db.file.path
#'
#' @return
#' @export
#'
#' @examples
importIPUMSMEPS <- function(db.file.path) {
  # Create or connect to the DuckDB database
  con <- dbConnect(duckdb::duckdb(), db.file.path)
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dat <- tbl(con, 'full_data')
  multum_class_code <- tbl(con, 'multum_class_code')

  # Join Multum Lexicon table to codes to get classes

  data_rx_med <- filter(dat, RECTYPE == 'F') %>%
    select(notNAFunc(.)) %>%
    select(YEARF, DUPERSIDF, MEPSIDF, RXNAME, starts_with('RXFEX'),
           RXDRGNAM, starts_with('MULT')) %>%
    mutate(rx_med = 1)
  # MEPSID is hte UID for person
  data_person <- filter(dat, RECTYPE == 'P')%>%
    select(notNAFunc(.)) %>%
    select(-PERNUM, -DUID, -PID, -PANEL, -ends_with('PLD'),
           -PANELYR, -RELYR, -SAQWEIGHT)


  # I think this data isn't needed?
  # MEPSIDM is the UID for presc_med
  # data_presc_med <- filter(data, RECTYPE == 'M') %>%
  #   select(where(not_all_na))
  # joined_person_presc <- left_join(data_presc_med, data_person, by = c('MEPSIDM' = 'MEPSID'))
  #
  # joined_person_presc %>%
  #   group_by(MEPSIDM, RXDRGNAM) %>%
  #   count()

  # Person level data joined with prescription data
  # Am I aggregating person level or
  # I don't think PERNUM is the right variable to be joining on


  data_rx_med <- data_rx_med %>%
    left_join(
      distinct(multum_class_code, first_level_category_name, first_level_category_id),
      by = c('MULTC1' = 'first_level_category_id')
    ) %>%
    left_join(
      distinct(multum_class_code, second_level_category_name, second_level_category_id),
      by = c('MULTC1S1' = 'second_level_category_id')
    ) %>%
    rename(second_level_category_name1 = second_level_category_name) %>%
    left_join(
      distinct(multum_class_code, second_level_category_name, second_level_category_id),
      by = c('MULTC1S2' = 'second_level_category_id')
    ) %>%
    rename(second_level_category_name2 = second_level_category_name) %>%
    left_join(
      distinct(multum_class_code, third_level_category_name, third_level_category_id),
      by = c('MULTC1S1S1' = 'third_level_category_id')
    ) %>%
    rename(third_level_category_name1 = third_level_category_name) %>%
    left_join(
      distinct(multum_class_code, third_level_category_name, third_level_category_id),
      by = c('MULTC1S1S2' = 'third_level_category_id')
    ) %>%
    rename(third_level_category_name2 = third_level_category_name) %>%
    left_join(
      distinct(multum_class_code, third_level_category_name, third_level_category_id),
      by = c('MULTC1S2S1' = 'third_level_category_id')
    ) %>%
    rename(third_level_category_name3 = third_level_category_name) %>%
    mutate(second_level_category_name = paste(second_level_category_name1, second_level_category_name2, sep = ", "),
           third_level_category_name = paste(third_level_category_name1, third_level_category_name2, third_level_category_name3, sep = ", ")) %>%
    select(-second_level_category_name1, -second_level_category_name2, -third_level_category_name1, -third_level_category_name2,
           -third_level_category_name3)

  diab_rx_med <- data_rx_med %>%
    filter(str_detect(third_level_category_name,
                      'SULFONYLUREAS|BIGUANIDES|INSULIN|ALPHA-GLUCOSIDASE INHIBITORS|THIAZOLIDINEDIONES|MEGLITINIDES|ANTIDIABETIC COMBINATIONS|DIPEPTIDYL PEPTIDASE 4 INHIBITORS|AMYLIN ANALOGS|GLP-1 RECEPTOR AGONISTS|SGLT-2 INHIBITORS'))

  joined_person_rx <- full_join(diab_rx_med, data_person, by = c('MEPSIDF' = 'MEPSID')) %>%
    mutate(across(starts_with('MULT'), ~as.integer(.x)))

  years <- joined_person_rx %>%
    pull(YEARF) %>%
    unique() %>%
    sort()

  for (i in years) {
    temp_dat <- joined_person_rx %>%
      filter(YEARF == i) %>%
      #filter(DCSDIABDX == 2) %>%
      collect()
    dbWriteTable(con, glue::glue('rx_data_{i}'), temp_dat, overwrite = TRUE)
  }
  # Can get each individuals unique medications. Could then tally the # of each type of medication a subgroup has, or
  # proportion of subgroups taking certain medications
  digest_dat <- joined_person_rx %>%
    filter(YEARF >= 2020) %>%
    #filter(DCSDIABDX == 2) %>%
    collect() %>%
    digest::digest()
  dbDisconnect(con, shutdown = TRUE)
  return(digest_dat)
}

#' Title
#'
#' @param ipums_year
#' @param multum_class_code
#' @param diabetes_drugs
#' @param manual_coded_drug_names
#'
#' @return
#' @export
#'
#' @examples
importIPUMSrx <- function(ipums_year, multum_class_code, diabetes_drugs,
                          manual_coded_drug_names) {

  db_file_path <- "data/ipums_rx.db"

  # Create or connect to the DuckDB database
  con <- dbConnect(duckdb::duckdb(), db_file_path)

  on.exit(dbDisconnect(con, shutdown = TRUE))

  dat <- tbl(con, 'full_data')
  multum_class_code <- tbl(con, 'multum_class_code') |>
    collect()
  # Join Multum Lexicon table to codes to get classes
  # RECTYPE == f is for RX data
  rx <- filter(dat, RECTYPE == 'F') %>%
    #select(notNAFunc(.)) %>%
    select(YEAR = YEARF,
           DUPERSID = DUPERSIDF,
           PANEL = PANELF,
           MEPSID = MEPSIDF,
           RXNAME,
           starts_with('RXFEX'),
           starts_with('RX'),
           RXDRGNAM,
           starts_with('MULT')) %>%
    mutate(rx_med = 1) |>
    filter(YEAR == ipums_year) |>
    collect()

  # MEPSID is hte UID for person
  # RECTYPE == P is for person level

  fyc <- filter(dat, RECTYPE == 'P')%>%
    select(notNAFunc(.)) %>%
    filter(YEAR == ipums_year) |>
    select(-PERNUM, -ends_with('PLD'),
           -PANELYR, -RELYR, -SAQWEIGHT) |>
    collect()


  fyc <- fyc %>%
    select(DUID, PID,
           DUPERSID = MEPSID,
           PANEL,
           starts_with('AGE'),
           starts_with('RACE'),
           starts_with('HISP'),
           starts_with('SEX'),
           starts_with('STRATANN'),
           starts_with('EDUC'),
           starts_with('PSUANN'),
           starts_with('COVERTYPE'), # Insurance coverage variable
           starts_with('HIDEG'), # Highest degree - want only HIDEGYR and HIDEG
           starts_with('EDUYRDG'), #
           starts_with('TTLP'), # Total income
           starts_with('POVCAT'), # Categorical poverty status
           starts_with('REGION'),
           starts_with('DIAB'), # 3 different variable names for diabetes, all start with DIABDX. this should also grab the DAIBW weight vari
           starts_with('DCSDIA'), # Diagnosed with diabetes by a physician
    )
  if ('TC1S3' %in% names(rx)) {
    rx <- rx %>%
      # First level
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('MULTC1' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name1 = first_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('MULTC1S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      # Second levels
      rename(second_level_category_name1 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('MULTC1S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name2 = second_level_category_name) %>%
      # Third level
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S1S1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name1 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S1S2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name2 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S2S1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name3 = third_level_category_name)


  } else {
    rx <- rx %>%
      # First level
      left_join(
        distinct(multum_class_code, first_level_category_name, first_level_category_id),
        by = c('MULTC1' = 'first_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(first_level_category_name1 = first_level_category_name) %>%

      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('MULTC1S1' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      # Second levels
      rename(second_level_category_name1 = second_level_category_name) %>%
      left_join(
        distinct(multum_class_code, second_level_category_name, second_level_category_id),
        by = c('MULTC1S2' = 'second_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(second_level_category_name2 = second_level_category_name) %>%
      # Third level
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S1S1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name1 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S1S2' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name2 = third_level_category_name) %>%
      left_join(
        distinct(multum_class_code, third_level_category_name, third_level_category_id),
        by = c('MULTC1S2S1' = 'third_level_category_id'),
        relationship = 'many-to-one'
      ) %>%
      rename(third_level_category_name3 = third_level_category_name)

  }
  # Filter down to just diabetes drugs
  rx <- rx %>%
    filter(if_any(contains('level_category'),
                  ~str_detect(.x, paste(diabetes_drugs, collapse = '|')))) %>%
    rename(third_level_category_name = third_level_category_name1) %>%
    select(-ends_with('name1'), -ends_with('name2'), -ends_with('name3'),
           -ends_with('name4'), -ends_with('name5'),
           -ends_with('name6'), -ends_with('name7'), -starts_with('PHART'))



  joined_person_rx <- full_join(fyc, rx, by = c('DUPERSID' = 'MEPSID')) |>
    rename(MEPSID = DUPERSID)
  # joined_person_rx <- joined_person_rx |>
  #   mutate(DIABDX = case_when(
  #     DCSDIABDX == 2 ~ 1,
  #     DCSDIABDX == 1 ~ 0,
  #     DCSDIABDX == 0 ~ -9,
  #     TRUE ~ DCSDIABDX
  #   ))
  joined_person_rx <- joined_person_rx %>%
    # rename_with(~str_extract(.x, 'DIABDX'),
    #             starts_with('DIABDX')) |>
    # If a person has no drugs listed, they get a "none"
    mutate(third_level_category_name = case_when(is.na(third_level_category_name) ~ 'None',
                                                 TRUE ~ third_level_category_name))
  # Rename some of the variables for using existing code
  joined_person_rx$DIABWF <- joined_person_rx$DIABWEIGHT

  joined_person_rx$VARPSU <- joined_person_rx$PSUANN
  joined_person_rx$VARSTR <- joined_person_rx$STRATANN

  joined_person_rx

}

#' Make MEPS variable!s factors
#'
#' @param meps_data
#'
#' @return
#' @export
#'
#' @examples
createIPUMSFactors <- function(meps_data) {

  # Recode education
  meps_data <- meps_data %>%
    mutate(
      HIDEG =
        as.factor(EDUC) |>
          fct_collapse(
            `Less than high school` = c(100:116),
            `High school` = c(200, 201, 202),
            `Greater than high school` = c(300, 301, 302, 303, 400, 500, 501, 502, 503, 504, 505),
            `Not available` = c(996:999)
          ),
      HIDEG = case_when(HIDEG == 'Not available' ~ NA,
                        TRUE ~ HIDEG)
      ) |>
    mutate(HIDEG = fct_drop(HIDEG))

  # Recode race
  meps_data <- meps_data %>%
    mutate(
      RACEA = as.factor(RACEA) |>
        fct_collapse(
          `White/not Hispanic` = 100,
          `Black/not Hispanic` = 200,
          `Asian/not Hispanic` = c(400:434),
          other_level = 'Other Race/not Hispanic'
        ),
      RACETHNX = case_when(HISPYN == 2 ~ 'Hispanic',
                           HISPYN == 1 ~ RACEA)
    )
  # Sex
  meps_data <- meps_data %>%
    mutate(SEX = recode_factor(as.factor(SEX),
                               `1` = 'Male',
                               `2` = 'Female'))

  # Insurance
  meps_data <- meps_data %>%
    mutate(
      COVERTYPE = as.factor(COVERTYPE) |>
        recode_factor(
          `1` = 'Any private',
          `2` = 'Public only',
          `4` = 'Uninsured',
        ) |>
        fct_collapse(
          `Any private` = 'Any private',
          `Public only` = 'Public only',
          `Uninsured` = 'Uninsured',
          other_level = 'Other'
        )
    )

  # Recode POVCAT
  meps_data <- meps_data %>%
    mutate(
      POVCAT =
        as.factor(POVCAT) %>%
        recode_factor(
          `1` = 'Poor (<100% of poverty line)',
          `2` = 'Near poor',
          `3` = 'Low income',
          `4` = 'Middle income',
          `5` = 'High income'
        ) |>
        fct_collapse(
          `Poor (<100% of poverty line)` = "Poor (<100% of poverty line)",
          `Low income (100% - <200% of poverty line)` = c('Near poor', 'Low income'),
          `Middle income (200% - 399% of poverty line)` = c('Middle income'),
          `High income (>= 400% of poverty line)` = 'High income'
        )
    )

  # Recode expenditures
  meps_data <- meps_data |>
    mutate(
      Copay = as.factor(
        case_when(RXFEXPSELF == 0 ~ '0',
                  RXFEXPSELF <= 40 ~ '>0-40',
                  #RXFEXPSELF <= 40 ~ '>20-40',
                  # RXFEXPSELF <= 30 ~ '>20-30',
                  RXFEXPSELF <= 75 ~ '>40-75',
                  RXFEXPSELF > 75 ~ '>75')
      ),
      Copay_6cat = as.factor(
        case_when(RXFEXPSELF == 0 ~ '0',
                  RXFEXPSELF <= 10 ~ '>0-10',
                  RXFEXPSELF <= 20 ~ '>10-20',
                  RXFEXPSELF <= 30 ~ '>20-30',
                  RXFEXPSELF <= 75 ~ '>30-75',
                  RXFEXPSELF > 75 ~ '>75')
      )
    )

  # Diabetes
  meps_data <- meps_data %>%
    mutate(
      DIABDX =
        as.factor(DCSDIABDX) %>%
          fct_collapse(
            `1` = 'No',
            `2` = 'Yes',
            other_level = 'Unknown'
          )
      )

  # Make sure they're factors
  meps_data <- meps_data %>%
    mutate(AGEX = AGE,
           AGE = case_when(AGEX < 18 ~ '< 18',
                           AGEX < 44 ~ '18 - 44',
                           AGEX < 64 ~ '45 - 64',
                           AGEX < 75 ~ '65 - 74',
                           AGEX >= 75 ~ '75+',
                           TRUE ~ NA),
           AGE_7cat = case_when(AGEX < 10 ~ '< 10',
                                AGEX <= 19 ~ '10 - 19',
                                AGEX <= 39 ~ '20 - 39',
                                AGEX <= 59 ~ '40 - 59',
                                AGEX <= 64 ~ '60 - 64',
                                AGEX <= 74 ~ '65 - 74',
                                AGE >= 75 ~ '75+',
                                TRUE ~ NA),
           AGE_iqviacat = case_when(AGEX < 10 ~ NA,
                                    AGEX <= 19 ~ NA,
                                    AGEX <= 39 ~ '20 - 39',
                                    AGEX <= 59 ~ '40 - 59',
                                    AGEX <= 64 ~ '60 - 64',
                                    AGEX <= 74 ~ '65 - 74',
                                    AGE >= 75 ~ '75+',
                                    TRUE ~ NA),
           AGE_iqviacat_not_insulin = case_when(AGEX < 10 ~ NA,
                                                AGEX <= 19 ~ NA,
                                                AGEX <= 39 ~ '20 - 39',
                                                AGEX <= 59 ~ '40 - 59',
                                                AGEX <= 74 ~ '60 - 74',
                                                AGE >= 75 ~ '75+',
                                                TRUE ~ NA)) %>%
    filter(AGEX > 18) %>%
    mutate(AGE = as.factor(AGE),
           AGE_7cat = as.factor(AGE_7cat),
           RACETHNX = as.factor(RACETHNX))

  # Clean up some of the drug names
  meps_data <- meps_data %>%
    mutate(
      third_level_category_name = case_when(
        third_level_category_name %in% c(
          'ALPHA-GLUCOSIDASE INHIBITORS',
          'AMYLIN ANALOGS',
          'MEGLITINIDES'
        ) ~ 'Other',
        str_detect(third_level_category_name, 'Pre-mixed') ~ 'Pre-mixed',
        TRUE ~ third_level_category_name
      ),
      insulin_indicator = case_when(third_level_category_name %in% c('Basal', 'Bolus', 'Human',
                                                                     'Analog', 'Pre-mixed', 'Human And Analog') ~ 'Insulin',
                                    third_level_category_name %in% c('Basal/Analog', 'Basal/Human', 'Bolus/Analog',
                                                                     'Bolus/Human', 'Pre-mixed/Human', 'Pre-mixed/Analog',
                                                                     'Pre-mixed/Human And Analog') ~ 'Insulin',
                                    str_detect(third_level_category_name, 'Insulin') ~ 'Insulin',
                                    TRUE ~ 'Not Insulin'),
      drug_cat_insulin = case_when(insulin_indicator == 'Insulin' ~ third_level_category_name,
                                   TRUE ~ 'Not Insulin'),
      drug_cat_not_insulin = case_when(insulin_indicator == 'Not Insulin' ~ third_level_category_name,
                                       TRUE ~ 'All insulins')
      # insulin_group = paste0(insulin_indicator, ' - ', insulin_type)
    )

}

#' Title
#'
#'  This summarizes over all drugs at the person level (how much did a person spend in a given year)
#'
#' @param meps.data
#'
#' @return
#' @export
#'
#' @examples
personLevelExpenditures <- function(meps.data) {
  meps.data |>
    group_by(
      MEPSID,
      VARPSU,
      #PANEL,
      DIABWF,
      VARSTR,
      DIABDX,
      SEX,
      HIDEG,
      RACETHNX,
      AGE,
      COVERTYPE
    ) |>
    summarise(across(starts_with('RXFEX'), ~sum(.x, na.rm = TRUE)),
              RXDAYSUP = sum(RXDAYSUP, na.rm = TRUE)) |>
    mutate( oop_cost_per_day = RXFEXPSELF / RXDAYSUP,
            oop_cost_30_day_supply = oop_cost_per_day * 30,
            oop_cost_30_day_supply = ifelse(is.infinite(oop_cost_30_day_supply),
                                            NA,
                                            oop_cost_30_day_supply))
}


#' Title
#'  This summarises person expenditures on a drug (how much did a person spend on a drug in a given year)
#' @param meps.data
#'
#' @return
#' @export
#'
#' @examples
personDrugLevelExpenditures <- function(meps.data) {
  meps.data |>
    group_by(
      MEPSID,
      VARPSU,
      #PANEL,
      DIABWF,
      VARSTR,
      DIABDX,
      SEX,
      HIDEG,
      RACETHNX,
      AGE,
      COVERTYPE,
      third_level_category_name
    ) |>
    summarise(across(starts_with('RXFEX'), ~sum(.x, na.rm = TRUE)),
              RXDAYSUP = sum(RXDAYSUP, na.rm = TRUE)) |>
    mutate( oop_cost_per_day = RXFEXPSELF / RXDAYSUP,
            oop_cost_30_day_supply = oop_cost_per_day * 30,
            oop_cost_30_day_supply = ifelse(is.infinite(oop_cost_30_day_supply),
                                            NA,
                                            oop_cost_30_day_supply)) |>
    mutate(drug_insurance = as.factor(paste0(third_level_category_name, "__", COVERTYPE)))
}

#' Title
#'
#' @param data Data with price column
#' @param cpi CPI data with year and CPI column
#' @param year
#'
#' @return
#' @export
#'
#' @examples
calculatePriceAdjustment <- function(data, cpi, year){
  base_cpi <- cpi |>
    filter(Year == 2021) |>
    pull(cpi)

  target_cpi <- cpi |>
    filter(Year == year) |>
    pull(cpi)

  cpi_ratio <- base_cpi / target_cpi

  data |>
    mutate(RXFEXPSELF_unadjusted = RXFEXPSELF) |>
    mutate(RXFEXPSELF = RXFEXPSELF * cpi_ratio) |>
    mutate( oop_cost_per_day = RXFEXPSELF / RXDAYSUP,
            oop_cost_30_day_supply = oop_cost_per_day * 30,
            oop_cost_30_day_supply = ifelse(is.infinite(oop_cost_30_day_supply),
                                            NA,
                                            oop_cost_30_day_supply))
}

medianExpenditures <- function(srvy, by.category = FALSE, category = c('drugs_only', 'drugs_insurance')) {

  if (by.category == FALSE) {
    if (category == 'drugs_only') {
      svy_results <- svyby(~oop_cost_30_day_supply,
                           ~third_level_category_name,
                           srvy, svyquantile, quantiles = 0.5, qrulte = 'hf7')
      svy_results <- rename(svy_results, Level = third_level_category_name, SE = se.oop_cost_30_day_supply)
      svy_results <- svy_results |>
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
        )
        )
    } else if (category == 'drugs_insurance') {
      svy_results <- svyby(~oop_cost_30_day_supply,
                           ~drug_insurance,
                           srvy, svyquantile, quantiles = 0.5)
      svy_results <- rename(svy_results, Level = drug_insurance, SE = se.oop_cost_30_day_supply)
      svy_results <- svy_results |>
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
        )
        )
    }


  }
  row.names(svy_results) <- NULL
  svy_results
}

#' Title
#'
#' @param .data
#' @param ...
#' @param age.adjust
#' @param age.adjust.var Character string of variable adjusted over
#' @param age.standards
#'
#' @return
#' @export
#'
#' @examples
srvyEstimatesInsulin <- function(.data, ..., age.adjust = FALSE, age.adjust.var, age.standards) {
  options(survey.lonely.psu="adjust")
    # Setup survey, subset to just insulin and diabetes, drop unused facotrs
  insulin_svy <- .data |>
    as_survey_design(
      ids = VARPSU,
      weights = DIABWF,
      strata = VARSTR,
      nest = TRUE
    ) |>
    subset(insulin_indicator == 'Insulin' & DIABDX == '2') |>
    mutate(drug_cat_insulin = fct_drop(drug_cat_insulin),
           AGE = AGE_iqviacat_not_insulin)
  if (age.adjust == TRUE) {
    form <- as.formula(paste0('~', age.adjust.var))
    insulin_svy <- svystandardize(insulin_svy,
                                  by = ~AGE,
                                  over = form,
                                  population = age.standards,
                                  excluding.missing = form)
  }

  insulin_svy_results <- insulin_svy |>
    group_by(...) |>
    summarise(total = survey_total(vartype = c('se', 'ci'), na.rm = TRUE),
              perc = survey_prop(vartype = c('se', 'ci'), proportion = TRUE,  na.rm = TRUE,
                                 prop_method = 'beta'),
              n = n(),
              deff = survey_mean(deff = TRUE, na.rm = TRUE)) |>
    select(-deff, -deff_se, deff = deff_deff) |>
    mutate(n_eff = n / deff) |>
    mutate(low_ci = exp(log(total) + qt(0.025, df = pmin(n, n_eff) - 1) * sqrt(total_se ^2 / total ^ 2)),
           high_ci = exp(log(total) + qt(0.975, df = pmin(n, n_eff) - 1) * sqrt(total_se ^2 / total ^ 2)),
           rel_ci = ((high_ci - low_ci) / total) * 100,
           df = degf(insulin_svy)
    ) |>
    select(..., total, total_se, low_ci, high_ci, rel_ci,  perc, perc_se, perc_low, perc_upp, n, deff, n_eff, df) |>
    rename(Level = drug_cat_insulin, Number = total, SE = total_se, LL = low_ci, HL = high_ci,
           Percent = perc, SE.1 = perc_se, LL.1 = perc_low, UL.1 = perc_upp)
  # if (names(insulin_svy_results)[1] != 'Level') {
  #   insulin_svy_results <- insulin_svy_results |>
  #     rename(group = 1)
  # } else {
  #   insulin_svy_results$group <- '-'
  # }
  return(insulin_svy_results)

}

#' Title
#'
#' @param .data
#' @param ...
#' @param age.adjust
#' @param age.adjust.var Character string of variable to adjust over
#' @param age.standards
#'
#' @return
#' @export
#'
#' @examples
srvyEstimatesNotInsulin <- function(.data, ..., age.adjust = FALSE, age.adjust.var, age.standards) {
  options(survey.lonely.psu="adjust")
  not_insulin_svy <- .data |>
    as_survey_design(
      ids = VARPSU,
      weights = DIABWF,
      strata = VARSTR,
      nest = TRUE
    ) |>
    subset( DIABDX == '2' & drug_cat_not_insulin %!in% c('None', '<NA>') & !is.na(drug_cat_not_insulin)) |>
    mutate(drug_cat_not_insulin = fct_drop(drug_cat_not_insulin),
           AGE = AGE_iqviacat_not_insulin)
  if (age.adjust == TRUE) {
    form <- as.formula(paste0('~', age.adjust.var))
    not_insulin_svy <- svystandardize(not_insulin_svy,
                                  by = ~AGE,
                                  over = form,
                                  population = age.standards,
                                  excluding.missing = form)
  }
  not_insulin_svy_results <- not_insulin_svy |>
    group_by(...) |>
    summarise(total = survey_total(vartype = c('se', 'ci'), na.rm = TRUE),
              perc = survey_prop(vartype = c('se', 'ci'), proportion = TRUE, na.rm = TRUE,
                                 prop_method = 'beta'),
              n = n(),
              deff = survey_mean(deff = TRUE, na.rm = TRUE)) |>
    select(-deff, -deff_se, deff = deff_deff) |>
    mutate(n_eff = n / deff) |>
    mutate(low_ci = exp(log(total) + qt(0.025, df = pmin(n, n_eff) - 1) * sqrt(total_se ^2 / total ^ 2)),
           high_ci = exp(log(total) + qt(0.975, df = pmin(n, n_eff) - 1) * sqrt(total_se ^2 / total ^ 2)),
           rel_ci = ((high_ci - low_ci) / total) * 100,
           df = degf(not_insulin_svy)
    ) |>
    select(..., total, total_se, low_ci, high_ci, rel_ci,  perc, perc_se, perc_low, perc_upp, n, deff, n_eff, df) |>
    rename(Level = drug_cat_not_insulin, Number = total, SE = total_se, LL = low_ci, HL = high_ci,
           Percent = perc, SE.1 = perc_se, LL.1 = perc_low, UL.1 = perc_upp)
  # if (names(not_insulin_svy_results)[1] != 'Level') {
  #   not_insulin_svy_results <- not_insulin_svy_results |>
  #     rename(group = 1)
  # } else {
  #   not_insulin_svy_results$group <- '-'
  # }
  return(not_insulin_svy_results)
}

srvyEstimates <- function(srvyResultsInsulin, srvyResultsNotInsulin) {

  out <- bind_rows(srvyResultsInsulin, srvyResultsNotInsulin)
  out <- mutate(out,
                Flag_n = if_else(n < 10 | n_eff < 10, 'Low sample size', NA),
                Flag_ci = if_else(rel_ci > 160, 'CI too wide', NA),
                Flag_df = if_else(df < 8, 'Degrees of freedom too low', NA),
                Flags = paste(Flag_n, Flag_ci, Flag_df, sep = ', '),
                Flags = gsub("(^,|,$|NA, |, NA)", "", Flags),
                Flags = if_else(Flags == 'NA', NA, Flags)) |>
    select(-Flag_n, -Flag_ci, -Flag_df)
  return(out)
}

pooledGraphs <- function(data) {
  data <- data |>
    mutate(insulin_indicator = case_when(str_detect(Prescription, 'Insulin -') ~ 'Insulin',
                                         TRUE ~ 'Not insulin'),
           data = case_when(data == 'IQVIA' ~ 'NPA',
                            TRUE ~ data),
           data = factor(data, levels = c('NPA', 'MEPS')))

  insulin_data <- data |>
    filter(insulin_indicator == 'Insulin')
  not_insulin_data <- data |>
    filter(insulin_indicator != 'Insulin')

  insulin_plot <- insulin_data |>
    filter(stratifier == 'Overall') |>
    mutate(Prescription = str_remove_all(Prescription, 'Insulin - ')) |>
    mutate(Prescription = factor(Prescription,
                                levels = c('Bolus/Human',
                                           'Bolus/Analog',
                                           'Basal/Human',
                                           'Basal/Analog',
                                           'Pre-mixed',
                                           'Pre-mixed/Human',
                                           'Pre-mixed/Analog',
                                           'Insulin/Unknown'))
           ) |>
    mutate(total = sum(Number, na.rm = TRUE), .by = data) |>
    mutate(Percent = (Number / total) * 100) |>
    ggplot(aes(
      x = Prescription,
      y = Percent,
      group = data,
      fill = data
    )) +
    geom_bar(stat = 'identity', position = 'dodge') +
    theme_classic() +
    scale_y_continuous(expand = c(0, 0),
                       limits = c(0, 60)) +
    scale_x_discrete(labels = function(x) {sub('/', '\n', x)}) +
    ggsci::scale_fill_lancet() +
    labs(fill = 'Data Source',
         y = 'Percent of total insulin prescriptions',
         x = 'Class')

  not_insulin_plot <- not_insulin_data |>
    filter(stratifier == 'Overall') |>
    mutate(total = sum(Number, na.rm = TRUE), .by = data) |>
    mutate(Percent = (Number / total) * 100) |>
    mutate(Prescription = factor(Prescription,
                                 levels = c('All insulins',
                                            'Biguanides',
                                            'Sulfonylureas',
                                            'TZD',
                                            'DPP-4i',
                                            'GLP-1RA',
                                            'SGLT2i',
                                            'Unknown'))) |>
    ggplot(aes(
      x = Prescription,
      y = Percent,
      group = data,
      fill = data
    )) +
    geom_bar(stat = 'identity', position = 'dodge') +
    theme_classic() +
    scale_y_continuous(expand = c(0, 0),
                       limits = c(0, 60)) +
    scale_x_discrete(labels = function(x) {sub('/', '\n', x)}) +
    ggsci::scale_fill_lancet() +
    labs(fill = 'Data Source',
         y = 'Percent of total prescriptions',
         x = 'Class')


  wrap_plots(insulin_plot,
             not_insulin_plot,
             guides = 'collect') &
    theme(axis.text.x = element_text(angle = 45,
                                     hjust = 1),
          legend.position = 'bottom',
          legend.direction = 'horizontal',
          axis.text = element_text(color = 'black'))

}

prepGTMepsIqvia <- function(data, appendix = FALSE) {
  data <- data |>
    select(data,
           Prescription,
           stratifier,
           group,
           Number,
           LL, HL,
           total,
           Percent,
           Flags) |>
    mutate(
      Number = formatC(round(Number, digits = -3), format = "d", big.mark = ","),
      AppendixNumber = case_when(data == 'MEPS' ~ glue::glue("{Number} ({LL}–{HL})"),
                                 TRUE ~ Number),
      Prescription = case_when(
        str_detect(Prescription, 'Insulin') ~ str_remove_all(Prescription, 'Insulin - '),
        TRUE ~ Prescription
      ),
      Number = case_when(!is.na(Flags) ~ '-', TRUE ~ as.character(Number))
    ) |>
    mutate(stratifier = factor(
      stratifier,
      levels = c(
        'Overall',
        'Sex',
        'Age',
        'Insurance',
        'Copay',
        'Race',
        'Education',
        'Poverty',
        'Prescribing medical specialty'
      )
    ),
    group = factor(
      group,
      levels = c(
        ' ',
        'Female',
        'Male',
        '< 10',
        '10 - 19',
        '20 - 39',
        '40 - 59',
        '60 - 64',
        '60 - 74',
        '65 - 74',
        '75+',
        'Public only',
        'Any private',
        'Uninsured',
        '$0',
        '>$0-$10',
        '>$10-$20',
        '>$20-$30',
        '>$30-$75',
        '>$75',
        'Unknown',
        'Asian/not Hispanic',
        'Black/not Hispanic',
        'Hispanic',
        'Other Race/not Hispanic',
        'White/not Hispanic',
        'Less than high school',
        'High school',
        'Greater than high school',
        'Poor (<100% of poverty line)',
        'Low income (100% - <200% of poverty line)',
        'Middle income (200% - 399% of poverty line)',
        'High income (>= 400% of poverty line)',
        'Cardiology',
        'Emergency Med',
        'Endocrinology',
        'General surgery',
        'Geriatrics',
        'Nephrology',
        'NP/PA',
        'OBGYN',
        'Other',
        'Primary care'
      )
    )) |>
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
    # filter(!is.na(Prescription),
    #        stratifier %nin% c('Education', 'Poverty', 'Race')) |>
    filter(!is.na(Prescription),
           group != 'Unknown') |>
    select(data, Prescription, Number, AppendixNumber, Percent, stratifier, group, Flags) |>
    mutate(Percent = specifyDecimal(Percent, 1),
           Percent = case_when(!is.na(Flags) ~ '-',
                               is.na(Percent) ~ '-',
                               Percent == 'NA' & data == 'MEPS' & group %in% c('>0-10',
                                                                               '>10-20',
                                                                               '>20-30',
                                                                               '0',
                                                                               'Uninsured',
                                                                               'Asian/not Hispanic') ~ '-',
                               Percent == 'NA' ~ 'N/A',
                               TRUE ~ as.character(Percent)))
  if (appendix == FALSE) {
  data |>
      mutate(table_value = case_when(stratifier == 'Overall' ~ Number,
                                     TRUE ~ as.character(Percent))) |>
    pivot_wider(
      id_cols = c(stratifier, group),
      names_from = c(data, Prescription),
      values_from = table_value
    ) |>
    arrange(stratifier, group) |>
    mutate(stratifier = case_when(stratifier == 'Overall' ~ 'Overall (n)',
                                  TRUE ~ paste0(stratifier, ' (%)')))
  } else {
    data |>
      mutate(
             AppendixNumber = case_when(!is.na(Flags) ~ '-',
                                  is.na(Percent) ~ '-',
                                  AppendixNumber == 'NA' & data == 'MEPS' & group %in% c('>0-10',
                                                                                 '>10-20',
                                                                                 '>20-30',
                                                                                 '0',
                                                                                 'Uninsured',
                                                                                 'Asian/not Hispanic') ~ '-',
                                 AppendixNumber == 'NA' ~ 'N/A',
                                 AppendixNumber == 'NA (NA–NA)' ~ 'N/A',
                                 TRUE ~ as.character(AppendixNumber))) |>
      mutate(table_value = AppendixNumber) |>
      pivot_wider(
        id_cols = c(stratifier, group),
        names_from = c(data, Prescription),
        values_from = table_value
      ) |>
      arrange(stratifier, group) |>
      mutate(stratifier = case_when(stratifier == 'Overall' ~ 'Overall (95% CI)',
                                    TRUE ~ paste0(stratifier, ' (95% CI)')))
  }
}
