meps_iqvia |>
  select(data,
         Prescription,
         stratifier,
         group,
         Number,
         total,
         Percent,
         Flags) |>
  mutate(
    Number = formatC(round(Number, digits = -3), format = 'd', big.mark = ","),
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
      '60 - 74',
      '65 - 74',
      '75+',
      'Less than high school',
      'High school',
      'Greater than high school',
      'Poor (<100% of poverty line)',
      'Low income (100% - 200% of poverty line)',
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
      '>75',
      'Unknown'
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
  filter(!is.na(Prescription),
         stratifier %nin% c('Education', 'Poverty', 'Race')) |>
  filter(!is.na(Prescription),
         group != 'Unknown') |>
  select(data, Prescription, Number, Percent, stratifier, group, Flags) |>
  mutate(Percent = specifyDecimal(Percent, 1),
         Percent = case_when(!is.na(Flags) ~ '-',
                             TRUE ~ as.character(Percent)),
         table_value = case_when(stratifier == 'Overall' ~ Number,
                                 TRUE ~ as.character(Percent))) |>
  pivot_wider(
    id_cols = c(stratifier, group),
    names_from = c(data, Prescription),
    values_from = table_value
  ) |>
  arrange(stratifier, group) |>
  select(stratifier, group, contains('/'), contains('Pre-mixed'), contains('All')) |>
  gt(
    rowname_col = 'group',
    groupname_col = 'stratifier'
  ) |>
  tab_spanner_delim(
    delim = '_',
    reverse = TRUE
  )
