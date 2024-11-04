#' tar_target(survey_list,
#'            createSurveyDesign("data/ipums_rx.db", years, survey_dat),
#'            pattern = map(years),
#'            deployment = 'main',
#'            iteration = 'list'),
#'
#' # tar_target(save_survey_list,
#' #            {
#' #              write_rds(survey_list, glue::glue('output/survey_designs_{years}.rds'))
#' #              print(glue::glue('output/survey_designs_{years}.rds'))
#' #              },
#' #            pattern = map(survey_list, years),
#' #            deployment = 'main',
#' #            format = 'file'),
#'
#' tar_target(third_level_counts,
#'            svyby(~third_level_category_name, ~DCSDIABDX, survey_list, svytotal) %>%
#'              mutate(year = years),
#'            pattern = map(survey_list, years),
#'            deployment = 'main',
#'            iteration = 'vector'),
#'
#' tar_target(third_level_counts_df,
#'            third_level_counts %>%
#'              rename_with(~ paste0('estimate.', .x, recycle0 = TRUE), starts_with('third')) %>%
#'              pivot_longer(contains('third_level'),
#'                           names_pattern = "(.*)\\.third_level_category_name(.*)",
#'                           names_to = c('type', 'rx'),
#'                           values_to = 'count') %>%
#'              pivot_wider(id_cols = c('DCSDIABDX', 'year', 'rx'),
#'                          names_from = 'type',
#'                          values_from = 'count')
#' ),
#'
#' tar_target(rx_counts,
#'            svyby(~RXDRGNAM, ~DCSDIABDX, survey_list, svytotal) %>%
#'              mutate(year = years),
#'            pattern = map(survey_list, years),
#'            deployment = 'main',
#'            iteration = 'vector'),
#'
#' tar_target(rx_counts_df,
#'            rx_counts %>%
#'              rename_with(~ paste0('estimate.', .x, recycle0 = TRUE), starts_with('RXDRGNAM')) %>%
#'              pivot_longer(contains('RXDRGNAM'),
#'                           names_pattern = "(.*)\\.RXDRGNAM(.*)",
#'                           names_to = c('type', 'rx'),
#'                           values_to = 'count') %>%
#'              pivot_wider(id_cols = c('DCSDIABDX', 'year', 'rx'),
#'                          names_from = 'type',
#'                          values_from = 'count')
#' ),
#'
#' tar_target(third_level_table,
#'            {
#'              map2(survey_list, years, function(.x, .y) {
#'                assign("survey_object", .x, envir = .GlobalEnv)
#'                set_survey('survey_object')
#'                set_output(max_levels = 2e5)
#'                surveytable::set_count_int()
#'                temp <- tab_subset('third_level_category_name', 'DCSDIABDX')
#'                temp <- bind_rows(temp, .id = 'DCSDIABDX')
#'                temp$year <- .y
#'                temp
#'              }) %>%
#'                bind_rows()
#'            }),
#'
#' tar_target(rx_name_table,
#'            {
#'              map2(survey_list, years, function(.x, .y) {
#'                assign("survey_object", .x, envir = .GlobalEnv)
#'                set_survey('survey_object')
#'                set_output(max_levels = 2e5)
#'                surveytable::set_count_int()
#'                temp <- tab_subset('RXNAME', 'DCSDIABDX')
#'                temp <- bind_rows(temp, .id = 'DCSDIABDX')
#'                temp$year <- .y
#'                temp
#'              }) %>%
#'                bind_rows()
#'            }),
#'
#' tar_target(rx_table_output,
#'            {
#'              write_csv(rx_name_table, 'output/prescription_counts.csv')
#'              'output/prescription_counts.csv'
#'            },
#'            format = 'file',
#'            deployment = 'main'),
#' tar_target(third_level_table_output,
#'            {
#'              write_csv(third_level_table, 'output/drug_category_counts.csv')
#'              'output/drug_category_counts.csv'
#'            },
#'            format = 'file',
#'            deployment = 'main'),
#'
#' tar_target(third_level_table_all,
#'            {
#'              map2(survey_list, years, function(.x, .y) {
#'                assign("survey_object", .x, envir = .GlobalEnv)
#'                set_survey('survey_object')
#'                set_output(max_levels = 2e5)
#'                surveytable::set_count_int()
#'                temp <- tab('third_level_category_name')
#'                temp$year <- .y
#'                temp
#'              }) %>%
#'                bind_rows()
#'            }),
#'
#' tar_target(rx_name_table_all,
#'            {
#'              map2(survey_list, years, function(.x, .y) {
#'                assign("survey_object", .x, envir = .GlobalEnv)
#'                set_survey('survey_object')
#'                set_output(max_levels = 2e5)
#'                surveytable::set_count_int()
#'                temp <- tab('RXNAME')
#'                temp$year <- .y
#'                temp
#'              }) %>%
#'                bind_rows()
#'            }),
#'
#' tar_target(rx_table_output_all,
#'            {
#'              write_csv(rx_name_table_all, 'output/prescription_counts_all.csv')
#'              'output/prescription_counts_all.csv'
#'            },
#'            format = 'file',
#'            deployment = 'main'),
#' tar_target(third_level_table_output_all,
#'            {
#'              write_csv(third_level_table_all, 'output/drug_category_counts_all.csv')
#'              'output/drug_category_counts_all.csv'
#'            },
#'            format = 'file',
#'            deployment = 'main')
#'
#'
#' # tar_target(survey_table_counts,
#' #            my_tab_subset(survey_list, 'third_level_category_name', 'DCSDIABDX'),
#' #            pattern = map(survey_list),
#' #            deployment = 'main',
#' #            iteration = 'list'),
#'
#' # tar_target(pov_survey_counts,
#' #            survey_list %>%
#' #              group_by(POVCAT) %>%
#' #              survey_count() %>%
#' #              mutate(year = years),
#' #            pattern = map(survey_list, years),
#' #            iteration = 'vector',
#' #            deployment = 'main')
#'
#' #' Read in IPUMS MEPS data
#' #'
#' #' Read in IPUMS MEPS data on RX and person level, then join them together.
#' #' Also add in Multim Lexicon data for drugs.
#' #' Split data by year and write to data.base to try and circumvent in memory issues
#' #'
#' #' @param db.file.path
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' importIPUMSMEPS <- function(db.file.path) {
#'   # Create or connect to the DuckDB database
#'   con <- dbConnect(duckdb::duckdb(), db.file.path)
#'   on.exit(dbDisconnect(con, shutdown = TRUE))
#'
#'   dat <- tbl(con, 'full_data')
#'   multum_class_code <- tbl(con, 'multum_class_code')
#'
#'   # Join Multum Lexicon table to codes to get classes
#'
#'   data_rx_med <- filter(dat, RECTYPE == 'F') %>%
#'     select(notNAFunc(.)) %>%
#'     select(YEARF, DUPERSIDF, MEPSIDF, RXNAME, starts_with('RXFEX'),
#'            RXDRGNAM, starts_with('MULT')) %>%
#'     mutate(rx_med = 1)
#'   # MEPSID is hte UID for person
#'   data_person <- filter(dat, RECTYPE == 'P')%>%
#'     select(notNAFunc(.)) %>%
#'     select(-PERNUM, -DUID, -PID, -PANEL, -ends_with('PLD'),
#'            -PANELYR, -RELYR, -SAQWEIGHT)
#'
#'
#'   # I think this data isn't needed?
#'   # MEPSIDM is the UID for presc_med
#'   # data_presc_med <- filter(data, RECTYPE == 'M') %>%
#'   #   select(where(not_all_na))
#'   # joined_person_presc <- left_join(data_presc_med, data_person, by = c('MEPSIDM' = 'MEPSID'))
#'   #
#'   # joined_person_presc %>%
#'   #   group_by(MEPSIDM, RXDRGNAM) %>%
#'   #   count()
#'
#'   # Person level data joined with prescription data
#'   # Am I aggregating person level or
#'   # I don't think PERNUM is the right variable to be joining on
#'
#'
#'   data_rx_med <- data_rx_med %>%
#'     left_join(
#'       distinct(multum_class_code, first_level_category_name, first_level_category_id),
#'       by = c('MULTC1' = 'first_level_category_id')
#'     ) %>%
#'     left_join(
#'       distinct(multum_class_code, second_level_category_name, second_level_category_id),
#'       by = c('MULTC1S1' = 'second_level_category_id')
#'     ) %>%
#'     rename(second_level_category_name1 = second_level_category_name) %>%
#'     left_join(
#'       distinct(multum_class_code, second_level_category_name, second_level_category_id),
#'       by = c('MULTC1S2' = 'second_level_category_id')
#'     ) %>%
#'     rename(second_level_category_name2 = second_level_category_name) %>%
#'     left_join(
#'       distinct(multum_class_code, third_level_category_name, third_level_category_id),
#'       by = c('MULTC1S1S1' = 'third_level_category_id')
#'     ) %>%
#'     rename(third_level_category_name1 = third_level_category_name) %>%
#'     left_join(
#'       distinct(multum_class_code, third_level_category_name, third_level_category_id),
#'       by = c('MULTC1S1S2' = 'third_level_category_id')
#'     ) %>%
#'     rename(third_level_category_name2 = third_level_category_name) %>%
#'     left_join(
#'       distinct(multum_class_code, third_level_category_name, third_level_category_id),
#'       by = c('MULTC1S2S1' = 'third_level_category_id')
#'     ) %>%
#'     rename(third_level_category_name3 = third_level_category_name) %>%
#'     mutate(second_level_category_name = paste(second_level_category_name1, second_level_category_name2, sep = ", "),
#'            third_level_category_name = paste(third_level_category_name1, third_level_category_name2, third_level_category_name3, sep = ", ")) %>%
#'     select(-second_level_category_name1, -second_level_category_name2, -third_level_category_name1, -third_level_category_name2,
#'            -third_level_category_name3)
#'
#'   diab_rx_med <- data_rx_med %>%
#'     filter(str_detect(third_level_category_name,
#'                       'SULFONYLUREAS|BIGUANIDES|INSULIN|ALPHA-GLUCOSIDASE INHIBITORS|THIAZOLIDINEDIONES|MEGLITINIDES|ANTIDIABETIC COMBINATIONS|DIPEPTIDYL PEPTIDASE 4 INHIBITORS|AMYLIN ANALOGS|GLP-1 RECEPTOR AGONISTS|SGLT-2 INHIBITORS'))
#'
#'   joined_person_rx <- full_join(diab_rx_med, data_person, by = c('MEPSIDF' = 'MEPSID')) %>%
#'     mutate(across(starts_with('MULT'), ~as.integer(.x)))
#'
#'   years <- joined_person_rx %>%
#'     pull(YEARF) %>%
#'     unique() %>%
#'     sort()
#'
#'   for (i in years) {
#'     temp_dat <- joined_person_rx %>%
#'       filter(YEARF == i) %>%
#'       #filter(DCSDIABDX == 2) %>%
#'       collect()
#'     dbWriteTable(con, glue::glue('rx_data_{i}'), temp_dat, overwrite = TRUE)
#'   }
#'   # Can get each individuals unique medications. Could then tally the # of each type of medication a subgroup has, or
#'   # proportion of subgroups taking certain medications
#'   digest_dat <- joined_person_rx %>%
#'     filter(YEARF >= 2020) %>%
#'     #filter(DCSDIABDX == 2) %>%
#'     collect() %>%
#'     digest::digest()
#'   dbDisconnect(con, shutdown = TRUE)
#'   return(digest_dat)
#' }
#'
#' # So this function works if the object being passed in the console
#' # is a survey design object. It doesn't seem to work if I pass the object from
#' # its index in a list (survey_list[[1]]). Something about the list is mucking it up?
#' #
#' surveyTableCounts <- function(survey.object) {
#'   survey.design <- survey.object
#'   set_survey('survey.design')
#'   tab('third_level_category_name')
#'   #tab_subset('third_level_category_name', 'DCSDIABDX')
#' }
#'
#' set_survey2 <- function(design) {
#'   assert_that(!is.null(design), msg = "The survey design object must not be NULL.")
#'   assert_that(inherits(design, "survey.design"), msg = "The object must be a survey.design.")
#'
#'   options(surveytable.survey = design)
#'
#'   # Zero weights cause issues with tab()
#'   if (any(design$prob == Inf)) {
#'     dl <- attr(design, "label")
#'     if (is.null(dl)) dl <- deparse(substitute(design))
#'     assert_that(is.string(dl), nzchar(dl))
#'     dl <- paste(dl, "(positive weights only)")
#'
#'     design <- survey_subset(design, design$prob < Inf, label = dl)
#'
#'     #message(paste("* ", deparse(substitute(design)), ": retaining positive weights only."))
#'   }
#'
#'   assert_that(all(design$prob > 0), all(design$prob < Inf))
#'
#'   dl <- attr(design, "label")
#'   if (is.null(dl)) dl <- deparse(substitute(design))
#'   assert_that(is.string(dl), nzchar(dl))
#'   options(surveytable.survey_label = dl)
#'
#'   out <- list(
#'     `Survey name` = dl,
#'     `Number of variables` = ncol(design$variables),
#'     `Number of observations` = nrow(design$variables)
#'   )
#'   class(out) <- "simple.list"
#'   print(out)
#'   print(design)
#'
#'   message("* To adjust how counts are rounded, see ?set_count_int")
#'   invisible(NULL)
#' }
#'
#'
#' my_tab_subset <-
#'   function(survey, vr, vrby, lvls = c()
#'            , test = FALSE, alpha = 0.05
#'            , drop_na = getOption("surveytable.drop_na")
#'            , max_levels = getOption("surveytable.max_levels")
#'            , screen = getOption("surveytable.screen")
#'            , csv = getOption("surveytable.csv")
#'   ) {
#'     #browser()
#'     assertthat::assert_that(test %in% c(TRUE, FALSE)
#'                             , alpha > 0, alpha < 0.5)
#'     design = survey
#'     nm = names(design$variables)
#'     assertthat::assert_that(vr %in% nm, msg = paste("Variable", vr, "not in the data."))
#'     assertthat::assert_that(vrby %in% nm, msg = paste("Variable", vrby, "not in the data."))
#'     assertthat::assert_that(is.factor(design$variables[,vr])
#'                             || is.logical(design$variables[,vr])
#'                             || is.numeric(design$variables[,vr])
#'                             , msg = paste0(vr, ": must be factor, logical, or numeric. Is "
#'                                            , class(design$variables[,vr])[1] ))
#'
#'     lbl = attr(design$variables[,vrby], "label")
#'     if (is.logical(design$variables[,vrby])) {
#'       design$variables[,vrby] %<>% factor
#'     }
#'     assertthat::assert_that(is.factor(design$variables[,vrby])
#'                             , msg = paste0(vrby, ": must be either factor or logical. Is "
#'                                            , class(design$variables[,vrby])[1] ))
#'     design$variables[,vrby] %<>% droplevels %>% .fix_factor()
#'     attr(design$variables[,vrby], "label") = lbl
#'
#'     lvl0 = levels(design$variables[,vrby])
#'     if (!is.null(lvls)) {
#'       assertthat::assert_that(all(lvls %in% lvl0))
#'       lvl0 = lvls
#'     }
#'
#'     ret = list()
#'     if (is.logical(design$variables[,vr]) || is.factor(design$variables[,vr])) {
#'       for (ii in lvl0) {
#'         d1 = design[which(design$variables[,vrby] == ii),]
#'         attr(d1$variables[,vr], "label") = paste0(
#'           .getvarname(design, vr), " ("
#'           , .getvarname(design, vrby), " = ", ii
#'           , ")")
#'         ret[[ii]] = .tab_factor(design = d1
#'                                 , vr = vr
#'                                 , drop_na = drop_na
#'                                 , max_levels = max_levels
#'                                 , screen = screen
#'                                 , csv = csv)
#'         if (test) {
#'           ret[[paste0(ii, " - test")]] = .test_factor(design = d1
#'                                                       , vr = vr
#'                                                       , drop_na = drop_na
#'                                                       , alpha = alpha
#'                                                       , screen = screen, csv = csv)
#'         }
#'       }
#'     } else if (is.numeric(design$variables[,vr])) {
#'       rA = NULL
#'       for (ii in lvl0) {
#'         d1 = design[which(design$variables[,vrby] == ii),]
#'         r1 = .tab_numeric_1(design = d1, vr = vr)
#'         rA %<>% rbind(r1)
#'       }
#'       df1 = data.frame(Level = lvl0)
#'       assertthat::assert_that(nrow(df1) == nrow(rA))
#'       rA = cbind(df1, rA)
#'       attr(rA, "title") = paste0(.getvarname(design, vr)
#'                                  , " (for different levels of "
#'                                  , .getvarname(design, vrby), ")")
#'       ret[["Means"]] = .write_out(rA, screen = screen, csv = csv)
#'
#'       if (test) {
#'         nlvl = length(lvl0)
#'         assertthat::assert_that(nlvl >= 2L
#'                                 , msg = paste0("For ", vrby, ", at least 2 levels must be selected. "
#'                                                , "Has: ", nlvl))
#'         if ( !(alpha %in% c(0.05, 0.01, 0.001)) ) {
#'           warning("Value of alpha is not typical: ", alpha)
#'         }
#'         frm = as.formula(paste0("`", vr, "` ~ `", vrby, "`"))
#'
#'         rT = NULL
#'         for (ii in 1:(nlvl-1)) {
#'           for (jj in (ii+1):nlvl) {
#'             lvlA = lvl0[ii]
#'             lvlB = lvl0[jj]
#'             d1 = design[which(design$variables[,vrby] %in% c(lvlA, lvlB)),]
#'             r1 = data.frame(`Level 1` = lvlA, `Level 2` = lvlB, check.names = FALSE)
#'             r1$`p-value` = svyttest(frm, d1)$p.value
#'             rT %<>% rbind(r1)
#'           }
#'         }
#'
#'         rT$Flag = ""
#'         idx = which(rT$`p-value` <= alpha)
#'         rT$Flag[idx] = "*"
#'
#'         rT$`p-value` %<>% round(3)
#'
#'         attr(rT, "title") = paste0("Comparison of "
#'                                    , .getvarname(design, vr)
#'                                    , " across all possible pairs of ", .getvarname(design, vrby))
#'         attr(rT, "footer") = paste0("*: p-value <= ", alpha)
#'         ret[["p-values"]] = .write_out(rT, screen = screen, csv = csv)
#'       }
#'     } else {
#'       stop("How did we get here?")
#'     }
#'
#'     if (length(ret) == 1L) return(invisible(ret[[1]]))
#'     ret <- bind_rows(ret, .id = 'DCSDIABDX')
#'     #invisible(ret)
#'     return(ret)
#'   }
#'
#'
#' # Helper functions --------------------------------------------------------
#'
#' .fix_factor <- function(xx) {
#'   assert_that(is.factor(xx))
#'   idx = which(is.na(xx))
#'   if (length(idx) > 0) {
#'     lvl = levels(xx)
#'     lvl_new = c(lvl, "<N/A>") %>% make.unique
#'     val0 = lvl_new %>% tail(1)
#'     xx %<>% factor(levels = lvl_new, exclude = NULL)
#'     xx[idx] = val0
#'   }
#'
#'   lvl = levels(xx)
#'   idx = which(is.na(lvl))
#'   if (length(idx) > 0) {
#'     lvl[idx] = c(lvl, "<N/A>") %>% make.unique %>% tail(1)
#'     levels(xx) = lvl
#'   }
#'
#'   assert_that(noNA(xx), noNA(levels(xx)))
#'   xx
#' }
#'
#' .getvarname <- function(design, vr) {
#'   nm = attr(design$variables[,vr], "label")
#'   if (is.null(nm)) nm = vr
#'   nm
#' }
#'
#' .tab_factor <- function(design, vr, drop_na, max_levels, screen, csv) {
#'   nm = names(design$variables)
#'   assert_that(vr %in% nm, msg = paste("Variable", vr, "not in the data."))
#'
#'   lbl = .getvarname(design, vr)
#'   if (is.logical(design$variables[,vr])) {
#'     design$variables[,vr] %<>% factor
#'   }
#'   assert_that(is.factor(design$variables[,vr])
#'               , msg = paste0(vr, ": must be either factor or logical. Is ",
#'                              class(design$variables[,vr])[1] ))
#'   design$variables[,vr] %<>% droplevels
#'   if (drop_na) {
#'     design = design[which(!is.na(design$variables[,vr])),]
#'     lbl %<>% paste("(knowns only)")
#'   } else {
#'     design$variables[,vr] %<>% .fix_factor
#'   }
#'   assert_that(noNA(design$variables[,vr]), noNA(levels(design$variables[,vr])))
#'   attr(design$variables[,vr], "label") = lbl
#'
#'   nlv = nlevels(design$variables[,vr])
#'   if (nlv < 2) {
#'     assert_that(all(design$variables[,vr] == design$variables[1,vr]))
#'     mp = .total(design)
#'     assert_that(ncol(mp) %in% c(4L, 5L))
#'     fa = attr(mp, "footer")
#'     mp = cbind(
#'       data.frame(Level = design$variables[1,vr])
#'       , mp)
#'     if (!is.null(fa)) {
#'       attr(mp, "footer") = fa
#'     }
#'     attr(mp, "num") = 2:5
#'     attr(mp, "title") = .getvarname(design, vr)
#'     return(.write_out(mp, screen = screen, csv = csv))
#'   } else if (nlv > max_levels) {
#'     # don't use assert_that
#'     # if multiple tables are being produced, want to go to the next table
#'     warning(vr
#'             , ": categorical variable with too many levels: "
#'             , nlv, ", but ", max_levels
#'             , " allowed. Try increasing the max_levels argument or "
#'             , "see ?set_output"
#'     )
#'     return(invisible(NULL))
#'   }
#'
#'   frm = as.formula(paste0("~ `", vr, "`"))
#'
#'   ##
#'   counts_df = svyby(frm, frm, design, unwtd.count) %>%
#'     as_tibble() %>%
#'     mutate(third_level_category_name = paste0('third_level_category_name', third_level_category_name))
#'
#'
#'
#'
#'   ##
#'   sto = svytotal(frm, design) # , deff = TRUE)
#'   mmcr = data.frame(x = as.numeric(sto)
#'                     , s = sqrt(diag(attr(sto, "var"))) )
#'   mmcr_tibble <- as_tibble(mmcr, rownames = 'third_level_category_name')
#'   mmcr_tibble <- left_join(mmcr_tibble, counts_df)
#'   counts <- pull(mmcr_tibble, counts)
#'
#'   #assert_that(length(counts) == nlv)
#'   if (getOption("surveytable.do_present")) {
#'     pro = getOption("surveytable.present_restricted") %>% do.call(list(counts))
#'   } else {
#'     pro = list(flags = rep("", length(counts)), has.flag = c())
#'   }
#'
#'   #browser()
#'   mmcr$samp.size = .calc_samp_size(design = design, vr = vr, counts = counts)
#'   mmcr$counts = counts
#'
#'   df1 = degf(design)
#'   mmcr$degf = df1
#'
#'   # Equation 24 https://www.cdc.gov/nchs/data/series/sr_02/sr02-200.pdf
#'   # DF should be as here, not just sample size.
#'   mmcr$k = qt(0.975, pmax(mmcr$samp.size - 1, 0.1)) * mmcr$s / mmcr$x
#'   mmcr$lnx = log(mmcr$x)
#'   mmcr$ll = exp(mmcr$lnx - mmcr$k)
#'   mmcr$ul = exp(mmcr$lnx + mmcr$k)
#'
#'   if (getOption("surveytable.do_present")) {
#'     pco = getOption("surveytable.present_count") %>% do.call(list(mmcr))
#'   } else {
#'     pco = list(flags = rep("", nrow(mmcr)), has.flag = c())
#'   }
#'
#'   mmcr = mmcr[,c("x", "s", "ll", "ul")]
#'   mmc = getOption("surveytable.tx_count") %>% do.call(list(mmcr))
#'   names(mmc) = getOption("surveytable.names_count")
#'
#'   ##
#'   lvs = design$variables[,vr] %>% levels
#'   assert_that( noNA(lvs) )
#'   ret = NULL
#'   for (lv in lvs) {
#'     design$variables$.tmp = NULL
#'     design$variables$.tmp = (design$variables[,vr] == lv)
#'     # Korn and Graubard, 1998
#'     xp = svyciprop(~ .tmp, design, method="beta", level = 0.95)
#'     ret1 = data.frame(Proportion = xp %>% as.numeric
#'                       , SE = attr(xp, "var") %>% as.numeric %>% sqrt)
#'
#'     ci = attr(xp, "ci") %>% t %>% data.frame
#'     names(ci) = c("LL", "UL")
#'     if (is.na(ci$LL)) ci$LL = 0
#'     if (is.na(ci$UL)) ci$UL = 1
#'     ret1 %<>% cbind(ci)
#'
#'     ret1$`n numerator` = sum(design$variables$.tmp)
#'     ret1$`n denominator` = length(design$variables$.tmp)
#'     ret = rbind(ret, ret1)
#'   }
#'   ret$degf = df1
#'
#'   if (getOption("surveytable.do_present")) {
#'     ppo = getOption("surveytable.present_prop") %>% do.call(list(ret))
#'   } else {
#'     nlvs = design$variables[, vr] %>% nlevels
#'     ppo = list(flags = rep("", nlvs), has.flag = c())
#'   }
#'
#'   mp2 = getOption("surveytable.tx_prct") %>% do.call(list(ret[,c("Proportion", "SE", "LL", "UL")]))
#'   names(mp2) = getOption("surveytable.names_prct")
#'
#'   ##
#'   assert_that(nrow(mmc) == nrow(mp2)
#'               , nrow(mmc) == nrow(mmcr)
#'               , nrow(mmc) == length(pro$flags)
#'               , nrow(mmc) == length(pco$flags)
#'               , nrow(mmc) == length(ppo$flags) )
#'
#'   mp = cbind(mmc, mp2)
#'   flags = paste(pro$flags, pco$flags, ppo$flags) %>% trimws
#'   if (any(nzchar(flags))) {
#'     mp$Flags = flags
#'   }
#'
#'   ##
#'   rownames(mp) = NULL
#'   mp = cbind(data.frame(Level = lvs), mp)
#'
#'   attr(mp, "num") = 2:5
#'   attr(mp, "title") = .getvarname(design, vr)
#'   mp %<>% .add_flags( c(pro$has.flag, pco$has.flag, ppo$has.flag) )
#'   .write_out(mp, screen = screen, csv = csv)
#' }
#'
#' .present_restricted <- function(counts, th.n = 5) {
#'   has.flag = c()
#'   flags = rep("", length(counts))
#'
#'   #
#'   c.bad = 1:(th.n - 1)
#'   c.bad %<>% c(sum(counts) - c.bad)
#'   bool = (counts %in% c.bad)
#'   if (any(bool)) {
#'     f1 = "R"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'   list(flags = flags, has.flag = has.flag)
#' }
#'
#' .calc_samp_size <- function(design, vr, counts) {
#'
#'   # In svytotal(frm, design, deff = TRUE), DEff sometimes
#'   # appears incorrect. If no variability, DEff = Inf.
#'   # Calculating "Kish's Effective Sample Size" directly, bypassing DEff
#'   #	deff = attr(sto, "deff") %>% diag
#'
#'   design$wi = 1 / design$prob
#'   design$wi[design$prob <= 0] = 0
#'   design$wi2 = design$wi^2
#'   sum_wi = by(design$wi, design$variables[,vr], sum) %>% as.numeric
#'   sum_wi2 = by(design$wi2, design$variables[,vr], sum) %>% as.numeric
#'   neff = sum_wi^2 / sum_wi2
#'   neff <- na.omit(neff)
#'   #assert_that(length(neff) == length(counts))
#'   pmin(counts, neff, na.rm = TRUE)
#' }
#'
#' .present_count <- function(mmcr) {
#'   has.flag = c()
#'   flags = rep("", nrow(mmcr))
#'
#'   mmcr$rci = (mmcr$ul - mmcr$ll) / mmcr$x
#'   mmcr$Display = (mmcr$samp.size >= 10 & mmcr$rci <= 1.60)
#'   mmcr$Display = ifelse(is.na(mmcr$counts), FALSE, mmcr$Display)
#'   bool = (!mmcr$Display)
#'   if (any(bool)) {
#'     f1 = "Cx"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   bool = (mmcr$Display & mmcr$degf < 8)
#'   if (any(bool)) {
#'     f1 = "Cdf"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   list(flags = flags, has.flag = has.flag)
#' }
#'
#' .tx_count_1k <- function(x) {
#'   round(x / 1e3)
#' }
#'
#' .present_prop <- function(ret) {
#'   ret$`n effective` = with(ret, Proportion * (1 - Proportion) / (SE ^ 2))
#'   ret$`CI width` = with(ret, UL - LL)
#'
#'   idx.bad = (ret$`n numerator` == 0L | ret$`n numerator` == ret$`n denominator`)
#'   ret$`n effective`[idx.bad] = ret$`n denominator`[idx.bad]
#'   ret$`CI width`[idx.bad] = 0
#'   ret$`n effective` <- ifelse(is.na(ret$`n effective`), 0, ret$`n effective`)
#'   idx.nbig = (ret$`n effective` > ret$`n denominator`)
#'
#'   ret$`n effective`[idx.nbig] = ret$`n denominator`[idx.nbig]
#'
#'   ret$`relative CI width` = with(ret, `CI width` / Proportion)
#'
#'   #
#'   ret$Display = as.logical(NA)
#'   ret$Display[idx.30 <- (ret$`n effective` < 30)] = FALSE # "no: Effective sample size < 30"
#'   ret$Display[!idx.30
#'               & (idx.s <- (ret$`CI width` <= 0.05))] = TRUE # "YES: Absolute confidence interval width < 5%"
#'   ret$Display[!(idx.30 | idx.s)
#'               & (idx.l <- (ret$`CI width` >= 0.30))] = FALSE # "no: Absolute confidence interval width > 30%"
#'   ret$Display[!(idx.30 | idx.s | idx.l)
#'               & (idx.r <- (ret$`relative CI width` > 1.30))] = FALSE # "no: Relative confidence interval width > 130%"
#'   ret$Display[!(idx.30 | idx.s | idx.l) & !idx.r] = TRUE # "YES: Relative confidence interval width < 130%"
#'   assert_that( noNA(ret$Display) )
#'
#'   #
#'   has.flag = c()
#'   flags = rep("", nrow(ret))
#'   bool = (!ret$Display)
#'   if (any(bool)) {
#'     f1 = "Px"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   bool = ( ret$Display & !(idx.30 | idx.s | idx.l) & !idx.r
#'            & (idx.c <- (ret$`CI width` / (1 - ret$Proportion) > 1.30)) )
#'   if (any(bool)) {
#'     f1 = "Pc"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   bool = (ret$Display & ret$degf < 8)
#'   if (any(bool)) {
#'     f1 = "Pdf"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   bool = (ret$Display & (
#'     ret$`n numerator` == 0L | ret$`n numerator` == ret$`n denominator`))
#'   if (any(bool)) {
#'     f1 = "P0"
#'     flags[bool] %<>% paste(f1)
#'     has.flag %<>% c(f1)
#'   }
#'
#'   list(flags = flags, has.flag = has.flag)
#' }
#'
#' .tx_prct <- function(x) {
#'   round(x * 100, 1)
#' }
#'
#' .add_flags <- function(df1, has.flag) {
#'   if (is.null(has.flag)) {
#'     attr(df1, "footer") = "(No flags.)"
#'   } else {
#'     v1 = c()
#'     for (ff in has.flag) {
#'       v1 %<>% c(switch(ff
#'                        , R = "R: If the data is confidential, suppress *all* estimates, SE's, CI's, etc."
#'                        , Cx = "Cx: suppress count (and rate)"
#'                        , Cr = "Cr: footnote count - RSE" # .present_count_3030
#'                        , Cdf = "Cdf: review count (and rate) - degrees of freedom"
#'                        , Px = "Px: suppress percent"
#'                        , Pc = "Pc: footnote percent - complement"
#'                        , Pdf = "Pdf: review percent - degrees of freedom"
#'                        , P0 = "P0: review percent - 0% or 100%"
#'                        , paste0(ff, ": unknown flag!")
#'       ))
#'     }
#'     attr(df1, "footer") = v1 %>% paste(collapse="; ")
#'   }
#'   df1
#' }
#'
#' .write_out <-
#'   function(df1, screen, csv) {
#'     if (!is.null(txt <- attr(df1, "title"))) {
#'       txt %<>% paste0(" {", getOption("surveytable.survey_label"), "}")
#'       attr(df1, "title") = txt
#'     }
#'
#'     if (screen) {
#'       hh = df1 %>% hux %>% set_all_borders
#'       if (!is.null(txt <- attr(df1, "title"))) {
#'         caption(hh) = txt
#'       }
#'       if (!is.null(nc <- attr(df1, "num"))) {
#'         number_format(hh)[-1,nc] = fmt_pretty()
#'       }
#'       if (!is.null(txt <- attr(df1, "footer"))) {
#'         hh %<>% add_footnote(txt)
#'       }
#'
#'       # See inside guess_knitr_output_format
#'       not_screen = (requireNamespace("knitr", quietly = TRUE)
#'                     && requireNamespace("rmarkdown", quietly = TRUE)
#'                     && guess_knitr_output_format() != "")
#'
#'       if (not_screen) {
#'         hh %>% print_html
#'       } else {
#'         gow = getOption("width")
#'         options(width = 10)
#'         hh %>% print_screen(colnames = FALSE, min_width = 0, max_width = max(gow * 1.5, 150, na.rm=TRUE))
#'         options(width = gow)
#'         cat("\n")
#'       }
#'     }
#'
#'     if (nzchar(csv)) {
#'       if (!is.null(txt <- attr(df1, "title"))) {
#'         write.table(txt, file = csv
#'                     , append = TRUE, row.names = FALSE
#'                     , col.names = FALSE
#'                     , sep = ",", qmethod = "double") %>% suppressWarnings
#'       }
#'       write.table(df1, file = csv
#'                   , append = TRUE, row.names = FALSE
#'                   , sep = ",", qmethod = "double") %>% suppressWarnings
#'       if (!is.null(txt <- attr(df1, "footer"))) {
#'         write.table(txt, file = csv
#'                     , append = TRUE, row.names = FALSE
#'                     , col.names = FALSE
#'                     , sep = ",", qmethod = "double") %>% suppressWarnings
#'       }
#'       cat("\n", file = csv, append = TRUE)
#'     }
#'
#'     # Important for integrating the output into other programming tasks
#'     names(df1) %<>% make.unique
#'     rownames(df1) = NULL
#'     invisible(df1)
#'   }
#'
#' recall_ltrb <- function (ht, template, sides = c("left", "top", "right", "bottom"))
#' {
#'   call <- sys.call(sys.parent(1L))
#'   call_names <- parse(text = paste0("huxtable::", sprintf(template,
#'                                                           sides)))
#'   for (cn in call_names) {
#'     call[[1]] <- cn
#'     call[[2]] <- quote(ht)
#'     ht <- eval(call, list(ht = ht), parent.frame(2L))
#'   }
#'   ht
#' }
#'
#'
#' # Code for expenditures at person drug level before switching to median -----
#' {
#'   assign("survey_object", expenditure_person_drug_level_srvy, envir = .GlobalEnv)
#'   survey_object <- svystandardize(
#'     survey_object,
#'     ~ AGE,
#'     ~ COVERTYPE,
#'     population = standard_age_proportions_18,
#'     excluding.missing = ~ AGE
#'   )
#'   set_survey('survey_object')
#'   set_output(max_levels = 2e5)
#'   surveytable::set_count_int()
#'   temp <-
#'     tab_subset('RXFEXPSELF', 'drug_insurance')
#'   temp$year <- years
#'   temp <- temp |>
#'     select(-`% known`) |>
#'     mutate(insulin_indicator = case_when(
#'       Level %in% c(
#'         'Basal/Analog',
#'         'Bolus/Analog',
#'         'Basal/Human',
#'         'Bolus/Human',
#'         'Pre-mixed/Human',
#'         'Pre-mixed/Human and Analog'
#'       ) ~ 'Insulin',
#'       TRUE ~ 'Not Insulin'
#'     ))
#'   temp
#' }
#'
#'
#'
#' # Code for conditional labeling tablecells --------------------------------
#'
#' # comparisons_gt <- gt(comparison_table)
#' #
#' # fill_column <- function(gtobj, column){
#' #   for(i in seq_along(comparison_table %>% pull(sym(column)))){
#' #     if (is.na(comparison_table[[column]][i])) { color <- 'black' }
#' #     else if (comparison_table[[column]][i] > 0) {color <-  'lightgreen'}
#' #     else if (comparison_table[[column]][i] == 0) {color <- 'grey50'}
#' #     else if (comparison_table[[column]][i] < 0) {color <-  'lightblue'}
#' #     gtobj <- gtobj %>%
#' #       tab_style(style = cell_fill(color = color),
#' #                 locations = cells_body(columns = column, rows = i)
#' #       )
#' #   }
#' #   gtobj
#' # }
#' #
#' # color_comparisons_gt <- comparisons_gt |>
#' #   fill_column('All insulins') |>
#' #   fill_column('Biguanides') |>
#' #   fill_column('DPP-4i') |>
#' #   fill_column('GLP-1RA') |>
#' #   fill_column('Insulin - Basal/Analog') |>
#' #   fill_column('Insulin - Basal/Human') |>
#' #   fill_column('Insulin - Bolus/Analog') |>
#' #   fill_column('Insulin - Bolus/Human') |>
#' #   fill_column('Insulin - Insulin/Unknown') |>
#' #   fill_column('Insulin - Pre-mixed/Analog') |>
#' #   fill_column('Insulin - Pre-mixed/Human') |>
#' #   fill_column('SGLT2i') |>
#' #   fill_column('Sulfonylureas') |>
#' #   fill_column('TZD') |>
#' #   fill_column('Unknown')
#' # gtsave(color_comparisons_gt, 'output/test_color.docx')
#'
#' #   * Pooled person level ------------------------------------------------------
# tar_target(pooled_person_svy,
#            pooled_person_dat %>%
#              ungroup() %>%
#              mutate(across(ends_with('name'), ~as.factor(.x)),
#                     across(any_of(diabetes_drugs), ~as.factor(.x)),
#                     across(contains('DIABDX'), ~as.factor(.x)),
#                     across(contains('RXNAME'), ~as.factor(.x))) %>%
#              svydesign(
#              ids = ~PSU9621,
#              weights = ~DIABWF,
#              strata = ~STRA9621,
#              data = .,
#              nest = TRUE
#            )),
#
# tar_target(crude_pooled_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab(diabetes_drugs[i])
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i],
#                           across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
#                    #filter(Level == 1) %>%
#                    select(-Level, drug, everything())
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#            }),
#
# tar_target(aa_pooled_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              survey_object <- svystandardize(survey_object,
#                                              ~AGE,
#                                              ~1,
#                                              population = standard_age_proportions_18,
#                                              excluding.missing = ~AGE)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab(diabetes_drugs[i])
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i],
#                           across(where(is.numeric) & !starts_with('year'), ~format(.x, big.mark = ','))) %>%
#                    #filter(Level == 1) %>%
#                    select(-Level, drug, everything())
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#            }),
#
# tar_target(pooled_sex_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              survey_object <- svystandardize(survey_object,
#                                              ~AGE,
#                                              ~SEX,
#                                              population = standard_age_proportions_18,
#                                              excluding.missing = ~AGE)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab_subset(diabetes_drugs[i], 'SEX')
#                  temp <- bind_rows(temp, .id = 'SEX')
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i]) %>%
#                    filter(Level == 1)
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#              table_df <- table_df %>%
#                select(drug, everything(), -Level)
#            }),
# tar_target(pooled_age_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              survey_object <- svystandardize(survey_object,
#                                              ~AGE,
#                                              ~1,
#                                              population = standard_age_proportions_18,
#                                              excluding.missing = ~AGE)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab_subset(diabetes_drugs[i], 'AGE')
#                  temp <- bind_rows(temp, .id = 'AGE')
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i]) %>%
#                    filter(Level == 1)
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#              table_df <- table_df %>%
#                select(drug, everything(), -Level)
#            }),
# tar_target(pooled_race_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              survey_object <- svystandardize(survey_object,
#                                              ~AGE,
#                                              ~RACETHNX,
#                                              population = standard_age_proportions_18,
#                                              excluding.missing = ~AGE)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab_subset(diabetes_drugs[i], 'RACETHNX')
#                  temp <- bind_rows(temp, .id = 'RACETHNX')
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i]) %>%
#                    filter(Level == 1)
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#              table_df <- table_df %>%
#                select(drug, everything(), -Level)
#            }),
# tar_target(pooled_edu_prop,
#            {
#              assign("survey_object", pooled_person_svy, envir = .GlobalEnv)
#              survey_object <- svystandardize(survey_object,
#                                              ~AGE,
#                                              ~HIDEG,
#                                              population = standard_age_proportions_18,
#                                              excluding.missing = ~AGE)
#              set_survey('survey_object')
#              set_output(max_levels = 2e5)
#              surveytable::set_count_int()
#              table_list <- list()
#              for (i in 1:length(diabetes_drugs)) {
#                if (diabetes_drugs[i] %in% names(pooled_person_svy$variables)) {
#                  temp <- tab_subset(diabetes_drugs[i], 'HIDEG')
#                  temp <- bind_rows(temp, .id = 'HIDEG')
#                  temp <- temp %>%
#                    mutate(drug = diabetes_drugs[i]) %>%
#                    filter(Level == 1)
#                  table_list[[i]] <- temp
#                }
#              }
#              table_df <- bind_rows(table_list)
#              table_df <- table_df %>%
#                select(drug, everything(), -Level)
#            }),
#


