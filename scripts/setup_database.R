# NOTE: To load data, you must download both the extract's data and the DDI
# and also set the working directory to the folder with these files (or change the path below).
library(tidyverse)
library(ipumsr)
library(duckdb)
library(rvest)

# Read the HTML page containing the table
html <- read_html("https://wwwn.cdc.gov/nchs/nhanes/1999-2000/RXQ_DRUG.htm#Appendix_3:_Multum_Lexicon_Therapeutic_Classification_Scheme_")

# Extract tables from the HTML page
tables <- html_table(html)

# Select the table of interest (e.g., the first table)
multum_class_code <- tables[[3]] %>%
  janitor::clean_names() %>%
  mutate(first_level_category_name = case_when(first_level_category_name == 'IMMUNOLOGIC AGENTS (DELETE)' ~ 'IMMUNOLOGIC AGENTS',
                                               TRUE ~ first_level_category_name))
write_csv(multum_class_code, 'data/multum_class_code.csv')
not_all_na <- function(x) all(!is.na(x))

ddi <- read_ipums_ddi("data/meps_00011.xml")
data <- read_ipums_micro(ddi)



# Specify the file path for the DuckDB database
db_file_path <- "data/ipums_rx.db"

# Create or connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_file_path)

# Write your DataFrame to the DuckDB database
dbWriteTable(con, "full_data", data, overwrite = TRUE)
dbWriteTable(con, 'multum_class_code', multum_class_code, overwrite = TRUE)

# Close the connection (optional but recommended)
dbDisconnect(con)

