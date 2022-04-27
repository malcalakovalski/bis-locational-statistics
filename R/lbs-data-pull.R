
# What is this? ---------------------------------------------------------------------------------------------------

# This script pulls the latest release of the BIS locational banking statistics (LBS). The LBS measure international banking activity from a residence perspective, focusing on the location of the banking office. The LBS capture outstanding claims (financial assets) and liabilities of internationally active banks located in reporting countries on counterparties residing in more than 200 countries.

# Setup -----------------------------------------------------------------------------------------------------------

librarian::shelf(tidyverse, janitor, vroom, readxl)


# Download data ---------------------------------------------------------------------------------------------------

# Download and unzip csv file to a temporary location
# This way we are always pulling from the most recent data
# without saving a large file to our local disk
url <- 'https://www.bis.org/statistics/full_lbs_d_pub_csv.zip'
temp <- tempfile()
download.file(url, temp)

lbs_full <-
  vroom(unz(temp, 'WS_LBS_D_PUB_csv_col.csv')) |> # Vroom reads large datasets quickly
  clean_names() |>
  select(-c(frequency, time_format, collection_indicator, organisation_visibility, time_period))


# Query unfiltered data -------------------------------------------------------------------------------------------

# For this analysis we will separate the BIS data into reported and derived values
# Before doing that apply the filters in common that we want to use
# for reported and derived to avoid repetition.
lbs_filtered <-
  lbs_full |>
  filter(measure == "S:Amounts outstanding / Stocks",
         currency_denomination == "TO1:All currencies",
         currency_type_of_reporting_country == "A:All currencies (=D+F+U)",
         type_of_reporting_institutions == "A:All reporting banks/institutions (domestic, foreign, consortium and unclassified)",
         parent_country == "5J:All countries (total)",
         position_type == "N:Cross-border",
         counterparty_sector %in% c("A:All sectors", "B:Banks, total", "N:Non-banks, total"))


# Reported --------------------------------------------------------------------------------------------------------


reported <-
  lbs_filtered|>
  filter(counterparty_country == "5J:All countries (total)") |>
  mutate(across(where(is.character),
                str_remove,
                pattern = "^(.*?):")) |>
  pivot_longer(where(is.numeric),
               names_to = 'date',
               values_to = 'reported') |>
  # This is end of year data so we can make it annual by only keeping Q4 observations
  filter(str_ends(date, 'q4')) |>
  mutate(date = str_extract(date, '[0-9]{4}') |> as.numeric()) |>
  filter(type_of_instruments %in% c('Debt securities', 'All instruments', 'Loans and deposits')) |>
  select(date, country = reporting_country, position = balance_sheet_position, instrument = type_of_instruments, sector = counterparty_sector, reported)


# Derived --------------------------------------------------------------------------------------------------------


# List of aggregates we want to exclude from counterparty countries
aggregates <-
  c("International organisations",
  "Offshore centres",
  "British Overseas Territories",
  "Residual developing Europe",
  "Residual former Serbia and Montenegro",
  "Residual former Netherlands Antilles",
  "Residual developing Latin America and Caribbean",
  "Residual offshore centres",
  "Residual developing Asia and Pacific",
  "Residual developed countries",
  "Residual former Yugoslavia",
  "Residual former Soviet Union",
  "Residual former Czechoslovakia",
  "Residual developing Africa and Middle East",
  "Developing Europe",
  "Non-European developed countries",
  "Emerging market and developing economies",
  "Developing Latin America and Caribbean",
  "Developing Africa and Middle East",
  "Developing Asia and Pacific",
  "Euro area",
  "All countries (total)",
  "European developed countries",
  "Unallocated location",
  "Developed countries")
# Negate %in% operator to exclude aggregates inside filter()
`%notin%` <- purrr::negate(`%in%`)

derived <-
  lbs_filtered |>
  filter(reporting_country == '5A:All reporting countries') |>
  mutate(across(where(is.character),
                str_remove,
                pattern = "^(.*?):")) |>
  filter(counterparty_country %notin% aggregates) |>
  pivot_longer(where(is.numeric),
               names_to = 'date',
               values_to = 'derived') |>
  filter(str_ends(date, 'q4')) |>
  mutate(date = str_extract(date, '[0-9]{4}') |> as.numeric()) |>
  filter(type_of_instruments %in% c('Debt securities', 'All instruments', 'Loans and deposits')) |>
  select(date, country = counterparty_country, position = balance_sheet_position, instrument = type_of_instruments, sector = counterparty_sector, derived) |>
  # Labels for derived data are inverted between assets and liabilities.
  # The reason is that for, say, loans and deposits the BIS calls assets
  # all claims by BIS-reporting banks vis-à-vis a given counterparty,
  # but from the counterparty perspective (which is the one we take) these are
  # liabilities vis-à-vis BIS-reporting banks
  mutate(position = case_when(position == 'Total claims' ~ 'Total liabilities',
                              position == 'Total liabilities' ~ 'Total claims'))


# Combine + Reshape ----------------------------------------------------------------------------------------------


lbs_long <-
  full_join(reported, derived) |>
  filter(country != 'All reporting countries')

# Drop sectoral detail for debt securities
debt_by_sector <- filter(lbs_long, instrument == 'Debt securities' & sector != 'All sectors')

lbs_wide <-
  lbs_long |>
  anti_join(debt_by_sector) |>
  mutate(position = str_remove(position, 'Total '),
         position = str_replace(position, 'claims', 'assets'),
         across(c(position, instrument, sector), str_to_lower)) |>
  mutate(instrument = case_when(instrument == 'all instruments' ~ 'all',
                                instrument == 'debt securities' ~ 'debt',
                                instrument == 'loans and deposits' ~ 'loans_dep'),
         sector = case_when(sector == 'all sectors' ~ 'total',
                            sector == 'banks, total' ~ 'bank',
                            sector == 'non-banks, total' ~ 'nonbank')) |>
  pivot_wider(names_from = c(sector, position, instrument),
              values_from = c(reported, derived)) |>
  rename_with( ~ str_remove(.x, '_all'), ends_with('_all')) |>
  select(date,
         country,
         reported_total_assets,
         reported_total_liabilities,
         reported_total_assets_loans_dep,
         reported_total_liabilities_loans_dep,
         derived_total_assets,
         derived_total_liabilities,
         derived_bank_assets,
         derived_nonbank_assets,
         derived_bank_liabilities,
         derived_nonbank_liabilities,
         derived_total_assets_loans_dep,
         derived_total_liabilities_loans_dep,
         derived_bank_assets_loans_dep,
         derived_nonbank_assets_loans_dep,
         derived_bank_liabilities_loans_dep,
         derived_nonbank_liabilities_loans_dep,
         derived_total_assets_debt,
         derived_total_liabilities_debt)


# Rename countries ------------------------------------------------------------------------------------------------

# We want the country ordering and names to match the EWN

# Dictionary of BIS-EWN country names provided by Gian Maria
dict <-
  read_xlsx('data/dictionary-bis-ewn.xlsx') |>
  clean_names() |>
  drop_na()

# Make a new data set for the ewn countries not present in the BIS
lbs_missing_countries <-
  read_xlsx('data/dictionary-bis-ewn.xlsx') |>
  clean_names() |>
  filter(is.na(bis)) |>
  select(country = ewn) |>
  # Fill dates
  crossing(date = seq(1970,  max(lbs_wide$date), by = 1))

# Country ordering, also provided by Gian Maria
order <- read_xlsx('data/country_ordering.xlsx')

lbs_final <-
  lbs_wide |>
  left_join(dict, by = c('country' = 'bis')) |>
  mutate(country = ewn) |>
  select(-ewn) |>
  # Expand date variable to include 1970-1977
  group_by(country) |>
  complete(date = seq(1970,  max(date), by = 1)) |>
  ungroup() |>
  # Match EWN country ordering
  arrange(match(country, unique(order$country))) |>
  drop_na(country) |>
  # Add (with blanks) the 9 countries not included in the BIS data
  full_join(lbs_missing_countries, by = c('country', 'date'))

writexl::write_xlsx(lbs_final, 'data/locational-statistics.xlsx')
