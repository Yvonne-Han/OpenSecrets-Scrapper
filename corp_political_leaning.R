library(dplyr)
library(readr)
library(DBI)

Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE = "crsp")
Sys.setenv(PGUSER = "yanzih1", PGPASSWORD = "temp_20190711")

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO mschabus")

org_donations <- tbl(pg, "org_donations")

corp_political_leaning <- org_donations %>%
  filter(cycle %in% c(2012, 2014, 2016, 2018)) %>%
  select(ticker, cycle, total, democrats, republicans) %>%
  mutate_if(bit64::is.integer64, as.double) %>%
  mutate(d_over_total = democrats / total, r_over_total = republicans / total) %>%
  mutate(political_leaning = if_else(democrats > republicans, "D", "R"))

corp_political_leaning_local <- corp_political_leaning %>%
  collect() %>%
  haven::write_dta("OpenSecrets/corp_political_leaning.dta")
