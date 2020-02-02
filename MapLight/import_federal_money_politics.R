library(dplyr)
library(readr)
library(DBI)
library(iterators)
library(data.table)

Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE = "crsp")
Sys.setenv(PGUSER = "yanzih1", PGPASSWORD = "temp_20190711")

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO mschabus")

datafiles <- Sys.glob("MapLight/federal_money_politics_data/*.csv")
for (i in 1:length(datafiles)) {
  file_in_process = as_tibble(fread(datafiles[i], colClasses = c("DonorZipCode" = "character", "TransactionAmount" = "double", "RecipientZipCode" = "character")))
  dbWriteTable(pg, "federal_money_politics", file_in_process, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

rs <- dbExecute(pg, "ALTER TABLE federal_money_politics OWNER TO mschabus")
