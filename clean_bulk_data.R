library(dplyr)
library(readr)
library(DBI)
library(iterators)

Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE = "crsp")
Sys.setenv(PGUSER = "yanzih1", PGPASSWORD = "temp_20190711")

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO mschabus")

#### Write functions to import data ####
# Candidates 
import_cands <- function(cands_file) {
  file_path = paste("raw_bulk_data/", cands_file, ".txt", sep = "")
  cands_lines <- read_lines(file_path)
  cands <- cands_lines %>%
    gsub("\\|,", "", .) %>%
    gsub("^\\|", "", .) %>%
    gsub("\\|$", "", .) %>%
    read_delim(delim = "|", col_names = c("cycle", "feccandid", "cid", "firstlastp", 
                                          "party", "distidrunfor", "distidcurr", 
                                          "currcand", "cyclecand", "crpico", "recipcode", "nopacs"), 
               col_types = "cccccccccccc") %>%
  return(cands)
}


# FEC Committees 
import_cmtes <- function(cmtes_file) {
  file_path = paste("raw_bulk_data/", cmtes_file, ".txt", sep = "")
  cmtes_lines <- read_lines(file_path)
  cmtes <- cmtes_lines %>%
    iconv("latin1", "UTF-8") %>%
    gsub("\\,,", "\\,||,", .) %>%
    gsub("\\,,", "\\,||,", .) %>% 
    gsub("\\,,", "\\,||,", .) %>% 
    gsub("\\,,", "\\,||,", .) %>% 
    gsub(",(?!.*,)", "\\,\\|", ., perl = TRUE) %>%
    gsub("\\|,", "", .) %>%
    gsub("^\\|", "", .) %>%
    read_delim(delim = "|", 
               quote = "", 
               col_names = c("cycle", "cmtelid", "pacshort", "affiliate", 
                                          "ultorg", "recipid", "recipcode", "feccandid", 
                                          "party", "primcode", "source", "sensitive", 
                                          "foreign", "active"), 
               col_types = "ccccccccccccci")
  return(cmtes)
}


# Pacs to candidates 
import_pacs <- function(pacs_file) {
  file_path = paste("raw_bulk_data/", pacs_file, ".txt", sep = "")
  pacs_lines <- read_lines(file_path)
  pacs <- pacs_lines %>%
    iconv("latin1", "UTF-8") %>%         # Correct for unexpected encoding (if any)
    gsub("\\,,", "\\,||,", .) %>%        # if two consecutive commas exist, add delimeter and replace with ,||, 
    gsub("\\,,", "\\,||,", .) %>%   
    gsub("\\,,", "\\,||,", .) %>% 
    gsub("\\|\\,(\\-?\\d+),(\\d\\d/\\d\\d/\\d\\d\\d\\d),", "\\|\\,\\|\\1\\|\\,\\|\\2\\|\\,", .) %>%     # if amount, date are not properly identfied, add delimiter
    gsub("\\|,(\\-?\\d+),", "\\|\\,\\|\\1\\|,", .) %>%                                                  # deal with situations where only amount exists
    gsub("\\|\\,$", "\\|\\,\\|\\|", .) %>%                                                              # if a comma is at the end of the string, add ,|| to fill in the last element
    gsub("\\|,", "", .) %>%             # replace delimiter
    gsub("^\\|", "", .) %>%             # delete the delimiter | at the beginning of the string
    gsub("\\|$", "", .) %>%             # delete the delimiter | at the end of the string 
    read_delim(delim = "|", 
               quote = "",
               col_names = c("cycle", "fecrecno", "pacid", "cid", 
                             "amount", "date", "realcode", "type", 
                             "di", "feccandid"), 
               col_types = "ccccdccccc")
  return(pacs)
}


# Pacs to Pacs 
import_pac_other <- function(pac_other_file) {
  file_path = paste("raw_bulk_data/", pac_other_file, ".txt", sep = "")
  pac_other_lines <- read_lines(file_path)
  pac_other <- pac_other_lines %>%
    iconv("latin1", "UTF-8") %>%
    gsub(",(\\d\\d/\\d\\d/\\d\\d\\d\\d),(\\-?\\d+.0),", "\\,\\|\\1\\|\\,\\|\\2\\|\\,", .) %>%    # Deal with no delimeters for date and amount
    gsub(",(\\-?\\d+.0),", ",\\|\\2\\|\\,", .) %>%                                               # If there is no date, adjust for the amount
    gsub("\\,,", "\\,||,", .) %>%
    gsub("\\,,", "\\,||,", .) %>%   
    gsub("\\,,", "\\,||,", .) %>% 
    gsub("\\|\\,\\|", "\\|", .) %>%
    gsub("^\\|", "", .) %>%
    gsub("(\\s)*\\|$", "", .) %>%
    read_delim(delim = "|", 
               quote = "",
               col_names = c("cycle", "fecrecno", "filerid", "donorcmte", 
                             "contriblendtrans", "city", "state", "zip", 
                             "fecoccemp", "primcode", "date", "amount", "recipid", 
                             "party", "otherid", "recipcode", "recipprimcode",
                             "amend", "report", "pg", "microfilm", "type",
                             "realcode", "source"), 
               col_types = "cccccccccccdcccccccccccc")
  return(pac_other)
}



# Individual contributions 
import_indivs <- function(indivs_file) {
  file_path = paste("raw_bulk_data/", indivs_file, ".txt", sep = "")
  indivs_lines <- read_lines(file_path)
  indivs <- indivs_lines %>%
    iconv("latin1", "UTF-8") %>%            # Correct for unexpected encoding (if any)
    gsub("\\|\\,,", "\\,||,", .) %>%        # if two consecutive commas exist, add delimeter and replace with ,||, 
    gsub("\\|\\,,", "\\,||,", .) %>%   
    gsub("\\|\\,,", "\\,||,", .) %>% 
    gsub("\\|\\|\\s+\\,", "\\|\\|\\,", .) %>%
    gsub("\\|\\s+\\,", "\\|\\,", .) %>%
    gsub("\\|\\,(\\d\\d/\\d\\d/\\d\\d\\d\\d),(\\-?\\d+),", "\\|\\,\\|\\1\\|\\,\\|\\2\\|\\,", .) %>%     # if amount, date are not properly identfied, add delimiter
    gsub("\\|,(\\-?\\d+),", "\\|\\,\\|\\1\\|,", .) %>%                                                  # deal with situations where only amount exists
    gsub("\\|,(\\d\\d/\\d\\d/\\d\\d\\d\\d),", "\\|\\,\\|\\1\\|,", .) %>%                                # deal with situations where only date exists
    gsub("(\\s)+\\|", "\\|", .) %>%
    gsub("(\\s)+$", "", .) %>%
    gsub("\\|,\\|", "|", .) %>%             # replace delimiter
    gsub("^\\|", "", .) %>%                 # delete the delimiter | at the beginning of the string
    gsub("\\|\\s?$", "", .) %>%             # delete the delimiter | at the end of the string 
    read_delim(delim = "|", 
               quote = "",
               col_names = c("cycle", "fectransid", "contribid", "contrib", 
                             "recipid", "orgname", "ultorg", "realcode", 
                             "date", "amount", "street", "city", "state",
                             "zip", "recipcode", "type", "cmteid", "otherid",
                             "gender", "microfilm", "occupation", "employer",
                             "source"), 
               col_types = "cccccccccdccccccccccccc")
  return(indivs)
}


# To deal with large indivs files, process a chunk at a time 
# To be used with the loop below for importing indivs
import_indivs_chunk <- function(indivs_file) {
  indivs <- indivs_file %>%
    iconv("latin1", "UTF-8") %>%            # Correct for unexpected encoding (if any)
    gsub("\\|\\,,", "\\,||,", .) %>%        # if two consecutive commas exist, add delimeter and replace with ,||, 
    gsub("\\|\\,,", "\\,||,", .) %>%   
    gsub("\\|\\,,", "\\,||,", .) %>% 
    gsub("\\|\\|\\s+\\,", "\\|\\|\\,", .) %>%
    gsub("\\|\\s+\\,", "\\|\\,", .) %>%
    gsub("\\|\\,(\\d\\d/\\d\\d/\\d\\d\\d\\d),(\\-?\\d+),", "\\|\\,\\|\\1\\|\\,\\|\\2\\|\\,", .) %>%     # if amount, date are not properly identfied, add delimiter
    gsub("\\|,(\\-?\\d+),", "\\|\\,\\|\\1\\|,", .) %>%                                                  # deal with situations where only amount exists
    gsub("\\|,(\\d\\d/\\d\\d/\\d\\d\\d\\d),", "\\|\\,\\|\\1\\|,", .) %>%                                # deal with situations where only date exists
    gsub("(\\s)+\\|", "\\|", .) %>%
    gsub("(\\s)+$", "", .) %>%
    gsub("\\|,\\|", "|", .) %>%             # replace delimiter
    gsub("^\\|", "", .) %>%                 # delete the delimiter | at the beginning of the string
    gsub("\\|\\s?$", "", .) %>%             # delete the delimiter | at the end of the string 
    read_delim(delim = "|", 
               quote = "",
               col_names = c("cycle", "fectransid", "contribid", "contrib", 
                             "recipid", "orgname", "ultorg", "realcode", 
                             "date", "amount", "street", "city", "state",
                             "zip", "recipcode", "type", "cmteid", "otherid",
                             "gender", "microfilm", "occupation", "employer",
                             "source"), 
               col_types = "cccccccccdccccccccccccc")
  return(indivs)
}


#### Create tables and save them on the server ####
# Candidates 
for (year in c(12, 14, 16, 18)) {
  cands_file_name = paste("cands", year, sep = "")
  cands_file <- import_cands(cands_file_name)
  dbWriteTable(pg, "cands", cands_file, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

rs <- dbExecute(pg, "ALTER TABLE cands OWNER TO mschabus")


# FEC Committees 
for (year in c(12, 14, 16, 18)) {
  cmtes_file_name = paste("cmtes", year, sep = "")
  cmtes_file <- import_cmtes(cmtes_file_name)
  dbWriteTable(pg, "cmtes", cmtes_file, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

rs <- dbExecute(pg, "ALTER TABLE cmtes OWNER TO mschabus")


# indivs
for (year in c(12, 14, 16, 18)) {
  indivs_file_name = paste("raw_bulk_data/indivs", year, ".txt", sep = "")
  indivs_line <- ireadLines(indivs_file_name, n = 1000000)
  while (TRUE) {
    chunk <- try(nextElem(indivs_line), silent = TRUE)
    if (class(chunk) == "try-error") break
    indivs_chunk <- import_indivs_chunk(chunk)
    dbWriteTable(pg, "indivs", indivs_chunk, overwrite = FALSE, append = TRUE, row.names = FALSE)
  }
}

rs <- dbExecute(pg, "ALTER TABLE indivs OWNER TO mschabus")


# Pacs to candidates 
for (year in c(12, 14, 16, 18)) {
  pacs_file_name = paste("pacs", year, sep = "")
  pacs_file <- import_pacs(pacs_file_name)
  dbWriteTable(pg, "pacs", pacs_file, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

rs <- dbExecute(pg, "ALTER TABLE pacs OWNER TO mschabus")


# Pacs to others
for (year in c(12, 14, 16, 18)) {
  pac_other_file_name = paste("pac_other", year, sep = "")
  pac_other_file <- import_pac_other(pac_other_file_name)
  dbWriteTable(pg, "pac_other", pac_other_file, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

rs <- dbExecute(pg, "ALTER TABLE pac_other OWNER TO mschabus")