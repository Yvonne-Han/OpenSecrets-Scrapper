library(dplyr)
library(readr)
library(DBI)

#### Write functions to import data ####
# Candidates #
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
               col_types = "cccccccccccc")
  return(cands)
}


# FEC Committees # 
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


# Pacs to candidates # 
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


# Pacs to Pacs #
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



# Individual contributions #
import_indivs <- function(indivs_file) {
  file_path = paste("raw_bulk_data/", indivs_file, ".txt", sep = "")
  indivs_lines <- read_lines(file_path, skip = 3000000, n_max = 3000000)
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


