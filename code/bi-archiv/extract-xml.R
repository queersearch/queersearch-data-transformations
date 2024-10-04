library(tidyverse)
library(rrapply)
library(xml2)
library(here)
library(googlesheets4)

INPUT_FILE <- "data/input/datenbriefkasten/2024-09-27-Bi(-+) Archiv Export dublin_core_data - ID 110.xml"
THIS_INST <- "biarchiv"


cli::cli_alert_info("get data from {INPUT_FILE}")

xml_raw <- xml2::as_list(xml2::read_xml(here(INPUT_FILE)))

xml_list <- rrapply::rrapply(
  xml_raw$archiveEntry, 
  how = "melt",
  options = list(namecols = TRUE, simplify =T)
) |> as_tibble() |> 
  select(-L5)

xml_as_df_long <- xml_list |> 
  mutate(L2 = str_replace(L2, "1", ""),
         L3 = str_replace(L3, "1", ""),
         L4 = str_replace(L4, "1", ""),
         L3 = ifelse(is.na(L3), "", L3),
         L4 = ifelse(is.na(L4), "", L4),
         id_helper = glue::glue("{L1} {L2} {L3} {L4}")
  ) |> 
  unnest_longer(value)


xml_as_df_wide <- xml_as_df_long |> 
  reframe(value = glue::glue_collapse(value, sep = ";"), .by = id_helper) |> 
  mutate(inst = THIS_INST)
  
  


clipr::write_clip(xml_as_df)

write_sheet(data = xml_as_df_wide, ss = "1piP_gnek5t1MorEEShWkTldyMCrqsBT8dJmfmY65ZbU")
write_sheet(data = xml_as_df_long, ss = "1piP_gnek5t1MorEEShWkTldyMCrqsBT8dJmfmY65ZbU")

