library(tidyverse)
library(rrapply)
library(xml2)
library(here)
library(googlesheets4)

INPUT_FILE <- "data/input/datenbriefkasten/queersearch_radusch.xml"
THIS_INST <- "ffbiz"


cli::cli_alert_info("get data from {INPUT_FILE}")

xml_raw <- xml2::as_list(xml2::read_xml(here(INPUT_FILE)))

xml_list <- rrapply::rrapply(
  xml_raw$FFBIZ, 
  how = "list"
)


xml_as_df <- map_df(xml_list , function(x) {
  helper_id <- unlist(x$id)
  tbl <- x |> 
    enframe() |> 
    unnest_longer(value) |> 
    unnest_longer(value) |> 
    mutate(helper_id = helper_id)
  return(tbl)
}) |> 
  # mehrere schlagworte o.ä. in einer Zeile zusammenpacken
  reframe(value = glue::glue_collapse(value, sep = ";"), .by = c(helper_id, name)) |> 
  pivot_wider(id_cols = helper_id, names_from = name, values_from = value) |> 
  mutate(inst = "ffbiz")




