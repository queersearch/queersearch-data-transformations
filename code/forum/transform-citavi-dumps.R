library(tidyverse)
library(DBI)
library(testdat)

CITAVI_BOOKS <- "Archiv fhm Bücher (Team).ctt4"
CITAVI_DIGITALES <- "Archiv fhm Digitales (Team).ctt4"
CITAVI_POSTER <- "Archiv fhm Poster (Team).ctt4"

all_dumps <- c(CITAVI_BOOKS, CITAVI_DIGITALES, CITAVI_POSTER)

expected_nr_tbls <- 28


# Test if dumps have same structure ---------------------------------------

tbls_in_dumps_long <- map_df(all_dumps, function(this_dump) {
  con <- dbConnect(RSQLite::SQLite(), here::here(paste0("data/", this_dump)))
  tmp <- dbListTables(con) %>% enframe() %>% mutate(dump = this_dump)
  DBI::dbDisconnect(con)
  tmp
})

tbls_in_dumps_wide <- tbls_in_dumps_long %>% 
  mutate(dump = str_remove_all(dump, "Archiv fhm |\\(Team\\).ctt4| ")) %>% 
  pivot_wider(names_from = dump, values_from = value)

test_that(
  desc = "Alle Dumps haben die gleiche Anzahl an Tabellen: nämlich genau 28",
  expect_equal(tbls_in_dumps_long %>% count(dump) %>% filter(n != expected_nr_tbls) %>% nrow() , 0)
)

test_that(
  desc = "Alle Dumps enthalten die gleichen Tavbellen(namen)",
  expect_equal(
    tbls_in_dumps_wide %>% 
      rowwise() %>% 
      select(-name) %>% 
      mutate(is_equal = n_distinct(c_across(everything())) == 1) %>% 
      ungroup() %>% 
      filter(is_equal == FALSE) %>% 
      nrow(),
    0
  )
)

# Test: Mit Büchern, später Funktion -------------------------------------

this_dump <- CITAVI_POSTER

con <- dbConnect(RSQLite::SQLite(), here::here(paste0("data/", this_dump)))

dump_cat <- str_remove_all(this_dump, "Archiv fhm |\\(Team\\).ctt4| ")

tbls <- tbls_in_dumps_wide %>% select(name, any_of(dump_cat)) %>% pull(any_of(dump_cat))

all_tbls_res <- setNames(map(tbls, function(this_tbl) {
  sql <- glue::glue_sql("SELECT * FROM {this_tbl}; ", .con = con)
  query <- DBI::dbSendQuery(con, sql)
  res <- DBI::dbFetch(query) %>% as_tibble()
  
}), tbls)
  


# how many entries has each of the tables? --------------------------------

nrows_of_tbls <- all_tbls_res %>% map_df(nrow) %>% 
  pivot_longer(everything()) %>% 
  rename(tbl = name, nrow = value) %>% 
  arrange(desc(nrow))


nrows_of_tbls


# Reference tbl a central hub ---------------------------------------------
# the Reference table is the central table in a Citavi dump
# Other metadata like Person or Editor are stored in 

# keep all columns that are not empty
non_empty_cols <- janitor::remove_empty(all_tbls_res$Reference, which = "cols") 

# all links to other tables end with "ID"
reference_tbls_names <- non_empty_cols %>% 
  names() %>% 
  enframe() %>% 
  filter(str_detect(value, "ID$"))

content_tbls_names <- non_empty_cols %>% 
  names() %>% 
  enframe() %>% 
  filter(!str_detect(value, "ID$"))


test_that(desc = "count of reference_tbls and reference_tbls is non_empty_cols", 
          expect_equal(nrow(reference_tbls_names) + nrow(content_tbls_names), ncol(non_empty_cols)))


# Not all tables are populated with any kind of data. 
# Moving data is always a process that needs human oversight - at least if you don't want to move stuff that is unneeded

reference_tbls_names <- nrows_of_tbls %>% filter(str_detect(tbl, "Reference"))

# Which referenced tables to exist in the dump?
# Note: `Author`, `Editor`, `Collaborator` live in `Person` Table
helper_reference_tbls <- reference_tbls_names %>% 
  mutate(referenced_tbl = str_remove(tbl, "Reference"),
         referenced_tbl = case_when(
           referenced_tbl %in% c("Author", "Editor", "Collaborator", "Organization") ~ "Person",
           TRUE ~ referenced_tbl
         ),
         exists = referenced_tbl %in% names(all_tbls_res)
  ) %>% 
  filter(referenced_tbl != "")



  
# create output files ----------------------------------------------------


output <- helper_reference_tbls %>% 
  filter(nrow>0) %>% 
  pmap_df(function(...) {
    
    current <- tibble(...)
    cli::cli_alert_info("Fetch data for: {current$tbl} that is stored in {current$referenced_tbl}")
    referenced_tbl_id <- paste0(current$referenced_tbl, "ID")

  print(all_tbls_res[[current$referenced_tbl]])
    
   result <-  all_tbls_res[["Reference"]] %>%
     janitor::remove_empty(., which = "cols") %>% 
      left_join(all_tbls_res[[current$tbl]] %>% janitor::remove_empty(., which = "cols"), 
                by = setNames("ReferenceID", "ID")) %>% 
     select(-Index) %>% 
    left_join(all_tbls_res[[current$referenced_tbl]] %>% janitor::remove_empty(., which = "cols") %>% 
                select("ID", contains(tolower("name"))), 
                by = setNames("ID", referenced_tbl_id), suffix = c("", paste0("_", current$referenced_tbl)))
   
   if (current$referenced_tbl == "Keyword") {
     result <- result %>% rename(Keyword = Name)
     result
   } else {
     result
   }
   
  }) %>% 
  left_join(all_tbls_res$Location %>% 
              distinct(ID, Address), by = join_by(ID)) %>% 
  left_join(all_tbls_res$SeriesTitle %>% 
              distinct(ID, SeriesTitle = Name), by = join_by(SeriesTitleID == ID)) %>% 
  distinct()




keywords <- output %>% 
  select(ID, Title, Keyword) %>% 
  #group_by(ID) %>% 
  #summarise(Keywords = paste0(Keyword, collapse = ";"))
  #summarise(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"))
  reframe(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"), .by = ID)

authors <- output %>% 
  #filter(ID == "98ca627f-4fea-4363-98b8-a0822c362bf6") %>% 
  distinct(ID, FirstName, MiddleName, LastName) %>% 
  #filter(!if_any(c(FirstName, MiddleName, LastName), is.na)) %>% 
  #janitor::remove_empty_rows("rows")
  mutate(across(c(FirstName, MiddleName, LastName), ~replace_na(.,"")),
         author_name = trimws(str_replace_all(paste(FirstName, MiddleName, LastName), "\\s{2,}", " "))) %>% 
  filter(author_name != "") %>% 
  reframe(author = glue::glue_collapse(na.omit(author_name), sep = ";"), .by = ID)

publisher <- output %>% 
  #filter(ID == "98ca627f-4fea-4363-98b8-a0822c362bf6") %>% 
  distinct(ID, Name) %>% 
  filter(!if_any(c(Name), is.na)) %>% 
  #janitor::remove_empty_rows("rows")
  reframe(publisher = glue::glue_collapse(na.omit(Name), sep = ";"), .by = ID)


output_export <- output %>% 
  select(
    -CreatedBy,
    -CreatedBySid,
    -contains("CreatedBy.*"),
    -contains("Modif"), -contains("HasLabel"), -starts_with("Page"), -ShortTitleUpdateType,
    -FirstName, -MiddleName, -LastName, -PersonID,
    -Keyword, -KeywordID,
    -Name, -PublisherID) %>% 
  left_join(keywords, by = "ID") %>% 
  left_join(authors, by = "ID") %>% 
  left_join(publisher, by = "ID") %>% 
  distinct() %>% 
  mutate(Notes = str_remove_all(Notes, "\r"))



test_that(
  desc = "same number",
  expect_equal(
    output %>% distinct(ID) %>% nrow(),
    nrow(output_export)
  )
)


write_csv(output_export, "data/books.csv")


output_export %>% select(-AbstractRTF, -TableOfContentsRTF, -Abstract, -TableOfContents)  %>% clipr::write_clip()
output_export %>% filter(row_number()< 3) %>% select(-AbstractRTF, -TableOfContentsRTF)  %>% 
  filter(row_number() < 2) %>% 
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(everything()) %>% 
  clipr::write_clip()



# Poster ------------------------------------------------------------------



output <- helper_reference_tbls %>% 
  filter(nrow>0) %>% 
  pmap_df(function(...) {
    
    current <- tibble(...)
    cli::cli_alert_info("Fetch data for: {current$tbl} that is stored in {current$referenced_tbl}")
    referenced_tbl_id <- paste0(current$referenced_tbl, "ID")
    
    print(all_tbls_res[[current$referenced_tbl]])
    
    result <-  all_tbls_res[["Reference"]] %>%
      janitor::remove_empty(., which = "cols") %>% 
      left_join(all_tbls_res[[current$tbl]] %>% janitor::remove_empty(., which = "cols"), 
                by = setNames("ReferenceID", "ID")) %>% 
      select(-Index) %>% 
      left_join(all_tbls_res[[current$referenced_tbl]] %>% janitor::remove_empty(., which = "cols") %>% 
                  select("ID", contains(tolower("name"))), 
                by = setNames("ID", referenced_tbl_id), suffix = c("", paste0("_", current$referenced_tbl)))
    
    if (current$referenced_tbl == "Keyword") {
      result <- result %>% rename(Keyword = Name)
      result
    } else {
      result
    }
    
  }) %>% 
  left_join(all_tbls_res$Location %>% 
              distinct(ID, Address), by = join_by(ID)) %>% 
  # left_join(all_tbls_res$SeriesTitle %>% 
  #             distinct(ID, SeriesTitle = Name), by = join_by(SeriesTitleID == ID)) %>% 
  distinct()




keywords <- output %>% 
  select(ID, Title, Keyword) %>% 
  #group_by(ID) %>% 
  #summarise(Keywords = paste0(Keyword, collapse = ";"))
  #summarise(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"))
  reframe(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"), .by = ID)

authors <- output %>% 
  #filter(ID == "98ca627f-4fea-4363-98b8-a0822c362bf6") %>% 
  distinct(ID, FirstName, MiddleName, LastName) %>% 
  #filter(!if_any(c(FirstName, MiddleName, LastName), is.na)) %>% 
  #janitor::remove_empty_rows("rows")
  mutate(across(c(FirstName, MiddleName, LastName), ~replace_na(.,"")),
         author_name = trimws(str_replace_all(paste(FirstName, MiddleName, LastName), "\\s{2,}", " "))) %>% 
  filter(author_name != "") %>% 
  reframe(author = glue::glue_collapse(na.omit(author_name), sep = ";"), .by = ID)

# publisher <- output %>% 
#   #filter(ID == "98ca627f-4fea-4363-98b8-a0822c362bf6") %>% 
#   distinct(ID, Name) %>% 
#   filter(!if_any(c(Name), is.na)) %>% 
#   #janitor::remove_empty_rows("rows")
#   reframe(publisher = glue::glue_collapse(na.omit(Name), sep = ";"), .by = ID)


output_export <- output %>% 
  select(
    -CreatedBy,
    -CreatedBySid,
    -contains("CreatedBy.*"),
    -contains("Modif"), -contains("HasLabel"), -starts_with("Page"), -ShortTitleUpdateType,
    -FirstName, -MiddleName, -LastName, -PersonID,
    -Keyword, -KeywordID) %>% 
  left_join(keywords, by = "ID") %>% 
  left_join(authors, by = "ID") %>% 
  #left_join(publisher, by = "ID") %>% 
  distinct() %>% 
  mutate(Notes = str_remove_all(Notes, "\r"))



test_that(
  desc = "same number",
  expect_equal(
    output %>% distinct(ID) %>% nrow(),
    nrow(output_export)
  )
)


write_csv(output_export %>% select(-AbstractRTF, -Abstract), "data/output/poster.csv", na = "")


output_export %>% select(-AbstractRTF, -Abstract)  %>% clipr::write_clip()
output_export %>% filter(row_number()< 3) %>% select(-AbstractRTF, -TableOfContentsRTF)  %>% 
  filter(row_number() < 2) %>% 
  mutate(across(everything(), as.character)) %>% 
  pivot_longer(everything()) %>% 
  clipr::write_clip()


# Digitales ---------------------------------------------------------------



this_dump <- CITAVI_DIGITALES

con <- dbConnect(RSQLite::SQLite(), here::here(paste0("data/", this_dump)))

dump_cat <- str_remove_all(this_dump, "Archiv fhm |\\(Team\\).ctt4| ")

tbls <- tbls_in_dumps_wide %>% select(name, any_of(dump_cat)) %>% pull(any_of(dump_cat))

all_tbls_res <- setNames(map(tbls, function(this_tbl) {
  sql <- glue::glue_sql("SELECT * FROM {this_tbl}; ", .con = con)
  query <- DBI::dbSendQuery(con, sql)
  res <- DBI::dbFetch(query) %>% as_tibble()
  
}), tbls)



# how many entries has each of the tables? --------------------------------

nrows_of_tbls <- all_tbls_res %>% map_df(nrow) %>% 
  pivot_longer(everything()) %>% 
  rename(tbl = name, nrow = value) %>% 
  arrange(desc(nrow))


nrows_of_tbls


# Reference tbl a central hub ---------------------------------------------
# the Reference table is the central table in a Citavi dump
# Other metadata like Person or Editor are stored in 

# keep all columns that are not empty
non_empty_cols <- janitor::remove_empty(all_tbls_res$Reference, which = "cols") 

# all links to other tables end with "ID"
reference_tbls_names <- non_empty_cols %>% 
  names() %>% 
  enframe() %>% 
  filter(str_detect(value, "ID$"))

content_tbls_names <- non_empty_cols %>% 
  names() %>% 
  enframe() %>% 
  filter(!str_detect(value, "ID$"))


test_that(desc = "count of reference_tbls and reference_tbls is non_empty_cols", 
          expect_equal(nrow(reference_tbls_names) + nrow(content_tbls_names), ncol(non_empty_cols)))


# Not all tables are populated with any kind of data. 
# Moving data is always a process that needs human oversight - at least if you don't want to move stuff that is unneeded

reference_tbls_names <- nrows_of_tbls %>% filter(str_detect(tbl, "Reference"))

# Which referenced tables to exist in the dump?
# Note: `Author`, `Editor`, `Collaborator` live in `Person` Table
helper_reference_tbls <- reference_tbls_names %>% 
  mutate(referenced_tbl = str_remove(tbl, "Reference"),
         referenced_tbl = case_when(
           referenced_tbl %in% c("Author", "Editor", "Collaborator", "Organization") ~ "Person",
           TRUE ~ referenced_tbl
         ),
         exists = referenced_tbl %in% names(all_tbls_res)
  ) %>% 
  filter(referenced_tbl != "")


output <- helper_reference_tbls %>% 
  filter(nrow>0) %>% 
  pmap_df(function(...) {
    
    current <- tibble(...)
    cli::cli_alert_info("Fetch data for: {current$tbl} that is stored in {current$referenced_tbl}")
    referenced_tbl_id <- paste0(current$referenced_tbl, "ID")
    
    print(all_tbls_res[[current$referenced_tbl]])
    
    result <-  all_tbls_res[["Reference"]] %>%
      janitor::remove_empty(., which = "cols") %>% 
      left_join(all_tbls_res[[current$tbl]] %>% janitor::remove_empty(., which = "cols"), 
                by = setNames("ReferenceID", "ID")) %>% 
      select(-Index) %>% 
      left_join(all_tbls_res[[current$referenced_tbl]] %>% janitor::remove_empty(., which = "cols") %>% 
                  select("ID", contains(tolower("name"))), 
                by = setNames("ID", referenced_tbl_id), suffix = c("", paste0("_", current$referenced_tbl)))
    
    if (current$referenced_tbl == "Keyword") {
      result <- result %>% rename(Keyword = Name)
      result
    } else {
      result
    }
    
  }) %>% 
  left_join(all_tbls_res$Location %>% 
              distinct(ID, Address), by = join_by(ID)) %>% 
  left_join(all_tbls_res$SeriesTitle %>% 
              distinct(ID, SeriesTitle = Name), by = join_by(SeriesTitleID == ID)) %>% 
  distinct()

keywords <- output %>% 
  select(ID, Title, Keyword) %>% 
  #group_by(ID) %>% 
  #summarise(Keywords = paste0(Keyword, collapse = ";"))
  #summarise(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"))
  reframe(Keywords = glue::glue_collapse(na.omit(Keyword), sep = ";"), .by = ID)

authors <- output %>% 
  distinct(ID, FirstName, MiddleName, LastName) %>% 
  #filter(!if_any(c(FirstName, MiddleName, LastName), is.na)) %>% 
  #janitor::remove_empty_rows("rows")
  mutate(across(c(FirstName, MiddleName, LastName), ~replace_na(.,"")),
         author_name = trimws(str_replace_all(paste(FirstName, MiddleName, LastName), "\\s{2,}", " "))) %>% 
  filter(author_name != "") %>% 
  reframe(author = glue::glue_collapse(na.omit(author_name), sep = ";"), .by = ID)

publisher <- output %>% 
  distinct(ID, Name) %>% 
  filter(!if_any(c(Name), is.na)) %>% 
  #janitor::remove_empty_rows("rows")
  reframe(length = glue::glue_collapse(na.omit(Name), sep = ";"), .by = ID)

output_export <- output %>% 
  select(
    -CreatedBy,
    -CreatedBySid,
    -contains("CreatedBy.*"),
    -contains("Modif"), -contains("HasLabel"), -starts_with("Page"), -ShortTitleUpdateType,
    -FirstName, -MiddleName, -LastName, -PersonID,
    -Keyword, -KeywordID,
    -Name, -PublisherID, 
    -TitleTagged, 
    -AbstractRTF) %>% 
  left_join(keywords, by = "ID") %>% 
  left_join(authors, by = "ID") %>% 
  left_join(publisher, by = "ID") %>% 
  distinct() %>% 
  mutate(Notes = str_remove_all(Notes, "\r")) 
  



test_that(
  desc = "same number",
  expect_equal(
    output %>% distinct(ID) %>% nrow(),
    nrow(output_export)
  )
)


write_csv(output_export, "data/output/digitales.csv", na = "")

