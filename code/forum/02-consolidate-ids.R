
# goal -------------------------------------------------------------------
# Problem: In RemoveNA habe ich die 
# https://github.com/queersearch/ptf-setup/issues/16

library(config)
library(tidyverse)
library(testdat)


# setup ------------------------------------------------------------------

config_file <- "static/config.yaml"
config <- config::get(file = config_file, "removena_db")

con <- DBI::dbConnect(RMariaDB::MariaDB(),
  host = config$host,
  user = config$user,
  password = config$password,
  port = config$port,
  dbname = config$dbname
)


# import data sources ----------------------------------------------------


# 1) uuid-forum_id: in removena i didn't save that
uuid_forum_id_file <- "data/input/books_id.csv"
uuid_forum_id <- read_csv(here::here(uuid_forum_id_file)) |>
  rename(citavi_id = sku)

# 2) access removena db = local MySQL DB ---------------------------------

entities_per_type <- tbl(con, "entities_per_type") |> collect()

books_authors <- tbl(con, "books_authors") |> select(id, book_id) |> collect() |> 
  mutate(id = trimws(id)) |> 
  # fix some inconsistencies
  mutate(
    id = case_when(
      # patricia highsmith
      book_id %in% c(1553, 1632, 2872) ~ "book_author_1013",
      book_id %in% c(2872) ~ "book_author_1553",
      TRUE ~ id
      )
    )

# 3) current books export  -----------------------------------------------

current_books_file <- "data/output/books.csv"
current_books_raw <- read_csv(here::here(current_books_file)) 


# 4) Persons with Wikidata QID --------------------------------------------

fg_persons <- read_csv("data/input/factgrid/persons.csv") 
fg_items <- read_csv("data/input/factgrid/all-items.csv")

persons <- fg_persons |> 
  left_join(books_authors, by = join_by(forum_id==id)) |> filter(!is.na(book_id))

# 5) Join -----------------------------------------------------------------

current_books |> 
  left_join(uuid_forum_id, by = join_by(ID == citavi_id)) |> 
  select(ID, Title, author, book_id) |>
  left_join(persons, by = join_by(book_id)) |> 
  filter(is.na(forum_id)) |> View()



current_books_with_uuid <- current_books_raw |> 
  left_join(uuid_forum_id, by = join_by(ID == citavi_id)) |> 
  select(ID, Title, author, book_id)


current_books_with_uuid_qid <- current_books_with_uuid |> 
   left_join(fg_items |> 
               filter(fg_instanceLabel == "Mensch(en)") |> 
              left_join(books_authors, by = join_by(forum_id==id)) |> filter(!is.na(book_id)), 
            by = join_by(book_id))
 
 


current_books_with_uuid_qid |> 
  filter(is.na(forum_id)) |> 
  nrow()

 curr2 |>   filter(is.na(forum_id)) |> View()

 

books_authors |> 
  filter(book_id == 1912) |> 
  left_join(curr, by = join_by(book_id))


fg_items |> filter(forum_id == "book_author_1510")


# Christa Reinig Fix
christa_reinig <- current_books |> 
  filter(str_detect(author, "Christa Reinig")) |> 
  distinct(book_id) |> 
  filter(!is.na(book_id)) |> 
  left_join(books_authors, by = join_by(book_id)) |> 
  distinct(book_id) |> 
  mutate(id = "book_author_565")

books_authors <- bind_rows(books_authors, christa_reinig) |> 
  filter(id != "NA") |> 
  distinct()


testthat::test_that(
  desc = "Highsmith is correct",
  expect_equal(
  books_authors |> filter(book_id %in% c(1553, 1632, 2872)) |> pull(id) |> unique(), "book_author_1013"
  )
)


# join -------------------------------------------------------------------









current_books |> left_join(persons, by = join_by(book_id)) |> 
  select(Id, Title, author, ) 




current_books |> select(ID, Title, author, book_id) |> 
  filter(!is.na(book_id)) |> 
  left_join(entities_per_type |> select(-poster_id, -publisher_book_id), by = join_by(book_id)) |> 
  filter(is.na(id)) |> View()


# test auch am ende: 
testthat::test_that(
desc = "Highsmith is correct",
expect_equal(
  books_authors |> filter(book_id %in% c(1553, 1632, 2872)) |> pull(id) |> unique(), "book_author_1013"
)
)