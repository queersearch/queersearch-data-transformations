library(googlesheets4)
library(tidyverse)

current_books_file <- "data/output/books.csv"
current_books_raw <- read_csv(here::here(current_books_file))
current_books <- current_books_raw |> 
  mutate(Title = paste0(Title, " :", Subtitle))
  

template_raw <- read_sheet("1VC4Hwr2R5w__Aip0s48Sa-ZoLe6RfX7Q56TAhVCjpEw", sheet = "bücher-metadaten")
template <- template_raw |> 
  filter(`behalten?`=="ja") 



template
