## Make sure you have the {duckdb} and {dplyr} packages installed.

library(duckdb)
library(dplyr)

## Follow along the tutorial by typing the code in this file.

con <- dbConnect(duckdb::duckdb(), ":memory:")

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  head()

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  colnames()

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  count()

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  distinct(ST)

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  distinct(ST, year)

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  count(ST, year) |> 
  arrange(ST, year) |> 
  collect()

tbl(con, "read_parquet('data/pums_personal_sample.parquet')") |> 
  summarise(n = sum(PWGTP, na.rm = TRUE), .by = c(ST, year)) |> 
  arrange(ST, year) |> 
  collect()

dbDisconnect(con)

con2 <- dbConnect(duckdb(), dbdir = "pums-small2.duckdb", 
                  read_only = FALSE)

dbExecute(con2,
          "CREATE TABLE pums_personal_small 
          AS SELECT * FROM read_parquet('data/pums_personal_sample.parquet')")
dbListTables(con2)


tbl(con2, "pums_personal_small")