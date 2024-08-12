
# PARTE I FORMATO PARQUET -------------------------------------------------




library(arrow)
library(data.table)
library(dplyr)

df = fread("E:/base_bahia_5anos.csv")


df = df %>% mutate(id = row_number() ) 

setwd("c:/projeto_sim")
write_parquet(df,"base_bahia.parquet")

?write_parquet