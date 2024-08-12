
#SCRIPT Solicitado por JURACY_______________________________________________

library(arrow)
library(dplyr)
library(tidylog)
library(data.table)
library(lubridate)

# Definir o diretório de trabalho
setwd("D:/projeto_sim/dados")   
getwd()
# Ler dados do arquivo parquet
df <- read_parquet("base_bahia.parquet")

# Verificar os nomes das colunas
print(names(df))

# Calcular idade ----------------------------------------------------------



# Criar faixa etária conforme IBGE
df <- df %>%
  mutate(faixa_etaria = case_when(
    IDADE < 1 ~ "Menos de 1 ano",
    IDADE >= 1 & IDADE <= 4 ~ "1 a 4 anos",
    IDADE >= 5 & IDADE <= 9 ~ "5 a 9 anos",
    IDADE >= 10 & IDADE <= 14 ~ "10 a 14 anos",
    IDADE >= 15 & IDADE <= 19 ~ "15 a 19 anos",
    IDADE >= 20 & IDADE <= 24 ~ "20 a 24 anos",
    IDADE >= 25 & IDADE <= 29 ~ "25 a 29 anos",
    IDADE >= 30 & IDADE <= 34 ~ "30 a 34 anos",
    IDADE >= 35 & IDADE <= 39 ~ "35 a 39 anos",
    IDADE >= 40 & IDADE <= 44 ~ "40 a 44 anos",
    IDADE >= 45 & IDADE <= 49 ~ "45 a 49 anos",
    IDADE >= 50 & IDADE <= 54 ~ "50 a 54 anos",
    IDADE >= 55 & IDADE <= 59 ~ "55 a 59 anos",
    IDADE >= 60 & IDADE <= 64 ~ "60 a 64 anos",
    IDADE >= 65 & IDADE <= 69 ~ "65 a 69 anos",
    IDADE >= 70 & IDADE <= 74 ~ "70 a 74 anos",
    IDADE >= 75 & IDADE <= 79 ~ "75 a 79 anos",
    IDADE >= 80 ~ "80 anos ou mais"
  ))


# Criando variável ano ----------------------------------------------------


df <- df %>%
  mutate(anonasc = substr(DTNASC, 5, 8),
         anobito = substr(DTOBITO, 5, 8),
         anonasc = as.numeric(anonasc),
         anobito = as.numeric(anobito),
         IDADECALC = anobito - anonasc,
         IDADEANO = IDADECALC)



# Data com lubridate ------------------------------------------------------

# Ajustando os formatos de datas, com {lubridate} -------------------------

df <- df %>%                          # dados e operador pipe
  mutate(DTOBITO = dmy (DTOBITO), # converte para Date
         DTNASC = dmy (DTNASC),
         DTINVESTIG  = dmy (DTINVESTIG),
         DTCADASTRO = dmy (DTCADASTRO))
           # Aten?ao ao formato dmy()


# FILTROS -----------------------------------------------------------------

names(df)



df <- df %>%
  mutate(anobito = year(DTOBITO))
table(df$anobito)
    

df2 <- df %>% select(anobito, DTOBITO, DTNASC)

glimpse(df)

obitos_ocorrencia_2020 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNOCOR) %>%
  summarise(OBITOS_OCORRENCIA = n())

obitos_residencia_2020 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNRES) %>%
  summarise(OBITOS_RESIDENCIA = n())



df <- df %>% 
  mutate(idade  = as.numeric(DTOBITO - DTNASC))
  

# Adicionar taxa e população ----------------------------------------------



obitos_2020 <- full_join(obitos_residencia_2020, obitos_ocorrencia_2020, by = c("CODMUNRES" = "CODMUNOCOR"))

obitos_2020 <- obitos_2018 %>%
  mutate(TAXA_OBITOS = OBITOS_RESIDENCIA / populacao * 100000)



# Juntando os dataframes de residência e ocorrência
obitos_2020 <- full_join(obitos_residencia_2020, obitos_ocorrencia_2020, by = c("CODMUNRES" = "CODMUNOCOR"))

# Criando a taxa de óbitos
obitos_2020 <- obitos_2020 %>%
  mutate(TAXA_OBITOS = OBITOS_RESIDENCIA / populacao  * 100000)

# Salvando os dados em formato parquet
write_parquet(obitos_2020, "obitos_2020.parquet")


