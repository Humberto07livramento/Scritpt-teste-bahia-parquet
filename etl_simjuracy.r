

# PARTE I FORMATO PARQUET -------------------------------------------------




library(arrow)
library(data.table)
library(dplyr)

df = fread("c:/projeto_sim/base_bahia_5anos.csv",fill = TRUE)


df = df %>% mutate(id = row_number() ) 

setwd("c:/projeto_sim")
write_parquet(df,"base_bahia.parquet")

?write_parquet



# FAZENDO SIMULAÇÕES NO R GERANDO VARIÁVEL IDADE --------------------------


table(df$SEXO,useNA = "always")
table(df$RACACOR,useNA = "always")
table(df$DTOBITO)
df$DTOBITO <- as.character(df$DTOBITO)
df$DTNASC <- as.character(df$DTNASC)


#CALCULAR IDADE NO R 

anonasc <- substr(df$DTNASC,5,8)

anobito <- substr(df$DTOBITO,5,8)

anonasc <- as.numeric(anonasc)
anobito <- as.numeric(anobito)

IDADECALC <- anobito - anonasc

df$IDADEANO <- IDADECALC

table(df$IDADEANO)

#  FAIXA ETARIA 

df$FAIXA_ETARIA  <- cut(df$IDADEANO, breaks = c(0, 18, 30, 50, Inf), labels = c("0-18", "19-30", "31-50", "51+"))

table(df$FAIXA_ETARIA)
head(df)
summary(df)


#SCRIPT Solicitado por JURACY_______________________________________________

library(arrow)
library(dplyr)
library(data.table)

# Definir o diretório de trabalho
setwd("c:/projeto_sim")
getwd()
# Ler dados do arquivo parquet
df <- read_parquet("base_bahia.parquet")

# Verificar os nomes das colunas
print(names(df))

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

# Contar óbitos para o ano de 2020 por município de residência
obitos_residencia_2020 <- df %>%
  filter(anobito == 2020) %>%
  group_by(CODMUNRES) %>%
  summarise(OBITOS_RESIDENCIA = n())

# Contar óbitos para o ano de 2020 por município de ocorrência
obitos_ocorrencia_2020 <- df %>%
  filter(anobito == 2020) %>%
  group_by(CODMUNOCOR) %>%
  summarise(OBITOS_OCORRENCIA = n())

# Juntar os dois dataframes de residência e ocorrência
obitos_2020 <- obitos_residencia_2020 %>%
  full_join(obitos_ocorrencia_2020, by = c("CODMUNRES" = "CODMUNOCOR"))

# Criar taxa usando mutate
obitos_2020 <- obitos_2020 %>%
  mutate(TAXA_OBITOS = OBITOS_RESIDENCIA / POPULACAO * 100000)

# Salvar dados em parquet contendo a taxa e valores absolutos
write_parquet(obitos_2020, "obitos_2020.parquet")






# EXPLORANDO COMANDOS R ---------------------------------------------------

# Carregando os pacotes necessários
library(dplyr)
library(rio)
library(tidyverse)
library(lubridate)
library(janitor)

# Seu data frame
df <- df  # Aqui você deve carregar seus dados

# Visualizando a estrutura das colunas que começam com "DT_"
df %>%
  select(starts_with("DT_")) %>%
  glimpse()


# Convertendo as colunas para formato Date
df <- df %>%
  mutate(
    DTOBITO = dmy(DTOBITO),
    DTNASC = dmy(DTNASC),
    DTCADASTRO = dmy(DTCADASTRO),
    DTINVESTIG = dmy(DTINVESTIG),
    DTRECORIGA = dmy(DTRECORIGA)
  )

# Visualizando a estrutura das colunas que começam com "DT_"
df %>%
  select(starts_with("DT_")) %>%
  glimpse()



# Criando novas variáveis de idade e tempo 
df <- df %>%
  mutate(
    idade  = as.numeric(DTOBITO - DTNASC),
    Temp_investigacao = as.numeric(DTOBITO - DTINVESTIG),
    Temp_digitacao    = as.numeric(DTOBITO - DTCADASTRO)
  )


