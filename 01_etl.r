
#SCRIPT Solicitado por JURACY_______________________________________________

library(arrow)
library(dplyr)
library(tidylog)
library(data.table)
library(lubridate)
library(janitor)
library(rio)
library(openxlsx)

# Definir o diret√≥rio de trabalho
setwd("D:/projeto_sim/dados")   
getwd()
# Ler dados do arquivo parquet
df <- read_parquet("base_bahia.parquet")

# Verificar os nomes das colunas
print(names(df))

# Calcular idade ----------------------------------------------------------

library(dplyr)  

# Calcular idade ----------------------------------------------------------  

df <- df %>%  
  mutate(  
    unidade = as.numeric(substr(IDADE, 1, 1)),  # Corrigido a construÁ„o do mutate  
    quantidade = as.numeric(substr(IDADE, 2, 3))  # Usar 'IDADE' consistentemente  
  ) %>%  
  mutate(  
    idade_tratada = case_when(  
      unidade == 0 ~ quantidade / 60,   # Convers„o de minutos para anos  
      unidade == 1 ~ quantidade,          # Anos  
      unidade == 2 ~ quantidade / 24,     # Dias para anos  
      unidade == 3 ~ quantidade / 12,     # Meses para anos  
      unidade == 4 ~ quantidade,          # Sem convers„o  
      unidade == 5 ~ 100 + quantidade,    # Anos mais 100  
      TRUE ~ NA_real_                     # Caso padr„o  
    )  
  ) %>%  
  select(-unidade, -quantidade)  # Remove colunas auxiliares  


# Criar faixa et·ria conforme IBGE  
df <- df %>%  
  mutate(faixa_etaria = case_when(  
    idade_tratada < 1 ~ "Menos de 1 ano",  
    idade_tratada >= 1 & idade_tratada <= 4 ~ "1 a 4 anos",  
    idade_tratada >= 5 & idade_tratada <= 9 ~ "5 a 9 anos",  
    idade_tratada >= 10 & idade_tratada <= 14 ~ "10 a 14 anos",  
    idade_tratada >= 15 & idade_tratada <= 19 ~ "15 a 19 anos",  
    idade_tratada >= 20 & idade_tratada <= 24 ~ "20 a 24 anos",  
    idade_tratada >= 25 & idade_tratada <= 29 ~ "25 a 29 anos",  
    idade_tratada >= 30 & idade_tratada <= 34 ~ "30 a 34 anos",  
    idade_tratada >= 35 & idade_tratada <= 39 ~ "35 a 39 anos",  
    idade_tratada >= 40 & idade_tratada <= 44 ~ "40 a 44 anos",  
    idade_tratada >= 45 & idade_tratada <= 49 ~ "45 a 49 anos",  
    idade_tratada >= 50 & idade_tratada <= 54 ~ "50 a 54 anos",  
    idade_tratada >= 55 & idade_tratada <= 59 ~ "55 a 59 anos",  
    idade_tratada >= 60 & idade_tratada <= 64 ~ "60 a 64 anos",  
    idade_tratada >= 65 & idade_tratada <= 69 ~ "65 a 69 anos",  
    idade_tratada >= 70 & idade_tratada <= 74 ~ "70 a 74 anos",  
    idade_tratada >= 75 & idade_tratada <= 79 ~ "75 a 79 anos",  
    idade_tratada >= 80 ~ "80 anos ou mais"  
  ))  

# Visualizar dados  

table(df$faixa_etaria)


# Criando vari√°vel ano ----------------------------------------------------


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

obitos_ocorrencia_2018 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNOCOR) %>%
  summarise(OBITOS_OCORRENCIA = n())

obitos_residencia_2018 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNRES) %>%
  summarise(OBITOS_RESIDENCIA = n())



df <- df %>% 
  mutate(idade  = as.numeric(DTOBITO - DTNASC))
  

# Adicionar taxa e popula√ß√£o ----------------------------------------------



obitos_2020 <- full_join(obitos_residencia_2018, obitos_ocorrencia_2020, by = c("CODMUNRES" = "CODMUNOCOR"))

obitos_2020 <- obitos_2018 %>%
  mutate(TAXA_OBITOS = OBITOS_RESIDENCIA / populacao * 100000)



# Juntando os dataframes de resid√™ncia e ocorr√™ncia
obitos_2020 <- full_join(obitos_residencia_2018, obitos_ocorrencia_2020, by = c("CODMUNRES" = "CODMUNOCOR"))

# Criando a taxa de √≥bitos
obitos_2020 <- obitos_2020 %>%
  mutate(TAXA_OBITOS = OBITOS_RESIDENCIA / populacao  * 100000)

# Salvando os dados em formato parquet
write_parquet(obitos_2020, "obitos_2018.parquet")


