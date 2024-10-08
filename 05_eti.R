library(arrow)
library(dplyr)
library(tidylog)
library(data.table)
library(lubridate)
library(openxlsx)
library(stringr)
library(writexl)

# Definir o diret?rio de trabalho
setwd("D:/projeto_sim/dados")   
getwd()

# Ler dados do arquivo parquet
df <- read_parquet("base_bahia.parquet")

# Verificar os nomes das colunas
print(names(df))
library(dplyr)

df <- df %>%
  mutate(
    unidade = as.numeric(substr(IDADE, 1, 1)),  # Extrair a unidade
    quantidade = as.numeric(substr(IDADE, 2, 3))  # Extrair a quantidade
  ) %>%
  mutate(
    idade_tratada = case_when(
      unidade %in% 0:3 ~ 0,  # Para unidade 0 a 3, consideramos 0 anos
      unidade == 4 ~ quantidade,  # Unidade 4 j� est� em anos
      unidade == 5 ~ 100 + quantidade,  # Unidade 5 � anos + 100
      TRUE ~ NA_real_  # Caso padr�o
    )
  ) %>%
  select(-unidade, -quantidade)  # Remove colunas auxiliares



# Criar faixa et?ria conforme IBGE  
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

# Contar o n?mero de ocorr?ncias para cada faixa et?ria
df_faixa_etaria <- df %>%
  group_by(faixa_etaria) %>%
  summarise(contagem = n()) %>%
  arrange(faixa_etaria)

# Exportar para Excel
write_xlsx(df_faixa_etaria, "faixa_etaria.xlsx")



# Criando vari?vel ano ----------------------------------------------------
df <- df %>%
  mutate(anonasc = substr(DTNASC, 5, 8),
         anobito = substr(DTOBITO, 5, 8),
         anonasc = as.numeric(anonasc),
         anobito = as.numeric(anobito),
         IDADECALC = anobito - anonasc,
         IDADEANO = IDADECALC)

# Data com lubridate ------------------------------------------------------
df <- df %>%  
  mutate(DTOBITO = dmy(DTOBITO), 
         DTNASC = dmy(DTNASC),
         DTINVESTIG = dmy(DTINVESTIG),
         DTCADASTRO = dmy(DTCADASTRO))

# FILTROS -----------------------------------------------------------------
df <- df %>%
  mutate(anobito = year(DTOBITO))
table(df$anobito)

df2 <- df %>% select(anobito, DTOBITO, DTNASC)

# Agrupar ?bitos por munic?pio de ocorrencia e residencia em 2018
obitos_ocorrencia_2018 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNOCOR) %>%
  summarise(OBITOS_OCORRENCIA = n())

obitos_residencia_2018 <- df %>%
  filter(anobito == 2018) %>%
  group_by(CODMUNRES) %>%
  summarise(OBITOS_RESIDENCIA = n())

# Adicionar taxa e popula?ao ----------------------------------------------
# Ler dados do IBGE e criar parquet
ibgeba <- fread("D:/projeto_sim/dados/ibge_bahia_2018.csv")
write_parquet(ibgeba, "ibgeba.parquet")

ibgeba <- clean_names(ibgeba)


# Ler dados do IBGE
df3 <- read_parquet("ibgeba.parquet")



# Separar c?digo e nome do munic?pio em df3

library(dplyr)
library(tidyr)

# Ler dados do IBGE
df3 <- read_parquet("ibgeba.parquet")

# Separar c�digo e nome do munic�pio em df3
df3 <- df3 %>%
  separate(MUNICIPIO, into = c("CODMUNRES", "MUNICIPIO"), sep = 6, fill = "right")


# Corrigindo o caracter de espa?o " " excedente

# Carregue o pacote stringr
library(stringr)

# Ou, se voc? deseja remover espa?os extras ao redor das strings
df3$CODMUNRES <- str_trim(df3$CODMUNRES)

names(df3)

# Converter a coluna CODMUNRES em df3 de <character> para <integer>
df3$CODMUNRES <- as.integer(df3$CODMUNRES)

# Realizar o full_join novamente
obitos_2018 <- full_join(obitos_residencia_2018, df3, by = "CODMUNRES")


# Calcular a taxa de ?bitos

obitos_2018 <- obitos_2018 %>%
  mutate(TAXA_OBITOS = (OBITOS_RESIDENCIA / POP_RES) * 10000)


# Salvando os dados em formato parquet
write_parquet(obitos_2018, "obitos_2018.parquet")

#Exportando para excell 

#install.packages("writexl")

#library(writexl)

# Salvar o dataframe como um arquivo Excel
write_xlsx(obitos_2018, "obitos_2018.xlsx")

#install.packages("openxlsx")


library(openxlsx)

# Criar um novo arquivo Excel
write.xlsx(obitos_2018, file = "obitos_2018.xlsx")

