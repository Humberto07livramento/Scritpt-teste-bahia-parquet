# PARTE I FORMATO PARQUET -------------------------------------------------

library(arrow)
library(data.table)
library(dplyr)

# Caminho da pasta (Formatado para o R)
caminho_pasta <- "C:/Users/Beatriz/Documents"

# 1. Leitura do arquivo (assumindo que o csv está nessa pasta)
arquivo_entrada <- file.path(caminho_pasta, "teste3.csv")
df <- fread(arquivo_entrada)

# 2. CORREÇÃO OBRIGATÓRIA: Nomes Duplicados
# Isso resolve o erro: "Can't transform a data frame with duplicate names"
names(df) <- make.unique(names(df))

# 3. Transformação (Criar ID)
df <- df %>% 
  mutate(id = row_number())

# 4. Escrita do Parquet (Salva na mesma pasta)
arquivo_saida <- file.path(caminho_pasta, "teste3.parquet")
write_parquet(df, arquivo_saida)

# Confirmação
message("Arquivo salvo com sucesso em: ", arquivo_saida)

