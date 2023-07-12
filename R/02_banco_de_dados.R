#-------------------------------------
# Projeto integrador 1 - Script 2
#-------------------------------------

# Integrantes:
# Bruna Mattioli de Oliveira
# Gabriel Andrade Varga

#-------------------------------------------------------------------------
# Pacotes utilizados
#-------------------------------------------------------------------------
library(tidyverse)
library(lubridate)
library(RPostgres)

#-------------------------------------------------------------------------
# Seção 1 - Importação do conjunto de dados no R e tratamento dos dados
#-------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Instituição Financeira
#--------------------------------------------------------------------------------

participantes_B3 <- read_csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/participantDonwload.csv", col_names = TRUE)

corretoras_base <- filter(participantes_B3, Perfil == "PARTICIPANTE DE NEGOCIAÇÃO PLENO")

corretoras <- select(corretoras_base, CNPJ, Nome)

#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Empresa
#--------------------------------------------------------------------------------

instrumentos_B3 <- read_csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/InstrumentsConsolidatedFile_20210507_1.csv", 
                             col_names = TRUE, col_types = NULL, locale(encoding = "ISO-8859-1"))

# Esta tabela contém as informações de ticker, empresa

instrumentos_B3_filtrado <- filter(instrumentos_B3, (SctyCtgyNm == 'SHARES' | SctyCtgyNm == 'UNIT') & SgmtNm == "CASH" & !str_detect(TckrSymb, 'TAXA')) %>%
  select('TckrSymb', 'Asst','CrpnNm', 'CorpGovnLvlNm')
instrumentos_B3_filtrado <- replace(instrumentos_B3_filtrado, list = is.na(instrumentos_B3_filtrado), values = "OUTRO") %>%
  filter(!str_detect(CorpGovnLvlNm, 'BALCÃO')) %>%
  select('TckrSymb', 'Asst','CrpnNm') %>%
  rename(ticker_completo = TckrSymb, ticker = Asst, nome = CrpnNm)

nome_empresa <- distinct(instrumentos_B3_filtrado, ticker, nome)


#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Setor
#--------------------------------------------------------------------------------
base_setor <- read.csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/Setorial_B3_Consolidado.csv",
                        col.names = c("id_setorial", "empresa_listagem","codigo_listagem","segmento_listagem","setor_economico","subsetor","segmento"),
                        encoding = 'UTF-8')

segmento <- select(base_setor, segmento) %>% distinct(segmento)

#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Ação
#--------------------------------------------------------------------------------

ticker <- distinct(instrumentos_B3_filtrado, ticker_completo, ticker)

#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Carteira Recomendada
#--------------------------------------------------------------------------------

infos_money_times <- read.csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/infos_money_times.csv")

infos_money_times_bd <- read.csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/infos_money_times_bd.csv")

#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#--------------------------------------------------------------------------------
# Importação e tratamento dos dados da Entidade / Tabela: Preço Fechamento
#--------------------------------------------------------------------------------

ultimo_preco <- read.csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/Ações_Consolidado.csv") %>%
  distinct(ACAO, DATA, FECHAMENTO) %>%
  mutate(DATA = dmy(DATA))

#--------------------------------------------------------------------------------
# Fim
#--------------------------------------------------------------------------------

#-----------------------------------------------------------
# Seção 2 - Importação dos dados para o Banco de Dados 
#-----------------------------------------------------------

#-----------------------------------------------------------
# Conexão com PostgreSQL
#-----------------------------------------------------------

# aqui estamos definindo nossa conexão e atribuindo essa conexão à variável con
con <- dbConnect(Postgres(),                 
                 user = "postgres",                 
                 password = "vagan9ch",                 
                 host = "localhost",                 
                 port = 5432,                 
                 dbname = "Projeto_Integrador_1")

#visualizando alguns dados sobre a conexão estabelecida
con


#---------------------------------------------------------------
# Importação da Entidade / Tabela: Instituição Financeira no BD
#---------------------------------------------------------------

for (i in 1:length(corretoras$CNPJ)){
  dbExecute(con, 
            "INSERT INTO instituicao_financeira(cnpj_inst_financ, nm_inst_financ) 
            VALUES ($1,$2)", list(corretoras$CNPJ[i],corretoras$Nome[i]))
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Empresa no BD
#---------------------------------------------------------------

for (i in 1:length(nome_empresa$nome)){
  dbExecute(con, 
            "INSERT INTO empresa(nm_empresa) 
            VALUES ($1)", list(nome_empresa$nome[i]))
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Setor no BD
#---------------------------------------------------------------

for (i in 1:length(segmento$segmento)){
  dbExecute(con, 
            "INSERT INTO setor(nm_segmento) 
            VALUES ($1)", list(segmento$segmento[i]))
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Ação no BD
#---------------------------------------------------------------

j <- 1
while (j <= 705){ #percorrendo a base de ticker
  id_setor2 = 0
  id_empresa2 = 0
  
  i <- 1
  while (i <= 445){ #percorrendo a base_setor
    if (base_setor$codigo_listagem[i] == ticker$ticker[j]){
      id_setor2 <- as.data.frame(dbGetQuery(con,"SELECT id_setor FROM setor WHERE 
                              setor.nm_segmento = $1", list(base_setor$segmento[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 497){ #percorrendo a base de nome_empresa
    if (ticker$ticker[j] == nome_empresa$ticker[x]){
      
      id_empresa2 <- as.data.frame(dbGetQuery(con,"SELECT id_empresa FROM empresa WHERE 
             empresa.nm_empresa = $1", list(nome_empresa$nome[x])))
      break
    }
    x <- x+1
  }
  if(id_setor2 != 0){
    dbExecute(con, 
              "INSERT INTO acao(ticker_acao,nm_ticker_acao,id_setor, id_empresa)
              VALUES ($1,$2,$3, $4)", list(ticker$ticker_completo[j],
                                           ticker$ticker[j],
                                           id_setor2$id_setor,
                                           id_empresa2$id_empresa))
  }
  else{
    id_setor2 <- as.data.frame(dbGetQuery(con,"SELECT id_setor FROM setor WHERE 
             setor.nm_segmento = 'Outros'"))
    dbExecute(con, 
              "INSERT INTO acao(ticker_acao,nm_ticker_acao,id_setor, id_empresa)
              VALUES ($1,$2,$3, $4)", list(ticker$ticker_completo[j],
                                           ticker$ticker[j],
                                           id_setor2$id_setor,
                                           id_empresa2$id_empresa))
  }
  j <- j+1
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Carteira Recomendada no BD
#---------------------------------------------------------------

j <- 1
while (j <= 211){ #percorrendo a base de info_money_times_bd
  id_carteira2 = 0
  i <- 1
  while (i <= 101){ #percorrendo a corretoras
    if (corretoras$Nome[i] == infos_money_times_bd$corretora[j]){
      id_carteira2 <- dbGetQuery(con,"SELECT id_inst_financ FROM instituicao_financeira WHERE 
             instituicao_financeira.nm_inst_financ = $1", list(corretoras$Nome[i]))
      break
    }
    i <- i+1
  }
  if(id_carteira2 != 0){
    dbExecute(con, 
              "INSERT INTO carteira_recomendada(data_carteira,url_carteira,id_inst_financ)
              VALUES ($1,$2, $3)", list(infos_money_times_bd$data_noticia[j],
                                        infos_money_times_bd$url[j],
                                        id_carteira2$id_inst_financ))
  }
  j <- j+1
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Ação_Carteira no BD
#---------------------------------------------------------------

# Coluna ticker1
j <- 1
while (j <= 211){ #percorrendo a base de info_money_times
  id_acao2 <- 0
  id_carteira3 <- 0
  peso <- 20
  
  i <- 1
  while (i <= 213){ #percorrendo a base infos_money_times_bd
    if (infos_money_times_bd$url[i] == infos_money_times$url[j]){
      id_carteira3 <- as.data.frame(dbGetQuery(con,"SELECT id_carteira FROM carteira_recomendada WHERE
                              carteira_recomendada.url_carteira = $1", list(infos_money_times_bd$url[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 705){ #percorrendo a base ticker
    if (infos_money_times$ticker1[j] == ticker$ticker_completo[x]){ 
      
      id_acao2 <- as.data.frame(dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[x])))
      break
    }
    x <- x+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO acao_carteira_recomendada(acao_id_acao, carteira_recomendada_id_carteira, peso)
              VALUES ($1,$2,$3)", list(id_acao2$id_acao,
                                       id_carteira3$id_carteira,
                                       peso))
  }
  j <- j+1
}

# Coluna ticker2
j <- 1
while (j <= 211){ #percorrendo a base de info_money_times
  id_acao2 <- 0
  id_carteira3 <- 0
  peso <- 20
  
  i <- 1
  while (i <= 213){ #percorrendo a base infos_money_times_bd
    if (infos_money_times_bd$url[i] == infos_money_times$url[j]){
      id_carteira3 <- as.data.frame(dbGetQuery(con,"SELECT id_carteira FROM carteira_recomendada WHERE
                              carteira_recomendada.url_carteira = $1", list(infos_money_times_bd$url[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 705){ #percorrendo a base ticker
    if (infos_money_times$ticker2[j] == ticker$ticker_completo[x]){ 
      
      id_acao2 <- as.data.frame(dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[x])))
      break
    }
    x <- x+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO acao_carteira_recomendada(acao_id_acao, carteira_recomendada_id_carteira, peso)
              VALUES ($1,$2,$3)", list(id_acao2$id_acao,
                                       id_carteira3$id_carteira,
                                       peso))
  }
  j <- j+1
}

# Coluna ticker3
j <- 1
while (j <= 211){ #percorrendo a base de info_money_times
  id_acao2 <- 0
  id_carteira3 <- 0
  peso <- 20
  
  i <- 1
  while (i <= 213){ #percorrendo a base infos_money_times_bd
    if (infos_money_times_bd$url[i] == infos_money_times$url[j]){
      id_carteira3 <- as.data.frame(dbGetQuery(con,"SELECT id_carteira FROM carteira_recomendada WHERE
                              carteira_recomendada.url_carteira = $1", list(infos_money_times_bd$url[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 705){ #percorrendo a base ticker
    if (infos_money_times$ticker3[j] == ticker$ticker_completo[x]){ 
      
      id_acao2 <- as.data.frame(dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[x])))
      break
    }
    x <- x+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO acao_carteira_recomendada(acao_id_acao, carteira_recomendada_id_carteira, peso)
              VALUES ($1,$2,$3)", list(id_acao2$id_acao,
                                       id_carteira3$id_carteira,
                                       peso))
  }
  j <- j+1
}

# Coluna ticker4
j <- 1
while (j <= 211){ #percorrendo a base de info_money_times
  id_acao2 <- 0
  id_carteira3 <- 0
  peso <- 20
  
  i <- 1
  while (i <= 213){ #percorrendo a base infos_money_times_bd
    if (infos_money_times_bd$url[i] == infos_money_times$url[j]){
      id_carteira3 <- as.data.frame(dbGetQuery(con,"SELECT id_carteira FROM carteira_recomendada WHERE
                              carteira_recomendada.url_carteira = $1", list(infos_money_times_bd$url[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 705){ #percorrendo a base ticker
    if (infos_money_times$ticker4[j] == ticker$ticker_completo[x]){ 
      
      id_acao2 <- as.data.frame(dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[x])))
      break
    }
    x <- x+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO acao_carteira_recomendada(acao_id_acao, carteira_recomendada_id_carteira, peso)
              VALUES ($1,$2,$3)", list(id_acao2$id_acao,
                                       id_carteira3$id_carteira,
                                       peso))
  }
  j <- j+1
}

# Coluna ticker5
j <- 1
while (j <= 211){ #percorrendo a base de info_money_times
  id_acao2 <- 0
  id_carteira3 <- 0
  peso <- 20
  
  i <- 1
  while (i <= 213){ #percorrendo a base infos_money_times_bd
    if (infos_money_times_bd$url[i] == infos_money_times$url[j]){
      id_carteira3 <- as.data.frame(dbGetQuery(con,"SELECT id_carteira FROM carteira_recomendada WHERE
                              carteira_recomendada.url_carteira = $1", list(infos_money_times_bd$url[i])))
      break
    }
    i <- i+1
  }
  
  x<- 1
  while(x <= 705){ #percorrendo a base ticker
    if (infos_money_times$ticker5[j] == ticker$ticker_completo[x]){ 
      
      id_acao2 <- as.data.frame(dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[x])))
      break
    }
    x <- x+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO acao_carteira_recomendada(acao_id_acao, carteira_recomendada_id_carteira, peso)
              VALUES ($1,$2,$3)", list(id_acao2$id_acao,
                                       id_carteira3$id_carteira,
                                       peso))
  }
  j <- j+1
}


#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------

#---------------------------------------------------------------
# Importação da Entidade / Tabela: Preços Fechamento no BD
#---------------------------------------------------------------

j <- 1
while (j <= 780){ #percorrendo a base de ultimo_preco
  id_acao2 = 0
  i <- 1
  while (i <= 705){ #percorrendo a ticker
    if (ticker$ticker_completo[i] == ultimo_preco$ACAO[j]){
      id_acao2 <- dbGetQuery(con,"SELECT id_acao FROM acao WHERE 
             acao.ticker_acao = $1", list(ticker$ticker_completo[i]))
      break
    }
    i <- i+1
  }
  if(id_acao2 != 0){
    dbExecute(con, 
              "INSERT INTO ultimo_preco_acao(data_ultimo_preco,ultimo_preco,id_acao)
              VALUES ($1,$2,$3)", list(ultimo_preco$DATA[j],
                                       ultimo_preco$FECHAMENTO[j],
                                       id_acao2$id_acao))
  }
  j <- j+1
}

#---------------------------------------------------------------
# Fim
#---------------------------------------------------------------