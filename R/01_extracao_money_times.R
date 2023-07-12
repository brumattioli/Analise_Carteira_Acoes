#-------------------------------------
# Projeto integrador 1 - Script 1
#-------------------------------------

# Integrantes:
# Bruna Mattioli de Oliveira
# Gabriel Andrade Varga

#-------------------------------------------------------------------------
# Pacotes utilizados
#-------------------------------------------------------------------------
library(rvest)
library(tidyverse)
library(lubridate)

#--------------------------------------------------------------------------------------
# Seção 1 - Webscraping das informações do site de notícias financeiras  Money Times
#--------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------
# Importação de banco de dados em csv com os links selecionados do site Money Times
#-----------------------------------------------------------------------------------

links_money_times <- read_csv2("G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/links_money_times.csv", 
                               locale = locale(encoding = "UTF-8"))
links_money_times

#------------------------------------------------------------------------------------------------
# Criação do dataset com as informações necessárias para o trabalho  capturadas via Webscraping
#------------------------------------------------------------------------------------------------

# Criação do dataset 
extracao_money_times <- links_money_times

# Looping para popular as informações de webscraping - título e data
for(i in 1:length(extracao_money_times$id_url)) {
  
  # Inicializar as variáveis
  extracao_money_times$data[i] <- " "
  extracao_money_times$titulo[i] <- " "
  
  extracao_money_times$empresa1[i] <- " " 
  extracao_money_times$empresa2[i] <- " " 
  extracao_money_times$empresa3[i] <- " " 
  extracao_money_times$empresa4[i] <- " " 
  extracao_money_times$empresa5[i] <- " " 
  
  extracao_money_times$ticker1[i] <- " " 
  extracao_money_times$ticker2[i] <- " " 
  extracao_money_times$ticker3[i] <- " " 
  extracao_money_times$ticker4[i] <- " " 
  extracao_money_times$ticker5[i] <- " " 
  
  extracao_money_times$peso1[i] <- " " 
  extracao_money_times$peso2[i] <- " " 
  extracao_money_times$peso3[i] <- " " 
  extracao_money_times$peso4[i] <- " " 
  extracao_money_times$peso5[i] <- " " 
  
  # WebScraping das informações
  
  # Extração da data da notícia
  extracao_money_times$data[i] <- read_html(extracao_money_times$url[i]) %>% html_nodes(".single-meta__date") %>% html_text() 
  
  # Extração do título da notícia
  extracao_money_times$titulo[i] <- read_html(extracao_money_times$url[i]) %>% html_nodes(".single__title") %>% html_text() 
  
  # Extração da tabela da notícia que contém as informações da carteira semanal recomendada
  tabela_temp <- read_html(extracao_money_times$url[i]) %>% html_table() %>% unlist()
  
  # Criação do campo com o nome da empresa da carteira semanal recomendada
  extracao_money_times$empresa1[i] <- tabela_temp[1]
  extracao_money_times$empresa2[i] <- tabela_temp[2] 
  extracao_money_times$empresa3[i] <- tabela_temp[3]
  extracao_money_times$empresa4[i] <- tabela_temp[4] 
  extracao_money_times$empresa5[i] <- tabela_temp[5]
  
  # Criação do campo com o ticker da empresa da carteira semanal recomendada
  extracao_money_times$ticker1[i] <- tabela_temp[6] 
  extracao_money_times$ticker2[i] <- tabela_temp[7] 
  extracao_money_times$ticker3[i] <- tabela_temp[8] 
  extracao_money_times$ticker4[i] <- tabela_temp[9] 
  extracao_money_times$ticker5[i] <- tabela_temp[10] 
  
  # Criação do campo com o peso da ação da carteira semanal recomendada
  extracao_money_times$peso1[i] <- 20
  extracao_money_times$peso2[i] <- 20
  extracao_money_times$peso3[i] <- 20
  extracao_money_times$peso4[i] <- 20
  extracao_money_times$peso5[i] <- 20
}

extracao_money_times

#-----------------------------------------------------------
# Fim
#-----------------------------------------------------------

#-----------------------------------------------------------
# Tratamento da base e criação de campos complementares
#-----------------------------------------------------------

# Manipulação do Dataset para capturar outras informações
infos_money_times <- mutate(extracao_money_times, data_temp = str_sub(data, end = 10),
                            data_hora = dmy(data_temp),
                            
                            dia = format(data_hora, '%d'),
                            mes = format(data_hora, '%m'),
                            ano = format(data_hora, '%Y'),
                            data_completa = paste0(as.character(dia),"-", as.character(mes), "-", as.character(ano)),
                            fonte_noticia = "Money Times",
                            id_noticia = id_url,
                            url_noticia = url,
                            
                            data_noticia = as.Date(format(data_hora,'%Y-%m-%d')),
                            
                            id_carteira = id_url,
                            data_carteira = data_hora, 
                            validade_carteira = data_hora + 5,
                            
                            corretora = case_when(
                              (str_detect(str_to_lower(extracao_money_times$titulo), "xp")) ~ "XP INVESTIMENTOS CCTVM S/A",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "terra")) ~ "TERRA INVESTIMENTOS DTVM LTDA",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "ativa")) ~ "ATIVA INVESTIMENTOS S/A CTCV",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "btg")) ~ "BTG PACTUAL CTVM S/A",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "rumo")) ~ "ICAP DO BRASIL CTVM LTDA",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "elite")) ~ "ELITE CCVM LTDA",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "mycap")) ~ "ICAP DO BRASIL CTVM LTDA",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "my cap")) ~ "ICAP DO BRASIL CTVM LTDA",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "guide")) ~ "GUIDE INVESTIMENTOS SA CORRETORA DE VALORES",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "necton")) ~ "NECTON INVESTIMENTOS S.A. CVMC",
                              (str_detect(str_to_lower(extracao_money_times$titulo), "nova futura")) ~ "NOVA FUTURA CTVM LTDA",
                              TRUE ~ "VERIFICAR"))
                      
infos_money_times

# Exportar para CSV

write.csv2(infos_money_times, "G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/infos_money_times.csv")

#-----------------------------------------------------------
# Fim
#-----------------------------------------------------------

#------------------------------------------------------------------------
# Criação de uma base com menos campos para importação no Banco de Dados
#------------------------------------------------------------------------

infos_money_times_bd <- select(infos_money_times, id_url, url, data_noticia, ticker1, ticker2, ticker3, ticker4, ticker5,
                               corretora)

infos_money_times_bd

# Exportar para CSV

write.csv2(infos_money_times, "G:/My Drive/Especialização/Disciplinas/Módulo 1/Projeto Integrador/Projeto Final/Bases/infos_money_times_bd.csv")

#-----------------------------------------------------------
# Fim
#-----------------------------------------------------------

