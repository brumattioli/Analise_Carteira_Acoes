#-------------------------------------
# Projeto integrador 1 - Script 3
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
library(ggplot2)

#-------------------------------------------------------------
# Seção 1 - Conexão com o BD e Análise Exploratória dos Dados
#-------------------------------------------------------------

#-----------------------------------------------------------
# Conexão com PostgreSQL
#-----------------------------------------------------------

#aqui estamos definindo nossa conexão e atribuindo essa conexão à variável con
con <- dbConnect(Postgres(),                 
                 user = "postgres",                 
                 password = "vagan9ch",                 
                 host = "localhost",                 
                 port = 5432,                 
                 dbname = "Projeto_Integrador_1")

#-----------------------------------------------------------
# Queries criadas para a matéria de IGBD
#-----------------------------------------------------------

#-----------------------------------------------------------
# 1 consulta envolvendo projeção e seleção
#-----------------------------------------------------------

# Selecionar todos os tickers das ações e os respectivos nomes das empresas que sejam do segmento 'Bancos':

#-------------
# Tabela
#-------------

consulta1 <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, nm_empresa
                                        FROM setor as s, acao as a, empresa as e
                                        WHERE a.id_empresa = e.id_empresa AND s.id_setor = a.id_setor 
                                        AND nm_segmento = 'Bancos'"))
consulta1

#-----------------------------------------------------------
# Fim
#-----------------------------------------------------------

#------------------------------------------------------------------------------------
# 1 consulta que envolva junção externa (LEFT OUTER JOIN, ou RIGHT OUTER JOIN ou
# FULL OUTER JOIN)
#------------------------------------------------------------------------------------

# Retornar quais instituições financeiras divulgaram ao menos uma carteira recomendada e 
# trazer a data da divulgação da carteira. Com isto, elaborar um gráfico de barras contendo
# a quantidade de carteiras recomendadas por corretora:

#-------------
# Tabela
#-------------

consulta2 <- as_tibble(dbGetQuery(con, "SELECT nm_inst_financ, COUNT(nm_inst_financ) AS qtd_divulgacoes
                                       FROM carteira_recomendada as c
                                       LEFT JOIN instituicao_financeira as i ON i.id_inst_financ = c.id_inst_financ
                                       GROUP BY nm_inst_financ
                                       ORDER BY nm_inst_financ"))

consulta2

consulta2_ajuste <- mutate(consulta2, nm_corretora = case_when(consulta2$nm_inst_financ == "XP INVESTIMENTOS CCTVM S/A" ~ "XP Investimentos",
                                                                consulta2$nm_inst_financ == "TERRA INVESTIMENTOS DTVM LTDA" ~ "Terra Investimentos",
                                                                consulta2$nm_inst_financ == "ATIVA INVESTIMENTOS S/A CTCV" ~ "Ativa Investimentos",
                                                                consulta2$nm_inst_financ == "BTG PACTUAL CTVM S/A" ~ "BTG Pactual",
                                                                consulta2$nm_inst_financ == "ICAP DO BRASIL CTVM LTDA" ~ "MyCap",
                                                                consulta2$nm_inst_financ == "ELITE CCVM LTDA" ~ "Elite Investimentos",
                                                                consulta2$nm_inst_financ == "GUIDE INVESTIMENTOS SA CORRETORA DE VALORES" ~ "Guide Investimentos",
                                                                consulta2$nm_inst_financ == "NECTON INVESTIMENTOS S.A. CVMC" ~ "Necton Investimentos",
                                                                consulta2$nm_inst_financ == "NOVA FUTURA CTVM LTDA" ~ "Nova Futura Investimentos",
                                                               TRUE ~ "VERIFICAR"))
 
# Tabela                                                                                                                             
consulta2_ajuste

#-------------------
# Gráfico de Barras
#-------------------

grafico_consulta2_final <- ggplot(data = consulta2_ajuste) +
  geom_bar(stat = "identity", mapping = aes(x = nm_corretora, y = as.integer(qtd_divulgacoes), fill = nm_corretora)) +
  labs(x = "Corretora", y = "Quantidade de recomendações") +
  theme(axis.title = element_text(size = 10), plot.title = element_text(size = 12, face = "bold")) +
  ggtitle("Quantidade de carteiras recomendadas pelas corretoras no período completo da base") +
  scale_y_continuous(limits = c(0, 45), n.breaks = 9) +
  scale_fill_discrete(name = "Corretoras") +
  scale_x_discrete(labels = (c("Ativa\nInvestimentos", "BTG\nPactual", "Elite\nInvestimentos",
                               "Guide\nInvestimentos", "MyCap", "Necton\nInvestimentos",
                               "Nova Futura\nInvestimentos", "Terra\nInvestimentos", "XP\nInvestimentos")))

grafico_consulta2_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
# 1 consulta que envolva pelo menos uma operação de conjunto (UNION, INTERSECT,
# EXCEPT)
#------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

# Retornar o nome de todas as instituições financeiras que não divulgaram nenhuma 
# carteira recomendada:
  
consulta3 <- as_tibble(dbGetQuery(con, "SELECT nm_inst_financ FROM instituicao_financeira
                                        WHERE id_inst_financ IN
                                        (SELECT id_inst_financ FROM instituicao_financeira
                                        EXCEPT
                                        SELECT id_inst_financ FROM carteira_recomendada)
                                        ORDER BY nm_inst_financ"))

consulta3

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
# 1 consulta que envolva divisão relacional
#------------------------------------------------------------------------------------
#-------------
# Tabela
#-------------

# Retornar todas as ações que estiveram em todas as carteiras recomendadas nos 10 primeiros dias de abril

consulta4 <- as_tibble(dbGetQuery(con, "SELECT ticker_acao FROM acao
                                        WHERE NOT EXISTS
                                        (SELECT DISTINCT carteira_recomendada_id_carteira
                                        FROM acao_carteira_recomendada WHERE carteira_recomendada_id_carteira IN
                                        (SELECT id_carteira FROM carteira_recomendada WHERE data_carteira BETWEEN '2021-04-01' AND '2021-04-10')
                                        EXCEPT (SELECT carteira_recomendada_id_carteira FROM acao_carteira_recomendada
                                        WHERE acao.id_acao = acao_carteira_recomendada.acao_id_acao))"))
consulta4

#------------------------------------------------------------------------------------
# 1 consulta com operação de agregação e agrupamento (Função de agregação + GROUP BY)
#------------------------------------------------------------------------------------

# Retornar quantas vezes as ações do segmento ‘Exploração, Refino e Distribuição’ apareceram nas 
# carteiras recomendadas divulgadas pela instituição financeira 'XP INVESTIMENTOS CCTVM S/A':

consulta5 <- as_tibble(dbGetQuery(con, "SELECT COUNT(ticker_acao) AS qtd_acoes
                                        FROM carteira_recomendada as c, instituicao_financeira as i, acao as a, 
                                        setor as s, acao_carteira_recomendada as ac
                                        WHERE c.id_inst_financ = i.id_inst_financ 
                                        AND c.id_carteira = ac.carteira_recomendada_id_carteira
                                        AND ac.acao_id_acao = a.id_acao AND s.id_setor = a.id_setor 
                                        AND s.nm_segmento = 'Exploração, Refino e Distribuição'
                                        AND i.nm_inst_financ = 'XP INVESTIMENTOS CCTVM S/A'"))
consulta5

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-----------------------------------------------------------
# Análises extras para a matéria de IACD
#-----------------------------------------------------------

#------------------------------------------------------------------------------------
# 1.1 - Qual a ação que foi mais recomendada durante o período completo da base?
#------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

acao_mais_recomendada <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, COUNT(data_carteira) AS qtd_acoes_recomendadas
                                                   FROM carteira_recomendada as c, acao_carteira_recomendada as ac, acao as a
                                                   WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao
                                                   GROUP BY ticker_acao
                                                   ORDER BY qtd_acoes_recomendadas DESC"))

acao_mais_recomendada

max_acao_mais_recomendada <- filter(acao_mais_recomendada, qtd_acoes_recomendadas == max(qtd_acoes_recomendadas))
max_acao_mais_recomendada

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------
# 1.2 - Qual a variação de preço de fechamento da ação mais recomendada no mês de Abril/21?
#-------------------------------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

b3sa3 <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, ultimo_preco, data_ultimo_preco
                                    FROM acao as a, ultimo_preco_acao as u
                                    WHERE a.id_acao = u.id_acao AND ticker_acao = 'B3SA3'"))

b3sa3

#-------------------
# Gráfico de Linha
#-------------------

grafico_b3sa3 <- ggplot(data = b3sa3) + 
                 geom_line(aes(x = data_ultimo_preco, y = ultimo_preco, colour = "orange")) +
                 scale_color_discrete(name = "Ação", labels = c("B3SA3")) +
                 theme_classic() +
                 labs(x = "Data", y = "Preço de Fechamento da Ação") +
                 theme(axis.title = element_text(size = 10), plot.title = element_text(size = 12, face = "bold")) +
                 ggtitle("Preço da Ação B3SA3 no mês de Abril de 2021") +
                 scale_y_continuous(limits = c(50, 57), n.breaks = 20) +
                 scale_x_date(date_breaks = 'day', date_labels = '%b %d\n%a') 
 

grafico_b3sa3

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
# 2 - Quais os 10 setores que têm mais ações recomendadas?
#------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

setor <- as_tibble(dbGetQuery(con, "SELECT nm_segmento, COUNT(data_carteira) AS qtd_divulgacoes
                                         FROM acao as a, acao_carteira_recomendada as acr, carteira_recomendada as c,
                                              setor as s
                                         WHERE c.id_carteira = acr.carteira_recomendada_id_carteira AND acr.acao_id_acao = a.id_acao AND a.id_setor = s.id_setor
                                         GROUP BY nm_segmento
                                         ORDER BY qtd_divulgacoes DESC"))
setor

setor2 <- top_n(setor, n = 10)

setor2

#-------------------
# Gráfico de Barras
#-------------------

grafico_setor_final <- ggplot(data = setor2) +
  geom_bar(stat = "identity", mapping = aes(x = nm_segmento, y = as.integer(qtd_divulgacoes), fill = nm_segmento)) +
  labs(x = "Segmentos", y = "Quantidade de recomendações") +
  theme(axis.title = element_text(size = 10), plot.title = element_text(size = 12, face = "bold")) +
  ggtitle("Quantidade de ações recomendadas pelas corretoras dos 10 maiores setores") +
  scale_y_continuous(limits = c(0, 100), n.breaks = 10) +
  scale_fill_discrete(name = "Segmentos")


grafico_setor_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------
# 3.1 - Qual a carteira recomendada do grupo com base nas 5 ações mais recomendadas em todo o período?
#-------------------------------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

acao_recomendada <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, COUNT(data_carteira) AS qtd_divulgacoes
                                         FROM acao as a, acao_carteira_recomendada as acr, carteira_recomendada as c
                                         WHERE c.id_carteira = acr.carteira_recomendada_id_carteira AND acr.acao_id_acao = a.id_acao 
                                         GROUP BY ticker_acao
                                         ORDER BY qtd_divulgacoes DESC"))
acao_recomendada

acao_recomendada2 <- top_n(acao_recomendada, n = 5)

acao_recomendada2

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------
# 3.2 - Qual a média de preço de fechamento das ações que compõem a carteira recomendada do grupo no mês de abril?
#-----------------------------------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

preco <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, AVG(ultimo_preco) AS media_preco
                                    FROM carteira_recomendada as c, acao_carteira_recomendada as ac, acao as a, ultimo_preco_acao as u
                                    WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao 
                                    AND a.id_acao = u.id_acao AND ticker_acao IN ('B3SA3', 'MGLU3', 'VALE3', 'WEGE3', 'VVAR3')
                                    GROUP BY ticker_acao"))
preco

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------
# 4 - Qual a empresa que mais apareceu nas carteiras durante o mês de Janeiro de 2021?
#-------------------------------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

empresa_mais_recomendada <- as_tibble(dbGetQuery(con, "SELECT nm_empresa, COUNT(data_carteira) AS qtd_recomendacoes
                                                       FROM carteira_recomendada as c, acao_carteira_recomendada as ac, acao as a, empresa as e
                                                       WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao
                                                       AND a.id_empresa = e.id_empresa AND data_carteira BETWEEN '2021-01-01' AND '2021-01-31'
                                                       GROUP BY nm_empresa
                                                       ORDER BY qtd_recomendacoes DESC"))

empresa_mais_recomendada


max_empresa_mais_recomendada <- filter(empresa_mais_recomendada, qtd_recomendacoes == max(qtd_recomendacoes))
max_empresa_mais_recomendada

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------
# 5 - Quais as ações que apareceram em mais de 2 carteiras recomendadas na última semana? E no último mês?
#-------------------------------------------------------------------------------------------------------------

#----------------------
# 5.1 Na última semana
#----------------------

#-------------
# Tabela
#-------------

acoes_semana <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, COUNT(data_carteira) AS qtd_carteiras
                                           FROM carteira_recomendada as c, acao as a, acao_carteira_recomendada as ac
                                           WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao
                                           AND data_carteira BETWEEN '2021-04-19' AND '2021-04-26'
                                           GROUP BY ticker_acao
                                           ORDER BY qtd_carteiras DESC"))
acoes_semana

acoes_semana_final <- filter(acoes_semana, qtd_carteiras > 2)
acoes_semana_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------
# Gráfico de Barras
#-------------------

grafico_acoes_semana_final <- ggplot(data = acoes_semana_final) +
  geom_bar(stat = "identity", mapping = aes(x = ticker_acao, y = as.integer(qtd_carteiras), fill = ticker_acao)) +
  labs(x = "Ticker da Ação", y = "Quantidade de carteiras") +
  theme(axis.title = element_text(size = 10), plot.title = element_text(size = 12, face = "bold")) +
  ggtitle("Distribuição de ações que apareceram em mais de 2 carteiras recomendadas entre os dias 
19/04/2021 e 26/04/2021") +
  scale_y_continuous(limits = c(0, 5), n.breaks = 5) +
  scale_fill_discrete(name = "Ticker") 

grafico_acoes_semana_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#----------------------
# 5.2 No último mês
#----------------------

#-------------
# Tabela
#-------------

acoes_mes <- as_tibble(dbGetQuery(con, "SELECT ticker_acao, COUNT(data_carteira) AS qtd_carteiras 
                                        FROM carteira_recomendada as c, acao as a, acao_carteira_recomendada as ac
                                        WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao
                                        AND data_carteira BETWEEN '2021-04-01' AND '2021-04-26'
                                        GROUP BY ticker_acao
                                        ORDER BY qtd_carteiras DESC"))
acoes_mes

acoes_mes_final <- filter(acoes_mes, qtd_carteiras > 2)
acoes_mes_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------
# Gráfico de Barras
#-------------------

grafico_acoes_mes_final <- ggplot(data = acoes_mes_final) +
  geom_bar(stat = "identity", mapping = aes(x = ticker_acao, y = as.integer(qtd_carteiras), fill = ticker_acao)) +
  labs(x = "Ticker da Ação", y = "Quantidade de carteiras") +
  theme(axis.title = element_text(size = 10), plot.title = element_text(size = 12, face = "bold")) +
  ggtitle("Distribuição de ações que apareceram em mais de 2 carteiras recomendadas no mês de Abril/21") +
  scale_y_continuous(limits = c(0, 8), n.breaks = 8) +
  scale_fill_discrete(name = "Ticker") 

grafico_acoes_mes_final

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------
# 6 - Qual o setor econômico / segmento que teve mais ações recomendadas no mês de Fevereiro de 2021?
#-------------------------------------------------------------------------------------------------------------

#-------------
# Tabela
#-------------

setor_fev21 <- as_tibble(dbGetQuery(con, "SELECT nm_segmento, COUNT(data_carteira) AS qtd_acoes_recomendadas
                                          FROM carteira_recomendada as c, acao_carteira_recomendada as ac, acao as a, setor as s
                                          WHERE c.id_carteira = ac.carteira_recomendada_id_carteira AND ac.acao_id_acao = a.id_acao 
                                          AND a.id_setor = s.id_setor AND data_carteira BETWEEN '2021-02-01' AND '2021-02-28'
                                          GROUP BY nm_segmento
                                          ORDER BY qtd_acoes_recomendadas DESC"))

setor_fev21

max_setor_fev21 <- filter(setor_fev21, qtd_acoes_recomendadas == max(qtd_acoes_recomendadas))
max_setor_fev21

#------------------------------------------------------------------------------------
# Fim
#------------------------------------------------------------------------------------


