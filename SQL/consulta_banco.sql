-- Consulta envolvendo projeção e seleção

-- Selecionar todos os tickers das ações e os respectivos nomes das empresas que sejam do segmento 'Bancos':

SELECT ticker_acao, nm_empresa
FROM setor as s, acao as a, empresa as e
WHERE a.id_empresa = e.id_empresa AND s.id_setor = a.id_setor AND nm_segmento = 'Bancos';


-- Consulta que envolva junção externa (LEFT OUTER JOIN, ou RIGHT OUTER JOIN ou FULL OUTER JOIN)

-- Retornar quais instituições financeiras divulgaram ao menos uma carteira recomendada e trazer a data da divulgação da carteira. Com isto, elaborar um gráfico de barras contendo 
-- a quantidade de carteiras recomendadas por corretora:

SELECT nm_inst_financ, COUNT(data_carteira) AS qtd_divulgacoes
FROM carteira_recomendada as c
LEFT JOIN instituicao_financeira as i ON i.id_inst_financ = c.id_inst_financ
GROUP BY nm_inst_financ
ORDER BY nm_inst_financ

-- Consulta que envolva pelo menos uma operação de conjunto (UNION, INTERSECT, EXCEPT)

-- Retornar o nome de todas as instituições financeiras que não divulgaram nenhuma carteira recomendada:

SELECT nm_inst_financ FROM instituicao_financeira
WHERE id_inst_financ IN
(SELECT id_inst_financ FROM instituicao_financeira
EXCEPT
SELECT id_inst_financ FROM carteira_recomendada)
ORDER BY nm_inst_financ;


-- Consulta que envolva divisão relacional
-- Retornar todas as ações que estiveram em todas as carteiras recomendadas nos 10 primeiros dias de abril:

SELECT ticker_acao FROM acao
WHERE NOT EXISTS
(SELECT DISTINCT carteira_recomendada_id_carteira
FROM acao_carteira_recomendada WHERE carteira_recomendada_id_carteira IN
  (SELECT id_carteira FROM carteira_recomendada WHERE data_carteira BETWEEN '2021-04-01' AND '2021-04-10')
EXCEPT (SELECT carteira_recomendada_id_carteira FROM acao_carteira_recomendada
WHERE acao.id_acao = acao_carteira_recomendada.acao_id_acao))

-- Consulta com operação de agregação e agrupamento (Função de agregação + GROUP BY)
-- Retornar quantas vezes as ações do segmento ‘Exploração, Refino e Distribuição’ apareceram nas carteiras recomendadas divulgadas pela instituição financeira 'XP INVESTIMENTOS CCTVM S/A':

SELECT COUNT(ticker_acao) AS qtd_acoes
FROM carteira_recomendada as c, instituicao_financeira as i, acao as a, setor as s, acao_carteira_recomendada as ac
WHERE c.id_inst_financ = i.id_inst_financ 
AND c.id_carteira = ac.carteira_recomendada_id_carteira
AND ac.acao_id_acao = a.id_acao AND s.id_setor = a.id_setor 
AND s.nm_segmento = 'Exploração, Refino e Distribuição'
AND i.nm_inst_financ = 'XP INVESTIMENTOS CCTVM S/A'