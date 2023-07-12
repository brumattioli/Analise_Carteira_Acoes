CREATE TABLE public.acao
(
    "id_acao" serial NOT NULL,
    "ticker_acao" varchar(10) NOT NULL,
	nm_ticker_acao VARCHAR(4) NOT NULL,
    "id_setor" integer NOT NULL,
    "id_empresa" integer NOT NULL,
    CONSTRAINT acao_pk PRIMARY KEY ("id_acao")
);	

CREATE TABLE public.setor
(
    "id_setor" serial NOT NULL,
    "nm_segmento" varchar(100) NOT NULL,
    CONSTRAINT setor_pk PRIMARY KEY ("id_setor")
);

CREATE TABLE public."carteira_recomendada"
(
    "id_carteira" serial NOT NULL,
    "data_carteira" date NOT NULL,
	url_carteira VARCHAR(300) NOT NULL,
    "id_inst_financ" integer NOT NULL,
    CONSTRAINT carteira_rec_pk PRIMARY KEY ("id_carteira")
);

CREATE TABLE public."acao_carteira_recomendada"
(
    "acao_id_acao" integer NOT NULL,
    "carteira_recomendada_id_carteira" integer NOT NULL,
    peso double precision NOT NULL,
    CONSTRAINT acao_carteira_pk PRIMARY KEY ("acao_id_acao", "carteira_recomendada_id_carteira")
);

CREATE TABLE public."instituicao_financeira"
(
    "id_inst_financ" serial NOT NULL,
    "cnpj_inst_financ" varchar(14) NOT NULL,
    "nm_inst_financ" varchar(100) NOT NULL,
    CONSTRAINT inst_financ_pk PRIMARY KEY ("id_inst_financ")
);

CREATE TABLE public.empresa
(
    "id_empresa" serial NOT NULL,
    "nm_empresa" varchar(200) NOT NULL,
    CONSTRAINT empresa_pk PRIMARY KEY ("id_empresa")
);

CREATE TABLE public."ultimo_preco_acao"
(
    "data_ultimo_preco" date NOT NULL,
    "ultimo_preco" double precision NOT NULL,
    "id_acao" integer NOT NULL,
    CONSTRAINT ultimo_preco_pk PRIMARY KEY ("data_ultimo_preco", "id_acao")
);

ALTER TABLE public.acao
    ADD CONSTRAINT setor_fk FOREIGN KEY ("id_setor")
    REFERENCES public.setor ("id_setor");
	
ALTER TABLE public.acao
    ADD CONSTRAINT empresa_fk FOREIGN KEY ("id_empresa")
    REFERENCES public.empresa ("id_empresa");

ALTER TABLE public."ultimo_preco_acao"
    ADD CONSTRAINT acao_fk FOREIGN KEY ("id_acao")
    REFERENCES public.acao ("id_acao");
	
ALTER TABLE public."acao_carteira_recomendada"
    ADD CONSTRAINT acao_fk FOREIGN KEY ("acao_id_acao")
    REFERENCES public.acao ("id_acao");

ALTER TABLE public."acao_carteira_recomendada"
    ADD CONSTRAINT carteira_fk FOREIGN KEY ("carteira_recomendada_id_carteira")
    REFERENCES public."carteira_recomendada" ("id_carteira");

ALTER TABLE public."carteira_recomendada"
    ADD CONSTRAINT inst_financ_fk FOREIGN KEY ("id_inst_financ")
    REFERENCES public."instituicao_financeira" ("id_inst_financ");