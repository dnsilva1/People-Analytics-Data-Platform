# Databricks notebook source
# Databricks notebook source

# ==========================================
# Projeto: People Analytics
# Camada: Gold
# Notebook: 06_gold_validacao
#
# Objetivo:
# Validar a integridade e consistência das
# tabelas da camada Gold após sua construção.
#
# Função:
# Quality Gate da camada Gold.
#
# Resultado:
# PASS -> Todas as validações foram aprovadas
# FAIL -> Uma ou mais validações apresentaram erro
#
# Tabelas avaliadas:
# - people_analytics.gold.dim_calendario
# - people_analytics.gold.dim_colaborador
# - people_analytics.gold.fato_ferias
# - people_analytics.gold.fato_absenteismo
# - people_analytics.gold.fato_turnover
# ==========================================


from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURAÇÃO
# ============================================================

print("=" * 70)
print("VALIDAÇÃO DA CAMADA GOLD")
print("=" * 70)

schema_gold = "people_analytics.gold"

tabela_dim_calendario = f"{schema_gold}.dim_calendario"
tabela_dim_colaborador = f"{schema_gold}.dim_colaborador"
tabela_fato_ferias = f"{schema_gold}.fato_ferias"
tabela_fato_absenteismo = f"{schema_gold}.fato_absenteismo"
tabela_fato_turnover = f"{schema_gold}.fato_turnover"


# Lista onde serão armazenados os resultados
resultados = []


def registrar_resultado(
    teste,
    categoria,
    tabela,
    qtd_erro,
    descricao,
    severidade="ALTO"
):
    """
    Registra o resultado de uma validação.
    """

    status = "PASS" if qtd_erro == 0 else "FAIL"

    resultados.append(
        (
            teste,
            categoria,
            tabela,
            int(qtd_erro),
            status,
            severidade,
            descricao
        )
    )


# ============================================================
# 2. VERIFICAÇÃO DE EXISTÊNCIA DAS TABELAS
# ============================================================

print("\n")
print("=" * 70)
print("1. EXISTÊNCIA DAS TABELAS GOLD")
print("=" * 70)


tabelas_gold = [
    tabela_dim_calendario,
    tabela_dim_colaborador,
    tabela_fato_ferias,
    tabela_fato_absenteismo,
    tabela_fato_turnover
]


for tabela in tabelas_gold:

    existe = spark.catalog.tableExists(tabela)

    registrar_resultado(
        teste="tabela_gold_existe",
        categoria="ESTRUTURA",
        tabela=tabela,
        qtd_erro=0 if existe else 1,
        descricao=(
            "Tabela Gold encontrada."
            if existe
            else "Tabela Gold não encontrada."
        ),
        severidade="CRITICO"
    )


# Se alguma tabela não existir, não faz sentido continuar.
df_resultados_inicial = spark.createDataFrame(
    resultados,
    [
        "teste",
        "categoria",
        "tabela",
        "qtd_erro",
        "status",
        "severidade",
        "descricao"
    ]
)

display(df_resultados_inicial)


tabelas_faltantes = (
    df_resultados_inicial
    .filter(F.col("status") == "FAIL")
    .count()
)


if tabelas_faltantes > 0:

    print(
        f"ERRO: {tabelas_faltantes} tabela(s) Gold não encontrada(s)."
    )

    raise Exception(
        "Validação Gold interrompida: existem tabelas Gold ausentes."
    )


# ============================================================
# 3. LEITURA DAS TABELAS
# ============================================================

print("\n")
print("=" * 70)
print("2. LEITURA DAS TABELAS GOLD")
print("=" * 70)


df_dim_calendario = spark.table(tabela_dim_calendario)

df_dim_colaborador = spark.table(tabela_dim_colaborador)

df_fato_ferias = spark.table(tabela_fato_ferias)

df_fato_absenteismo = spark.table(tabela_fato_absenteismo)

df_fato_turnover = spark.table(tabela_fato_turnover)


# ============================================================
# 4. VALIDAÇÃO DA DIM_CALENDARIO
# ============================================================

print("\n")
print("=" * 70)
print("3. VALIDAÇÃO - DIM_CALENDARIO")
print("=" * 70)


# ------------------------------------------------------------
# 4.1 Data nula
# ------------------------------------------------------------

qtd_datas_nulas = (
    df_dim_calendario
    .filter(F.col("data").isNull())
    .count()
)

registrar_resultado(
    teste="dim_calendario_data_obrigatoria",
    categoria="CHAVE",
    tabela=tabela_dim_calendario,
    qtd_erro=qtd_datas_nulas,
    descricao="A coluna data deve estar preenchida.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 4.2 Datas duplicadas
# ------------------------------------------------------------

qtd_datas_duplicadas = (
    df_dim_calendario
    .groupBy("data")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

registrar_resultado(
    teste="dim_calendario_data_unica",
    categoria="CHAVE",
    tabela=tabela_dim_calendario,
    qtd_erro=qtd_datas_duplicadas,
    descricao="Cada data deve existir uma única vez.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 4.3 Continuidade do calendário
# ------------------------------------------------------------

periodo_calendario = (
    df_dim_calendario
    .agg(
        F.min("data").alias("data_min"),
        F.max("data").alias("data_max"),
        F.count("*").alias("qtd_registros")
    )
    .collect()[0]
)


data_min_calendario = periodo_calendario["data_min"]
data_max_calendario = periodo_calendario["data_max"]
qtd_registros_calendario = periodo_calendario["qtd_registros"]


if data_min_calendario is not None and data_max_calendario is not None:

    qtd_dias_esperados = (
        data_max_calendario - data_min_calendario
    ).days + 1

else:

    qtd_dias_esperados = 0


erro_continuidade = abs(
    qtd_registros_calendario - qtd_dias_esperados
)


registrar_resultado(
    teste="dim_calendario_continuidade",
    categoria="INTEGRIDADE",
    tabela=tabela_dim_calendario,
    qtd_erro=erro_continuidade,
    descricao=(
        "O calendário deve possuir todos os dias entre "
        "a menor e a maior data."
    ),
    severidade="ALTO"
)


print(f"Data inicial: {data_min_calendario}")
print(f"Data final: {data_max_calendario}")
print(f"Registros encontrados: {qtd_registros_calendario}")
print(f"Registros esperados: {qtd_dias_esperados}")


# ============================================================
# 5. VALIDAÇÃO DA DIM_COLABORADOR
# ============================================================

print("\n")
print("=" * 70)
print("4. VALIDAÇÃO - DIM_COLABORADOR")
print("=" * 70)


# ------------------------------------------------------------
# 5.1 ID nulo
# ------------------------------------------------------------

qtd_id_colaborador_nulo = (
    df_dim_colaborador
    .filter(F.col("id_colaborador").isNull())
    .count()
)

registrar_resultado(
    teste="dim_colaborador_id_obrigatorio",
    categoria="CHAVE",
    tabela=tabela_dim_colaborador,
    qtd_erro=qtd_id_colaborador_nulo,
    descricao="O id_colaborador deve estar preenchido.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 5.2 ID duplicado
# ------------------------------------------------------------

qtd_id_colaborador_duplicado = (
    df_dim_colaborador
    .groupBy("id_colaborador")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

registrar_resultado(
    teste="dim_colaborador_id_unico",
    categoria="CHAVE",
    tabela=tabela_dim_colaborador,
    qtd_erro=qtd_id_colaborador_duplicado,
    descricao="Cada colaborador deve possuir apenas um registro.",
    severidade="CRITICO"
)


# ============================================================
# 6. VALIDAÇÃO - FATO_FERIAS
# ============================================================

print("\n")
print("=" * 70)
print("5. VALIDAÇÃO - FATO_FERIAS")
print("=" * 70)


# ------------------------------------------------------------
# 6.1 ID nulo
# ------------------------------------------------------------

qtd_id_ferias_nulo = (
    df_fato_ferias
    .filter(F.col("id_ferias").isNull())
    .count()
)

registrar_resultado(
    teste="fato_ferias_id_obrigatorio",
    categoria="CHAVE",
    tabela=tabela_fato_ferias,
    qtd_erro=qtd_id_ferias_nulo,
    descricao="O id_ferias deve estar preenchido.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 6.2 ID duplicado
# ------------------------------------------------------------

qtd_id_ferias_duplicado = (
    df_fato_ferias
    .groupBy("id_ferias")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

registrar_resultado(
    teste="fato_ferias_id_unico",
    categoria="CHAVE",
    tabela=tabela_fato_ferias,
    qtd_erro=qtd_id_ferias_duplicado,
    descricao="Cada id_ferias deve existir uma única vez.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 6.3 Integridade referencial - colaborador
# ------------------------------------------------------------

df_colaboradores = (
    df_dim_colaborador
    .select("id_colaborador")
    .distinct()
)


qtd_ferias_sem_colaborador = (
    df_fato_ferias.alias("f")
    .join(
        df_colaboradores.alias("d"),
        F.col("f.id_colaborador") ==
        F.col("d.id_colaborador"),
        "left"
    )
    .filter(F.col("d.id_colaborador").isNull())
    .count()
)


registrar_resultado(
    teste="fato_ferias_integridade_colaborador",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_ferias,
    qtd_erro=qtd_ferias_sem_colaborador,
    descricao=(
        "Todo id_colaborador presente na fato_ferias "
        "deve existir na dim_colaborador."
    ),
    severidade="CRITICO"
)


# ============================================================
# 7. VALIDAÇÃO - FATO_ABSENTEISMO
# ============================================================

print("\n")
print("=" * 70)
print("6. VALIDAÇÃO - FATO_ABSENTEISMO")
print("=" * 70)


# ------------------------------------------------------------
# 7.1 ID nulo
# ------------------------------------------------------------

qtd_id_absenteismo_nulo = (
    df_fato_absenteismo
    .filter(F.col("id_absenteismo").isNull())
    .count()
)

registrar_resultado(
    teste="fato_absenteismo_id_obrigatorio",
    categoria="CHAVE",
    tabela=tabela_fato_absenteismo,
    qtd_erro=qtd_id_absenteismo_nulo,
    descricao="O id_absenteismo deve estar preenchido.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 7.2 ID duplicado
# ------------------------------------------------------------

qtd_id_absenteismo_duplicado = (
    df_fato_absenteismo
    .groupBy("id_absenteismo")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

registrar_resultado(
    teste="fato_absenteismo_id_unico",
    categoria="CHAVE",
    tabela=tabela_fato_absenteismo,
    qtd_erro=qtd_id_absenteismo_duplicado,
    descricao="Cada id_absenteismo deve existir uma única vez.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 7.3 Integridade referencial - colaborador
# ------------------------------------------------------------

qtd_absenteismo_sem_colaborador = (
    df_fato_absenteismo.alias("f")
    .join(
        df_colaboradores.alias("d"),
        F.col("f.id_colaborador") ==
        F.col("d.id_colaborador"),
        "left"
    )
    .filter(F.col("d.id_colaborador").isNull())
    .count()
)


registrar_resultado(
    teste="fato_absenteismo_integridade_colaborador",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_absenteismo,
    qtd_erro=qtd_absenteismo_sem_colaborador,
    descricao=(
        "Todo id_colaborador presente na fato_absenteismo "
        "deve existir na dim_colaborador."
    ),
    severidade="CRITICO"
)


# ============================================================
# 8. VALIDAÇÃO - FATO_TURNOVER
# ============================================================

print("\n")
print("=" * 70)
print("7. VALIDAÇÃO - FATO_TURNOVER")
print("=" * 70)


# ------------------------------------------------------------
# 8.1 ID nulo
# ------------------------------------------------------------

qtd_id_turnover_nulo = (
    df_fato_turnover
    .filter(F.col("id_turnover").isNull())
    .count()
)

registrar_resultado(
    teste="fato_turnover_id_obrigatorio",
    categoria="CHAVE",
    tabela=tabela_fato_turnover,
    qtd_erro=qtd_id_turnover_nulo,
    descricao="O id_turnover deve estar preenchido.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 8.2 ID duplicado
# ------------------------------------------------------------

qtd_id_turnover_duplicado = (
    df_fato_turnover
    .groupBy("id_turnover")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

registrar_resultado(
    teste="fato_turnover_id_unico",
    categoria="CHAVE",
    tabela=tabela_fato_turnover,
    qtd_erro=qtd_id_turnover_duplicado,
    descricao="Cada id_turnover deve existir uma única vez.",
    severidade="CRITICO"
)


# ------------------------------------------------------------
# 8.3 Integridade referencial - colaborador
# ------------------------------------------------------------

qtd_turnover_sem_colaborador = (
    df_fato_turnover.alias("f")
    .join(
        df_colaboradores.alias("d"),
        F.col("f.id_colaborador") ==
        F.col("d.id_colaborador"),
        "left"
    )
    .filter(F.col("d.id_colaborador").isNull())
    .count()
)


registrar_resultado(
    teste="fato_turnover_integridade_colaborador",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_turnover,
    qtd_erro=qtd_turnover_sem_colaborador,
    descricao=(
        "Todo id_colaborador presente na fato_turnover "
        "deve existir na dim_colaborador."
    ),
    severidade="CRITICO"
)


# ============================================================
# 9. RECONCILIAÇÃO SILVER × GOLD
# ============================================================

print("\n")
print("=" * 70)
print("8. RECONCILIAÇÃO SILVER × GOLD")
print("=" * 70)


# ------------------------------------------------------------
# 9.1 Colaboradores
# ------------------------------------------------------------

df_silver_colaboradores = spark.table(
    "people_analytics.silver.colaboradores"
)

qtd_silver_colaboradores = (
    df_silver_colaboradores.count()
)

qtd_gold_colaboradores = (
    df_dim_colaborador.count()
)

erro_colaboradores = abs(
    qtd_silver_colaboradores -
    qtd_gold_colaboradores
)

registrar_resultado(
    teste="reconciliacao_colaboradores",
    categoria="RECONCILIACAO",
    tabela=tabela_dim_colaborador,
    qtd_erro=erro_colaboradores,
    descricao=(
        "A quantidade de registros da dim_colaborador "
        "deve ser igual à Silver colaboradores após "
        "as transformações realizadas."
    ),
    severidade="ALTO"
)


# ------------------------------------------------------------
# 9.2 Férias
# ------------------------------------------------------------

df_silver_ferias = spark.table(
    "people_analytics.silver.ferias"
)

qtd_silver_ferias = df_silver_ferias.count()
qtd_gold_ferias = df_fato_ferias.count()

erro_ferias = abs(
    qtd_silver_ferias -
    qtd_gold_ferias
)

registrar_resultado(
    teste="reconciliacao_ferias",
    categoria="RECONCILIACAO",
    tabela=tabela_fato_ferias,
    qtd_erro=erro_ferias,
    descricao=(
        "A quantidade de registros da fato_ferias "
        "deve ser igual à Silver ferias."
    ),
    severidade="ALTO"
)


# ------------------------------------------------------------
# 9.3 Absenteísmo
# ------------------------------------------------------------

df_silver_absenteismo = spark.table(
    "people_analytics.silver.absenteismo"
)

qtd_silver_absenteismo = df_silver_absenteismo.count()
qtd_gold_absenteismo = df_fato_absenteismo.count()

erro_absenteismo = abs(
    qtd_silver_absenteismo -
    qtd_gold_absenteismo
)

registrar_resultado(
    teste="reconciliacao_absenteismo",
    categoria="RECONCILIACAO",
    tabela=tabela_fato_absenteismo,
    qtd_erro=erro_absenteismo,
    descricao=(
        "A quantidade de registros da fato_absenteismo "
        "deve ser igual à Silver absenteismo."
    ),
    severidade="ALTO"
)


# ------------------------------------------------------------
# 9.4 Turnover
# ------------------------------------------------------------

df_silver_turnover = spark.table(
    "people_analytics.silver.turnover"
)

qtd_silver_turnover = df_silver_turnover.count()
qtd_gold_turnover = df_fato_turnover.count()

erro_turnover = abs(
    qtd_silver_turnover -
    qtd_gold_turnover
)

registrar_resultado(
    teste="reconciliacao_turnover",
    categoria="RECONCILIACAO",
    tabela=tabela_fato_turnover,
    qtd_erro=erro_turnover,
    descricao=(
        "A quantidade de registros da fato_turnover "
        "deve ser igual à Silver turnover."
    ),
    severidade="ALTO"
)


# ============================================================
# 10. INTEGRIDADE DAS DATAS COM DIM_CALENDARIO
# ============================================================

print("\n")
print("=" * 70)
print("9. INTEGRIDADE DAS DATAS COM DIM_CALENDARIO")
print("=" * 70)


df_datas_calendario = (
    df_dim_calendario
    .select("data")
    .distinct()
)


# ------------------------------------------------------------
# 10.1 Férias
# ------------------------------------------------------------

qtd_ferias_data_inicio_fora_calendario = (
    df_fato_ferias.alias("f")
    .join(
        df_datas_calendario.alias("d"),
        F.to_date(F.col("f.data_inicio")) ==
        F.col("d.data"),
        "left"
    )
    .filter(F.col("d.data").isNull())
    .count()
)


registrar_resultado(
    teste="fato_ferias_data_inicio_calendario",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_ferias,
    qtd_erro=qtd_ferias_data_inicio_fora_calendario,
    descricao=(
        "A data_inicio das férias deve existir "
        "na dim_calendario."
    ),
    severidade="ALTO"
)


# ------------------------------------------------------------
# 10.2 Absenteísmo
# ------------------------------------------------------------

qtd_absenteismo_data_fora_calendario = (
    df_fato_absenteismo.alias("f")
    .join(
        df_datas_calendario.alias("d"),
        F.to_date(F.col("f.data_falta")) ==
        F.col("d.data"),
        "left"
    )
    .filter(F.col("d.data").isNull())
    .count()
)


registrar_resultado(
    teste="fato_absenteismo_data_calendario",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_absenteismo,
    qtd_erro=qtd_absenteismo_data_fora_calendario,
    descricao=(
        "A data_falta deve existir na dim_calendario."
    ),
    severidade="ALTO"
)


# ------------------------------------------------------------
# 10.3 Turnover
# ------------------------------------------------------------

qtd_turnover_data_fora_calendario = (
    df_fato_turnover.alias("f")
    .join(
        df_datas_calendario.alias("d"),
        F.to_date(F.col("f.data_desligamento")) ==
        F.col("d.data"),
        "left"
    )
    .filter(F.col("d.data").isNull())
    .count()
)


registrar_resultado(
    teste="fato_turnover_data_calendario",
    categoria="INTEGRIDADE_REFERENCIAL",
    tabela=tabela_fato_turnover,
    qtd_erro=qtd_turnover_data_fora_calendario,
    descricao=(
        "A data_desligamento deve existir "
        "na dim_calendario."
    ),
    severidade="ALTO"
)


# ============================================================
# 11. RESUMO FINAL DAS VALIDAÇÕES
# ============================================================

print("\n")
print("=" * 70)
print("10. RESUMO FINAL")
print("=" * 70)


df_resultados = spark.createDataFrame(
    resultados,
    [
        "teste",
        "categoria",
        "tabela",
        "qtd_erro",
        "status",
        "severidade",
        "descricao"
    ]
)


display(
    df_resultados
    .orderBy(
        F.when(F.col("status") == "FAIL", 0).otherwise(1),
        F.col("severidade"),
        F.col("categoria"),
        F.col("teste")
    )
)


# ============================================================
# 12. RESUMO EXECUTIVO
# ============================================================

qtd_testes = df_resultados.count()

qtd_pass = (
    df_resultados
    .filter(F.col("status") == "PASS")
    .count()
)

qtd_fail = (
    df_resultados
    .filter(F.col("status") == "FAIL")
    .count()
)


print("\n")
print("=" * 70)
print("RESULTADO DA VALIDAÇÃO GOLD")
print("=" * 70)

print(f"Total de testes: {qtd_testes}")
print(f"Testes aprovados: {qtd_pass}")
print(f"Testes reprovados: {qtd_fail}")


# ============================================================
# 13. VALIDAÇÕES REPROVADAS
# ============================================================

if qtd_fail > 0:

    print("\n")
    print("VALIDAÇÕES COM FALHA:")
    print("-" * 70)

    display(
        df_resultados
        .filter(F.col("status") == "FAIL")
        .select(
            "teste",
            "categoria",
            "tabela",
            "qtd_erro",
            "severidade",
            "descricao"
        )
    )


# ============================================================
# 14. QUALITY GATE
# ============================================================

if qtd_fail == 0:

    print("\n")
    print("=" * 70)
    print("STATUS FINAL: APROVADO")
    print("=" * 70)
    print("Todas as validações da camada Gold foram aprovadas.")

else:

    print("\n")
    print("=" * 70)
    print("STATUS FINAL: REPROVADO")
    print("=" * 70)
    print(
        f"{qtd_fail} validação(ões) apresentaram falha."
    )

    raise Exception(
        f"Quality Gate Gold reprovado: {qtd_fail} validação(ões) falharam."
    )