# Classificação de Dados — People Analytics

## 1. Objetivo

Documentar os critérios utilizados para classificar os dados das tabelas da camada Silver, identificando sua finalidade, sensibilidade e necessidade de avaliação, contribuindo para a governança e o uso responsável das informações.

## 2. Escopo

A classificação contempla as seguintes tabelas da camada Silver:

- `people_analytics.silver.absenteismo`
- `people_analytics.silver.colaboradores`
- `people_analytics.silver.ferias`
- `people_analytics.silver.turnover`

Foram inventariadas e classificadas 71 colunas.

## 3. Categorias de classificação

- **Identificador:** campos utilizados para identificar registros ou relacionar entidades.
- **Pessoal:** informações pessoais dos colaboradores.
- **Profissional:** informações relacionadas à atuação profissional.
- **Financeiro:** informações de natureza financeira.
- **Saúde e afastamento:** informações relacionadas à saúde, faltas e afastamentos.
- **Temporal:** datas e referências de períodos.
- **Métrica operacional:** indicadores e medidas utilizadas nas análises.
- **Técnico e auditoria:** campos técnicos, de processamento e rastreabilidade.
- **Classificação pendente:** campos que ainda precisam de avaliação.

## 4. Sensibilidade dos dados

A sensibilidade preliminar é atribuída conforme a natureza e a finalidade de cada campo. Essa classificação deve apoiar decisões futuras sobre acesso, proteção, compartilhamento e utilização dos dados.

A classificação preliminar não substitui uma avaliação formal de privacidade, segurança ou conformidade legal.

## 5. Validação

A validação considera:

- Cobertura de todas as colunas inventariadas.
- Ausência de duplicidade na combinação de esquema, tabela e coluna.
- Ausência de campos com classificação pendente.
- Preenchimento do status de validação.
- Conferência da persistência dos registros na tabela de governança.

## 6. Persistência e rastreabilidade

As classificações são armazenadas na tabela Delta:

`people_analytics.monitoring.classificacao_dados`

A gravação utiliza inclusão de registros (`append`), permitindo preservar o histórico das execuções. Como cada execução pode registrar novamente as mesmas 71 colunas, a tabela deve ser interpretada como um histórico de classificações, e não apenas como um cadastro com uma linha por coluna.

## 7. Resultado

A classificação das 71 colunas das quatro tabelas Silver foi concluída e validada no fluxo do notebook `04_classificacao_dados`.

A etapa estabelece uma base documentada para as próximas atividades de governança de dados do projeto People Analytics.
