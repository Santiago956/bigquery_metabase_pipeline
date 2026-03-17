# Dados_por_Todos-Desafio_01

Projeto de engenharia e análise de dados com **GCS + BigQuery + Metabase (Docker)** para exploração de dados de filmes do **MovieLens**.

## Objetivo

Construir uma camada analítica no BigQuery e disponibilizar visualizações no Metabase com foco em:

- média de avaliações por filme;
- evolução temporal de avaliações;
- relação entre popularidade e qualidade;
- atividade de usuários;
- performance por gênero.

## Arquitetura

1. **GCS (bronze)**: armazenamento dos CSVs de origem.
2. **BigQuery `raw_external`**: tabelas externas apontando para arquivos no bucket.
3. **BigQuery `analytics`**: dimensões, fatos e views para consumo analítico.
4. **Metabase (Docker)**: camada de BI para dashboards.

## Estrutura do repositório

```text
.
├── docker-compose.yml
├── creds/
│   └── service_account.json
├── metabase-data/
├── scr/
│   ├── raw/
│   │   └── ddl_raw.sql
│   └── analytics/
│       └── ddl_analytics.sql
└── README.md
```

## Pré-requisitos

- Conta GCP com permissão em **BigQuery** e **Cloud Storage**.
- `gcloud` configurado localmente.
- `docker` + `docker compose`.
- Arquivos CSV do MovieLens no formato esperado pelos DDLs.

## Setup rápido

### 1) Criar bucket e subir arquivos

No GCS, crie um bucket e organize os arquivos em `bronze/`:

- `movies.csv`
- `belief_data.csv`
- `ratings_for_additional_users.csv`
- `movie_elicitation_set.csv`
- `user_rating_history.csv`
- `user_recommendation_history.csv`

Exemplo de URI esperada no SQL:

`gs://SEU_BUCKET/bronze/movies.csv`

### 2) Criar camada raw (external tables)

Arquivo: `scr/raw/ddl_raw.sql`

Substitua placeholders:

- `{{PROJECT_ID}}` → seu project id no GCP
- `{{BUCKET_NAME}}` → nome do bucket GCS

Execute o script no BigQuery.

### 3) Criar camada analytics

Arquivo: `scr/analytics/ddl_analytics.sql`

Substitua `{{seu_id_de_projeto}}` pelo seu project id e execute no BigQuery.

> Observação importante: o arquivo possui referências que precisam estar consistentes com seu projeto (ex.: trechos com `bigquery-case-01` e referências sem project id explícito).

### 4) Subir o Metabase em Docker

```bash
docker compose up -d
```

Metabase disponível em:

`http://localhost:3000`

### 5) Conectar BigQuery no Metabase

No primeiro acesso ao Metabase:

1. Adicione um novo banco do tipo **BigQuery**.
2. Use o conteúdo do service account em `creds/service_account.json`.
3. Selecione o projeto e dataset `analytics`.

## Sugestão de dashboard

Com base nas views do script analítico:

- `vw_top_movies_avg`
- `vw_ratings_time_series`
- `vw_scatter_popularity_vs_quality`
- `vw_user_activity`
- `vw_genre_performance`

## Executando localmente

Parar ambiente:

```bash
docker compose down
```

Resetar banco interno do Metabase (se necessário):

```bash
rm -f metabase-data/metabase.db*
```

## Segurança

- Não versione credenciais reais.
- Mantenha `creds/service_account.json` apenas localmente.
- Prefira contas de serviço com menor privilégio possível.
