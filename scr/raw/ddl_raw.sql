/* Script de Criação da Camada Raw (External Tables)
   Instruções: Substitua {{PROJECT_ID}} e {{BUCKET_NAME}} pelos seus valores reais.
*/

-- 1. Criação do Dataset (Camada Raw)
CREATE SCHEMA IF NOT EXISTS `{{PROJECT_ID}}.raw_external`
  OPTIONS (
    location = "us"
  );

-- 2. Tabela de Filmes
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.movies` (
  movieId STRING,
  title STRING,
  genres STRING
)
  OPTIONS (
    format = 'CSV',
    uris = ['gs://{{BUCKET_NAME}}/bronze/movies.csv'],
    skip_leading_rows = 1
  );

-- 3. Dados de Crença (Belief Data)
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.belief_data` (
  userId STRING,
  movieId STRING,
  isSeen STRING,
  watchDate STRING,
  userElicitRating STRING,
  userPredictRating STRING,
  userCertainty STRING,
  tstamp STRING,
  movied_idx STRING,
  source STRING,
  systemPredictRating STRING
)
  OPTIONS (
    format = 'CSV',
    uris = ['gs://{{BUCKET_NAME}}/bronze/belief_data.csv'],
    skip_leading_rows = 1
  );

-- 4. Ratings de Usuários Adicionais
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.ratings_for_additional_users` (
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING
)
  OPTIONS (
    format = 'CSV',
    uris = ['gs://{{BUCKET_NAME}}/bronze/ratings_for_additional_users.csv'],
    skip_leading_rows = 1
  );

-- 5. Set de Elicitação de Filmes
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.movie_elicitation_set` (
  movieId STRING,
  month_idx STRING,
  source STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{BUCKET_NAME}}/bronze/movie_elicitation_set.csv'],
  skip_leading_rows = 1
);

-- 6. Histórico de Notas dos Usuários (Ratings)
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.user_rating_history` (
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{BUCKET_NAME}}/bronze/user_rating_history.csv'],
  skip_leading_rows = 1
);

-- 7. Histórico de Recomendações Geradas
CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.raw_external.user_recommendation_history` (
  userId STRING,
  tstamp STRING,
  movieId STRING,
  predictedRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{BUCKET_NAME}}/bronze/user_recommendation_history.csv'],
  skip_leading_rows = 1
);