

-------------------------------------------------------------------------------
-- 1. DIMENSÃO: MOVIES
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{{seu_id_de_projeto}}.analytics.dim_movies`
CLUSTER BY sk_movie
AS
SELECT
  TO_HEX(MD5(CAST(movieId AS STRING))) AS sk_movie,
  SAFE_CAST(movieId AS INT64) AS movie_id,
  
  TRIM(REGEXP_REPLACE(title, r'\s*\(\d{4}\)\s*$', '')) AS movie_title,
  
  SAFE_CAST(REGEXP_EXTRACT(title, r'\((\d{4})\)') AS INT64) AS release_year,
  
  -- 3. GÊNEROS: Tratamento de nulos antes do split
  SPLIT(IFNULL(genres, '(no genres listed)'), '|') AS genres_array,
  
  title AS original_title_raw
FROM `{{seu_id_de_projeto}}.raw_external.movies`;

-------------------------------------------------------------------------------
-- 2. TABELA FATO: RATINGS & INTERATIONS (Unificada)
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{{seu_id_de_projeto}}.analytics.fact_ratings`
-- Particionamento mensal para otimizar custos e performance
PARTITION BY DATE_TRUNC(final_interaction_date, MONTH)
-- Clusterização por Filme e Usuário para acelerar joins e filtros de BI
CLUSTER BY sk_movie, user_id
AS
SELECT
  -- Chave de Identidade Única (Baseada no registro de crença original)
  TO_HEX(MD5(CONCAT(bd.userId, '|', bd.movieId, '|', bd.tstamp))) AS sk_rating,
  
  -- Chave Estrangeira para Dimensão
  TO_HEX(MD5(CAST(bd.movieId AS STRING))) AS sk_movie,
  
  SAFE_CAST(bd.userId AS INT64) AS user_id,
  SAFE_CAST(bd.movieId AS INT64) AS movie_id,

  -- TRATAMENTO CATEGÓRICO: isSeen (Documentação: 1=Yes, 0=No, -1=Sem Resposta)
  CASE 
    WHEN SAFE_CAST(bd.isSeen AS INT64) = 1 THEN 'Yes'
    WHEN SAFE_CAST(bd.isSeen AS INT64) = 0 THEN 'No'
    WHEN SAFE_CAST(bd.isSeen AS INT64) = -1 THEN 'Ignored/No Response'
    ELSE 'Unknown' 
  END AS is_seen_label,

  -- REGRA DE NEGÓCIO: Fallback de Nota Real (Elicitada > Histórica)
  -- NULLIF remove o valor -1 dos cálculos estatísticos
  COALESCE(
      NULLIF(SAFE_CAST(bd.userElicitRating AS FLOAT64), -1.0), 
      NULLIF(SAFE_CAST(rh.rating AS FLOAT64), -1.0)
    ) AS final_rating,

  -- REGRA DE NEGÓCIO: Fallback de Data (Sincronizada com a origem da nota)
  COALESCE(
      SAFE_CAST(bd.tstamp AS TIMESTAMP), 
      SAFE_CAST(rh.tstamp AS TIMESTAMP)
    ) AS final_interaction_date,

  -- MÉTRICAS DE PREDIÇÃO E EXPECTATIVA
  NULLIF(SAFE_CAST(bd.userPredictRating AS FLOAT64), -1.0) AS user_predicted_rating,
  SAFE_CAST(bd.systemPredictRating AS FLOAT64) AS system_predicted_rating,
  
  -- CATEGORIZAÇÃO DE CONFIANÇA (UX de Dados)
  CASE
    WHEN SAFE_CAST(bd.userCertainty AS INT64) >= 4 THEN 'High Confidence'
    WHEN SAFE_CAST(bd.userCertainty AS INT64) BETWEEN 2 AND 3 THEN 'Medium Confidence'
    WHEN SAFE_CAST(bd.userCertainty AS INT64) = 1 THEN 'Low Confidence'
    ELSE 'Not Specified'
  END AS user_confidence_level,

  -- ORIGEM DA ELICITAÇÃO (Conforme Seção 2.4 do README do dataset)
  CASE 
    WHEN SAFE_CAST(bd.source AS INT64) = 1 THEN 'Broad Sampling'
    WHEN SAFE_CAST(bd.source AS INT64) = 2 THEN 'Elicitation w/ Recommendation'
    WHEN SAFE_CAST(bd.source AS INT64) = 3 THEN 'Sample New Movies'
    ELSE 'Other'
  END AS elicitation_source_name,

  -- RASTREABILIDADE
  CASE 
      WHEN bd.userElicitRating IS NOT NULL THEN 'ELICITED'
      WHEN rh.rating IS NOT NULL THEN 'HISTORY_FALLBACK'
      ELSE 'EXPECTATION_ONLY'
    END AS rating_origin_type

FROM `{{seu_id_de_projeto}}.raw_external.belief_data` AS bd
LEFT JOIN `{{seu_id_de_projeto}}.raw_external.user_rating_history` AS rh
  ON SAFE_CAST(bd.userId AS STRING) = SAFE_CAST(rh.userId AS STRING) 
  AND SAFE_CAST(bd.movieId AS STRING) = SAFE_CAST(rh.movieId AS STRING);

-------------------------------------------------------------------------------
-- VIEW: TOP 10 FILMES POR MÉDIA DE AVALIAÇÕES
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW `analytics.vw_top_movies_avg`
AS
SELECT 
  movies.movie_title,
  ROUND(AVG(ratings.final_rating), 2) AS average_rating,
  COUNT(ratings.sk_rating) AS total_votes -- Adicionado para transparência
FROM `{{seu_id_de_projeto}}.analytics.dim_movies` AS movies
JOIN `{{seu_id_de_projeto}}.analytics.fact_ratings` AS ratings
  ON movies.sk_movie = ratings.sk_movie
-- Garantimos que só entram notas reais (Elicitadas ou Históricas)
WHERE ratings.final_rating IS NOT NULL
GROUP BY 1
-- Filtro de "Relevância"
HAVING total_votes >= 100 
ORDER BY average_rating DESC, total_votes DESC
LIMIT 10;


-------------------------------------------------------------------------------
-- VIEW: HEATMAP DE AVALIAÇÕES
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW `{{seu_id_de_projeto}}.analytics.vw_ratings_time_series` AS
SELECT 
    DATE_TRUNC(DATE(final_interaction_date), MONTH) AS reference_month,
    COUNT(*) AS total_ratings,
    ROUND(AVG(final_rating), 2) AS avg_rating_period,
    -- Distinção entre o que veio de histórico e o que foi resposta direta (Elicited)
    COUNTIF(rating_origin_type = 'ELICITED') AS elicited_count,
    COUNTIF(rating_origin_type = 'HISTORY_FALLBACK') AS historical_count
FROM `bigquery-case-01.analytics.fact_ratings`
WHERE final_interaction_date IS NOT NULL
GROUP BY 1
ORDER BY 1;


-------------------------------------------------------------------------------
-- VIEW: POPULARIDADE VS QUALIDADE
-------------------------------------------------------------------------------


CREATE OR REPLACE VIEW `{{seu_id_de_projeto}}.analytics.vw_scatter_popularity_vs_quality` AS
SELECT 
    d.movie_title,
    -- Eixo X: Popularidade (Métrica Agregada)
    COUNT(f.sk_rating) AS total_votes,
    -- Eixo Y: Qualidade (Métrica Agregada)
    ROUND(AVG(f.final_rating), 2) AS average_rating,
    -- Atributos para filtros
    d.release_year,
    d.genres_array[SAFE_OFFSET(0)] AS primary_genre
FROM `{{seu_id_de_projeto}}.analytics.fact_ratings` AS f
JOIN `{{seu_id_de_projeto}}.analytics.dim_movies` AS d 
  ON f.sk_movie = d.sk_movie
WHERE f.final_rating IS NOT NULL
GROUP BY 
    d.movie_title, 
    d.release_year, 
    primary_genre;


-------------------------------------------------------------------------------
-- VIEW: ATIVIDADE DO USUÁRIO
-------------------------------------------------------------------------------


CREATE OR REPLACE VIEW `{{seu_id_de_projeto}}.analytics.vw_user_activity` AS
SELECT 
    user_id,
    -- 1. Volume de Interações Reais
    COUNTIF(rating_origin_type = 'ELICITED') AS total_elicited_ratings,
    COUNTIF(rating_origin_type = 'HISTORY_FALLBACK') AS total_historical_ratings,
    
    -- 2. Engajamento com o Sistema
    COUNTIF(is_seen_label = 'Ignored/No Response') AS total_ignored_requests,
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(is_seen_label != 'Ignored/No Response'), 
            COUNT(*)
        ) * 100, 2
    ) AS response_rate_percentage,

    -- 3. Perfil de Certeza (Forçando conversão para numérico)
    -- O SAFE_CAST aqui garante que, se houver um valor inválido, ele vira NULL e não quebra a query
    ROUND(AVG(SAFE_CAST(user_confidence_level AS FLOAT64)), 2) AS avg_user_certainty_score,

    -- 4. Diversidade de Gêneros
    COUNT(DISTINCT genre) AS distinct_genres_explored

FROM `{{seu_id_de_projeto}}.analytics.fact_ratings` AS f
-- Join lateral para explodir os gêneros e contar a diversidade por usuário
LEFT JOIN UNNEST((
    SELECT genres_array 
    FROM `{{seu_id_de_projeto}}.analytics.dim_movies` AS d 
    WHERE d.sk_movie = f.sk_movie
)) AS genre
GROUP BY user_id;


-------------------------------------------------------------------------------
-- VIEW: PERFORMANCE POR GÊNERO
-------------------------------------------------------------------------------


CREATE OR REPLACE VIEW `{{seu_id_de_projeto}}.analytics.vw_genre_performance` AS
SELECT 
    genre,
    -- 1. Qualidade: Média das notas reais por gênero
    ROUND(AVG(f.final_rating), 2) AS avg_rating,
    
    -- 2. Popularidade: Volume de avaliações
    COUNT(f.sk_rating) AS total_votes,
    
    -- 3. Engajamento: Quantidade de usuários únicos interessados no gênero
    COUNT(DISTINCT f.user_id) AS unique_users_count,
    
    -- 4. Predição: Média do que o sistema previu para este gênero
    ROUND(AVG(f.system_predicted_rating), 2) AS avg_system_predicted_rating
FROM `{{seu_id_de_projeto}}.analytics.fact_ratings` AS f
-- O CROSS JOIN UNNEST "explode" o array de gêneros em linhas individuais
CROSS JOIN UNNEST((
    SELECT genres_array 
    FROM `{{seu_id_de_projeto}}.analytics.dim_movies` AS d 
    WHERE d.sk_movie = f.sk_movie
)) AS genre
WHERE f.final_rating IS NOT NULL
GROUP BY genre
ORDER BY avg_rating DESC;


-------------------------------------------------------------------------------
-- VIEW: HEATMAP DE AVALIAÇÕES POR HORA E DIA DA SEMANA
-------------------------------------------------------------------------------

SELECT * FROM (
  SELECT 

    CONCAT(
      LPAD(CAST(DIV(EXTRACT(HOUR FROM final_interaction_date), 3) * 3 AS STRING), 2, '0'), 
      ':00 - ', 
      LPAD(CAST(DIV(EXTRACT(HOUR FROM final_interaction_date), 3) * 3 + 3 AS STRING), 2, '0'),
      ':00'
    ) AS hour_range,
    
    
    FORMAT_DATE('%A', final_interaction_date) AS day_name,
    

    sk_rating
  FROM `analytics.fact_ratings`
)
PIVOT(
  COUNT(sk_rating) 

  FOR day_name IN ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday')
)
ORDER BY hour_range ASC