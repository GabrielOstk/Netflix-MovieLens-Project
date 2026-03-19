CREATE SCHEMA IF NOT EXISTS `netflix-pipeline-gabrielostk.netflix_silver`;


CREATE OR REPLACE TABLE `netflix-pipeline-gabrielostk.netflix_silver.movies_cleaned` AS
SELECT
  SAFE_CAST(movieId AS INT64) AS movie_id,
  TRIM(REGEXP_REPLACE(title, r'( \([^\)]*\))+$', '')) AS title,
  SAFE_CAST(REGEXP_EXTRACT(title, r'\((\d{4})\)$') AS INT64) AS release_year
FROM `netflix-pipeline-gabrielostk.netflix_raw.raw_movies`
WHERE SAFE_CAST(movieId AS INT64) IS NOT NULL;


-- Genres Relationship Table: Created to deal with N:N relationship between movies and genres.
CREATE OR REPLACE TABLE `netflix-pipeline-gabrielostk.netflix_silver.genres_cleaned` AS
SELECT
  SAFE_CAST(movieId AS INT64) AS movie_id,
  TRIM(genre) AS genre
FROM `netflix-pipeline-gabrielostk.netflix_raw.raw_movies`,
UNNEST(SPLIT(genres, '|')) AS genre
WHERE genres IS NOT NULL AND genres != ''
   AND SAFE_CAST(movieId AS INT64) IS NOT NULL;
   

CREATE OR REPLACE TABLE `netflix-pipeline-gabrielostk.netflix_silver.user_rating_history_cleaned` AS
SELECT
  SAFE_CAST(userId AS INT64) AS user_id,
  SAFE_CAST(movieId AS INT64) AS movie_id,
  ROUND(SAFE_CAST(rating AS FLOAT64),2) AS rating,
  SAFE_CAST(tstamp AS TIMESTAMP) AS rating_ts
FROM `netflix-pipeline-gabrielostk.netflix_raw.raw_user_rating_history`
WHERE SAFE_CAST(userId AS INT64) IS NOT NULL
  AND SAFE_CAST(movieId AS INT64) IS NOT NULL
  AND SAFE_CAST(rating AS FLOAT64) IS NOT NULL
  AND SAFE_CAST(tstamp AS TIMESTAMP) IS NOT NULL
  AND SAFE_CAST(rating AS FLOAT64) <> -1;
  

CREATE OR REPLACE TABLE `netflix-pipeline-gabrielostk.netflix_silver.ratings_for_additional_users_cleaned` AS
SELECT
  SAFE_CAST(userId AS INT64) AS user_id,
  SAFE_CAST(movieId AS INT64) AS movie_id,
  ROUND(SAFE_CAST(rating AS FLOAT64),2) AS rating,
  SAFE_CAST(tstamp AS TIMESTAMP) AS rating_ts
FROM `netflix-pipeline-gabrielostk.netflix_raw.raw_ratings_for_additional_users`
WHERE SAFE_CAST(userId AS INT64) IS NOT NULL
  AND SAFE_CAST(movieId AS INT64) IS NOT NULL
  AND SAFE_CAST(rating AS FLOAT64) IS NOT NULL
  AND SAFE_CAST(tstamp AS TIMESTAMP) IS NOT NULL
  AND SAFE_CAST(rating AS FLOAT64) <> -1;
  

CREATE OR REPLACE TABLE `netflix-pipeline-gabrielostk.netflix_silver.user_recommendations_cleaned` AS
SELECT
  SAFE_CAST(userId AS INT64) AS user_id,
  SAFE_CAST(movieId AS INT64) AS movie_id,
  TIMESTAMP_SECONDS(SAFE_CAST(tstamp AS INT64)) AS recommendation_timestamp,
  ROUND(SAFE_CAST(predictedRating AS FLOAT64),2) AS predicted_rating
FROM `netflix-pipeline-gabrielostk.netflix_raw.raw_user_recommendation_history`
WHERE SAFE_CAST(userId AS INT64) IS NOT NULL
  AND SAFE_CAST(movieId AS INT64) IS NOT NULL
  AND SAFE_CAST(tstamp AS INT64) IS NOT NULL
  AND SAFE_CAST(predictedRating AS FLOAT64) IS NOT NULL;

