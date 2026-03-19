
CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_movies`
(
  movieID STRING,
  title STRING,
  genres STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/movies.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_user_rating_history`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/user_rating_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_ratings_for_additional_users`
(
  userId STRING,
  movieId STRING,
  rating STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/ratings_for_additional_users.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_belief_data`
(
  userId STRING,
  movieId STRING,
  isSeen STRING,
  watchDate STRING,
  userElicitRating STRING,
  userPredictRating STRING,
  userCertainty STRING,
  tstamp STRING,
  month_idx STRING,
  source STRING,
  systemPredictRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/belief_data.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_movie_elicitation_set`
(
  userId STRING,
  month_idx STRING,
  source STRING,
  tstamp STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/movie_elicitation_set.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-gabrielostk.netflix_raw.raw_user_recommendation_history`
(
  userId STRING,
  tstamp STRING,
  movieId STRING,
  predictedRating STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://gabrielostk-netflix-data/bronze/user_recommendation_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);
