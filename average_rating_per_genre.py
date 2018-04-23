# Calculate the average rating of each genre
# Note: this is a python-spark implementation of the java-spark code shown in the lecture (MovieLensLarge)
# In order to run this, we use spark-submit, but with a different argument
# pyspark  \
#   --master yarn-client \
#   --num-executors 3 \
#   AverageRatingPerGenre.py

from pyspark import SparkContext

# This function convert entries of movies.csv into key,value pair of the following format
# movie_id -> genre
# since there may be multiple genre per movie, this function returns a list of key,value pair
def pair_movie_to_genre(record):
  try:
    movie_id, title, genre_list = record.strip().split(",")
    genres = genre_list.strip().split("|")
    return [(movie_id, (title, genre.strip())) for genre in genres]
  except:
    return []

# This function convert entries of ratings.csv into key,value pair of the following format
# movie_id -> rating
def extract_rating(record):
  try:
    user_id, movie_id, rating, timestamp = record.split(",")
    rating = float(rating)
    return (movie_id, rating)
  except:
    return ()

# This functions convert tuples of (genre, rating) into key,value pair of the following format
# genre -> rating
def map_to_pair(line):
  genre, rating = line
  return (genre, rating)

# This function is used by the aggregateByKey function to merge rating of the same keys in the same combiner
# This function takes in either the starting value (0.0,0) or the result of previous call to marge_rating function
# in the form of (total rating, number of rating), and the value currently being 'processed'.
def marge_rating(accumulated_pair, current_rating):
  rating_total, rating_count = accumulated_pair
  rating_total += current_rating
  rating_count += 1
  return (rating_total, rating_count)

# This function is used by the aggregateByKey function to merge rating of the same keys from different combiner
# This function takes in the result of marge_rating function from different combiners (in the form of (total rating, number of rating))
def merge_combiners(accumulated_pair_1, accumulated_pair_2):
  rating_total_1, rating_count_1 = accumulated_pair_1
  rating_total_2, rating_count_2 = accumulated_pair_2
  return (rating_total_1+rating_total_2, rating_count_1+rating_count_2)

# This function takes the statistic of ratings per genre and calculate the average rating
def map_average_rating(line):
  genre, rating_total_count = line
  rating_average = rating_total_count[0]/rating_total_count[1]
  return (genre, rating_total_count)

# This is line is important for submitting python jobs through spark-submit!
# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
# Thus the code under the if statement will only be evaluated when the script is called from command line
if __name__ == "__main__":
  sc = SparkContext(appName="Average Rating per Genre")
  ratings = sc.textFile("/share/movie/ratings.csv")
  movie_data = sc.textFile("/share/movie/movies.csv")

  movie_ratings = ratings.map(extract_rating)
  movie_genre = movie_data.flatMap(pair_movie_to_genre) # we use flatMap as there are multiple genre per movie

  genre_ratings = movie_genre.join(movie_ratings).values().map(map_to_pair)
  genre_ratings_average = genre_ratings.aggregateByKey((0.0,0), marge_rating, merge_combiners, 1).map(map_average_rating)

  genre_ratings_average.saveAsTextFile("pySparkRatingPerGenre")
