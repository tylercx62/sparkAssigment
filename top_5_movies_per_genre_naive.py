# Calculate the top 5 movies per genre
# Note: this is a python-spark implementation of the java-spark code from the tutorial example (MLGenreTopMoviesNaive.java)
# In order to run this, we use spark-submit, but with a different argument
# pyspark  \
#   --master yarn-client \
#   --num-executors 3 \
#   Top5MoviesPerGenreNaive.py

from pyspark import SparkContext

# This function convert entries of movies.csv into key,value pair of the following format
# movie_id -> genre
# since there may be multiple genre per movie, this function returns a list of key,value pair
def pair_movie_to_genre(record):
  try:
    # csv column is seperated by ","
    movie_id, title, genre_list = record.strip().split(",")
    #genres is a list coz split() will make it a list
    genres = genre_list.strip().split("|")
    return [(movie_id, (title, genre.strip())) for genre in genres]
  except:
    return []

# This function convert entries of ratings.csv into key,value pair of the following format
# movie_id -> 1
def extract_rating(record):
  try:
    user_id, movie_id, rating, timestamp = record.strip().split(",")
    return (movie_id.strip(), 1)
  except:
    return ()

# This function is used by reduceByKey function to merge count of the same key
# This functions takes in two values - merged count from previous call of sum_rating_count, and the currently processed count
def sum_rating_count(reduced_count, current_count):
  return reduced_count+current_count

# This functions convert tuples of ((title, genre), number of rating) into key,value pair of the following format
# genre -> (title, number of rating)
def map_to_pair(record):
  title_genre, count = record
  title, genre = title_genre
  return (genre, (title, count))

# This functions is used to calculate the top 5 movies per genre
# The input of this function is in the form of (genre, [(title, number of rating)+])
def naive_top_movies(record):
  genre, title_rating_list = record
  # Sorted function takes in an iterator (such as list) and returns back a sorted list.
  # The key argument is an optional argument used to choose custom key for sorting.
  # In this case, since the key is the number of rating, we needed to use the lambda function to 'present' the number of rating
  # as key rather than the title.
  sorted_rating_list = sorted(title_rating_list, key=lambda rec:rec[-1], reverse=True)
  return genre, sorted_rating_list[:5]

# This is line is important for submitting python jobs through spark-submit!
# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
# Thus the code under the if statement will only be evaluated when the script is called from command line
if __name__ == "__main__":
  sc = SparkContext(appName="Top 5 Movies per Genre Naive")
  
  #userId,movieId,tag,timestamp
  ratings = sc.textFile("/share/movie/ratings.csv")
  
  #movieId,title,genres
  movie_data = sc.textFile("/share/movie/movies.csv")

  movie_ratings_count = ratings.map(extract_rating).reduceByKey(sum_rating_count)
  movie_genre = movie_data.flatMap(pair_movie_to_genre)

  genre_ratings = movie_genre.join(movie_ratings_count).values().map(map_to_pair)
  genre_movies = genre_ratings.groupByKey(1)
  genre_top5_movies = genre_movies.map(naive_top_movies)

  genre_top5_movies.saveAsTextFile("pySparkTop5MoviesPerGenreNaive")
