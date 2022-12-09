import pyspark

spark = pyspark.sql.SparkSession \
    .builder \
        .appName ("Python Spark SQL example connection") \
        .config('spark.driver.extraClassPath', "./driver/postgresql-42.5.1.jar") \
        .getOrCreate()




## EXTRACT: Read the tables 'Movies' and 'users' from db using Spark jdbc

def extract_movies_into_df():
    df_movies = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable","movies") \
        .option("user", "postgres") \
        .option("password", "passpostgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df_movies

def extract_users_into_df():
    df_users = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable","users") \
        .option("user", "postgres") \
        .option("password", "passpostgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df_users





##TRANSFER:  Calculate the Average Rating and store in a dataframe 

def transform_avg_ratings(df_movies, df_users):
    df_avg_rating = df_users.groupBy("movie_id").mean("rating")

    #Join between dataframes "movies" and "users"
    df_joined = df_movies.join(
        df_avg_rating, 
        df_movies.id == df_avg_rating.movie_id)
    df_joined.drop('movie_id')

    return df_joined


##LOAD: Load the Dataframe back into the Database
def load_df_into_db(df_joined):
    mode = 'overwrite'
    url = 'jdbc:postgresql://localhost:5432/etl_pipeline'
    properties = {
        'user' : 'postgres',
        'password' : 'passpostgres',
        "driver" : "org.postgresql.Driver"
    }
    df_joined.write.jdbc(
        url = url,
        table = "avg_ratings_test",
        mode = mode,
        properties = properties)



## MAIN METHOD

if __name__ == "__main__":
    df_movies = extract_movies_into_df()
    df_users = extract_users_into_df()
    df_ratings = transform_avg_ratings(df_movies, df_users)
    
    #Evaluation section
    print (df_movies.show())
    print (df_users.show())
    print (df_ratings.show())

    #Load back into Database
    load_df_into_db(df_ratings)


print('Script End reached, finishing.')


