# etl.py
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import warnings
warnings.filterwarnings("ignore")

def run_etl():
    spark = SparkSession.builder \
        .appName("ETL Pipeline Netflix Data") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Data path
    raw_data_path = "C:/Users/bikas/Desktop/airflow_pyspark/data/netflix_titles.csv"
    transformed_data_path = "C:/Users/bikas/Desktop/airflow_pyspark/data/transformed/transformed_netflix_titles.parquet"

    # Extract
    def extract_data():
        return spark.read.csv(raw_data_path, header = True, inferSchema = True)

    # Transform
    def transform_data(df):
        df_cleaned = df.dropna(subset=["title", "release_year", "rating"])
        df_filtered = df_cleaned.filter(col("release_year") >= 2000)
        df_movies = df_filtered.filter(col("type") == "Movie")
        df_tv_shows = df_filtered.filter(col("type") == "TV Show")
        rating_count = df_filtered.groupBy("rating").agg(count("*").alias("count"))

        return df_filtered, df_movies, df_tv_shows, rating_count

    # Load
    def load_data(df_filtered, df_movies, df_tv_shows, df_analytics):
        df_filtered.write.mode("overwrite").parquet(transformed_data_path)

        # Rating distribution
        df_analytics_pd = df_analytics.toPandas()

        df_analytics_pd.plot(kind = 'bar', x = 'rating', y = 'count', legend = False)
        plt.title('Distribution of Movies by Rating')
        plt.ylabel('Count')
        plt.xlabel('Rating')
        plt.tight_layout()
        plt.savefig('C:/Users/bikas/Desktop/airflow_pyspark/data/transformed/rating_distribution.png')

        # Category distribution
        category_count = pd.DataFrame({
            'Category': ['Movies', 'TV Shows'],
            'Count': [df_movies.count(), df_tv_shows.count()]
        })

        category_count.plot(kind = 'bar', x = 'Category', y = 'Count', legend = False)
        plt.title('Entertainment Category Distribution')
        plt.ylabel('Count')
        plt.tight_layout()
        plt.savefig('C:/Users/bikas/Desktop/airflow_pyspark/data/transformed/category_distribution.png')

        # Genre Distribution
        genre_count = df_filtered.groupBy("listed_in").agg(count("*").alias("count")).orderBy("count", ascending=False)
        genre_count_pd = genre_count.limit(5).toPandas()

        plt.figure(figsize=(15, 10))
        colors = ['#FF5733', '#33FF57', '#3357FF', '#FF33A8', '#FF5733']
        genre_count_pd.plot(kind = 'bar', x = 'listed_in', y = 'count', color = colors, legend = False)
        plt.title('Top 5 genre', pad=20)
        plt.ylabel('Count')
        plt.xlabel('Genre')
        plt.xticks(rotation = 45, ha = 'right')

        plt.tight_layout()
        plt.savefig('C:/Users/bikas/Desktop/airflow_pyspark/data/transformed/genre_distribution.png')

    raw_data = extract_data()
    df_filtered, df_movies, df_tv_shows, analytics_data = transform_data(raw_data)
    load_data(df_filtered, df_movies, df_tv_shows, analytics_data)

    print("ETL Process Completed..")
