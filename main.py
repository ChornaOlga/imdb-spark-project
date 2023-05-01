# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import imdb_data_schemas as schemas
import columns as c
from imdb_tsv_df_io import import_tsv_to_df, export_df_to_csv


def main():
    spark_session = (SparkSession.builder
                                 .master('local')
                                 .appName('imdb_task app')
                                 .config(conf=SparkConf())
                                 .getOrCreate())

    title_ratings_path = 'imdb_datasets/title.ratings.tsv.gz'
    title_ratings_df = import_tsv_to_df(spark_session,
                                        title_ratings_path,
                                        schemas.title_ratings_schema)

    top_ratings = title_ratings_df.filter(f.col(c.averageRating) > 6)
    export_df_to_csv(top_ratings, 'csv_results/top_ratings')


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
