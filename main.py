# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f


def main():
    spark_session = (SparkSession.builder
                             .master('local')
                             .appName('imdb_task app')
                             .config(conf=SparkConf())
                             .getOrCreate())

    title_ratings_path = 'imdb_datasets/title.ratings.tsv.gz'
    title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                         t.StructField('averageRating', t.DoubleType(), True),
                                         t.StructField('numVotes', t.IntegerType(), True)])
    title_ratings_df = spark_session.read.csv(title_ratings_path,
                                              sep=r'\t',
                                              header=True,
                                              schema=title_ratings_schema)
    title_ratings_df.show()


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
