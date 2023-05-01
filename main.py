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
from transformation_tasks import tt1, tt2, tt3


def main():
    spark_session = (SparkSession.builder
                                 .master('local')
                                 .appName('imdb_task app')
                                 .config(conf=SparkConf())
                                 .getOrCreate())

    #title_akas_path = 'imdb_datasets/title.akas.tsv.gz'
    #name_basics_path = 'imdb_datasets/name.basics.tsv.gz'
    title_basics_path = 'imdb_datasets/title.basics.tsv.gz'

    # title_akas_df = import_tsv_to_df(spark_session,
    #                                  title_akas_path,
    #                                  schemas.title_akas_schema)
    # name_basics_df = import_tsv_to_df(spark_session,
    #                                   name_basics_path,
    #                                   schemas.name_basics_schema)
    title_basics_df = import_tsv_to_df(spark_session,
                                      title_basics_path,
                                      schemas.title_basics_schema)

    # all titles of series/movies etc. that are available in Ukrainian
    # export_df_to_csv(tt1.transformation_task_1(title_akas_df), 'csv_results/ukrainian_titles')

    # list of people’s names, who were born in the 19th century
    # export_df_to_csv(tt2.transformation_task_2(name_basics_df), 'csv_results/persons_born_19_century')

    # titles of all movies that last more than 2 hours
    export_df_to_csv(tt3.transformation_task_3(title_basics_df), 'csv_results/movies_with_runtime_over2h')


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
