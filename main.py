"""
This module provides the main entry point for the IMDb data processing pipeline.
It imports the necessary modules and functions from other files and applies
a series of transformations on the imported TSV files. Finally, it exports the
resulting DataFrames to CSV files.

Module contents:
- main: the main function that orchestrates the data processing pipeline.
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession

from imdb_tsv_df_io import import_tsv_to_df, export_df_to_csv
from transformation_package import tt1, tt2, tt3, tt4, tt5, tt6, tt7, tt8

import imdb_data_schemas as schemas


def main():
    """
    Orchestrates the data processing pipeline by importing TSV files to DataFrames,
    applying a series of transformations on the DataFrames, and exporting the
    resulting DataFrames to CSV files.

    Args:
        None

    Returns:
        None
    """
    # Create a SparkSession object
    spark_session = (SparkSession.builder
                                 .master('local')
                                 .appName('imdb_task app')
                                 .config(conf=SparkConf())
                                 .getOrCreate())

    # Define paths to the TSV files
    title_akas_path = 'imdb_datasets/title.akas.tsv.gz'
    name_basics_path = 'imdb_datasets/name.basics.tsv.gz'
    title_basics_path = 'imdb_datasets/title.basics.tsv.gz'
    title_principals_path = 'imdb_datasets/title.principals.tsv.gz'
    title_episode_path = 'imdb_datasets/title.episode.tsv.gz'
    title_ratings_path = 'imdb_datasets/title.ratings.tsv.gz'

    # Import TSV files to DataFrames
    title_akas_df = import_tsv_to_df(spark_session,
                                     title_akas_path,
                                     schemas.title_akas_schema)
    name_basics_df = import_tsv_to_df(spark_session,
                                      name_basics_path,
                                      schemas.name_basics_schema)
    title_basics_df = import_tsv_to_df(spark_session,
                                       title_basics_path,
                                       schemas.title_basics_schema)
    title_principals_df = import_tsv_to_df(spark_session,
                                           title_principals_path,
                                           schemas.title_principals_schema)
    title_episode_df = import_tsv_to_df(spark_session,
                                        title_episode_path,
                                        schemas.title_episode_schema)
    title_ratings_df = import_tsv_to_df(spark_session,
                                        title_ratings_path,
                                        schemas.title_ratings_schema)

    # Apply transformations to the DataFrames and export to CSV files

    # All titles of series/movies etc. that are available in Ukrainian
    export_df_to_csv(tt1.transformation_task_1(title_akas_df),
                     'csv_results/ukrainian_titles')
    # List of people’s names, who were born in the 19th century
    export_df_to_csv(tt2.transformation_task_2(name_basics_df),
                     'csv_results/persons_born_19_century')
    # Titles of all movies that last more than 2 hours
    export_df_to_csv(tt3.transformation_task_3(title_basics_df),
                     'csv_results/movies_with_runtime_over2h')
    # Names of people, corresponding movies/series and characters they played in those films
    export_df_to_csv(tt4.transformation_task_4(title_principals_df, title_basics_df, name_basics_df),
                     'csv_results/people_in_titles_and_there_characters')
    # Adult movies/series etc. there are per region
    export_df_to_csv(tt5.transformation_task_5(title_akas_df, title_basics_df),
                     'csv_results/adult_movies_per_region')
    # Top 50 TV Series by the number of episodes
    export_df_to_csv(tt6.transformation_task_6(title_episode_df, title_basics_df),
                     'csv_results/top_series_by_number_of_episodes')
    # Top 10 titles of the most popular movies/series etc. by each decade
    export_df_to_csv(tt7.transformation_task_7(title_basics_df, title_ratings_df),
                     'csv_results/titles_of_most_popular_by_each_decade')
    # Top 10 titles of the most popular movies/series etc. by each genre
    export_df_to_csv(tt8.transformation_task_8(title_basics_df, title_ratings_df),
                     'csv_results/titles_of_most_popular_by_each_genre')

    # ------------------Alternative 7 and 8 task-------------------------------------#
    alternative = True
    # Top 10 titles of the most popular movies/series etc. by each decade
    export_df_to_csv(tt7.transformation_task_7(title_basics_df, title_ratings_df, alternative),
                     'csv_results/titles_of_most_popular_by_each_decade_alternative')
    # Top 10 titles of the most popular movies/series etc. by each genre
    export_df_to_csv(tt8.transformation_task_8(title_basics_df, title_ratings_df, alternative),
                     'csv_results/titles_of_most_popular_by_each_genre_alternative')
    # ------------------End of alternative 7 and 8 task-------------------------------#


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
