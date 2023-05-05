"""
This module contains PySpark schema definitions for various IMDb datasets.

PySpark schemas define the structure of a DataFrame. They specify the name, data type, and whether or not a column can contain null values.

Schemas:
    name_basics_schema: PySpark schema for the name.basics dataset.
    title_akas_schema: PySpark schema for the title.akas dataset.
    title_basics_schema: PySpark schema for the title.basics dataset.
    title_crew_schema: PySpark schema for the title.crew dataset.
    title_episode_schema: PySpark schema for the title.episode dataset.
    title_principals_schema: PySpark schema for the title.principals dataset.
    title_ratings_schema: PySpark schema for the title.ratings dataset.
"""
import pyspark.sql.types as t


name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), True),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField('birthYear', t.StringType(), True),
                                   t.StructField('deathYear', t.StringType(), True),
                                   t.StructField('primaryProfession', t.StringType(), True),
                                   t.StructField('knownForTitles', t.StringType(), True)])

title_akas_schema = t.StructType([t.StructField('titleId', t.StringType(), True),
                                  t.StructField('ordering', t.IntegerType(), True),
                                  t.StructField('title', t.StringType(), True),
                                  t.StructField('region', t.StringType(), True),
                                  t.StructField('language', t.StringType(), True),
                                  t.StructField('types', t.StringType(), True),
                                  t.StructField('attributes', t.StringType(), True),
                                  t.StructField('isOriginalTitle', t.IntegerType(), True)])

title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                    t.StructField('titleType', t.StringType(), True),
                                    t.StructField('primaryTitle', t.StringType(), True),
                                    t.StructField('originalTitle', t.StringType(), True),
                                    t.StructField('isAdult', t.IntegerType(), True),
                                    t.StructField('startYear', t.StringType(), True),
                                    t.StructField('endYear', t.StringType(), True),
                                    t.StructField('runtimeMinutes', t.IntegerType(), True),
                                    t.StructField('genres', t.StringType(), True)])

title_crew_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                  t.StructField('directors', t.StringType(), True),
                                  t.StructField('writers', t.StringType(), True)])

title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                     t.StructField('parentTconst', t.StringType(), True),
                                     t.StructField('seasonNumber', t.IntegerType(), True),
                                     t.StructField('episodeNumber', t.IntegerType(), True)])

title_principals_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                        t.StructField('ordering', t.IntegerType(), True),
                                        t.StructField('nconst', t.StringType(), True),
                                        t.StructField('category', t.StringType(), True),
                                        t.StructField('job', t.StringType(), True),
                                        t.StructField('characters', t.StringType(), True)])

title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                     t.StructField('averageRating', t.DoubleType(), True),
                                     t.StructField('numVotes', t.IntegerType(), True)])
