import pyspark.sql.types as t


name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), True),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField('birthYear', t.DateType(), True),
                                   t.StructField('deathYear', t.DateType(), True),
                                   t.StructField('primaryProfession', t.StringType(), True),
                                   t.StructField('knownForTitles', t.StringType(), True)])

title_akas_schema = t.StructType([t.StructField('titleId', t.StringType(), True),
                                  t.StructField('ordering', t.IntegerType(), True),
                                  t.StructField('title', t.StringType(), True),
                                  t.StructField('region', t.StringType(), True),
                                  t.StructField('language', t.StringType(), True),
                                  t.StructField('types', t.StringType(), True),
                                  t.StructField('attributes', t.StringType(), True),
                                  t.StructField('isOriginalTitle', t.BooleanType(), True)])

title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), True),
                                    t.StructField('titleType', t.StringType(), True),
                                    t.StructField('primaryTitle', t.StringType(), True),
                                    t.StructField('originalTitle', t.StringType(), True),
                                    t.StructField('isAdult', t.BooleanType(), True),
                                    t.StructField('startYear', t.DateType(), True),
                                    t.StructField('endYear', t.DateType(), True),
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
