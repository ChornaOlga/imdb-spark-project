from pyspark.sql import Window

import pyspark.sql.functions as f

import columns as c


def transformation_task_8(titles_with_genres_df, ratings_df, is_alternative=0):
    titles_with_genres_df = titles_with_genres_df.filter(f.col(c.titleType) != 'tvEpisode')
    titles_with_genres_df = titles_with_genres_df.withColumn(c.genres, f.split(f.col(c.genres), ','))
    titles_with_genres_df = titles_with_genres_df.withColumn(c.genres, f.explode(f.col(c.genres)))
    title_basics_crop_genres_df = titles_with_genres_df.select(f.col(c.tconst), f.col(c.primaryTitle), f.col(c.genres))
    title_genre_rating_df = title_basics_crop_genres_df.join(ratings_df, on=c.tconst, how='left')

    window = Window.orderBy(f.col(c.averageRating).desc(), f.col(c.numVotes).desc()).partitionBy(c.genres)
    alternative_window = Window.orderBy(f.col(c.numVotes).desc(), f.col(c.averageRating).desc()).partitionBy(c.genres)
    if is_alternative:
        window = alternative_window

    title_genre_rating_window_df = title_genre_rating_df.withColumn('top_num_in_genre', f.row_number().over(window))
    top_10_title_genre_rating_window_df = title_genre_rating_window_df.filter(f.col('top_num_in_genre') < 11)
    return top_10_title_genre_rating_window_df
