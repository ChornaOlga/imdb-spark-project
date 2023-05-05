from pyspark.sql import Window

import pyspark.sql.functions as f

import columns as c


def transformation_task_7(titles_with_years_df, ratings_df, is_alternative=0):
    """
    Get 10 titles of the most popular movies/series etc. by each decade.

    Args:
        titles_with_years_df (DataFrame): A PySpark DataFrame with title data including year of release.
        ratings_df (DataFrame): A PySpark DataFrame with rating data.
        is_alternative (int, optional): If set to 1, uses alternative ordering based only on number of votes.
                                        Defaults to 0.

    Returns:
        A PySpark DataFrame with the top 10 titles of the most popular movies/series etc. by each decade.
    """
    title_basics_df = titles_with_years_df.withColumn(c.startYear, f.col(c.startYear).cast('int'))
    title_year_without_episodes_df = title_basics_df.filter(f.col(c.titleType) != 'tvEpisode')
    title_year_crop_df = title_year_without_episodes_df.select(
        f.col(c.tconst),
        f.col(c.primaryTitle),
        f.col(c.startYear))
    title_year_check_crop_df = title_year_crop_df.filter(f.col(c.startYear).isNotNull())
    title_year_rating_df = title_year_check_crop_df.join(ratings_df, on=c.tconst, how='inner')
    title_year_rating_with_decade_df = title_year_rating_df.withColumn(
        'decade', (f.col(c.startYear) / 10).cast('int'))
    title_rating_with_decade_df = title_year_rating_with_decade_df.drop(f.col(c.startYear), f.col(c.tconst))
    title_rating_with_decade_df = title_rating_with_decade_df.withColumn('decade', f.col('decade') * 10)

    window = Window.orderBy(f.col(c.averageRating).desc(), f.col(c.numVotes).desc()).partitionBy('decade')
    alternative_window = (Window.orderBy(f.col(c.numVotes).desc(), f.col(c.averageRating).desc())
                                .partitionBy('decade'))
    if is_alternative:
        window = alternative_window

    title_rating_window_decade_df = title_rating_with_decade_df.withColumn(
        'top_num_in_decade', f.row_number().over(window))
    top_10_title_rating_window_decade_df = title_rating_window_decade_df.filter(f.col('top_num_in_decade') < 11)
    return top_10_title_rating_window_decade_df
