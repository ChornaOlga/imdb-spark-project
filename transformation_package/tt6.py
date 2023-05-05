import pyspark.sql.functions as f

import columns as c


def transformation_task_6(title_episode_df, title_basics_df):
    """
    Get information about how many episodes in each TV Series. Get the top
    50 of them starting from the TV Series with the biggest quantity of episodes.

    Args:
        title_episode_df: A PySpark DataFrame containing the information about episodes of TV series.
        title_basics_df: A PySpark DataFrame containing the information about titles, including the TV series.

    Returns:
        A PySpark DataFrame containing the information about top 50 TV series with the biggest number of episodes.
        The DataFrame contains two columns: 'tvseries_title' and 'number_of_episodes', ordered by the number
        of episodes in descending order.
    """
    tvseries_by_number_of_episodes_df = (title_episode_df.groupBy(f.col(c.parentTconst))
                                                         .count()
                                                         .orderBy('count', ascending=False)
                                                         .limit(50))
    titles_crop_df = title_basics_df.select(f.col(c.tconst), f.col(c.primaryTitle))
    tvseries_by_number_of_episodes_with_titles_df = tvseries_by_number_of_episodes_df.join(
        titles_crop_df,
        tvseries_by_number_of_episodes_df[c.parentTconst] == titles_crop_df[c.tconst],
        how='inner')
    rename_tvseries_with_episodes_number = tvseries_by_number_of_episodes_with_titles_df.select(
        f.col(c.primaryTitle).alias('tvseries_title'),
        f.col('count').alias('number_of_episodes'))
    order_tvseries_by_number_of_episodes_with_titles_df = rename_tvseries_with_episodes_number.orderBy(
        'number_of_episodes', ascending=False)

    return order_tvseries_by_number_of_episodes_with_titles_df
