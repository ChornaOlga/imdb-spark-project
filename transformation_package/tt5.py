import pyspark.sql.functions as f

import columns as c


def transformation_task_5(titles_with_regions_df, isadult_df):
    """
    Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one.

    Args:
    - titles_with_regions_df: A PySpark DataFrame containing title information
      with regions.
    - isadult_df: A PySpark DataFrame containing information about whether a
      title is adult or not.

    Returns:
    - adult_titles_count_via_regions_df: A PySpark DataFrame containing
      information about the number of adult movies/series available per region,
      ordered in descending order of the number of adult movies/series available,
      and limited to the top 100 regions. It has the following columns:
        - region: The region for which the count is provided.
        - number_of_adult_movies_available: The number of adult movies/series available
          for the corresponding region.
    """
    isadult_titles_df = isadult_df.select(f.col(c.tconst), f.col(c.isAdult))
    adult_titles_df = isadult_titles_df.filter(f.col(c.isAdult) == 1)
    title_have_region = titles_with_regions_df.filter(f.col(c.region).isNotNull())
    adult_titles_with_full_regions_df = title_have_region.join(
        adult_titles_df,
        adult_titles_df[c.tconst] == title_have_region[c.titleId],
        how='inner')
    adult_titles_count_via_regions_df = (adult_titles_with_full_regions_df.groupBy(c.region)
                                                                          .count()
                                                                          .orderBy('count', ascending=False)
                                                                          .limit(100))
    adult_titles_count_via_regions_df = adult_titles_count_via_regions_df.withColumnRenamed(
        'count',
        'number_of_adult_movies_available')
    return adult_titles_count_via_regions_df
