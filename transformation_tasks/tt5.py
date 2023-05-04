import pyspark.sql.functions as f

import columns as c


def transformation_task_5(titles_with_regions_df, isadult_df):
    isadult_titles_df = isadult_df.select(f.col(c.tconst), f.col(c.isAdult))
    adult_titles_df = isadult_titles_df.filter(f.col(c.isAdult) == 1)
    title_have_region = titles_with_regions_df.filter(f.col(c.region).isNotNull())
    adult_titles_with_full_regions_df = title_have_region.join(adult_titles_df,
                                                               adult_titles_df[c.tconst] == title_have_region[
                                                                   c.titleId],
                                                               how='inner')
    adult_titles_count_via_regions_df = (adult_titles_with_full_regions_df.groupBy(c.region)
                                                                          .count()
                                                                          .orderBy('count', ascending=False)
                                                                          .limit(100))
    adult_titles_count_via_regions_df = adult_titles_count_via_regions_df.withColumnRenamed('count',
                                                                                            'number_of_adult_movies_available')
    return adult_titles_count_via_regions_df
