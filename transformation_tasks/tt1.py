import pyspark.sql.functions as f

import columns as c


def transformation_task_1(df_to_transform):
    titles_df = df_to_transform.select(f.col(c.title))
    ua_titles_df = titles_df.filter(f.col(c.region) == 'UA')
    return ua_titles_df
