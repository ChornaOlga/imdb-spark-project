import pyspark.sql.functions as f

import columns as c


def transformation_task_1(df_to_transform):
    """
    Filters the input DataFrame based on the 'region' column and returns a new DataFrame containing
    only the rows with 'region' equal to 'UA'.

    Args:
        df_to_transform: A PySpark DataFrame to transform. It should contain a column named 'region'.

    Returns:
        A new PySpark DataFrame containing only the rows where 'region' is equal to 'UA'.
    """
    titles_df = df_to_transform.select(f.col(c.title))
    ua_titles_df = titles_df.filter(f.col(c.region) == 'UA')
    return ua_titles_df
