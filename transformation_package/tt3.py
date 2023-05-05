import pyspark.sql.functions as f

import columns as c


def transformation_task_3(df_to_transform):
    """
    Filters a PySpark DataFrame to select only movies with a runtime greater than 120 minutes.

    Args:
        df_to_transform: A PySpark DataFrame with information about movies/series and it`s runtime.

    Returns:
        A PySpark DataFrame containing only the movies whose runtime is greater than 120 minutes.
    """
    title_with_runtime_df = df_to_transform.select(f.col(c.primaryTitle))
    movies_with_runtime_df = title_with_runtime_df.filter(f.col(c.titleType) == 'movie')
    movies_with_runtime_over2h_df = movies_with_runtime_df.filter(f.col(c.runtimeMinutes) > 120)
    return movies_with_runtime_over2h_df
