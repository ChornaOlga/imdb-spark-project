import pyspark.sql.functions as f

import columns as c


def transformation_task_2(df_to_transform):
    """
    This function takes a PySpark DataFrame as input and returns a new DataFrame containing only the names
    of persons born in the 20th century (between 1900 and 1999), which are in the column 'primaryName' of
    the input DataFrame.

    Args:
        df_to_transform (pyspark.sql.dataframe.DataFrame): A PySpark DataFrame containing data about persons,
            with at least the following columns:
                         - primaryName: The name of a person.
                         - birthYear: The year in which the person was born, in the format 'YYYY', or '\\N'
                                      if unknown.

    Returns:
        pyspark.sql.dataframe.DataFrame: A new PySpark DataFrame containing only the names of persons born in the
        20th century (between 1900 and 1999), which are in the column 'primaryName' of the input DataFrame.
    """
    persons_names_df = df_to_transform.select(f.col(c.primaryName))
    persons_born_19_df = persons_names_df.filter(f.col(c.birthYear)
                                                  .substr(startPos=0, length=2) == '19')
    return persons_born_19_df
