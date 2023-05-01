import pyspark.sql.functions as f

import columns as c


def transformation_task_2(df_to_transform):
    persons_names_df = df_to_transform.select(f.col(c.primaryName))
    persons_born_19_df = persons_names_df.filter(f.col(c.birthYear)
                                                 .substr(startPos=0, length=2) == '19')
    return persons_born_19_df
