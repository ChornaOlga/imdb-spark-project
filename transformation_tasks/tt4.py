import pyspark.sql.functions as f

import columns as c


def transformation_task_4(characters_df, titles_df, names_df):
    persons_in_titles = characters_df.select(f.col(c.tconst),
                                             f.col(c.nconst),
                                             f.col(c.characters))
    characters_in_titles = persons_in_titles.filter(f.col(c.characters).isNotNull())
    characters_with_full_titles_df = characters_in_titles.join(titles_df, on=c.tconst, how='left')
    characters_with_crop_titles_df = characters_with_full_titles_df.select(f.col(c.primaryTitle),
                                                                           f.col(c.nconst),
                                                                           f.col(c.characters))
    characters_and_titles_with_full_names_df = characters_with_crop_titles_df.join(names_df,
                                                                                   on=c.nconst,
                                                                                   how='left')
    characters_titles_names_df = characters_and_titles_with_full_names_df.select(f.col(c.primaryName),
                                                                                 f.col(c.primaryTitle),
                                                                                 f.col(c.characters))
    return characters_titles_names_df
