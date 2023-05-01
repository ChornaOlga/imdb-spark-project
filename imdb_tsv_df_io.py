

def import_tsv_to_df(spark_session, tsv_path, df_schema):
    result_df = spark_session.read.csv(tsv_path,
                                       sep=r'\t',
                                       header=True,
                                       nullValue=r'\N',
                                       dateFormat='YYYY',
                                       schema=df_schema)
    return result_df


def export_df_to_csv(df_to_write, path_to_write):
    df_to_write.coalesce(1).write.csv(path_to_write, header=True, mode='overwrite')
