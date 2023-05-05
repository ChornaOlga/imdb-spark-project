def import_tsv_to_df(spark_session, tsv_path, df_schema):
    """
    Imports a tab-separated value (TSV) file as a Spark DataFrame using the specified schema.

    Args:
        spark_session (pyspark.sql.session.SparkSession): The SparkSession instance to use for reading the TSV file.
        tsv_path (str): The file path of the TSV file to import.
        df_schema (pyspark.sql.types.StructType): The schema to use for parsing the TSV file.

    Returns:
        pyspark.sql.dataframe.DataFrame: The resulting DataFrame parsed from the TSV file.

    Example:
        # Define the schema for the TSV file
        tsv_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

        # Import the TSV file as a DataFrame
        spark = SparkSession.builder.appName("ImportTSVTest").getOrCreate()
        tsv_path = "path/to/my/tsv_file.tsv"
        df = import_tsv_to_df(spark, tsv_path, tsv_schema)
    """
    result_df = spark_session.read.csv(tsv_path,
                                       sep=r'\t',
                                       header=True,
                                       nullValue=r'\N',
                                       schema=df_schema)
    return result_df


def export_df_to_csv(df_to_write, path_to_write):
    """
    Writes a Spark DataFrame to a CSV file.

    Args:
        df_to_write (pyspark.sql.dataframe.DataFrame): The DataFrame to write to a CSV file.
        path_to_write (str): The file path where the CSV file should be written.

    Returns:
        None

    Example:
        # Define a DataFrame to write to a CSV file
        data = [("John", "Doe", 25), ("Jane", "Doe", 30), ("Bob", "Smith", 40)]
        schema = StructType([
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)

        # Export the DataFrame to a CSV file
        csv_path = "path/to/my/csv_file.csv"
        export_df_to_csv(df, csv_path)
    """
    df_to_write.coalesce(1).write.csv(path_to_write, header=True, mode='overwrite')
