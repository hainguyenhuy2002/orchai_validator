from pyspark.sql import SparkSession, DataFrame
from typing import Union


def get_spark(psql_jar_path: str="./lib/postgresql-42.5.0.jar") -> SparkSession:
    """
    Args:
        psql_jar_path: str
            the path will be a little bit different when using with notebook
    """
    return (
        SparkSession.builder.appName("Python Spark SQL basic example")
        .config("spark.jars", psql_jar_path)
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )


def query(
    spark: SparkSession,
    host: str,
    port: Union[str, int],
    user: str,
    password: str,
    database: str,
    table: str,
    schema: str=None,
) -> DataFrame:
    ### Remove prefix http and https
    __prefix = ['http://', 'https://']
    for p in __prefix:
        if host.startswith(p):
            host = host[len(p):]

    if schema is not None:
        table = f"{schema}.{table}"

    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option("user", user)
        .option("password", password)
        .option("customSchema", "tokens decimal(38,0), delegator_shares decimal(38,0), self_bonded decimal(38,0)")
        .option("dbtable", table)
        .load()
    )

    print("Successfully queried data from database")
    return df



