from pyspark.sql import SparkSession, DataFrame
from typing import Union
import psycopg2


__psql_connect__ = {}


def get_spark(psql_jar_path: str="./lib/postgresql-42.5.0.jar") -> SparkSession:
    """
    Args:
        psql_jar_path: str
            the path will be a little bit different when using with notebook
    """
    return (
        SparkSession.builder.appName("Python Spark SQL basic example")
        .config("spark.jars", psql_jar_path)
        .config("spark.executor.memory", "4G")
        .config("spark.driver.memory","18G")
        .config("spark.executor.cores","4")
        .config("spark.python.worker.memory","4G")
        .config("spark.driver.maxResultSize","6G")
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max","1024M")
        .getOrCreate()
    )


def query(
    spark: SparkSession,
    host: str,
    port: Union[str, int],
    user: str,
    password: str,
    database: str,
    table: str
) -> DataFrame:
    ### Remove prefix http and https
    __prefix = ['http://', 'https://']
    for p in __prefix:
        if host.startswith(p):
            host = host[len(p):]

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


def upload(
    df: DataFrame,
    host: str,
    port: Union[str, int],
    user: str,
    password: str,
    database: str,
    table: str,
    mode="append",
    **kwargs
):
    df = (
    df.write.format('jdbc')
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option('user', user)
        .option('password', password)
        .option('dbtable', table)
    )

    for k, v in kwargs.items():
        df = df.option(k, v)
    df.mode(mode).save()


def psql_connect(host, port, database, user, password, **kwargs):
    key = "#".join([str(s) for s in [host, port, database, user, password]])

    if __psql_connect__.get(key) is None:
        __psql_connect__[key] = psycopg2.connect(
            host=host, 
            port=port,
            database=database, 
            user=user, 
            password=password,
        )

    return __psql_connect__[key]
    

def get_max_height(cur, table):
    cur.execute(f"select max(block_height) from {table};")
    return cur.fetchall()[0][0]


def get_min_height(cur, table):
    cur.execute(f"select min(block_height) from {table};")
    return cur.fetchall()[0][0]

