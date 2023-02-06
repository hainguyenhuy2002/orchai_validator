from pyspark.sql import SparkSession, DataFrame
from typing import Union
import psycopg2
import os
from datetime import datetime as dt


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


def to_parquet(df: DataFrame, file, mode):
    df.write.mode(mode).parquet(file)


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


def yes_no(msg: str):
    print(msg, "([y]es / [n]o)")
    while True:
        ans = input()
        if ans.lower() == 'n':
            return False
        if ans.lower() == 'y':
            return True
        

class Logger:
    def __init__(self, file_path: str) -> None:
        file_path = os.path.abspath(file_path)
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        self.file_path = file_path

    def write(self, *msg, end="\n", sep=" "):
        print(*msg, end=end, sep=sep)
        file = open(file=self.file_path, mode="a", encoding="utf-8")
        print(*msg, end=end, sep=sep, file=file)
        file.close()
        del file


def get_logger(name: str, log_dir="output/logs"):
    name = name + "-" + dt.now().strftime('%Y-%m-%d_%H-%M-%S') + ".txt"
    return Logger(file_path=os.path.join(log_dir, name))