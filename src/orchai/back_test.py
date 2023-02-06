if __name__ == "__main__":
    import os, sys
    sys.path.append(os.getcwd())

from orchai.upload import get_spark
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


def create_APR(df: DataFrame, C_R_BASE):
    df = df.withColumn("1-c_r", (1 - F.col("commission_rate"))).orderBy("operator_address", "block_height")
    df = df.withColumn("APR", (1/(1-C_R_BASE) * (1- F.col("commission_rate"))))
    return df


def create_delta(df: DataFrame, block_num):
    w = Window.partitionBy('operator_address').orderBy('block_height').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = df.withColumn('subtract_de_token', F.when(
        (F.col("delegators_token")-F.first("delegators_token").over(w))>0 \
        ,F.col("delegators_token")-F.first("delegators_token").over(w))\
        .otherwise(0))
    
    df_delta = (
            df.groupBy("operator_address") \
                .agg({"subtract_de_token": "mean", \
                     "APR": "min"})
                .withColumnRenamed("avg(subtract_de_token)", "real_delta")\
                .withColumnRenamed("min(APR)", "APR")
    )
    df_delta = df_delta.withColumn("block_num", F.lit(block_num))
    
    return df_delta


def create_real_APR(real_df):
    df = real_df.withColumn("delta_APR_multiply", F.col("APR") * F.col("real_delta"))
    realAPR_df = df.groupBy("block_num") \
                .agg({"delta_APR_multiply": "sum", \
                       "real_delta": "sum"}) \
                .withColumnRenamed("sum(delta_APR_multiply)", "sum_delta_APR_multiply")\
                .withColumnRenamed("sum(real_delta)", "sum_real_delta")
    realAPR_df = realAPR_df.withColumn("real_APR", F.col("sum_delta_APR_multiply") / F.col("sum_real_delta"))
    return realAPR_df


def create_unreal_APR(predict_df, realAPRdf, block_num, col):
    
       df_percen = predict_df.withColumn("block_num", F.lit(block_num))
       df_percen = df_percen.join(realAPRdf, on="block_num", how="left")
       df_percen = df_percen.withColumn("unreal_delta", F.col(col) * F.col("sum_real_delta"))
       
       df_percen = df_percen.withColumn("unreal_delta_APR_multiply", F.col("APR") * F.col("unreal_delta"))

       unrealAPR_df = df_percen.groupBy("block_num") \
                     .agg({"unreal_delta_APR_multiply": "sum", \
                            "unreal_delta": "sum"}) \
                     .withColumnRenamed("sum(unreal_delta_APR_multiply)", "sum_unreal_delta_APR_multiply")\
                     .withColumnRenamed("sum(unreal_delta)", "sum_unreal_delta")
       unrealAPR_df = unrealAPR_df.withColumn("unreal_APR", F.col("sum_unreal_delta_APR_multiply") / F.col("sum_unreal_delta"))

       return unrealAPR_df


def join_df(real_df, predict_df):
    final_df = real_df.join(predict_df, on="block_num", how="right")
    final_df = final_df.drop("sum_delta_APR_multiply")
    final_df = final_df.drop("sum_real_delta")
    final_df = final_df.drop("sum_unreal_delta_APR_multiply")
    final_df = final_df.drop("sum_unreal_delta")
    return final_df


def back_test(path: str, C_R_BASE: int, start_block: int, end_block: int, step_block: int, timestamp_block: int, col: str, spark=None, save: bool=True):
    spark = get_spark() if spark is None else spark
    df = spark.read.parquet(path)

    for i in range(start_block, end_block+1, step_block):
        if i == start_block:
            APR_df = create_APR(df, C_R_BASE)
            real_df = APR_df.filter(F.col("block_height").between(i, i + timestamp_block))

            real_APR_df = create_delta(real_df, i)
            real_APR_df = create_real_APR(real_APR_df)

            predict_df = APR_df.filter(F.col("block_height") == i)
            unreal_APR_df = create_unreal_APR(predict_df, real_APR_df, i, col=col)
            final_df = join_df(real_APR_df, unreal_APR_df)
        else:
            APR_df = create_APR(df, C_R_BASE)
            real_df = APR_df.filter(F.col("block_height").between(i, i + timestamp_block))

            real_APR_df = create_delta(real_df, i)
            real_APR_df = create_real_APR(real_APR_df)

            predict_df = APR_df.filter(F.col("block_height") == i)
            unreal_APR_df = create_unreal_APR(predict_df, real_APR_df, i, col=col)
            merge_df = join_df(real_APR_df, unreal_APR_df)
            final_df = final_df.union(merge_df)
        
    final_df = final_df.withColumn("res", F.when(final_df.real_APR > final_df.unreal_APR, 0).otherwise(1))
    if save:
        final_df.write.parquet("data/backtest_data")

    truth = final_df.agg({"res" : "sum"}).collect()[0][0]
    df_cnt = final_df.count()

    return df_cnt


def get_back_test_result(spark):
    final_df = spark.read.parquet("data/backtest_data")
    if "res" not in final_df.columns:
        final_df = final_df.withColumn("res", F.when(final_df.real_APR > final_df.unreal_APR, 0).otherwise(1))
    truth = final_df.agg({"res" : "sum"}).collect()[0][0]
    return truth / final_df.count()


if __name__ == "__main__":
    print("Accuracy:", get_back_test_result(get_spark()))
