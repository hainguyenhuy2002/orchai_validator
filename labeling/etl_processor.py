import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame


class ETLProcessor(object):
    @staticmethod
    def data_scoring(
        df: DataFrame,
        accept_rate: float,
        concentration_level: float,
        vote_score: int,
        propose_score: int,
        A: int,
        B: int,
        C: int,
        D: int,
    ):
        """
        Args:
            df: DataFrame want to get label

            accept_rate: Hyperparameter to calculate comission score

            concentration_level: Hyperparameter to calculate self_bonded score

            vote_score: score for each vote (Hyperparameter to calculate vote_proposed_score)
            propose_score: score for each propose(Hyperparameter to calculate vote_proposed_score)

            A: voting_power_score weight
            B: comission_score weight
            C: self_bonded weight
            D: vote_proposed_score weight
        """
        df = ETLProcessor.preprocess(df)
       
        df = ETLProcessor.voting_power_score(df)
        print("------------------------------------------------")
        print("Successfully converted voting_power_score column")
        print("------------------------------------------------")

        df = ETLProcessor.commission_score(df, accept_rate)
        print("------------------------------------------------")
        print("Successfully converted commission_score column")
        print("------------------------------------------------")

        df = ETLProcessor.self_bonded_score(df, concentration_level)
        print("------------------------------------------------")
        print("Successfully converted self_bonded_score column")
        print("------------------------------------------------")

        df = ETLProcessor.vote_proposed_score(df, vote_score, propose_score)
        print("------------------------------------------------")
        print("Successfully converted vote_propose_score column")
        print("------------------------------------------------")

        df = ETLProcessor.final_score(df, A, B, C, D)
        print("------------------------------------------------")
        print("Sucessfully converted final_score")
        print("------------------------------------------------")

        return df

    @staticmethod
    def preprocess(df: DataFrame):
        ### Mapping False:0, True: 1
        for c in ["jailed", "vote", "propose"]:
            df = df.withColumn(c, F.col(c).cast("integer"))

        # FillNan for vote and propose
        df = df.withColumn("vote", F.coalesce(F.col("vote"), F.lit(0)))
        df = df.withColumn("propose", F.coalesce(F.col("propose"), F.lit(0)))

        ### Number of validator in each block
        num_val_blocks_df = (
            df.groupBy("block_height")
            .agg({"operator_address": "count"})
            .withColumnRenamed("count(operator_address)", "validators_count_per_block")
        )
        df = df.join(num_val_blocks_df, on="block_height", how="left")

        ### Number of token in each block
        token_block_df = (
            df.groupBy("block_height").agg({"tokens": "sum"}).withColumnRenamed("sum(tokens)", "total_token_amt_per_block")
        )
        df = df.join(token_block_df, on="block_height", how="left")

        ### Number of self bonded in each block
        self_bond_block_df = (
            df.groupBy("block_height")
            .agg({"self_bonded": "sum"})
            .withColumnRenamed("sum(self_bonded)", "total_self_bonded_amt_per_block")
        )
        df = df.join(self_bond_block_df, on="block_height", how="left")

        return df

    @staticmethod
    def voting_power_score(df: DataFrame):
        df = df.withColumn("tokens_proportion", df.tokens / df.total_token_amt_per_block)
        df = df.withColumn("mean_percentage", 1 / df.validators_count_per_block)
        df = df.withColumn("voting_power_score", 1 - df.tokens_proportion / df.mean_percentage / 2)
        df = df.withColumn(
            "voting_power_score", F.when(df.tokens_proportion > 2 * df.mean_percentage, 0).otherwise(df.voting_power_score)
        )
        df = df.drop("mean_percentage")

        return df

    @staticmethod
    def commission_score(df: DataFrame, accept_rate: float):
        df = df.withColumn("comission_score", 1 - df.commission_rate / accept_rate)
        df = df.withColumn("comission_score", F.when(df.commission_rate > accept_rate, 0).otherwise(df.comission_score))

        return df

    @staticmethod
    def self_bonded_score(df: DataFrame, concentration_level: float):
        df = df.withColumn(
            "self_bonded_score",
            df.self_bonded - df.total_self_bonded_amt_per_block / df.validators_count_per_block / concentration_level,
        )

        df = df.withColumn("self_bonded_score", F.when(df.self_bonded_score < 0, 0).otherwise(df.self_bonded_score))

        max_self_bonded_score_df = (
            df.groupBy("block_height")
            .agg({"self_bonded_score": "max"})
            .withColumnRenamed("max(self_bonded_score)", "max_self_bonded_score")
        )

        df = df.join(max_self_bonded_score_df, on="block_height", how="left")
        df = df.withColumn("self_bonded_score", df.self_bonded_score / df.max_self_bonded_score)
        df = df.drop("max_self_bonded_score")

        return df

    @staticmethod
    def vote_proposed_score(df: DataFrame, vote_score: int, propose_score: int):
        df = df.withColumn("vote_propose_point", vote_score * df.vote + propose_score * df.propose)

        vote_propose_score_df = df.select("operator_address", "block_height", "vote_propose_point").orderBy(
            "operator_address", "block_height"
        )

        cummulative_window = Window.partitionBy("operator_address").orderBy("block_height").rangeBetween(-599, 0)
        ### Window for moving average step
        ### Step now row and last 599 row

        lag_window = Window.partitionBy("operator_address").orderBy("block_height")
        ### Window for lag to check the null

        vote_propose_score_df = vote_propose_score_df.withColumn(
            ### Note that lag requires both column and lag amount to be specified
            ### It is possible to lag a column which was not the orderBy column
            "vote_propose_score",
            F.when(
                F.lag("vote_propose_point", 599).over(lag_window).isNotNull(), F.sum("vote_propose_point").over(cummulative_window)
            ),
        )

        df = df.join(vote_propose_score_df, on=["block_height", "operator_address"], how="left").orderBy("block_height")
        df = df.drop("vote_propose_point")
        df = df.na.drop()

        max_vote_propose_score_df = (
            df.groupBy("block_height")
            .agg({"vote_propose_score": "max"})
            .withColumnRenamed("max(vote_propose_score)", "max_vote_propose_score")
        )

        df = df.join(max_vote_propose_score_df, on="block_height", how="left").orderBy("block_height")
        df = df.withColumn("vote_propose_score", df.vote_propose_score / df.max_vote_propose_score)
        df = df.drop("max_vote_propose_score")

        return df

    @staticmethod
    def final_score(df: DataFrame, A: int, B: int, C: int, D: int):
        df = df.withColumn(
            "score", A * df.voting_power_score + B * df.comission_score + C * df.self_bonded_score + D * df.vote_propose_score
        )

        ### Jailed = True -> score = 0
        ### Unbonded -> score = 0

        df = df.withColumn("score", F.when(df.jailed == 1, 0).otherwise(df.score))
        df = df.withColumn("score", F.when(F.col("status").rlike("UNBONDED"), 0).otherwise(df.score))

        return df
