import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame
from orchai.constants import Constants


def validating(df):
    import numpy as np
    block_heights = df.groupBy("block_height").count().select(F.col("block_height"))
    block_heights = block_heights.toPandas().to_numpy().reshape((-1,))
    block_heights.sort()
    print("start-end final blocks:", block_heights[0], block_heights[-1])

    diff = np.diff(block_heights)
    for i, d in enumerate(diff):
        if d != 150:
            print("error block:", block_heights[i], block_heights[i + 1], d)


class ETLProcessor(object):
    @staticmethod
    def data_scoring(
        df: DataFrame,
        accept_rate: float,
        concentration_level: float,
        vote_proposed_win_size: int,
        label_win_size: int,
        B: int = None,
        A: int = None,
        C: int = None,
        D: int = None,
        cal_score: bool = True
    ):
        """
        Args
        ----
            df: DataFrame want to get label

            accept_rate: Hyperparameter to calculate comission score

            concentration_level: Hyperparameter to calculate self_bonded score

            vote_score: score for each vote (Hyperparameter to calculate vote_proposed_score)
            propose_score: score for each propose(Hyperparameter to calculate vote_proposed_score)

            A: voting_power_score weight
            B: commission_score weight
            C: self_bonded weight
            D: vote_proposed_score weight

            start_block, end_block, top_validators: I use these three parameters to filter the whole data with specific number of top validators. To choose the top validators, I make a survey on a small batch of data, calculate and then rank them in that small data.
            
            vote_proposed_win_size: number of previous blocks that calculate the vote_proposed_score(
                I calculate vote_proposed_score in a range of blocks rather than all blocks.
                Therefore, I take the mean of each validators' vote_propose_score in "vote_proposed_win_size" blocks.

            )
            
            label_win_size: number of blocks to shift(
                In here, I get a number of future blocks, take its mean and calculate my labels
            )

        Return
        ------
            return shape of data frame:

        """
        if cal_score:
            if A is None or B is None or C is None or D is None:
                raise ValueError("A, B, C, D must not be None")

        df = ETLProcessor.preprocess(df)
        print("------------------------------------------------")
       
        df = ETLProcessor.vote_score(df, vote_proposed_win_size)
        print("Successfully converted vote_propose_score column")
        # validating(df)
        print("------------------------------------------------")

        df = ETLProcessor.voting_power_score(df)
        print("Successfully converted voting_power_score column")
        # validating(df)
        print("------------------------------------------------")

        df = ETLProcessor.commission_score(df, accept_rate)
        print("Successfully converted commission_score column")
        # validating(df)
        print("------------------------------------------------")

        df = ETLProcessor.self_bonded_score(df, concentration_level)
        print("Successfully converted self_bonded_score column")
        # validating(df)
        print("------------------------------------------------")

        if cal_score:
            df = ETLProcessor.final_score(df, A, B, C, D)
            print("Sucessfully converted final_score")
            # validating(df)
            print("------------------------------------------------")

            df = ETLProcessor.shifting_data(df, label_win_size)
            print("Sucessfully shifting data")
            # validating(df)
            print("------------------------------------------------")
        else:
            print("Skip calculating score")
            print("------------------------------------------------")

        df = ETLProcessor.postprocess(df)
        
        return df

    @staticmethod
    def preprocess(df: DataFrame):
        df = df.withColumn("commission_rate",   (F.col("commission_rate") / 10**18))
        df = df.withColumn("self_bonded",       (F.col("self_bonded") / 10**18))
        df = df.withColumn("delegators_token",  (df.tokens - df.self_bonded))
        df = df.drop("delegator_shares")
        
        ### Mapping False: 0, True: 1
        for c in ["jailed", "vote", "propose"]:
            df = df.withColumn(c, F.col(c).cast("integer"))

        ### Fill Nan for vote and propose
        df = df.withColumn("vote", F.coalesce(F.col("vote"), F.lit(0)))
        df = df.withColumn("propose", F.coalesce(F.col("propose"), F.lit(0)))

        ### Number of validator in each block
        num_val_blocks_df = (
            df.groupBy("block_height")
              .agg({"operator_address": "count"})
              .withColumnRenamed("count(operator_address)", "validators_count_per_block")
        )
        df = df.join(num_val_blocks_df, on="block_height", how="left")

        ### Total tokens in each block
        token_block_df = (
            df.groupBy("block_height")
              .agg({"tokens": "sum"})
              .withColumnRenamed("sum(tokens)", "total_token_amt_per_block")
        )
        df = df.join(token_block_df, on="block_height", how="left")

        ### Total self bonded tokens in each block
        self_bond_block_df = (
            df.groupBy("block_height")
              .agg({"self_bonded": "sum"})
              .withColumnRenamed("sum(self_bonded)", "total_self_bonded_amt_per_block")
        )
        df = df.join(self_bond_block_df, on="block_height", how="left")

        return df

    @staticmethod
    def voting_power_score(df: DataFrame):
        ### Max tokens
        max_token_df = (
            df.groupBy("block_height")
              .agg({"tokens": "max"})
              .withColumnRenamed("max(tokens)", "max_tokens_per_block")
        )
        df = df.join(max_token_df, on="block_height", how="left")

        df = df.withColumn("voting_power_score", 1 - df.tokens / df.max_tokens_per_block)
        df = df.withColumn(
            "voting_power_score", 
            F.when(df.tokens / df.total_token_amt_per_block > (2 / df.validators_count_per_block), 0)
             .otherwise(df.voting_power_score)
        )

        df = df.drop("max_tokens_per_block")

        return df

    @staticmethod
    def commission_score(df: DataFrame, accept_rate: float):
        df = df.withColumn("commission_score", 1 - df.commission_rate / accept_rate)
        df = df.withColumn("commission_score", F.when(df.commission_rate > accept_rate, 0).otherwise(df.commission_score))

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
        df = df.withColumn("self_bonded_score", 
                            F.when(df.max_self_bonded_score == 0, 0)
                             .otherwise(df.self_bonded_score / df.max_self_bonded_score))
        df = df.drop("max_self_bonded_score")

        return df

    @staticmethod
    def vote_score(df: DataFrame, vote_proposed_win_size: int):
        assert vote_proposed_win_size % Constants.block_step == 0, "150 must be divisible by vote_proposed_win_size"
        size = vote_proposed_win_size // Constants.block_step - 1

        ### Window for moving average step
        ### Step now row and last "size" row
        cummulative_window = Window.partitionBy("operator_address").orderBy("block_height").rangeBetween(-size, 0)

        ### Window for lag to check the null
        lag_window = Window.partitionBy("operator_address").orderBy("block_height")

        df = df.withColumn(
            ### Using lag to get the null - rows that do not change
            "vote_score",
            F.when(
                F.lag("vote", size).over(lag_window).isNotNull(), F.sum("vote").over(cummulative_window)
            ),
        )

        df = df.na.drop()

        max_vote_score_df = (
            df.groupBy("block_height")
            .agg({"vote_score": "max"})
            .withColumnRenamed("max(vote_score)", "max_vote_score")
        )

        df = df.join(max_vote_score_df, on="block_height", how="left").orderBy("block_height")
        df = df.withColumn("vote_score", df.vote_score / df.max_vote_score)

        df = df.drop("max_vote_score")
        df = df.drop("vote")
        df = df.drop("propose")

        return df

    @staticmethod
    def final_score(df: DataFrame, A: int, B: int, C: int, D: int):
        df = df.withColumn(
            "score", A * df.voting_power_score + B * df.commission_score + C * df.self_bonded_score #+ D * df.vote_score
        )

        ### Jailed = True -> score = 0
        ### Unbonded -> score = 0

        df = df.withColumn("score", F.when(df.jailed == 1, 0).otherwise(df.score))
        df = df.withColumn("score", F.when(F.col("status").rlike("UNBONDED"), 0).otherwise(df.score))

        return df     

    @staticmethod
    def shifting_data(df: DataFrame, label_win_size: int):
        assert label_win_size % 150 == 0, "150 must be divisible by label_win_size"
        size = int(label_win_size // 150) # As we'll shift data after combining the data
        
        ### window is used for shifting "size" blocks and calculate the mean
        window = Window.partitionBy("operator_address").orderBy("block_height").rangeBetween(0, size)
        
        ### lag_window is used for getting the null blocks - the last blocks that do not change
        lag_window = Window.partitionBy("operator_address").orderBy("block_height")

        df = df.withColumn(
            "label", 
            F.when(F.lag("score", -size).over(lag_window).isNotNull(), F.mean("score").over(window))
        ).orderBy("block_height")

        df = df.na.drop()

        return df

    @staticmethod
    def postprocess(df: DataFrame):
        df = df.drop(
            "jailed", 
            "status",
            "validators_count_per_block",
            "total_token_amt_per_block",
            "total_self_bonded_amt_per_block"
        )

        return df

