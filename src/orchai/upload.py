import os
import time
import shutil
import numpy as np
import pyspark.sql.functions as F
from orchai.constants import Constants
from orchai.etl_processor import ETLProcessor
from orchai.tools import get_spark, query, upload, get_max_height, psql_connect, to_parquet, get_logger, yes_no


__logger__ = None

def print(*msg):
    global __logger__
    __logger__.write(*msg)


def sampling(spark, config, from_block: int, to_block: int):
    with open("config/validators.txt", 'r') as f:
        validators = f.read().split()
        
    table = f"""(
            select * from {config.src.table} 
            where block_height >= {from_block} and block_height <= {to_block}
                and operator_address in ({", ".join([f"'{v}'" for v in validators])})
            order by block_height 
        ) as t
    """

    df = query(
        spark,
        host=config.src.host,
        port=config.src.port,
        user=config.src.user,
        password=config.src.password,
        database=config.src.database,
        table=table
    )
    return df


def uploading(df, config):
    if hasattr(config.dest, "database"):
        upload(df, **config.dest)
    elif hasattr(config.dest, "file"):
        to_parquet(df, **config.dest)


def validating(df, last_end_block: int=None):
    block_heights = df.groupBy("block_height").count().select(F.col("block_height"))
    block_heights = block_heights.toPandas().to_numpy().reshape((-1,))
    block_heights.sort()
    print("start-end final blocks:", block_heights[0], block_heights[-1])

    if last_end_block is not None:
        if block_heights[0] - last_end_block != Constants.block_step:
            print("error block (last end):", last_end_block, block_heights[0], block_heights[0] - last_end_block)

    diff = np.diff(block_heights)
    for i, d in enumerate(diff):
        if d != Constants.block_step:
            print("error block:", block_heights[i], block_heights[i + 1], d)
    return block_heights[-1]
        

def get_batches(start_block, end_block, batch_size, vote_proposed_win_size, label_win_size):
    """
    sh |-------------------------------| st

         fh |------------------| ft
    
    sh: start head
    st: start tail
    fh: finish head
    ft: finish tail

    fh_0 = sh_0 + vote_propose_win_size - block_step
    ft_0 = st_0 - label_win_size

    fh_1 = ft_0 + block_step
    sh_1 = fh_1 - vote_propose_win_size + block_step
         = st_0 - label_win_size + block_step - vote_propose_win_size + block_step

    """
    block_step = Constants.block_step
    assert vote_proposed_win_size   % block_step == 0
    assert label_win_size           % block_step == 0
    total_blocks_per_batch          = (batch_size - 1) * block_step + 1
    total_blocks                    = end_block - start_block + 1
    assert total_blocks_per_batch   > vote_proposed_win_size - block_step + label_win_size
    assert total_blocks             > vote_proposed_win_size - block_step + label_win_size

    sh = start_block
    intervals = []
    while True:
        st = sh + (batch_size - 1) * block_step
        if st >= end_block:
            st = end_block
            num_blocks = st - sh + 1
            if num_blocks < vote_proposed_win_size - block_step + label_win_size:
                intervals[-1][1] = st
            else:
                intervals.append((sh, st))
            break
        intervals.append((sh, st))
        sh = st - label_win_size + block_step - vote_proposed_win_size + block_step
    return intervals


def run_uploading(config, start_block: int, end_block: int, spark=None, show_intervals=True, delete_old_file: bool=None, logger=None, skip_validate: bool=False):
    block_step             = Constants.block_step
    assert (end_block - start_block) % block_step == 0
    vote_proposed_win_size  = config.hp.etl.vote_proposed_win_size
    label_win_size          = config.hp.etl.label_win_size
    batch_size              = config.hp.upload.batch_size
    max_height              = get_max_height(psql_connect(**config.src).cursor(), config.src.table)
    assert max_height >= end_block, f"End block ({end_block}) excceed max block height ({max_height})"

    logger = get_logger("uploading") if logger is None else logger
    global __logger__
    __logger__ = logger

    if hasattr(config.dest, "file"):
        if os.path.exists(config.dest.file):
            if delete_old_file is None:
                if yes_no("| Delete " + config.dest.file):
                    shutil.rmtree(config.dest.file)
                elif yes_no("| Exit ??"):
                    exit()
            elif delete_old_file:
                shutil.rmtree(config.dest.file)

    ### Start uploading process
    batches = get_batches(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        vote_proposed_win_size=vote_proposed_win_size,
        label_win_size=label_win_size,
    )

    if show_intervals:
        print("| Intervals:")
        for idx, (start, end) in enumerate(batches):
            print(idx, "-", start, "->", end, ":", (end - start) / block_step + 1)

    if len(batches) == 0:
        raise ValueError("No interval found, please recheck arguments in config file and start-end blocks")

    spark = get_spark() if spark is None else spark
    last_end_block = None

    print("| Start")
    for idx, (start, end) in enumerate(batches):
        print(idx, "-", start, "->", end, ":", (end - start) / block_step + 1)
        time_start = time.time()

        df = sampling(spark, config, start, end)
        print("Successfully query data from database")
        
        df = ETLProcessor.data_scoring(df, **config.hp.etl)

        if not skip_validate:
            print("Start validating:")
            last_end_block = validating(df, last_end_block)

        print("uploading data ...")
        uploading(df, config)

        duration = time.time() - time_start
        print("End. Duration:", duration, "\n")

    