import os
import time
from labeling.etl_processor import ETLProcessor
from labeling.tools import get_spark, query, upload, get_max_height, psql_connect, to_parquet
from utils.logger import Logger


ckpt_logger = None
process_logger = Logger(f"output/log/uploading_at_{time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(time.time()))}.txt")


def log_ckpt(start, end):
    global ckpt_logger
    ckpt_logger.write(f"{start} -> {end}\n")


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


def parse_ckpt(ckpt_file):
    """
    first line is start-end blocks of full process
    """
    pc = open(ckpt_file, "r")
    _lines = pc.readlines()
    pc.close()

    ### Remove blank lines
    lines = []
    for line in _lines:
        line = line.split("\n")[0].strip()
        if len(line) > 0:
            lines.append(line)
    del _lines

    def parse_line(line: str):
        try:
            s, _, t = line.split()
            s, t = int(s), int(t)
        except:
            raise RuntimeError(f"Checkpoint file {ckpt_file} has wrong format")
        return s, t

    if len(lines) == 1:
        return parse_line(lines[0])

    idx = len(lines) - 1
    while idx >= 1:
        line = lines[idx].split("\n")[0].strip()
        if len(line) == 0:
            idx -= 1
            continue
        return parse_line(line)[1] + 1, parse_line(lines[0])[1]
    try:
        return parse_line(lines[0])
    except:
        return None, None


def get_batch_intervals(start_block, end_block, batch_size, vote_proposed_win_size, label_win_size):
    block_steps = 150
    assert (end_block - start_block) % block_steps == 0
    assert vote_proposed_win_size % block_steps == 0
    assert label_win_size % block_steps == 0
    assert (batch_size - 1) * block_steps >= vote_proposed_win_size + label_win_size - block_steps
    
    start_block = start_block + vote_proposed_win_size - block_steps
    intervals = []
    stop = False
    while start_block <= end_block and not stop:
        batch_start = start_block - vote_proposed_win_size + block_steps
        batch_end = batch_start + (batch_size - 1) * block_steps

        if batch_end >= end_block:
            batch_end = end_block
            stop = True

        assert batch_end >= batch_start
        assert (batch_end - batch_start) % block_steps == 0, f"Got {batch_start} -> {batch_end}"
        intervals.append((batch_start, batch_end))
        start_block = batch_end - label_win_size - block_steps
    return intervals


def main(config, start_block: int, end_block: int, checkpoint: str = None, show_intervals=True):
    """
    Args:
        checkpoint: checkpoint file path
    """
    ### Check window sizes condition
    vote_proposed_win_size = config.hp.etl.vote_proposed_win_size
    label_win_size = config.hp.etl.label_win_size
    batch_size = config.hp.upload.batch_size
    block_steps = 150

    max_height = get_max_height(psql_connect(**config.src).cursor(), config.src.table)
    assert max_height >= end_block, f"End block ({end_block}) excceed max block height ({max_height})"

    ### Load checkpoint
    global ckpt_logger
    if checkpoint is not None:
        s, e = parse_ckpt(checkpoint)
        if s is None:
            if start_block is None or end_block is None:
                raise ValueError(f"Start block, end block and checkpoint {checkpoint} has no information")
        else:
            start_block, end_block = s, e
    else:
        if start_block is None or end_block is None:
            raise ValueError("Must provide start-end blocks or checkpoint path")

        checkpoint = f"output/ckpt/ckpt_{start_block}_{end_block}.txt"
        if os.path.exists(checkpoint):
            # raise ValueError(f"Checkpoint {checkpoint} exists, please delete it")
            os.remove(checkpoint)
        ckpt_logger = open(checkpoint, "w")
        ckpt_logger.write(f"{start_block} -> {end_block}\n")
        ckpt_logger.close()

    ckpt_logger = open(checkpoint, "a")

    if hasattr(config.dest, "file"):
        if os.path.exists(config.dest.file):
            import shutil
            from labeling.tools import yes_no
            if yes_no("| Delete " + config.dest.file):
                shutil.rmtree(config.dest.file)
            elif yes_no("| Exit ??"):
                exit()

    ### Start uploading process
    process_logger.write("| Uploading process: from", start_block, "to", end_block)
    intervals = get_batch_intervals(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        vote_proposed_win_size=vote_proposed_win_size,
        label_win_size=label_win_size,
    )

    if show_intervals:
        print("| Intervals:")
        for idx, (batch_start, batch_end) in enumerate(intervals):
            print(idx, "-", batch_start, "->", batch_end)

    if len(intervals) == 0:
        raise ValueError("No interval found, please recheck arguments in config file and start-end blocks")

    spark = get_spark()
    import pyspark.sql.functions as F
    import numpy as np

    for idx, (batch_start, batch_end) in enumerate(intervals):
        __start_time = time.time()

        process_logger.write("| Start uploading from", batch_start, "to", batch_end, "| interval:", idx + 1, "/", len(intervals), "| batch size =", (batch_end - batch_start) / block_steps + 1)
        df = sampling(spark, config, batch_start, batch_end)
        
        process_logger.write("| Start ETL process")
        df = ETLProcessor.data_scoring(df, **config.hp.etl)
        bh = df.groupBy("block_height").count().select(F.col("block_height"))
        bb = bh.toPandas()
        bh = bb.to_numpy().reshape((-1,))
        bh.sort()
        print(bh[0], bh[-1])
        diff = np.diff(bh)
        for i, d in enumerate(diff):
            if d != block_steps:
                print(bh[i], bh[i + 1])
        
        process_logger.write("| Uploading data to database")
        uploading(df, config)

        process_logger.write("| Done")
        log_ckpt(batch_start, batch_end)

        __duration = time.time() - __start_time
        process_logger.write("| Duration:", __duration, "\n")


    process_logger.write("| Complete!")
    ckpt_logger.close()

    