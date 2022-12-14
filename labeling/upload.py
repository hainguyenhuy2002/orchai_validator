import os
import time
from labeling.etl_processor import ETLProcessor
from labeling.tools import get_spark, query, upload, get_min_height, get_max_height, psql_connect
from utils.logger import Logger


ckpt_logger = None
process_logger = Logger(f"output/log/uploading_at_{time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(time.time()))}.txt")


def log_ckpt(start, end):
    global ckpt_logger
    ckpt_logger.write(f"{start} -> {end}\n")


def sampling(spark, config, from_block: int, to_block: int):
    table = f"""(
            select * from {config.src.table} 
            where block_height >= {from_block} 
                and block_height <= {to_block}
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
    # df = df.drop('jailed', 'status', 'tokens', 'commission_rate', 'delegator_shares', 'self_bonded', 'propose', 'vote')
    upload(df, **config.dest)


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


def get_batch_intervals(start_block, global_end_block, batch_size, vote_proposed_win_size, combine_win_size, label_win_size, min_block):
    assert batch_size >= vote_proposed_win_size + combine_win_size + label_win_size
    assert label_win_size % combine_win_size == 0
    assert (batch_size - vote_proposed_win_size + 1) % combine_win_size == 0

    intervals = []
    while start_block <= global_end_block:
        batch_start = max(min_block, start_block - vote_proposed_win_size + 1)
        batch_end = batch_start + batch_size - 1
        
        if batch_end > global_end_block:
            batch_size = global_end_block - batch_start + 1
            batch_size = (batch_size - vote_proposed_win_size + 1) // combine_win_size * combine_win_size + vote_proposed_win_size - 1
            batch_end = batch_start + batch_size - 1
            
            if batch_start > batch_end:
                break
            
            if batch_size < vote_proposed_win_size + combine_win_size + label_win_size:
                break

        intervals.append((batch_start, batch_end))
        start_block = batch_end - label_win_size + 1
    
    return intervals


def main(config, start_block: int, end_block: int, checkpoint: str = None):
    """
    Args:
        checkpoint: checkpoint file path
    """
    ### Check window sizes condiction
    vote_proposed_win_size = config.hp.etl.vote_proposed_win_size
    combine_win_size = config.hp.etl.combine_win_size
    label_win_size = config.hp.etl.label_win_size
    batch_size = config.hp.upload.batch_size

    assert batch_size >= vote_proposed_win_size + combine_win_size + label_win_size
    assert label_win_size % combine_win_size == 0
    assert (batch_size - vote_proposed_win_size + 1) % combine_win_size == 0

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
            raise ValueError(f"Checkpoint {checkpoint} exists, please delete it")
        ckpt_logger = open(checkpoint, "w")
        ckpt_logger.write(f"{start_block} -> {end_block}\n")
        ckpt_logger.close()

    ckpt_logger = open(checkpoint, "a")

    ### Start uploading process
    process_logger.write("Uploading process: from", start_block, "to", end_block)
    min_block = get_min_height(psql_connect(**config.src).cursor(), config.src.table)
    spark = get_spark()

    intervals = get_batch_intervals(
        start_block=start_block,
        global_end_block=end_block,
        batch_size=batch_size,
        vote_proposed_win_size=vote_proposed_win_size,
        combine_win_size=combine_win_size,
        label_win_size=label_win_size,
        min_block=min_block)

    for idx, (batch_start, batch_end) in enumerate(intervals):
        __start_time = time.time()

        process_logger.write("Start uploading from", batch_start, "to", batch_end, "| interval:", idx + 1, "/", len(intervals), "| batch size =", batch_size)
        df = sampling(spark, config, batch_start, batch_end)
        
        process_logger.write("Start ETL processing")
        df = ETLProcessor.data_scoring(df, **config.hp.etl)
        
        process_logger.write("Uploading data to database")
        uploading(df, config)

        process_logger.write("Done")
        log_ckpt(batch_start, batch_end)

        __duration = time.time() - __start_time
        process_logger.write("Duration:", __duration, "\n")

    process_logger.write("Complete!")
    ckpt_logger.close()

    