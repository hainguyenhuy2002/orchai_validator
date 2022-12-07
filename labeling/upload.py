import os
import time
from labeling.etl_processor import ETLProcessor
from labeling.tools import get_spark, query, upload


def sampling(spark, config, from_block: int, to_block: int):
    cfg_src = {**config.src}
    cfg_src['table'] = f"""(
            select * from {cfg_src['table']} 
            where block_height >= {from_block} 
                and block_height <= {to_block}
            order by block_height 
        ) as t
    """

    df = query(spark, **cfg_src)
    return df


def uploading(df, config):
    df = df.drop('jailed', 'status', 'tokens', 'commission_rate', 'delegator_shares', 'self_bonded', 'propose', 'vote')
    upload(df, **config.dest)


def parse_ckpt(ckpt_file):
    """
    first line is start-end blocks of full process
    """
    pc = open(ckpt_file, 'r')
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


def main(config, start_block: int, end_block: int, checkpoint: str=None):
    """
    Args:
        checkpoint: checkpoint file path
    """
    window_size = config.hp.window_size
    batch_size   = config.hp.batch_size
    assert batch_size >= window_size

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

        checkpoint = f'ckpt_{start_block}_{end_block}.txt'
        if os.path.exists(checkpoint):
            raise ValueError(f"Checkpoint {checkpoint} exists, please delete it")
        ckpt_logger = open(checkpoint, 'w')
        ckpt_logger.write(f"{start_block} -> {end_block}")
        ckpt_logger.close()

    ckpt_logger = open(checkpoint, 'a')
    def log_ckpt(start, end):
        ckpt_logger.write(f"{start} -> {end}\n")

    print("Uploading process: from", start_block, "to", end_block, "with window size", window_size)

    start_block += window_size - 1
    spark = get_spark()

    while start_block <= end_block:
        __start_time = time.time()

        from_block = start_block - window_size + 1
        to_block = min(start_block + batch_size - 1, end_block)
        print("Start uploading from", start_block, "to", to_block, "/", end_block)

        df = sampling(spark, config, from_block, to_block)
        print("Start ETL processing")
        df = ETLProcessor.data_scoring(df, **config.hp)

        print("Uploading data to database")
        uploading(df, config)
        
        log_ckpt(start_block, to_block)
        start_block = to_block + 1

        __duration = time.time() - __start_time
        print("Duration:", __duration)

    print("Complete!")
    spark.stop()
    ckpt_logger.close()