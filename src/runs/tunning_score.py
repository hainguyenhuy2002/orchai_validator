import pandas as pd


def cal_score(row: pd.Series, A, B, C, D):
    row["score"] = (
        A * row["voting_power_score"] 
        + B * row["commission_score"]
        + C * row["self_bonded_score"]
        + D * row["vote_score"]
    )
    return row


def clone(filepath: str, A, B, C, D):
    df = pd.read_parquet(filepath)
    df = df.apply(cal_score, axis=1, A=A, B=B, C=C, D=D)
    return df



def get_params(param_grid: dict, keys: list=None, params_list: list=None):
    if params_list is None:
        params_list = {}
    
    if keys is None:
        keys = []

    if len(param_grid) == len(keys):
        global ITER
        global MIN_ITER
        ITER += 1
        min_iter = MIN_ITER if MIN_ITER is not None else ITER - 1
        max_iter = MAX_ITER if MAX_ITER is not None else ITER + 1
        if min_iter <= ITER <= max_iter:
            yield params
    else:
        for k, v in param_grid.items():
            if k not in keys:
                keys.append(k)
                get_params(param_grid, keys, params)
                keys.pop()


if __name__ == "__main__":
    param_grid = {
        "A": [7, 8, 9, 10],
        "B": [7, 8, 9, 10],
        "C": [2, 3, 4],
        "D": [7, 8, 9, 10],
    }

    for p in get_params(param_grid):
        print(**p)

    exit()

    import os, sys
    sys.path.append(os.path.join(os.getcwd(), "src"))

    from orchai.tools import get_spark, get_logger
    from orchai.back_test_pd import back_test
    from orchai.constants import Constants
    from omegaconf import OmegaConf
    from functools import partial

    param_grid = {
        "A": [7, 8, 9, 10],
        "B": [7, 8, 9, 10],
        "C": [2, 3, 4],
        "D": [7, 8, 9, 10],
    }

    start_block = 7059473
    end_block = 9583823
    config_file = "config/etl_file_1m.yaml"
    filepath = "data/etl_parquet_1m_no_score"

    config = OmegaConf.load(config_file)
    spark = get_spark()
    logger = get_logger('tunning')

    for a in param_grid["A"]:
        for b in param_grid["B"]:
            for c in param_grid["C"]:
                for d in param_grid["D"]:
                    df = clone(filepath, a, b, c, d)
                    
                    acc = back_test(
                        df,
                        start=start_block,
                        end=end_block,
                        hop_size=14400,
                        win_size=432000,
                        col="score"
                    )

                    logger.write("Score acc")
                    logger.write(a, b, c, d, " Acc:", acc)
                    exit()
