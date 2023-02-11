import pandas as pd


def get_params(param_grid: dict, start=None, end=None):
    def _get_params(param_grid: dict, idx: int, item: dict, params: list):
        if len(param_grid) == idx:
                params.append({k: v for k, v in item.items()})
        else:
            for i, k in enumerate(param_grid.keys()):
                if i == idx:
                    for v in param_grid[k]:
                        item.update({k: v})
                        _get_params(param_grid, idx=idx + 1, item=item, params=params)
                    break
    params = []
    _get_params(param_grid, 0, {}, params)
    return params[start : end]


if __name__ == "__main__":
    import os, sys
    sys.path.append(os.path.join(os.getcwd(), "src"))

    from orchai.tools import get_logger, get_spark
    from orchai.back_test_pd import back_test_reward
    from orchai.upload import run_uploading
    from omegaconf import OmegaConf
    from argparse import ArgumentParser
    import pickle as pkl

    parser = ArgumentParser()
    parser.add_argument("-s", "--start", type=int, default=None, help="Start iter")
    parser.add_argument("-e", "--end",   type=int, default=None, help="End iter")
    args = parser.parse_args()

    param_grid = {
        "A": [7, 8, 9, 10],
        "B": [7, 8, 9, 10],
        "C": [2, 3, 4],
        "D": [7, 8, 9, 10],
    }

    start_block     = 7059473
    end_block       = 9583823
    config_file     = "config/etl_file_1m_tune.yaml"
    config          = OmegaConf.load(config_file)
    logger          = get_logger('tunning')
    results         = []
    spark           = get_spark()
    best_acc        = 0
    best_param      = None

    for p in get_params(param_grid, start=args.start, end=args.end):
        logger.write(p)
        logger.write("Cloning")
        run_uploading(config, start_block, end_block, spark, overwrite=True, logger=logger, validate=False)
        df = pd.read_parquet(config.dest.file)

        logger.write("Reward backtesting")
        dt = back_test_reward(
            df,
            start=start_block,
            end=end_block,
            hop_size=14400,
            win_size=432000,
            col="score",
            use_tqdm=False
        )

        acc = dt[dt["fake_reward"] >= dt["real_reward"]].shape[0] / dt.shape[0]
        if acc > best_acc:
            best_param = p
        results.append((p, acc))
        logger.write("Acc:", acc, "| param:", p)
        logger.write("Best acc:", best_acc, "| param:", best_param)

    with open(f"results-{args.start}-{args.end}.pkl", "wb") as f:
        pkl.dump(results, f)
