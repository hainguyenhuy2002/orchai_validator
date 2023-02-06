if __name__ == "__main__":
    import os, sys
    sys.path.append(os.path.join(os.getcwd(), "src"))

    from orchai.tools import get_spark, get_logger
    from orchai.upload import run_uploading
    from orchai.back_test import back_test
    from orchai.constants import Constants
    from omegaconf import OmegaConf

    params = {
        "A": [7, 8, 9, 10],
        "B": [7, 8, 9, 10],
        "C": [2, 3, 4],
        "D": [7, 8, 9, 10],
    }

    start_block = 7059473
    end_block = 9583823
    config_file = "config/etl_file_1m.yaml"

    config = OmegaConf.load(config_file)
    spark = get_spark()
    logger = get_logger('tunning')

    for a in params["A"]:
        for b in params["B"]:
            for c in params["C"]:
                for d in params["D"]:
                    config.hp.etl.A = a
                    config.hp.etl.B = b
                    config.hp.etl.C = c
                    config.hp.etl.D = d
                    run_uploading(
                        config=config,
                        start_block=start_block,
                        end_block=end_block, 
                        spark=spark,
                        delete_old_file=True,
                        logger=logger)
                    logger.write("Score acc")
                    logger.write(a, b, c, d, " Acc:", back_test(
                        path=config.dest.file,
                        C_R_BASE=Constants.C_R_BASE,
                        start_block=start_block,
                        end_block=end_block,
                        step_block=Constants.block_step,
                        spark=spark,
                        save=True,
                        timestamp_block=432000,
                        col="score"
                    ))
                    logger.write("Label acc")
                    logger.write(a, b, c, d, " Acc:", back_test(
                        path=config.dest.file,
                        C_R_BASE=Constants.C_R_BASE,
                        start_block=start_block,
                        end_block=end_block,
                        step_block=Constants.block_step,
                        spark=spark,
                        save=True,
                        timestamp_block=432000,
                        col="label"
                    ))
                    exit()
