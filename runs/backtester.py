from argparse import ArgumentParser
from omegaconf import OmegaConf


if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.getcwd())

    from labeling.back_test import back_test

    parser = ArgumentParser()
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        required=True,
        help="")

    parser.add_argument(
        "-cr",
        "--C_R_BASE",
        type=float,
        required=True,
        help="")

    parser.add_argument(
        "-s",
        "--start_block",
        type=int,
        default=None,
        help=""
    )
    parser.add_argument(
        "-e",
        "--end_block",
        type=int,
        default=None,
        help=""
    )

    parser.add_argument(
        "-st",
        "--step_block",
        type=int,
        default=None,
        help="",
    )

    parser.add_argument(
        "-t",
        "--timestamp_block",
        type=int,
        default=None,
        help="",
    )

    parser.add_argument("--col", type=str, required=True, help="predict or score")

    args = parser.parse_args()

    back_test(**vars(args))
