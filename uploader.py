from labeling.upload import main
from argparse import ArgumentParser
from omegaconf import OmegaConf


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-cf", "--config", type=str, required=True, help="Path to config file (.yaml)")
    parser.add_argument(
        "-cp",
        "--checkpoint",
        type=str,
        default=None,
        help="Path to checkpoint. If no checkpoint then new checkpoint will be create at output/ckpt",
    )
    parser.add_argument(
        "-s", 
        "--start_block", 
        type=int, 
        default=None, 
        help="If start block is not given, it will be detected in checkpoint. Prefer checkpoint"
    )
    parser.add_argument(
        "-e", 
        "--end_block", 
        type=int, 
        default=None, 
        help="If start block is not given, it will be detected in checkpoint. Prefer checkpoint"
    )

    args = parser.parse_args()
    args.config = OmegaConf.load(args.config)
    
    main(**vars(args))
