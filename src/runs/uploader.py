from argparse import ArgumentParser
from omegaconf import OmegaConf


if __name__ == "__main__":
    import os, sys
    sys.path.append(os.path.join(os.getcwd(), "src"))
    
    from orchai.upload import run_uploading
    
    parser = ArgumentParser()
    parser.add_argument("-cf", "--config", type=str, required=True, help="Path to config file (.yaml)")
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

    parser.add_argument(
        "-d", 
        "--delete_old_file", 
        type=bool, 
        default=None, 
    )

    args = parser.parse_args()
    args.config = OmegaConf.load(args.config)
    
    run_uploading(**vars(args))
