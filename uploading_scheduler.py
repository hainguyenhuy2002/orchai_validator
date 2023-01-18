from labeling.upload import main, get_batch_intervals
from argparse import ArgumentParser
from omegaconf import OmegaConf
from labeling.tools import psql_connect, get_max_height, get_min_height


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-cf", "--config", type=str, required=True, help="Path to config file (.yaml)")
    parser.add_argument("-u", "--upload", action="store_true", help="Start uploading process")
    parser.add_argument("-bs", "--batch_size", type=int, default=0, help="Batch size, 0 means no batch")
    parser.add_argument("-k", action='store_true')

    args                    = parser.parse_args()
    args.batch_size         = max(args.batch_size, 0)
    args.config             = OmegaConf.load(args.config)
    
    min_src_height          = get_min_height(psql_connect(**args.config.src).cursor(), args.config.src.table)
    max_src_height          = get_max_height(psql_connect(**args.config.src).cursor(), args.config.src.table)
    max_dest_height         = get_max_height(psql_connect(**args.config.dest).cursor(), args.config.dest.table)

    vote_proposed_win_size  = args.config.hp.etl.vote_proposed_win_size
    combine_win_size        = 150
    label_win_size          = args.config.hp.etl.label_win_size
    batch_size              = args.config.hp.upload.batch_size

    intervals = get_batch_intervals(
        start_block=max_dest_height + combine_win_size,
        global_end_block=max_src_height,
        batch_size=batch_size,
        vote_proposed_win_size=vote_proposed_win_size,
        combine_win_size=combine_win_size,
        label_win_size=label_win_size,
        min_block=min_src_height
    )

    print("From", max_dest_height + combine_win_size, "to", max_src_height)

    print("Batch size =", args.batch_size)
    print("Total number of intervals =", len(intervals))
    print()

    def print_cmd(cmd: str):
        if args.k:
            print(f"!{cmd}")
        else:
            print(cmd)

    if args.batch_size == 0:
        print_cmd(f"python ./uploader.py -cf ./config/etl.yaml -s {intervals[0][0] + vote_proposed_win_size - 1} -e {intervals[-1][1]}")
    else:
        start = None
        for idx, (bs, be) in enumerate(intervals):
            if start is None:
                start = bs
            if (idx + 1) % args.batch_size == 0:
                print_cmd(f"python ./uploader.py -cf ./config/etl.yaml -s {start + vote_proposed_win_size - 1} -e {be}")
                start = None
        if start is not None:
            print_cmd(f"python ./uploader.py -cf ./config/etl.yaml -s {start + vote_proposed_win_size - 1} -e {intervals[-1][1]}")

    if args.upload:
        del args.upload
        main(**vars(args))

