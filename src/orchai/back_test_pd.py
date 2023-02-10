import pandas as pd
import decimal
from tqdm import tqdm
from functools import partial


def cal_delta(df: pd.DataFrame):
    df              = df.sort_values("block_height")
    num             = df.shape[0]
    ud_0            = df["delegators_token"].iloc[0]
    df["delegators_token"] = df["delegators_token"].apply(lambda x: max(x - ud_0, 0))
    delta           = df["delegators_token"].sum() / (num - 1) 
    block_height    = df["block_height"].iloc[0]
    df              = df[df["block_height"] == block_height]
    df["delta"]     = delta     
    df              = df.drop(columns=["operator_address", "delegators_token"])
    return df
    

def cal_reward(row: pd.Series, col_in, col_out):
    row[col_out] = decimal.Decimal(row[col_in]) * decimal.Decimal(1.0 - row["commission_rate"])
    return row


def process_item(df: pd.DataFrame, start, end, col):
    df                  = df[(df["block_height"] >= start) & (df["block_height"] <= end)]
    df                  = df.groupby("operator_address").apply(cal_delta)
    df                  = df.apply(cal_reward, axis=1, col_in="delta", col_out="real_reward")
    df[col]             = df[col] / df[col].sum()
    df                  = df.apply(cal_reward, axis=1, col_in=col, col_out="fake_reward")
    total_ud            = df["delta"].sum()
    df["fake_reward"]   = df["fake_reward"] * total_ud
    return {
        "block_height": df["block_height"].iloc[0],
        "real_reward" : df["real_reward"].sum(),
        "fake_reward" : df["fake_reward"].sum()
    }
    

def back_test_reward(df: pd.DataFrame, start: int, end: int, hop_size: int, win_size: int, col):
    df = df[["block_height", "operator_address", "delegators_token", "commission_rate", col]]
    results = {
        "block_height" : [],
        "real_reward"  : [],
        "fake_reward"  : [],
    }

    process = partial(process_item, df=df, col=col)
    args = [{
        "start": sb,
        "end": sb + win_size - 1
    } for sb in range(start, end, hop_size) if sb + win_size - 1 <= end]

    for arg in tqdm(args):
        result = process(**arg)
        results["block_height"].append(result["block_height"])
        results["real_reward"].append(result["real_reward"])
        results["fake_reward"].append(result["fake_reward"])

    return pd.DataFrame.from_dict(results)


if __name__ == "__main__":
    df = pd.read_parquet("data/etl_parquet_1m")
    dt = back_test_reward(df, start=7059473, end=9583823, hop_size=14400, win_size=432000, col="score")
    print(dt[dt["real_reward"] <= dt["fake_reward"]].shape[0] / dt.shape[0])