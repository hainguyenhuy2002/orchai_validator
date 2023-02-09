import pandas as pd
import decimal
from tqdm import tqdm


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


def back_test(df: pd.DataFrame, start: int, end: int, hop_size: int, win_size: int, col):
    df = df[["block_height", "operator_address", "delegators_token", "commission_rate", col]]
    results = {
        "block_height" : [],
        "real_reward"  : [],
        "fake_reward"  : [],
    }

    for sb in tqdm([sb for sb in range(start, end, hop_size) if sb + win_size - 1 <= end]):
        eb = sb + win_size - 1

        new_df                  = df[(df["block_height"] >= sb) & (df["block_height"] <= eb)]
        new_df                  = new_df.groupby("operator_address").apply(cal_delta)
        new_df                  = new_df.apply(cal_reward, axis=1, col_in="delta", col_out="real_reward")
        new_df[col]             = new_df[col] / new_df[col].sum()
        new_df                  = new_df.apply(cal_reward, axis=1, col_in=col, col_out="fake_reward")
        total_ud                = new_df["delta"].sum()
        new_df["fake_reward"]   = new_df["fake_reward"] * total_ud

        results["block_height"].append(new_df["block_height"].iloc[0])
        results["real_reward"].append(new_df["real_reward"].sum())
        results["fake_reward"].append(new_df["fake_reward"].sum())
    
    return pd.DataFrame.from_dict(results)


if __name__ == "__main__":
    df = pd.read_parquet("data/etl_parquet_1m")
    dt = back_test(df, start=7059473, end=9583823, hop_size=14400, win_size=432000, col="score")
    print(dt[dt["real_reward"] <= dt["fake_reward"]].shape[0] / dt.shape[0])