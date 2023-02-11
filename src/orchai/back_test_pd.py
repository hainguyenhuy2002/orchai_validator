import pandas as pd
import decimal
from tqdm import tqdm
from functools import partial
from math import log
#Decentralied back tgest
#Use Pielou's Evenness

def dec_score(df:pd.DataFrame, col:str):
    
    # df = df[(df['block_height']<= end) &(df['block_height'] >= start) &(df['block_height']% hop_size == start % hop_size)].sort_values('block_height')
    def calculate(df: pd.DataFrame):
        df['count'] = df[col].count()
        df['pi'] = df[col]/df[col].sum()
        # df['ln(pi)'] = df['pi'].apply(lambda x: log(x))
        df['ln(pi)'] = df['pi'].apply(lambda x: -9999999999 if (x ==0) else log(x))
        df['pi_mul_ln(pi)'] = df['pi'] * df['ln(pi)']
        df['total'] = df['pi_mul_ln(pi)'].sum()
        df['decentralized_score'] = df['total'].apply(lambda x: abs(x))/ df['count'].apply(lambda x: log(x))

        return df

    df = df.groupby('block_height').apply(calculate)
    df = df.groupby('block_height').agg({"decentralized_score":"mean"}).reset_index()
    return df

def preprocess(df:pd.DataFrame, predicted_col: str, start: int, end_time_stamp: int):
    df = df[(df['block_height']<= end_time_stamp) &(df['block_height'] >= start)]
    
    df = df.groupby("operator_address").apply(cal_delta)
    # df = df.drop(['operator_address', 'delegators_token'], axis = 1).reset_index()
    df["unreal_tokens"] = df['self_bonded'] + df[predicted_col]*(df['delta'].sum())/(df[predicted_col].sum())
    return df


def back_test_decentralized(df:pd.DataFrame, start:int, end: int, hop_size: int,win_size: int, predicted_col:str):
    #change type of columns to float
    df1 = df.drop('operator_address',axis = 1)
    df[df1.columns] = df[df1.columns].apply(pd.to_numeric)
    #starting for
    for i in range(start, end+1, hop_size):
        if i == start:    
            preprocecss_df = preprocess(df, predicted_col, i, i+ win_size)

            df_real_dec = dec_score(preprocecss_df,'tokens').rename(columns={'decentralized_score': 'real_decentralized_score'})

            df_unreal_dec = dec_score(preprocecss_df,'unreal_tokens').rename(columns={'decentralized_score': 'unreal_decentralized_score'})

            df_dec_final = df_real_dec.merge(df_unreal_dec, on = 'block_height', how = 'left')
        else:
            preprocecss_df = preprocess(df, predicted_col, i, i+ win_size)

            df_real_dec = dec_score(preprocecss_df,'tokens').rename(columns={'decentralized_score': 'real_decentralized_score'})

            df_unreal_dec = dec_score(preprocecss_df,'unreal_tokens').rename(columns={'decentralized_score': 'unreal_decentralized_score'})
            df_tem = df_real_dec.merge(df_unreal_dec, on = 'block_height', how = 'left')
            df_dec_final = pd.concat([df_dec_final, df_tem])

            df_dec_final['compare'] = (df_dec_final['unreal_decentralized_score'] >= df_dec_final['real_decentralized_score'])

            df_dec_final['accuracy'] = (df_dec_final['unreal_decentralized_score'] >= df_dec_final['real_decentralized_score']) / df_dec_final.shape[0]

            summarze = df_dec_final.agg({"accuracy":"sum"})

            df_final = df_dec_final.drop('accuracy', axis = 1)
    return summarze, df_final


  


#Reward back test
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

    summarize, df_decentralized = back_test_decentralized(df, start=7059473, end=9583823, hop_size=14400, win_size=432000, predicted_col="score")  
    #summarize is Series, df_centralized is dataframe
    print(summarize[0])