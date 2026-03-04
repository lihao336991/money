import akshare as ak
import time
import pandas as pd

def get_futures_data():
    try:
        df = ak.futures_main_sina(symbol='IM2402')
        return df
    except Exception as e:
        print(f"Error getting futures data: {e}")
        return None

def get_spot_data():
    max_retries = 3
    for i in range(max_retries):
        try:
            # 尝试使用 ak.stock_zh_index_daily 替代
            df = ak.stock_zh_index_daily(symbol="sz399852")
            return df
        except Exception as e:
            if i < max_retries - 1:
                print(f"Error getting spot data (attempt {i+1}): {e}. Retrying...")
                time.sleep(2)
            else:
                print(f"Failed to get spot data after {max_retries} attempts: {e}")
                return None

df = get_futures_data()
if df is not None:
    df['date'] = df['日期'].astype(str)
    # 2024-01-10 至 2024-02-08 期间的数据，只保留日期、收盘价、持仓量三列
    df_futures = df[(df['date'] >= '2024-01-10') & (df['date'] <= '2024-02-08')][['date', '收盘价', '持仓量']]
    df_futures = df_futures.rename(columns={'收盘价': 'futures_close', '持仓量': 'open_interest'})

    # 获取中证1000现货数据
    df_spot = get_spot_data()
    
    if df_spot is not None:
        df_spot['date'] = df_spot['date'].astype(str)
        # 统一日期格式
        df_spot['date'] = pd.to_datetime(df_spot['date']).dt.strftime('%Y-%m-%d')
        df_spot = df_spot[(df_spot['date'] >= '2024-01-10') & (df_spot['date'] <= '2024-02-08')][['date', 'close']]
        df_spot = df_spot.rename(columns={'close': 'spot_close'})

        # 合并数据
        merged_df = df_futures.merge(df_spot, on='date')

        # 计算基差：(期货收盘价 / 现货收盘价 - 1) * 100%
        merged_df['basis_percent'] = (merged_df['futures_close'] / merged_df['spot_close'] - 1) * 100

        # 计算持仓量变化
        merged_df['oi_change'] = merged_df['open_interest'].diff()

        print(merged_df[['date', 'spot_close', 'futures_close', 'basis_percent', 'open_interest', 'oi_change']])
    else:
        print("Could not retrieve spot data.")
else:
    print("Could not retrieve futures data.")