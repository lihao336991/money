import akshare as ak
df = ak.futures_main_sina(symbol='IM2506')
df['date'] = df['日期'].astype(str)
d = df[df['date'] == '2025-06-19']

print(d)
