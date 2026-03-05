import akshare as ak
df = ak.futures_main_sina(symbol='IM2603')
df['date'] = df['日期'].astype(str)
d = df[(df['date'] >= '2026-03-03') & (df['date'] <= '2026-03-10')][['date', '收盘价', '持仓量']]
# 获取中证1000现货数据

print(d)