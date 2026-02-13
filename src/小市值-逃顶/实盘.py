#encoding:gbk
import datetime
from datetime import datetime

import numpy as np
import requests


# ====================================================================
# 【全局配置】
# ====================================================================
class G():pass
g = G()


HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=599439e6-4132-48b6-a05a-c1fbb32e33d8'

# ====================================================================
# 【消息推送】
# ====================================================================
class Messager:
    def __init__(self, hook_url):
        self.hook_url = hook_url

    def send_message(self, text_content):
        try:
            current_time = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            payload = {"msgtype": "text", "text": {"content": current_time + text_content}}
            requests.post(self.hook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"推送失败: {e}")

messager = Messager(HOOK)

# ====================================================================
# 【核心逻辑】
# ====================================================================

def init(context):
    # 实盘定时任务：每天 14:55 执行
    if not context.do_back_test:
        context.run_time("record_smoothed_basis", "1nDay", "2025-01-01 14:55:00", "SH")
    print(">>> 7日加权基差监控已启动 (纯 Ex 接口版)")

g.window = 7                # 监控基差 7日窗口
def record_smoothed_basis(context):
    # 1. 直接获取连续主力合约代码 (规避换月数据断层)
    # 备注：IML0 是中金所 IM 连续主力
    main_continuous = "IML0.CFE" 
    main_stock = '000852.SH'  # 中证1000指数
    
    # 2. 获取数据 (增加 count 以确保对齐后仍有足够窗口)
    price_data = context.get_market_data_ex(
        fields=['close'],
        stock_list=[main_stock, main_continuous],
        period='1d',
        count=g.window + 5 
    )
    
    if main_stock not in price_data or main_continuous not in price_data:
        return

    # 3. 使用 Pandas 对齐数据
    df_idx = price_data[main_stock][['close']].rename(columns={'close': 'idx_close'})
    df_fut = price_data[main_continuous][['close']].rename(columns={'close': 'fut_close'})
    
    # 按时间戳合并
    df_merged = df_idx.join(df_fut, how='inner').dropna()
    
    if len(df_merged) < g.window:
        return

    # 4. 计算基差序列 (取最后 window 天)
    df_merged = df_merged.tail(g.window)
    # 基差 = (期货 / 现货 - 1) * 100
    df_merged['basis'] = (df_merged['fut_close'] / df_merged['idx_close'] - 1) * 100
    
    # 5. 计算 WMA
    weights = np.arange(1, g.window + 1)
    wma_basis = np.sum(df_merged['basis'].values * weights) / weights.sum()
    curr_basis = df_merged['basis'].iloc[-1]

    print(f"主力连续: {main_continuous} | 实时基差: {curr_basis:.2f}% | 7日加权: {wma_basis:.2f}%")

def handlebar(context):
    # 如果是回测，手动推进
    if context.do_back_test:
        # PTrade回测通常是按bar走的，逻辑可以放在这里或任务调度里
        pass