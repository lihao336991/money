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
g.index_code = '000852.SH'  # 中证1000指数
g.future_prefix = 'IM'      # 期货前缀
g.window = 7                # 7日窗口

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
# 【工具函数：完全通过 C 接口寻找主力合约】
# ====================================================================
def find_main_contract_via_ex(C):
    """
    不依赖 get_snapshot，直接使用 get_market_data_ex 寻找持仓量最大的合约
    """
    # 1. 获取中金所所有代码
    all_cfe = C.get_stock_list_in_sector('中金所')
    target_im = [code for code in all_cfe if code.startswith(g.future_prefix)]
    
    if not target_im:
        return None
    
    # 2. 获取这些合约最近 1 分钟的持仓量
    # open_interest 是持仓量的标准字段
    oi_data = C.get_market_data_ex(
        fields=['open_interest'], 
        stock_list=target_im, 
        period='1m', 
        count=1
    )
    
    max_oi = -1
    main_code = None
    
    for code in target_im:
        if code in oi_data and not oi_data[code].empty:
            # 拿到最近的一个持仓量数值
            current_oi = oi_data[code]['open_interest'].iloc[-1]
            if current_oi > max_oi:
                max_oi = current_oi
                main_code = code
                
    return main_code

# ====================================================================
# 【核心逻辑】
# ====================================================================

def init(C):
    # 实盘定时任务：每天 14:55 执行
    if not C.do_back_test:
        C.run_time("record_smoothed_basis", "1nDay", "2025-01-01 14:55:00", "SH")
    print(">>> 7日加权基差监控已启动 (纯 Ex 接口版)")

def record_smoothed_basis(C):
    # 1. 寻找主力合约
    main_contract = find_main_contract_via_ex(C)
    if not main_contract:
        print("未找到有效 IM 合约")
        return
    
    print(f"当前主力合约: {main_contract}")

    # 2. 获取 7 日历史日线数据
    end_time = datetime.now().strftime('%Y%m%d%H%M%S')
    codes = [g.index_code, main_contract]
    
    # 获取 7 天的 close
    price_data = C.get_market_data_ex(
        fields=['close'],
        stock_list=codes,
        period='1d',
        count=g.window,
        end_time=end_time
    )

    if g.index_code not in price_data or main_contract not in price_data:
        print("价格数据获取不全")
        return

    # 3. 提取 DataFrame 并计算基差
    df_index = price_data[g.index_code]
    df_future = price_data[main_contract]

    if len(df_index) < g.window or len(df_future) < g.window:
        print(f"数据长度不足: 指数 {len(df_index)}天, 期货 {len(df_future)}天")
        return

    # 计算 7 日基差序列 (期货 / 现货 - 1) * 100
    # 注意：这里需要对齐索引，iloc[-g.window:] 确保取到的是最后 7 个
    basis_series = (df_future['close'].values / df_index['close'].values - 1) * 100

    # 4. 计算 WMA (7日加权平均)
    # 权重为 [1, 2, 3, 4, 5, 6, 7]
    weights = np.arange(1, g.window + 1)
    wma_basis = np.sum(basis_series * weights) / weights.sum()
    
    # 当前原始基差
    curr_basis = basis_series[-1]

    # 5. 可视化与输出
    # plot("WMA_Basis", wma_basis)
    # plot("Raw_Basis", curr_basis)
    
    print(f"主力: {main_contract} | 实时基差: {curr_basis:.2f}% | 7日加权: {wma_basis:.2f}%")

    # 6. 报警判断
    if wma_basis < -2.0:
        msg = f"⚠️ [基差逃顶报警]\n主力合约: {main_contract}\n7日加权基差: {wma_basis:.2f}%\n原始实时基差: {curr_basis:.2f}%"
        messager.send_message(msg)

def handlebar(C):
    # 如果是回测，手动推进
    if C.do_back_test:
        # PTrade回测通常是按bar走的，逻辑可以放在这里或任务调度里
        pass