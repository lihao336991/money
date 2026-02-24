from datetime import datetime

import numpy as np
import requests
from jqdata import *

HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e861e0b4-b8e2-42ed-a21a-1edf90c41618'


def initialize(context):
    set_option('use_real_price', True)
    g.future_symbol = 'IC'
    g.index_code = '000905.XSHG'
    
    # 存储基差序列的窗口长度
    g.window = 7
    g.basis_list = []

    run_daily(record_smoothed_basis, time='14:55')
    

def record_smoothed_basis(context):
    today = context.current_dt.strftime('%Y-%m-%d')
    # 1. 获取最新基差率 (同前)
    main_contract = get_dominant_future(g.future_symbol, date=context.current_dt)
    
    # 使用 get_current_data 获取实时最新价
    current_data = get_current_data()
    spot_p = current_data[g.index_code].last_price
    future_p = current_data[main_contract].last_price
    print(f"当前日期: {today}, 指数收盘价: {spot_p}, 主力合约收盘价: {future_p}, 最新基差率: {curr_basis_rate:.2f}%")
    
    curr_basis_rate = (future_p / spot_p - 1) * 100
    
    # 2. 更新序列
    g.basis_list.append(curr_basis_rate)
    if len(g.basis_list) > g.window:
        g.basis_list.pop(0)
    
    if len(g.basis_list) < g.window: return

    # 3. 计算加权平均 (WMA)
    # 权重数组：[1, 2, 3, 4, 5]
    weights = np.arange(1, len(g.basis_list) + 1)
    wma_basis = np.sum(np.array(g.basis_list) * weights) / weights.sum()

    # 4. 绘图对比
    record(WMA_Basis = wma_basis)         # 平滑值：更有趋势感
    record(Zero_Line = 0)
    record(Panic_Line = -2)

    # 5. 策略应用逻辑
    # 只有平滑后的基差率跌破 -1.5，才视为真正的“信号确认”
    if wma_basis < -2:
        log.warn(">>> ⚠️ [平滑基差报警] 确认持续性贴水加深，当前WMA基差: %.2f" % wma_basis)