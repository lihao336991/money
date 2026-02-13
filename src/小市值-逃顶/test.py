from datetime import datetime

import numpy as np
import requests
from jqdata import *

HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e861e0b4-b8e2-42ed-a21a-1edf90c41618'

# ====================================================================
# 【健壮性模块 1：消息推送 Messager】
# 用于企业微信 Webhook 消息通知
# ====================================================================

class Messager:
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = False

    def set_is_test(self, is_test):
        self.is_test = is_test

    def send_message(self, text_content):
        if self.is_test:
            # 回测模式下只打印到日志
            print(f"【消息推送(测试)】{text_content}")
            return

        try:
            # 自动添加时间戳
            current_time = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            content = current_time + text_content
            
            payload = {
                "msgtype": "text",
                "text": {
                    "content": content
                }
            }
            # 发送 POST 请求到 Webhook
            response = requests.post(self.hook_url, json=payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"【消息推送失败】错误: {e}")

messager = Messager(HOOK)
# 测试模式
messager.set_is_test(True)

def initialize(context):
    set_option('use_real_price', True)
    g.future_symbol = 'IC'
    g.index_code = '000905.XSHG'
    
    # 存储基差序列的窗口长度
    g.window = 7
    g.basis_list = []

    run_daily(record_smoothed_basis, time='14:55')
    messager.send_message(">>> 小市值-逃顶通知已启动")
    

def record_smoothed_basis(context):
    today = context.current_dt.strftime('%Y-%m-%d')
    # 1. 获取最新基差率 (同前)
    main_contract = get_dominant_future(g.future_symbol, date=context.current_dt)
    
    # 使用 get_current_data 获取实时最新价
    current_data = get_current_data()
    spot_p = current_data[g.index_code].last_price
    future_p = current_data[main_contract].last_price
    
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
        messager.send_message(">>> ⚠️ [平滑基差报警] 贴水加深，当前WMA基差: %.2f" % wma_basis)