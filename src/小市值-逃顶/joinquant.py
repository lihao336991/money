import datetime

import numpy as np
import requests
from jqdata import *

# ====================================================================
# 【参数配置】
# ====================================================================
HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx' # 请替换为你的Hook

class Messager:
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = True 

    def send_message(self, text_content):
        if self.is_test:
            print(f"【消息推送(测试)】{text_content}")
            return
        try:
            current_time = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            payload = {"msgtype": "text", "text": {"content": current_time + text_content}}
            requests.post(self.hook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"推送失败: {e}")

messager = Messager(HOOK)

def initialize(context):
    set_option('use_real_price', True)
    # 开启防未来函数（可选，建议保持开启以确保策略真实性）
    set_option('avoid_future_data', True) 
    log.set_level('order', 'error')
    
    g.is_risk_warning = False      
    g.warning_start_date = None    
    g.basis_list = []              
    g.wma_window = 5               
    g.wma_weights = np.array([0.05, 0.1, 0.15, 0.3, 0.4]) # 由远到近的权重（对应basis_list从旧到新）
    
    g.basis_trigger = -2.0         
    g.breadth_trigger = 0.3
    g.basis_recovery = -1.2        
    g.breadth_recovery = 0.5       

    run_daily(market_risk_monitor, time='14:50')
    messager.send_message(">>> [风控状态机] 已启动：14:50实时逃顶版（严密版）。")

def market_risk_monitor(context):
    today = context.current_dt.date()
    
    # 1. 动态品种适配
    if today >= datetime.date(2022, 7, 22):
        target_future, target_spot = 'IM', '000852.XSHG'
    elif today >= datetime.date(2015, 4, 16):
        target_future, target_spot = 'IC', '000905.XSHG'
    else: return

    try:
        # 获取上一个交易日 (修复报错的核心：基于昨日市值筛选)
        trade_days = get_trade_days(end_date=today, count=2)
        prev_date = trade_days[0]

        # 2. 获取实时价格对象
        current_data = get_current_data()

        # --- 【实时基差计算】 ---
        spot_p = current_data[target_spot].last_price
        main_contract = get_dominant_future(target_future, date=today)
        future_p = current_data[main_contract].last_price
        
        if np.isnan(spot_p) or np.isnan(future_p) or spot_p == 0: return
        
        # 基差公式: $BasisRate = (\frac{Future}{Spot} - 1) \times 100$
        curr_basis_rate = (future_p / spot_p - 1) * 100
        g.basis_list.append(curr_basis_rate)
        if len(g.basis_list) > g.wma_window: g.basis_list.pop(0)
        
        if len(g.basis_list) == g.wma_window:
            wma_basis = np.sum(np.array(g.basis_list) * g.wma_weights)
        else:
            # 数据不足时降级为简单加权或平均（这里暂时用简单的线性加权）
            temp_weights = np.arange(1, len(g.basis_list) + 1)
            wma_basis = np.sum(np.array(g.basis_list) * temp_weights) / temp_weights.sum()

        # --- 【实时广度计算】 ---
        # 【修改点】：date参数改为 prev_date，避开15:00前不能查今天市值的限制
        q = query(valuation.code).filter(valuation.market_cap > 0).order_by(valuation.market_cap.asc()).limit(400)
        micro_stocks = get_fundamentals(q, date=prev_date)['code'].tolist()
        
        # 获取这些股票在“昨天”的收盘价
        pre_close_data = get_price(micro_stocks, end_date=prev_date, count=1, fields=['close'], panel=False)
        pre_close_dict = dict(zip(pre_close_data['code'], pre_close_data['close']))
        
        rise_count = 0
        valid_count = 0
        for stock in micro_stocks:
            if stock in pre_close_dict:
                curr_p = current_data[stock].last_price
                # 排除停牌及获取不到价格的情况
                if not np.isnan(curr_p) and not current_data[stock].paused:
                    valid_count += 1
                    if curr_p > pre_close_dict[stock]:
                        rise_count += 1
        
        micro_rise_ratio = float(rise_count) / valid_count if valid_count > 0 else 0.5

        # --- 【Ratio 计算】 ---
        # 中证1000: 000852.XSHG
        # 上证50: 000016.XSHG
        price_1000 = current_data['000852.XSHG'].last_price
        price_50 = current_data['000016.XSHG'].last_price
        
        ratio_1000_50 = 0
        if not np.isnan(price_1000) and not np.isnan(price_50) and price_50 != 0:
            ratio_1000_50 = price_1000 / price_50

        # 4. 可视化
        record(WMA_Basis = wma_basis)
        record(Ratio_1000_50 = ratio_1000_50)
        # record(Micro_Breadth = micro_rise_ratio * 10) 
        # record(Position_Status = 1 if not g.is_risk_warning else 0)

        # 5. 状态机切换逻辑
        risk_trigger = (wma_basis < g.basis_trigger and micro_rise_ratio < g.breadth_trigger)
        risk_recovery = (wma_basis > g.basis_recovery or micro_rise_ratio > g.breadth_recovery)

        if not g.is_risk_warning and risk_trigger:
            g.is_risk_warning = True
            g.warning_start_date = today
            msg = ">>> 🔴 [风险爆发] 14:50实时信号 | WMA基差:%.2f | 微盘广度:%.1f%%" % (wma_basis, micro_rise_ratio * 100)
            log.warn(msg); messager.send_message(msg)
            # 执行清仓
            sell_all_stocks(context)

        elif g.is_risk_warning and risk_recovery:
            duration = (today - g.warning_start_date).days
            g.is_risk_warning = False
            msg = ">>> 🟢 [风险解除] 持续:%d天" % duration
            log.info(msg); messager.send_message(msg)
            g.warning_start_date = None

    except Exception as e:
        log.error("风控实时计算出错: %s" % e)

def sell_all_stocks(context):
    """清仓逻辑"""
    if len(context.portfolio.positions) > 0:
        for stock in list(context.portfolio.positions.keys()):
            order_target(stock, 0)
        log.info("【操作】检测到风控信号，已执行全仓平仓。")

def handle_data(context, data):
    pass