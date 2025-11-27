# coding:gbk
import pandas as pd
import numpy as np
import math
import datetime

def init(C):
    # ---------------- 配置区域 ----------------
    C.account_id = '190200026196' 
    C.period = '1d'
    # ----------------------------------------

    C.stock_sum = 1       
    C.m_days = 25         
    C.min_money = 500     
    
    C.etf_pool = [
        "513100.SH", "513520.SH", "513030.SH", "518880.SH", "159980.SZ", 
        "159985.SZ", "501018.SH", "513130.SH", "510180.SH", "159915.SZ", 
        "512290.SH", "588120.SH", "515070.SH", "159851.SZ", "159637.SZ", 
        "159550.SZ", "512710.SH", "159692.SZ",
    ]

    # 【核心修改 1】定义全局变量用于在卖出和买入函数间传递目标
    C.today_target_positions = {} 

    # 【核心修改 2】拆分定时任务
    # 11:00 执行卖出
    C.run_time("execute_sell_logic", "1d", "11:00:00")
    # 11:05 执行买入 (预留5分钟让卖单成交和资金回笼)
    C.run_time("execute_buy_logic", "1d", "11:05:00")
    
    print("策略初始化完成，已设置为分步调仓模式")

def handlebar(C):
    pass

# -------------------- 拆分后的核心逻辑 --------------------

def execute_sell_logic(C):
    """
    第一阶段：计算信号 并 执行卖出
    """
    print(f"[{datetime.datetime.now()}] 阶段1: 开始计算信号并执行卖出...")
    
    # 1. 筛选目标ETF
    target_list = filter_etf(C)
    print("今日选中目标:", target_list)
    
    # 2. 计算目标持仓金额
    target_positions = {}
    total_asset = get_total_asset(C)
    
    if total_asset > 0 and target_list:
        per_value = total_asset / len(target_list)
        for code in target_list:
            target_positions[code] = per_value
    
    # 【重要】将计算好的目标存入全局变量 C，供买入阶段使用
    C.today_target_positions = target_positions
    
    # 3. 执行卖出操作
    # 获取当前持仓
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    current_holdings = {obj.m_strInstrumentID + '.' + obj.m_strExchangeID: obj for obj in positions}
    
    for code in list(current_holdings.keys()):
        pos_obj = current_holdings[code]
        price = get_safe_price(C, code)
        current_market_value = pos_obj.m_nVolume * price
        
        # 获取该标的的目标持仓市值（如果没有被选中，目标就是0）
        target_val = C.today_target_positions.get(code, 0.0)
        
        # 如果当前持仓 > 目标持仓，则卖出
        if current_market_value > target_val:
            diff = current_market_value - target_val
            if diff > max(C.min_money, price * 100):
                # 计算卖出量
                target_vol = int(target_val / price / 100) * 100
                vol_to_sell = pos_obj.m_nVolume - target_vol
                
                if vol_to_sell > 0:
                    print(f"执行卖出: {code}, 数量: {vol_to_sell}")
                    # 24 = 卖出
                    op_type = 24 
                    passorder(op_type, 1101, C.account_id, code, 5, price, vol_to_sell, "strategy_sell")

def execute_buy_logic(C):
    """
    第二阶段：执行买入
    """
    print(f"[{datetime.datetime.now()}] 阶段2: 资金应已回笼，开始执行买入...")
    
    # 读取第一阶段存下来的目标
    target_positions = C.today_target_positions
    if not target_positions:
        print("今日无买入目标或第一阶段未运行")
        return

    # 在实盘中，为了防止有未成交的卖单占用资金，建议先撤单（可选）
    if not C.do_back_test:
        cancel_unfilled_orders(C)

    # 重新获取持仓信息 (因为刚才卖出的可能已经成交了，持仓变了)
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    current_holdings = {obj.m_strInstrumentID + '.' + obj.m_strExchangeID: obj for obj in positions}
    
    for code, target_val in target_positions.items():
        price = get_safe_price(C, code)
        if price <= 0: continue

        current_vol = 0
        current_market_value = 0.0
        
        if code in current_holdings:
            current_vol = current_holdings[code].m_nVolume
            current_market_value = current_vol * price
            
        # 如果目标持仓 > 当前持仓，则买入
        if target_val > current_market_value:
            diff = target_val - current_market_value
            if diff > max(C.min_money, price * 100):
                # 计算买入量
                # 注意：这里最好再检查一下可用资金，防止计算误差导致废单
                # 但为保持逻辑简单，直接按目标下达
                target_vol_total = int(target_val / price / 100) * 100
                vol_to_buy = target_vol_total - current_vol
                
                if vol_to_buy > 0:
                    print(f"执行买入: {code}, 数量: {vol_to_buy}")
                    # 23 = 买入
                    op_type = 23
                    passorder(op_type, 1101, C.account_id, code, 5, price, vol_to_buy, "strategy_buy")

def cancel_unfilled_orders(C):
    """
    辅助函数：撤销当前策略的所有未成交挂单
    """
    orders = get_trade_detail_data(C.account_id, 'stock', 'order')
    for order in orders:
        # 48=未报, 49=待报, 50=已报, 51=已报待撤, 52=部成, 53=部成待撤
        # 根据实际情况判断状态，通常撤单针对 非最终状态的订单
        if order.m_nOrderStatus in [48, 49, 50, 52]: 
             cancel(order.m_strOrderID, C.account_id, 'stock')
             print(f"撤销未成交订单: {order.m_strInstrumentID}")

# -------------------- 下面保留你原有的辅助函数 --------------------

def get_total_asset(C):
    account_info = get_trade_detail_data(C.account_id, 'stock', 'account')
    if account_info:
        return account_info[0].m_dTotalAsset
    return 0

def get_safe_price(C, code):
    if C.do_back_test:
        data = C.get_market_data_ex(['close'], [code], period=C.period, count=1, subscribe=False)
        if code in data and not data[code].empty:
            return data[code].iloc[-1]['close']
        return 0
    else:
        tick = C.get_full_tick([code])
        if code in tick:
            return tick[code]['lastPrice']
        else:
            data = C.get_market_data_ex(['close'], [code], period=C.period, count=1, subscribe=False)
            if code in data and not data[code].empty:
                return data[code].iloc[-1]['close']
            return 0

def filter_etf(C):
    # ... (保持你原有的逻辑不变) ...
    scores = []
    history_data = C.get_market_data_ex(['close'], C.etf_pool, period=C.period, count=C.m_days + 5, subscribe=False)
    for etf in C.etf_pool:
        if etf not in history_data: continue
        df = history_data[etf]
        if len(df) < C.m_days: continue
        
        if not C.do_back_test:
            current_price = get_safe_price(C, etf)
            if current_price <= 0: continue
            closes = df['close'].values[-C.m_days:] 
            prices = np.append(closes, current_price)
        else:
            prices = df['close'].values[-(C.m_days + 1):]

        if len(prices) < 2: continue

        y = np.log(prices)
        x = np.arange(len(y))
        weights = np.linspace(1, 2, len(y))
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.exp(slope * 250) - 1
        y_pred = slope * x + intercept
        ss_res = np.sum(weights * (y - y_pred) ** 2)
        ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0
        score = annualized_returns * r2

        is_crash = False
        if len(prices) >= 4:
            if min(prices[-1]/prices[-2], prices[-2]/prices[-3], prices[-3]/prices[-4]) < 0.95:
                is_crash = True
        
        if is_crash: score = 0
            
        if 0 < score < 6:
            scores.append({'code': etf, 'score': score})
            
    df_score = pd.DataFrame(scores)
    if df_score.empty: return []
    df_score = df_score.sort_values(by='score', ascending=False)
    return df_score['code'].head(C.stock_sum).tolist()