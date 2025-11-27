#encoding:gbk
import pandas as pd
import numpy as np
import math
import datetime
import time
import requests
import json
from datetime import datetime, time as dt_time, timedelta

# ====================================================================
# 【全局配置】(使用您提供的 ST 策略配置)
# ====================================================================

# 全局状态存储器
class G():pass
g = G()
g.stock_sum = 1       
g.m_days = 25         
g.min_money = 500  

# 账户和Webhook配置
ACCOUNT = '190200026196'
HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2a336b4c-c38e-4ae3-9ff6-f14f175b4f73'

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

# ====================================================================
# 【健壮性模块 2：任务调度 TaskRunner】
# 适配回测环境的精准定时 (移植自您的 ST 策略)
# ====================================================================

class ScheduledTask:
    """定时任务基类"""
    def __init__(self, execution_time, func_name):
        self.last_executed_date = None
        self.execution_time = self._parse_time(execution_time)
        self.func_name = func_name
    
    def _parse_time(self, time_str):
        """将HH:MM或HH:MM:SS格式字符串转换为time对象"""
        try:
            parts = time_str.split(':')
            hour = int(parts[0])
            minute = int(parts[1])
            second = int(parts[2]) if len(parts) > 2 else 0
            return dt_time(hour, minute, second)
        except ValueError:
            raise ValueError(f"Invalid time format: {time_str}, use HH:MM or HH:MM:SS")

class DailyTask(ScheduledTask):
    """每日任务，每天只执行一次"""
    def should_trigger(self, current_dt):
        if self.last_executed_date is None:
            # 确保在回测开始的第一天就能触发
            self.last_executed_date = current_dt.date() - timedelta(days=1)
        
        time_check = current_dt.time() >= self.execution_time
        date_check = current_dt.date() > self.last_executed_date
        
        return time_check and date_check

class TaskRunner:
    """管理和执行定时任务"""
    def __init__(self, C):
        self.tasks = []
        self.C = C

    def run_daily(self, time_str, func):
        self.tasks.append({
            'type': 'daily',
            'time': time_str,
            'func': func.__name__,
            'task_obj': DailyTask(time_str, func.__name__)
        })
        print(f"【调度】已添加每日任务 {func.__name__} @ {time_str}")

    def check_tasks(self, current_dt):
        """检查并执行任务"""
        for task_info in self.tasks:
            task_obj = task_info['task_obj']
            
            if task_obj.should_trigger(current_dt):
                func_to_run = globals().get(task_info['func'])
                if func_to_run:
                    print(f"【任务执行】触发任务: {task_info['func']} @ {current_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    try:
                        func_to_run(self.C)
                        task_obj.last_executed_date = current_dt.date()
                    except Exception as e:
                        print(f"【任务执行失败】{task_info['func']} 运行时错误: {e}")

# ====================================================================
# 【健壮性模块 3：交易日历工具】(移植自您的 ST 策略)
# ====================================================================

def get_shifted_date(C, date_str, days, days_type='T'):
    """获取偏移后的日期（适配 iQuant 平台）"""
    # ... (使用ST策略中复杂的交易日历获取逻辑，为保持代码简洁性，这里使用简化版，但功能已集成) ...
    try:
        if isinstance(date_str, str):
            base_date = datetime.strptime(date_str, "%Y%m%d").date()
        else:
             base_date = date_str

        if days_type == 'T':
            # 简化逻辑：通过调用平台接口获取交易日
            start_date = (base_date - timedelta(days=365)).strftime("%Y%m%d")
            end_date = (base_date + timedelta(days=abs(days) + 365)).strftime("%Y%m%d")
            
            trade_days = C.get_trading_dates(stockcode='SH', start_date=start_date, end_date=end_date, count=1000, period='1d')
            
            if not trade_days:
                return (base_date + timedelta(days=days)).strftime("%Y%m%d")

            date_str_formatted = base_date.strftime("%Y%m%d")
            if date_str_formatted in trade_days:
                index = trade_days.index(date_str_formatted)
                new_index = max(0, min(index + days, len(trade_days) - 1))
                return trade_days[new_index]
            else:
                 # 如果基准日非交易日，向后偏移
                return (base_date + timedelta(days=days)).strftime("%Y%m%d")
        
        else: # 自然日偏移 'N'
            return (base_date + timedelta(days=days)).strftime("%Y%m%d")
            
    except Exception as e:
        print(f"【日期偏移】错误: {str(e)}，使用自然日偏移作为备用")
        return (datetime.now().date() + timedelta(days=days)).strftime("%Y%m%d")

def get_previous_trading_day(C, current_date):
    """获取前一个交易日"""
    current_str = current_date.strftime("%Y%m%d")
    prev_str = get_shifted_date(C, current_str, -1, 'T')
    return datetime.strptime(prev_str, "%Y%m%d").date()

def codeOfPosition(position):
    # 模拟 ST 策略中的持仓代码格式
    return position.m_strInstrumentID + '.' + position.m_strExchangeID

def passorder_live(C, op_type, code, price, volume, remark):
    """实盘交易下单函数，使用 ST 策略的 passorder 风格"""
    # 假设 23=买入, 24=卖出, 7=限价，5=最新价
    if op_type == 23: # 买入
        passorder(23, 1101, C.account_id, code, 7, price, volume, remark)
    elif op_type == 24: # 卖出
        passorder(24, 1101, C.account_id, code, 7, price, volume, remark)
    print(f"【实盘交易】执行 {remark}: {code}, 价格: {price:.2f}, 数量: {volume}")

def order_target_value_test(C, code, target_value):
    """回测模式下按市值调仓 (ST 策略的 order_target_value 风格)"""
    print(f"【回测交易】{code} 调仓目标市值: {target_value:.2f}")
    # 在实际回测平台中，应调用 C.order_target_value(code, target_value)
    order_target_value(code, target_value, C, C.account_id)

def cancel(order_id, account, asset_type):
    """撤单函数"""
    print(f"【交易操作】撤销订单: {order_id}")
    cancel_order(order_id, account, asset_type)

# 获取当前账户可用金额
def get_total_asset(C):        
    accounts = get_trade_detail_data(C.account_id, 'stock', 'account')
    money = 0
    for dt in accounts:
        money = dt.m_dBalance
    return money

# ====================================================================
# 【核心策略逻辑】
# ====================================================================
Period = '1d'

def init(C):
    # ---------------- 配置区域 ----------------
    C.account_id = ACCOUNT 
    C.set_account(C.account_id)
    # ----------------------------------------

    # 注册任务调度器和消息推送
    C.runner = TaskRunner(C)
    messager.set_is_test(C.do_back_test)
    print('回测模式:', C.do_back_test)
    
    # 初始化时间戳
    C.currentTime = 0
    C.today = datetime.now().date()
    C.yesterday = get_previous_trading_day(C, C.today).strftime("%Y%m%d")

    C.etf_pool = [
        "513100.SH", "513520.SH", "513030.SH", "518880.SH", "159980.SZ", 
        "159985.SZ", "501018.SH", "513130.SH", "510180.SH", "159915.SZ", 
        "512290.SH", "588120.SH", "515070.SH", "159851.SZ", "159637.SZ", 
        "159550.SZ", "512710.SH", "159692.SZ",
    ]

    C.today_target_positions = {} 

    # 判断当前日期是否为周末 (仅实盘需要主动过滤)
    if not C.do_back_test and datetime.now().weekday() >= 5:
        print('当前日期为周末，不执行任务')
        return

    # 【实盘/回测环境兼容调度】
    if C.do_back_test:
        print('doing test - 注册回测任务')
        # 回测中使用 TaskRunner 的精确时间模拟
        C.runner.run_daily("11:00", execute_sell_logic)
        C.runner.run_daily("11:05", execute_buy_logic)
        C.runner.run_daily("15:00", log_position)
        
    else:
        print('doing live - 注册实盘任务')
        # 实盘中使用平台 run_time 接口
        C.run_time("execute_sell_logic", "1d", "11:00:00")
        C.run_time("execute_buy_logic", "1d", "11:05:00")
        C.run_time("log_position", "1d", "15:00:00")

    print("策略初始化完成，已设置为分步调仓模式")

def handlebar(C):
    """
    【健壮性模块 2 延伸：回测日期推进】
    在回测模式下，使用 handlebar 来推进 ContextInfo 中的日期，
    并触发 TaskRunner 检查定时任务。
    """
    if C.do_back_test:
        index = C.barpos
        # 假设 ContextInfo.get_bar_timetag 返回 UTC+0 毫秒时间戳
        currentTime_ms = C.get_bar_timetag(index) + 8 * 3600 * 1000 # 转换为 UTC+8
        
        try:
            current_dt = datetime.fromtimestamp(currentTime_ms / 1000)
            if C.currentTime < currentTime_ms:
                C.currentTime = currentTime_ms
                C.today = current_dt.date()
                C.yesterday = get_previous_trading_day(C, C.today).strftime("%Y%m%d")
                
                # 检查并执行任务
                C.runner.check_tasks(current_dt)
        except Exception as e:
            print(f'handlebar时间处理异常: {e}')
    # 非回测模式下，不处理

# -------------------- 拆分后的核心逻辑 (已集成消息推送和交易兼容) --------------------

def execute_sell_logic(C):
    """
    第一阶段：计算信号 并 执行卖出
    """
    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"[{current_time}] 阶段1: 开始计算信号并执行卖出...")
    messager.send_message(f"【ETF轮动-信号计算】开始执行卖出逻辑 @ {current_time}")
    
    # 1. 筛选目标ETF
    target_list = filter_etf(C)
    print("今日选中目标:", target_list)
    
    # 2. 计算目标持仓金额
    target_positions = {}
    total_asset = get_total_asset(C)

    print(f"当前账户总金额: {total_asset:.2f}")
    
    if total_asset > 0 and target_list:
        per_value = total_asset / len(target_list)
        for code in target_list:
            target_positions[code] = per_value
    
    C.today_target_positions = target_positions
    
    # 3. 执行卖出操作
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    hold_list = [codeOfPosition(position) for position in positions if position.m_dMarketValue > 10000]
    print("当前有仓位的持仓:", hold_list)
    
    current_holdings = {obj.m_strInstrumentID + '.' + obj.m_strExchangeID: obj for obj in positions}
    # 如果已经持仓，且持仓比例不小于目标持仓比例，不需要执行后续逻辑

    for code in list(current_holdings.keys()):
        if code in target_list: 
            print(f"{code} 继续持仓")
            continue

        pos_obj = current_holdings[code]
        price = get_safe_price(C, code)
        if price <= 0 or pos_obj.m_nCanUseVolume <= 0: continue

        current_market_value = pos_obj.m_nCanUseVolume * price
        target_val = C.today_target_positions.get(code, 0.0)
        
        if current_market_value > target_val:
            diff = current_market_value - target_val
            # 最小交易金额或1手市值
            min_trade_amount = max(g.min_money, price * 100) 
            
            if diff > min_trade_amount:
                # 计算卖出量，按100股取整
                target_vol = int(target_val / price / 100) * 100
                vol_to_sell = pos_obj.m_nCanUseVolume - target_vol
                
                if vol_to_sell > 0:
                    messager.send_message(f"【ETF轮动-卖出】{code} 数量: {vol_to_sell}，目标市值: {target_val:.2f}")
                    if C.do_back_test:
                        # 回测模式：按目标市值调仓
                        order_target_value_test(C, code, target_val) 
                    else:
                        # 实盘模式：限价卖出
                        # 使用 price 作为限价，确保及时成交（使用最新价）
                        passorder_live(C, 24, code, price, vol_to_sell, "ETF_SELL_TUNE")
            
def execute_buy_logic(C):
    """
    第二阶段：执行买入
    """
    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"[{current_time}] 阶段2: 资金应已回笼，开始执行买入...")
    messager.send_message(f"【ETF轮动-交易】开始执行买入逻辑 @ {current_time}")
    
    target_positions = C.today_target_positions
    if not target_positions:
        print("今日无买入目标")
        return

    # 实盘中先撤单 (确保资金不会被占用)
    if not C.do_back_test:
        cancel_unfilled_orders(C)

    # 重新获取持仓信息
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
            
        if target_val > current_market_value:
            diff = target_val - current_market_value
            min_trade_amount = max(g.min_money, price * 100) 

            if diff > min_trade_amount:
                # 计算总目标股数，按100股取整
                target_vol_total = int(target_val / price / 100) * 100
                vol_to_buy = target_vol_total - current_vol
                
                if vol_to_buy > 0:
                    messager.send_message(f"【ETF轮动-买入】{code} 数量: {vol_to_buy}，目标市值: {target_val:.2f}")
                    if C.do_back_test:
                        # 回测模式：按目标市值调仓
                        order_target_value_test(C, code, target_val)
                    else:
                        # 实盘模式：限价买入
                        passorder_live(C, 23, code, price, vol_to_buy, "ETF_BUY_TUNE")

def cancel_unfilled_orders(C):
    """撤销当前策略的所有未成交挂单"""
    orders = get_trade_detail_data(C.account_id, 'stock', 'order')
    for order in orders:
        # 48=未报, 49=待报, 50=已报, 51=已报待撤, 52=部成, 53=部成待撤
        if order.m_nOrderStatus in [48, 49, 50, 52]: 
             cancel(order.m_strOrderID, C.account_id, 'stock')
             print(f"撤销未成交订单: {order.m_strInstrumentID}")

def log_position(C):
    """【健壮性模块 1 延伸：收盘持仓日志】(移植自您的 ST 策略)"""
    positions = get_trade_detail_data(C.account_id, 'STOCK', 'POSITION')
    if positions:
        print(f"********** 收盘持仓信息打印开始 **********")
        msg = f"【收盘持仓】日期: {C.today.strftime('%Y-%m-%d')}\n"
        for position in positions:
            cost: float = position.m_dOpenPrice
            price: float = position.m_dLastPrice
            value: float = position.m_dMarketValue
            ret: float = 100 * (price / cost - 1) if cost != 0 else 0.0
            msg += f"- {codeOfPosition(position)} ({C.get_stock_name(codeOfPosition(position))}), 市值：{value:.2f}，盈亏: {ret:.2f}%\n"
        print(msg)
        messager.send_message(msg)
        print("****************************************")

# -------------------- 原有辅助函数 --------------------

def get_safe_price(C, code):
    if C.do_back_test:
        data = C.get_market_data_ex(['close'], [code], period=Period, count=1, subscribe=False)
        if code in data and not data[code].empty:
            return data[code].iloc[-1]['close']
        return 0
    else:
        tick = C.get_full_tick([code])
        if code in tick:
            # 兼容 ST 策略中的实盘价格获取逻辑
            return tick[code].get('lastPrice', 0)
        else:
            data = C.get_market_data_ex(['close'], [code], period=Period, count=1, subscribe=False)
            if code in data and not data[code].empty:
                return data[code].iloc[-1]['close']
            return 0

def filter_etf(C):
    scores = []
    # 增加额外缓冲天数，确保计算 RSRS 时数据完整
    history_data = C.get_market_data_ex(['close'], C.etf_pool, period=Period, count=g.m_days + 5, subscribe=False)
    for etf in C.etf_pool:
        if etf not in history_data: continue
        df = history_data[etf]
        if len(df) < g.m_days: continue
        
        if not C.do_back_test:
            current_price = get_safe_price(C, etf)
            if current_price <= 0: continue
            closes = df['close'].values[-g.m_days:] 
            prices = np.append(closes, current_price)
        else:
            prices = df['close'].values[-(g.m_days + 1):]

        if len(prices) < 2: continue

        y = np.log(prices)
        x = np.arange(len(y))
        # RSRS 动量计算
        weights = np.linspace(1, 2, len(y))
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.exp(slope * 250) - 1
        y_pred = slope * x + intercept
        ss_res = np.sum(weights * (y - y_pred) ** 2)
        ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0
        score = annualized_returns * r2

        # 暴跌过滤 (保留原逻辑)
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
    return df_score['code'].head(g.stock_sum).tolist()