#encoding:gbk
import datetime
import math
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

# ====================================================================
# 【全局配置】(使用您提供的 ST 策略配置)
# ====================================================================

# 全局状态存储器
class G():pass
g = G()
g.stock_sum = 1       
g.m_days = 25         
g.min_money = 500  
g.switch_threshold = 0.1 # 换仓阈值：新标的得分需超过当前持仓 10% 才换仓
g.today_target_positions = {}

# 账户和Webhook配置
# 模拟账号
# ACCOUNT = '620000204906'
# 李慕凡 实盘
# ACCOUNT = '170100005993'
# 李浩的实盘
ACCOUNT = '190200026196'

HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=599439e6-4132-48b6-a05a-c1fbb32e33d8'

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
    def __init__(self, execution_time):
        self.last_executed = None
        self.execution_time = self._parse_time(execution_time)
    
    def _parse_time(self, time_str):
        """将HH:MM格式字符串转换为time对象"""
        try:
            return datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError("Invalid time format, use HH:MM")

# ===================== 以下为工具函数 ************************ 
class DailyTask(ScheduledTask):
    """每日任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = self.last_executed != current_dt.date()
        should = should1 and should2
        # 当前时间已过执行时间 且 当日未执行
        return should

class TaskRunner:
    def __init__(self, context):
        self.daily_tasks = []
        self.weekly_tasks = []
        self.context = context

    def run_daily(self, time_str, task_func):
        """注册每日任务
        Args:
            time_str: 触发时间 "HH:MM"
            task_func: 任务函数
        """
        print(task_func, 'task_func')
        self.daily_tasks.append( (DailyTask(time_str), task_func) )
    
    def check_tasks(self, bar_time):
        """在handlebar中调用检查任务
        Args:
            bar_time: K线结束时间(datetime对象)
        """
        # 处理每日任务
        for task, func in self.daily_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                task.last_executed = bar_time.date()
        
        # 处理每周任务
        for task, func in self.weekly_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                week_num = bar_time.isocalendar()[1]
                task.last_executed = f"{bar_time.year}-{week_num}"  # (year, week)

        
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
    # 生成唯一订单标识，防止重复单号导致委托失败
    import random
    unique_id = f"{remark}_{int(time.time())}_{random.randint(100, 999)}"
    
    print(f"【准备下单】Op: {op_type}, Code: {code}, Price: {price}, Vol: {volume}, ID: {unique_id}")

    # 假设 23=买入, 24=卖出, 7=限价，5=最新价
    if op_type == 23: # 买入
        try:
            # 1101 表示按股数下单
            passorder(23, 1101, C.account_id, code, 5, -1, volume, remark, 1, unique_id, C)
            messager.send_message(f"【实盘交易】执行 {remark}: {code}, 价格: {price:.2f}, 数量: {volume}")
        except Exception as e:
            print(f'买入股票(实盘)失败: {e}')
            messager.send_message(f"【下单失败】买入 {code} 异常: {e}")
        
    elif op_type == 24: # 卖出
        try:
            # 修正：原代码使用 1123 (按持仓比例)，但传入的是股数 volume。
            # 应改为 1101 (按股数下单)，与买入保持一致。
            passorder(24, 1101, C.account_id, code, 6, price, volume, remark, 1, unique_id, C)
            messager.send_message(f"【实盘交易】执行 {remark}: {code}, 价格: {price:.2f}, 数量: {volume}")
        except Exception as e:
            print(f'卖出股票(实盘)失败: {e}')
            messager.send_message(f"【下单失败】卖出 {code} 异常: {e}")

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

def get_strategy_asset(C):
    """计算策略可用总资产（扣除不在ETF池中的手动持仓市值）"""
    total_asset = get_total_asset(C)
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    manual_holdings_value = 0.0
    
    for pos in positions:
        code = codeOfPosition(pos)
        if code not in C.etf_pool:
            manual_holdings_value += pos.m_dMarketValue
            
    strategy_asset = total_asset - manual_holdings_value
    return strategy_asset

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
    print(f"当前运行模式: {'回测' if C.do_back_test else '实盘'}")
    
    # 初始化时间戳
    C.currentTime = 0
    C.today = datetime.now().date()
    C.yesterday = get_previous_trading_day(C, C.today).strftime("%Y%m%d")

    C.etf_pool = [    
        # 境外
        "513100.SH",  # 纳指ETF
        "513520.SH",  # 日经ETF
        "513030.SH",  # 德国ETF
        # 商品
        "518880.SH",  # 黄金ETF
        "159980.SZ",  # 有色ETF
        "159985.SZ",  # 豆粕ETF
        "501018.SH",  # 南方原油
        # 国内
        "513130.SH",  # 恒生科技
        "510180.SH", # 上证180
        "159915.SZ", # 创业板
        "588120.SH", # 科创100
        # 主题
        "512290.SH", # 生物医药
        "515070.SH", # 人工智能
        "159851.SZ", # 金融科技
        "159637.SZ", # 新能源车
        "159550.SZ", # 互联网
        "512710.SH", # 军工
        "159692.SZ", # 证券
        "562500.SH", # 机器人
    ]

    # 判断当前日期是否为非交易日 (仅实盘需要主动过滤)
    if not C.do_back_test and not is_trading(C):
        print('当前非交易时间，不执行任务')
        return

    # 【实盘/回测环境兼容调度】
    if C.do_back_test:
        print('doing test - 注册回测任务')
        # 回测中使用 TaskRunner 的精确时间模拟
        C.runner.run_daily("11:00", execute_sell_logic)
        C.runner.run_daily("11:05", execute_buy_logic)
        C.runner.run_daily("14:59", log_position)
        
    else:
        print('doing live - 注册实盘任务')
        # 实盘中使用平台 run_time 接口
        C.run_time("execute_sell_logic","1nDay","2025-12-01 11:00:00","SH")
        C.run_time("execute_buy_logic","1nDay","2025-12-01 11:03:00","SH")
        C.run_time("filter_etf","1nDay","2025-12-01 14:57:00","SH")
        C.run_time("log_position","1nDay","2025-12-01 15:00:00","SH")

    print("策略初始化完成，已设置为分步调仓模式")
    

def handlebar(C):
    """
    【健壮性模块 2 延伸：回测日期推进】
    在回测模式下，使用 handlebar 来推进 C 中的日期，
    并触发 TaskRunner 检查定时任务。
    """
    index = C.barpos
    currentTime = C.get_bar_timetag(index) + 8 * 3600 * 1000
    try:
        if C.currentTime < currentTime:
            C.currentTime = currentTime
            C.today = pd.to_datetime(currentTime, unit='ms')
            C.now = pd.to_datetime(currentTime, unit='ms')
    except Exception as e:
        print('handlebar异常', currentTime, e)

    if (datetime.now() - timedelta(days=1) > C.today) and not C.do_back_test:
        # print('非回测模式，历史不处理')
        return
    else:
        if C.do_back_test:
            # 新增属性，快捷获取当前日期
            index = C.barpos
            currentTime = C.get_bar_timetag(index) + 8 * 3600 * 1000
            # print('当前时间', currentTime)
            C.currentTime = currentTime
            C.today = pd.to_datetime(currentTime, unit='ms')
            C.now = pd.to_datetime(currentTime, unit='ms')
            current_dt = datetime.fromtimestamp(currentTime / 1000)
            yesterday_dt = get_previous_trading_day(C, current_dt.date())
            yesterday = yesterday_dt.strftime("%Y%m%d")
            C.yesterday = yesterday
            C.current_dt = current_dt
            # 检查并执行任务
            C.runner.check_tasks(C.today)

# -------------------- 拆分后的核心逻辑 (已集成消息推送和交易兼容) --------------------

def execute_sell_logic(C):
    """
    第一阶段：计算信号 并 执行卖出
    """
    current_time = C.now
    print(f"[{current_time}] 阶段1: 开始计算信号并执行卖出...")
    messager.send_message(f"【ETF轮动-信号计算】开始执行卖出逻辑 @ {current_time}")
    
    # 1. 筛选目标ETF
    target_list = filter_etf(C)
    print("今日选中目标:", target_list)
    messager.send_message(f"【ETF轮动-信号计算】今日选中目标: {target_list}, {[C.get_stock_name(code) for code in target_list]}")
    
    # 2. 计算目标持仓金额
    target_positions = {}
    # 使用策略专用资产计算（扣除手动持仓）
    strategy_asset = get_strategy_asset(C)

    print(f"当前账户总资产: {get_total_asset(C):.2f}, 策略可用资产: {strategy_asset:.2f}")
    
    if strategy_asset > 0 and target_list:
        per_value = strategy_asset / len(target_list)
        for code in target_list:
            target_positions[code] = per_value
    
    g.today_target_positions = target_positions
    print("今日目标持仓:", target_positions)
    
    # 3. 执行卖出操作
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    hold_list = [codeOfPosition(position) for position in positions if position.m_dMarketValue > 10000]
    print("当前有仓位的持仓:", hold_list)
    
    current_holdings = {obj.m_strInstrumentID + '.' + obj.m_strExchangeID: obj for obj in positions}
    # 如果已经持仓，且持仓比例不小于目标持仓比例，不需要执行后续逻辑

    for code in list(current_holdings.keys()):
        # 忽略不在ETF池中的手动持仓
        if code not in C.etf_pool:
            print(f"{code} 非策略标的(手动持仓)，忽略")
            continue

        if code in target_list: 
            print(f"{code} 继续持仓")
            continue

        pos_obj = current_holdings[code]
        price = get_safe_price(C, code)
        if price <= 0 or pos_obj.m_nCanUseVolume <= 0: continue

        current_market_value = pos_obj.m_nCanUseVolume * price
        target_val = g.today_target_positions.get(code, 0.0)
        
        if current_market_value > target_val:
            diff = current_market_value - target_val
            # 最小交易金额或1手市值
            min_trade_amount = max(g.min_money, price * 100) 
            
            if diff > min_trade_amount:
                # 计算卖出量，按100股取整
                target_vol = int(target_val / price / 100) * 100
                vol_to_sell = pos_obj.m_nCanUseVolume - target_vol
                
                print(f"【卖出检查】{code} 可用: {pos_obj.m_nCanUseVolume}, 目标持仓: {target_vol}, 需卖: {vol_to_sell}, 价格: {price}")
                
                if vol_to_sell > 0:
                    messager.send_message(f"【ETF轮动-卖出】{code} 数量: {vol_to_sell}，目标市值: {target_val:.2f}")
                    if C.do_back_test:
                        # 回测模式：按目标市值调仓
                        order_target_value_test(C, code, target_val) 
                    else:
                        # 实盘模式：限价卖出
                        # 简化拆单逻辑：超过100万股才分两笔委托
                        if vol_to_sell >= 1000000:
                            vol1 = int(vol_to_sell / 2 / 100) * 100
                            vol2 = vol_to_sell - vol1
                            
                            print(f"分两笔卖出: {vol1} + {vol2}")
                            passorder_live(C, 24, code, price, vol1, "ETF_SELL_TUNE_1")
                            time.sleep(0.2)
                            passorder_live(C, 24, code, price, vol2, "ETF_SELL_TUNE_2")
                        else:
                            passorder_live(C, 24, code, price, vol_to_sell, "ETF_SELL_TUNE")

def execute_buy_logic(C):
    """
    第二阶段：执行买入
    """
    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"[{current_time}] 阶段2: 资金应已回笼，开始执行买入...", g.today_target_positions)
    messager.send_message(f"【ETF轮动-交易】开始执行买入逻辑 @ {current_time}")
    
    target_positions = g.today_target_positions
    if not target_positions:
        messager.send_message("今日无买入目标")
        return

    # 实盘中先撤单 (确保资金不会被占用)
    if not C.do_back_test:
        cancel_unfilled_orders(C)

    # 重新获取持仓信息
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    current_holdings = {obj.m_strInstrumentID + '.' + obj.m_strExchangeID: obj for obj in positions}
    
    # 重新计算目标市值（修复资金不足bug）
    # 使用策略专用资产计算
    strategy_asset = get_strategy_asset(C)
    target_list = list(target_positions.keys())
    per_value = strategy_asset / len(target_list) if target_list else 0
    print(f"买入阶段重新计算: 策略总资产={strategy_asset:.2f}, 单标的={per_value:.2f}")

    for code in target_list:
        target_val = per_value - 1000 # 留1000块buffer，防止资金不足
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
                        # 实盘模式：限价拆单买入
                        vol1 = int(vol_to_buy / 2 / 100) * 100
                        vol2 = vol_to_buy - vol1 - 10000 # 这里留2000的buffer
                        vol3 = 6000
                        
                        print(f"分3笔买入: {vol1} + {vol2} + {vol3}")
                        passorder_live(C, 23, code, price, vol1, "ETF_BUY_TUNE_1")
                        time.sleep(0.2)
                        if vol2 > 0:
                            passorder_live(C, 23, code, price, vol2, "ETF_BUY_TUNE_2")
                        time.sleep(0.2)
                        passorder_live(C, 23, code, price, vol3, "ETF_BUY_TUNE_3")

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
        print("********** 收盘持仓信息打印开始 **********")
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
    ticksData = C.get_market_data_ex(
        [],                
        C.etf_pool,
        period="1m",
        start_time = (C.today - timedelta(days=1)).strftime('%Y%m%d%H%M%S'),
        end_time = C.today.strftime('%Y%m%d%H%M%S'),
        count=1,
        dividend_type = "follow",
        fill_data = False,
        subscribe = True
    )
    if code in ticksData and not ticksData[code].empty:
        return round(ticksData[code]["close"].iloc[0], 2)
    return 9999


def filter_etf(C):
    scores = []
    
    # 1. 确定当前时间对象
    if C.do_back_test:
        current_dt = datetime.fromtimestamp(C.currentTime / 1000)
    else:
        current_dt = datetime.now()
        
    # 计算昨天的日期字符串（用于实盘获取纯历史数据）
    yesterday_dt = get_previous_trading_day(C, current_dt.date())
    yesterday_str = yesterday_dt.strftime("%Y%m%d")
    today_str = current_dt.strftime("%Y%m%d")

    print(f"【排查日志】当前计算时间: {current_dt}, 历史数据截止(实盘用): {today_str}")

    # 2. 批量获取数据（区分回测和实盘策略）
    # 聚宽逻辑核心：m_days 的历史 + 1 个当前最新价 = 总长度 m_days + 1

    history_data = C.get_market_data_ex(
        ['close'], C.etf_pool, period=Period, 
        start_time='', end_time=yesterday_str, count=g.m_days + 10, 
        fill_data=False, subscribe=True
    )
    # print(f"【排查日志-1】获取到的历史数据: {history_data}")

    # 3. 逐个ETF计算
    for etf in C.etf_pool:
        if etf not in history_data:
            continue
            
        df = history_data[etf]
        
        # 实盘下，df 只包含截止到“昨天”的数据
        if len(df) < g.m_days:
            print(f" > 忽略: {etf} 实盘历史数据不足")
            continue
        
        # 获取实时价格
        current_price = get_safe_price(C, etf)
        if current_price <= 0:
            print(f" > 忽略: {etf} 实时价格无效")
            continue
        
        # 拼接：历史最后 m_days + 当前价
        closes_history = df['close'].values[-g.m_days:]
        prices = np.append(closes_history, current_price)

        # print(etf, "价格list", prices)
        # 再次校验长度
        if len(prices) < 2: continue

        # --- 以下逻辑与聚宽完全一致 ---
        
        # 1. 准备数据
        y = np.log(prices)
        x = np.arange(len(y))
        weights = np.linspace(1, 2, len(y)) # 线性加权

        # 2. 计算年化收益率 (Slope)
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.exp(slope * 250) - 1

        # 3. 计算 R²
        y_pred = slope * x + intercept
        ss_res = np.sum(weights * (y - y_pred) ** 2)
        ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0

        # 4. 计算得分
        score = annualized_returns * r2

        # 5. 过滤近3日跌幅超过5%的ETF (Crash Filter)
        # prices[-1] 是当前/今日，prices[-2] 是昨日
        # 逻辑：检查 (今日/昨日), (昨日/前日), (前日/大前日)
        if len(prices) >= 4:
            drop_1 = prices[-1] / prices[-2]
            drop_2 = prices[-2] / prices[-3]
            drop_3 = prices[-3] / prices[-4]
            if min(drop_1, drop_2, drop_3) < 0.95:
                score = 0
                if not C.do_back_test: print(f" > 过滤: {etf} 触发暴跌保护")

        # 6. 加入候选列表
        if 0 < score < 6:
            scores.append({'code': etf, 'score': score})
        else:
            print(f" > 过滤: {etf} 得分异常 ({score:.4f})")

    # 4. 排序与输出
    df_score = pd.DataFrame(scores)
    
    if df_score.empty:
        return []
    
    # 按得分降序排列
    df_score = df_score.sort_values(by='score', ascending=False)
    
    if not C.do_back_test:
        # 格式化输出 所有ETF得分
        msg = "【得分预览】\n"
        for _, row in df_score.iterrows():
            stock_name = C.get_stock_name(row['code'])
            msg += f"{stock_name} - {row['score']:.3f}\n"
        messager.send_message(msg)
        
    print(f"【排查日志-2】计算得到的得分: {df_score}")
    # --- 防抖逻辑 ---
    # 获取当前持仓
    positions = get_trade_detail_data(C.account_id, 'stock', 'position')
    current_holdings = [codeOfPosition(obj) for obj in positions]
    
    # 找到当前持仓中属于ETF池的标的
    held_etf = None
    for h in current_holdings:
        if h in C.etf_pool:
            held_etf = h
            break
            
    if held_etf and not df_score.empty:
        # 获取第一名
        top_etf = df_score.iloc[0]['code']
        top_score = df_score.iloc[0]['score']
        
        # 如果第一名不是当前持仓
        if top_etf != held_etf:
            # 检查当前持仓的得分
            held_row = df_score[df_score['code'] == held_etf]
            if not held_row.empty:
                held_score = held_row.iloc[0]['score']
                
                # 如果第一名得分没有超过当前持仓得分的 (1 + threshold) 倍
                if top_score < held_score * (1 + g.switch_threshold):
                    if not C.do_back_test:
                        print(f"【防抖动】当前持仓 {held_etf} (分:{held_score:.4f}) vs 第一名 {top_etf} (分:{top_score:.4f})")
                        print(f" > 优势不足 {g.switch_threshold*100}%，保持持仓")
                    return [held_etf]

    final_list = df_score['code'].head(g.stock_sum).tolist()
    return final_list



def orderError_callback(context, orderArgs, errMsg):
    messager.send_message(f"下单异常回调，订单信息{orderArgs}，异常信息{errMsg}")
    

# 前置增加开盘检测
def is_trading(ContextInfo):
    return ContextInfo.get_instrumentdetail('600000.SH')['IsTrading'] or ContextInfo.get_instrumentdetail('600036.SH')['IsTrading'] or ContextInfo.get_instrumentdetail('600519.SH')['IsTrading']