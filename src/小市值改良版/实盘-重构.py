
#coding:gbk

# 回测用的版本


import json
import time as nativeTime
import uuid
from datetime import datetime, time, timedelta
from typing import Any, List

import numpy as np
import pandas as pd
import requests

# ================ 设置账号 ================
# 腾腾实盘
MY_ACCOUNT = "190200051469"
# 我的模拟
# MY_ACCOUNT = "620000204906"

class G():pass
g = G()
# ================ 全局状态存储器 ================
g.cache_file = 'stock_list_cache.txt'   # 缓存的文件地址，桌面上的cache_list.txt
g.window = 7                # 监控基差 7日窗口


def init(context: Any):
    # 初始化策略环境及参数
    strategy.initialize(context)
    
    context.storage = Storage(context)
    target_list = context.storage.getStorage('target_list')
    if target_list:
        context.cache_target_list = target_list
        print(f"成功从缓存读取 target_list: {len(target_list)}只")
    
    context.runner = TaskRunner(context)
    messager.set_is_test(context.do_back_test)
    
    # 调试代码，实盘调试，慎用！！！！
    # testRunBuy(context)

    # 注册调度任务，所有任务均使用顶层包装函数（不使用 lambda 以确保可序列化）    
    # 判断当前日期是否为周末，如果是则直接返回
    if context.tm.weekday >= 5 and not context.do_back_test:  # 5表示周六，6表示周日
        print('当前日期为周末，不执行任务')
        return

    # 实盘和回测不一样的地方在于，可以使用run_time函数，不需要等到盘中才执行定时逻辑，因此部分逻辑执行时间可以前置
    if context.do_back_test:
        # -------------- 回测 -----每日执行任务 --------------------------------
        # 9am 检查昨日持仓
        context.runner.run_daily("9:35", check_holdings_yesterday_func)
        # 9:05am 准备股票列表
        context.runner.run_daily("9:40", prepare_stock_list_func)
        # 9:30 am 检查是否需要逃顶清空遗留仓位
        context.runner.run_daily("9:42", check_escape_top_position_func)
        # 10:00 am 止盈止损检测
        context.runner.run_daily("10:00", sell_stocks_func)
        
        # 14:30 pm 检查需要卖出的持仓
        context.runner.run_daily("14:30", trade_afternoon_func)
        # 14:50 pm 检查当日是否需要一键清仓
        context.runner.run_daily("14:50", close_account_func)    
        # 15:05 pm 每日收盘后打印一次持仓
        context.runner.run_daily("14:59", print_position_info_func)
        # -------------------每周执行任务 --------------------------------
        # 每周做一次调仓动作
        context.runner.run_weekly(1, "10:30", weekly_adjustment_func)
        # 每周调仓后买入股票
        context.runner.run_weekly(1, "10:35", weekly_adjustment_buy_func)
    else:
        # -------------- 实盘 -----每日执行任务 --------------------------------
        # 9am 检查昨日持仓
        context.run_time("check_holdings_yesterday_func","1nDay","2025-03-01 09:15:00","SH")
        # 9:05am 准备股票列表
        context.run_time("prepare_stock_list_func","1nDay","2025-03-01 09:20:00","SH")
        # 9:30 am 检查是否需要逃顶清空遗留仓位
        context.run_time("check_escape_top_position_func","1nDay","2025-03-01 09:30:00","SH")
        # 9:35 am 止盈止损检测
        context.run_time("sell_stocks_func","1nDay","2025-03-01 09:35:00","SH")
        # 14:30 pm 检查涨停破板，需要卖出的持仓
        context.run_time("trade_afternoon_func","1nDay","2025-03-01 14:30:00","SH")
        # 14:50 pm 检查当日是否到达空仓日，需要一键清仓
        context.run_time("close_account_func","1nDay","2025-03-01 14:50:00","SH")
        # 15:05 pm 每日收盘后打印一次持仓
        context.run_time("print_position_info_func","1nDay","2025-03-01 15:05:00","SH")
        # 15:10 pm 每日收盘后打印一次候选股票池
        context.run_time("log_target_list_info","1nDay","2025-03-01 15:10:00","SH")
        
        # -------------------每周执行任务 --------------------------------
        # 09:40 am 每周做一次调仓动作，尽量早，流动性充足
        context.run_time("weekly_adjustment_func","7nDay","2025-05-08 09:40:00","SH")
        # 09:50 am 每周调仓后买入股票
        context.run_time("weekly_adjustment_buy_func","7nDay","2025-05-08 09:50:00","SH")


class TradingStrategy:
    """
    交易策略类

    封装了选股、调仓、买卖、止损与风控管理的核心逻辑。
    通过类属性管理持仓、候选股票等状态，并使用状态机字典记录交易信号，
    便于后续调试、扩展和维护。
    """
    def __init__(self):
        # 策略基础配置和状态变量
        self.no_trading_today_signal: bool = False  # 【慎用！！！快捷平仓选项】当天是否执行空仓（资金再平衡）操作
        self.pass_april: bool = True                # 是否在04月或01月期间执行空仓策略
        self.run_stoploss: bool = False              # 是否启用止损策略

        # 持仓和调仓记录
        self.hold_list: List[str] = []             # 当前持仓股票代码列表
        self.yesterday_HL_list: List[str] = []       # 昨日涨停的股票列表（收盘价等于涨停价）
        self.target_list: List[str] = []             # 本次调仓候选股票列表
        self.not_buy_again: List[str] = []           # 当天已买入的股票列表，避免重复下单
        self.notified_codes: set = set()             # 当天已通知的股票代码列表，避免重复通知

        # 策略交易及风控的参数
        self.stock_num: int = 10                    # 每次调仓目标持仓股票数量
        self.up_price: float = 100.0               # 股票价格上限过滤条件（排除股价超过此值的股票）
        self.reason_to_sell: str = ''              # 记录卖出原因（例如：'limitup' 涨停破板 或 'stoploss' 止损）
        self.stoploss_strategy: int = 1            # 止损策略：1-个股止损；2-大盘止损；3-联合止损策略
        self.stoploss_limit: float = 0.88          # 个股止损阀值（成本价 × 0.88）
        self.stoploss_market: float = -0.94         # 大盘止损参数（若整体跌幅过大则触发卖出）
        
        self.pool = []
        self.pool_initialized = False

    def initialize(self, context: Any):
        """
        策略初始化函数

        配置交易环境参数，包括防未来数据、基准、滑点、订单成本以及日志输出等级。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # 注意：调度任务由全局包装函数统一注册，避免 lambda 导致序列化问题
        context.account = MY_ACCOUNT
        context.set_account(context.account)
        context.accountType = ""
        
        # 初始化时间管理器
        context.tm = TimeManager(context)
        
        # 兼容旧代码，同步时间变量
        context.currentTime = context.tm.timestamp
        context.today = context.tm.now

    # 根据股票代码和收盘价，计算次日涨跌停价格
    def get_limit_of_stock(self, stock_code, last_close):
        if str(stock_code).startswith(tuple(['3', '688'])):
            return [round(last_close * 1.2, 2), round(last_close * 0.8, 2)]
        return [round(last_close * 1.1, 2), round(last_close * 0.9, 2)]
    
    # 根据股票代码，查询公司总市值
    def get_market_cup(self, context, code):
        data = context.get_instrumentdetail(code)
        if data:
            TotalVolumn = data['TotalVolumn'] # 总股本
            price = data["PreClose"]
            if price and TotalVolumn:
                res = price * TotalVolumn
            else:
                return False
            return res

    def check_holdings_yesterday(self, context: Any):
        """
        检查并输出每只持仓股票昨日的交易数据（开盘价、收盘价、涨跌幅）。

        此方法只做了日志打印，因此初始版本不要也罢，后续再完善。
        """
        # 每日初始化已通知列表
        self.notified_codes.clear()
        
        # 这里给context挂一个positions持仓对象，仅盘前可以复用，盘中要实时取数据不能使用这个
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')

        if not self.positions:
            print("昨日没有持仓数据。")
            if not context.do_back_test:
                messager.sendMsg("今日交易已开始。昨日没有持仓数据。")
                messager.send_account_info(context)
            return

        if not context.do_back_test:
            messager.send_positions(context)
            messager.send_account_info(context)


    # 通用方法，返回给定list里昨日涨跌停的股票
    def find_limit_list(self, context, stock_list):
        high_list = []
        low_list = []
        if stock_list:
            data = context.get_market_data_ex(
                ['open', 'close'],                
                stock_list,
                period="1d",
                start_time = context.tm.get_past_date(14),
                end_time = context.tm.get_past_date(1),
                count=2,
                dividend_type = "follow",
                fill_data = False,
                subscribe = True
            )
            for stock in data:
                try:
                    df = data[stock]
                    df['pre'] = df['close'].shift(1)
                    df['high_limit'] = self.get_limit_of_stock(stock, df['pre'])[0]
                    df['low_limit'] = self.get_limit_of_stock(stock, df['pre'])[1]
                    df['is_down_to_low_limit'] = df['close'] == df['low_limit']
                    df['is_up_to_hight_limit'] = df['close'] == df['high_limit']
                    # 是否涨停
                    if df['is_up_to_hight_limit'].iloc[-1]:
                        high_list.append(stock)
                    # 是否跌停
                    if df['is_down_to_low_limit'].iloc[-1]:
                        low_list.append(stock)
                except:
                    print(f"股票{stock}涨跌停排查异常, 昨日数据：{df}")

        dic = {}
        dic['high_list'] = high_list
        dic['low_list'] = low_list
        return dic

    def prepare_stock_list(self, context: Any):
        """
        更新持仓股票列表和昨日涨停股票列表，同时判断是否为空仓日（资金再平衡日）。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        print("更新持仓股票列表和昨日涨停股票列表")
        # 根据当前日期判断是否为空仓日（例如04月或01月时资金再平衡）
        self.no_trading_today_signal = self.today_is_between(context)
        # 从当前持仓中提取股票代码，更新持仓列表
        if self.positions:
            self.hold_list = [self.codeOfPosition(position) for position in self.positions]
            print("持仓:", self.hold_list)
            # 取出涨停列表
            self.yesterday_HL_list = self.find_limit_list(context, self.hold_list)['high_list']
            print("昨日涨停:", self.yesterday_HL_list)
            messager.sendLog(f"昨日涨停股票: {self.yesterday_HL_list}")

    # 【回测时使用】回测初始状态跑一遍当时的市值前200名股票，之后都在这200只里选择，为了优化性能（取市值时只能跑全量最新价格，非常费性能）
    def get_stock_pool_when_test(self, context: Any):
        whole_list = context.get_stock_list_in_sector('中小综指')
        list = self.sort_by_market_cup(context, whole_list)
        self.pool = list[:100]
        self.pool_initialized = True
        return self.pool

    # 正常来说，是每次都从中小板取所有股票来筛选，但是回测性能太差，只用于实盘    
    def get_stock_pool(self, context: Any):
        return context.get_stock_list_in_sector('中小综指')

    # Position的完整品种代码
    def codeOfPosition(self, position):
        return position.m_strInstrumentID + '.' + position.m_strExchangeID
    
    def sort_by_market_cup(self, context, origin_list):
        ticks = context.get_market_data_ex(
            ['close'],                
            origin_list,
            period="1d",
            start_time = context.tm.get_past_date(1),
            end_time = context.tm.date_str,
            count=1,
            dividend_type = "follow",
            fill_data = False,
            subscribe = True
        )
        df_result = pd.DataFrame(columns=['code','name', 'lastPrice', 'market_cap', 'stock_num'])
        end_date = context.tm.date_str
        start_date = context.tm.get_past_date(365)
        eps = context.get_raw_financial_data(['利润表.净利润', '利润表.营业收入', '股本表.总股本'], origin_list, start_date, end_date)
        for code in origin_list:
            finance_list = list(eps[code]['利润表.净利润'].values())
            income_list = list(eps[code]['利润表.营业收入'].values())
            stock_num_list = list(eps[code]['股本表.总股本'].values())
            if finance_list and income_list and stock_num_list:
                finance = finance_list[-1]
                income = income_list[-1]
                stock_num = stock_num_list[-1]
            try:
                market_cap = ticks[code].iloc[0, 0] * stock_num
                if code in list(ticks.keys()) and market_cap >= 1000000000: # 最小也要超过10e
                    df_result = df_result.append({
                        'code': code,
                        'name': context.get_stock_name(code),
                        'market_cap': market_cap,
                        'lastPrice': ticks[code].iloc[0, 0],
                        'stock_num': stock_num
                        }, ignore_index=True)
            except Exception:
                # continue
                print(code, ticks[code])
        df_result = df_result.sort_values(by='market_cap', ascending=True)
        return list(df_result['code'])


    # 基本面选股：根据国九条，过滤净利润为负且营业收入小于1亿的股票
    def filter_stock_by_gjt(self, context, initial_list):
        print('开始每周选股环节（基本面初筛） =====================>')
        
        end_date = context.tm.date_str
        start_date = context.tm.get_past_date(365)
        eps = context.get_raw_financial_data(['利润表.净利润', '利润表.营业收入', '股本表.总股本', '利润表.截止日期'], initial_list, start_date, end_date)
        
        if eps is None:
            print("未获取到财务数据，跳过本次选股")
            return []

        df_result = pd.DataFrame(columns=['code', 'name', 'market_cap', 'lastPrice', 'stock_num'])
        
        ticks = context.get_market_data_ex(
            ['close'],                
            initial_list,
            period="1d",
            start_time = context.tm.get_past_date(12),
            end_time = context.tm.date_str,
            count=1,
            dividend_type = "follow",
            fill_data = False,
            subscribe = True
        )
        # 选不出来股的时候，这个注释打开看看有没有数
        # print(ticks, '看看tocks')
        for code in initial_list:
            # 1. 初始化变量，防止沿用上一只股票的数据
            finance = 0
            income = 0
            stock_num = 0

            # 2. 检查行情数据是否存在
            if code not in ticks or ticks[code] is None or ticks[code].empty:
                continue

            # 3. 检查基本面数据是否存在
            if code not in eps or eps[code] is None:
                continue

            # 基本面筛选，去年净利润大于1e，营业收入大于1e
            try:
                finance_list = list(eps[code]['利润表.净利润'].values())
                income_list = list(eps[code]['利润表.营业收入'].values())
                stock_num_list = list(eps[code]['股本表.总股本'].values())
                
                if finance_list and income_list and stock_num_list:
                    finance = finance_list[-1]
                    income = income_list[-1]
                    stock_num = stock_num_list[-1]
                else:
                    continue

                # 筛选出净利润大于0，营业收入大于1e的股票
                if finance > 0 and income > 100000000:
                    try:
                        # 获取公告日期（key）和统计日期（value）
                        pub_date = list(eps[code]['利润表.净利润'].keys())[-1]
                        stat_date = list(eps[code]['利润表.截止日期'].values())[-1] if '利润表.截止日期' in eps[code] and eps[code]['利润表.截止日期'] else '未知'
                        
                        finance_str = f"{finance/100000000:.2f}亿" if abs(finance) > 100000000 else f"{finance/10000:.2f}万"
                        income_str = f"{income/100000000:.2f}亿" if abs(income) > 100000000 else f"{income/10000:.2f}万"
                        
                        print(f"股票: {code} ({context.get_stock_name(code)}) | 公告日期: {pub_date} | 统计日期: {stat_date} | 净利润: {finance_str} | 营收: {income_str}")
                    except Exception as e:
                        print(f"打印财务信息出错 {code}: {e}")

                    market_cap = ticks[code].iloc[0, 0] * stock_num
                    df_result = df_result.append({
                        'code': code,
                        'name': context.get_stock_name(code),
                        'market_cap': market_cap,
                        'lastPrice': ticks[code].iloc[0, 0],
                        'stock_num': stock_num
                        }, ignore_index=True)
            except Exception as e:
                print(f"股票{code}基本面筛查异常: {e}")

        df_result = df_result.sort_values(by='market_cap', ascending=True)  
        # 缓存df对象，方便查询某只股票数据
        context.stock_df = df_result
        stock_list = list(df_result.code)
        # print("看看前20的股票", df_result[:20])
        return stock_list
    
    # 定期获取目标股票列表
    def internal_get_target_list(self, context: Any):
        # 缓存一条离线target_list，调仓日会拿实时数据与之比较，当有较多股票不一致时，发送警告给我
        context.cache_target_list = self.get_stock_list(context)
        messager.sendLog("离线调仓数据整理完毕，目标持股列表如下" )
        self.log_target_list(context, context.cache_target_list)

    def get_stock_list(self, context: Any):
        """
        选股模块：
        1. 从指定股票池（如 399101.XSHE 指数成分股）中获取初步股票列表；
        2. 应用多个过滤器筛选股票（次新股、科创股、ST、停牌、涨跌停等）；
        3. 基于基本面数据（EPS、市值）排序后返回候选股票列表。

        返回:
            筛选后的候选股票代码列表
        """
        # fromCache logic removed
        print('开始每周选股环节 =====================>')
        # 从指定指数中获取初步股票列表
        # 不每次取全量数据，这里首次
        if self.pool:
            initial_list = self.pool
        else:
            initial_list = self.get_stock_pool(context)
            
        initial_list = self.filter_kcbj_stock(initial_list)             # 过滤科创/北交股票
        
        # 依次应用过滤器，筛去不符合条件的股票
        initial_list = self.filter_new_stock(context, initial_list)   # 过滤次新股
        initial_list = self.filter_st_stock(context, initial_list)    # 过滤ST或风险股票
        initial_list = self.filter_paused_stock(context, initial_list)           # 过滤停牌股票
        
        initial_list = self.filter_stock_by_gjt(context, initial_list)             # 过滤净利润为负且营业收入小于1亿的股票
        
        initial_list = initial_list[:100]  # 限制数据规模，防止一次处理数据过大
        # 性能不好，回测不开
        initial_list = self.filter_limitup_stock(context, initial_list)   # 过滤当日涨停（未持仓时）的股票
        initial_list = self.filter_limitdown_stock(context, initial_list) # 过滤当日跌停（未持仓时）的股票
        
        # 取前2倍目标持仓股票数作为候选池
        final_list: List[str] = initial_list[:2 * self.stock_num]

        # TODO 增加更多选股因子：30日均成交量（流动性），涨停基因（1年内有过>5次涨停记录）

        print(f"候选股票{len(final_list)}只: {final_list}")

        context.storage.setStorage('target_list', final_list)

        return final_list

    def find_target_stock_list(self, context):
        self.target_list = self.get_stock_list(context)        
        target_list: List[str] = self.target_list[:self.stock_num]
        print('今日股票池:', target_list)
        for code in target_list:
            print(context.get_stock_name(code))

    def log_target_list(self, context: Any, stock_list: List[str]):
        """
        打印目标股票列表信息，用于人工确认程序无误（有时候平台接口抽风，选出来的股票并非小市值）。
        """
        print("***** 目标股票池信息如下：******")
        msg = ""
        for code in stock_list:
            if not context.stock_df[context.stock_df['code'] == code].empty:
                market_cap = context.stock_df[context.stock_df['code'] == code]['market_cap'].iloc[0] / 100000000
            else:
                market_cap = None  # 或其他默认值
            msg += f"股票代码：{code}，股票名称：{context.get_stock_name(code)}, 市值：{market_cap:.2f}\n"
        messager.sendLog(msg)


    def weekly_adjustment_select(self, context: Any):
        """
        每周调仓策略 - 选股阶段：
        如果非空仓日，选股得到目标股票列表，计算需买入和卖出的股票，并发送告警。
        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions if position.m_dMarketValue > 10000]
        print(self.no_trading_today_signal, '禁止交易信号')
        if not self.no_trading_today_signal:
            messager.sendLog("开始每周调仓 - 选股")
            self.target_list = self.get_stock_list(context)
            # 取目标持仓数以内的股票作为调仓目标
            target_list: List[str] = self.target_list[:self.stock_num]
            self.target_list = target_list
            print(f"每周调仓目标股票: {target_list}")
            self.log_target_list(context, target_list)
            print(f"当前持有股票: {self.hold_list}")
            
            # 计算调仓数量并发送告警
            stocks_to_sell = [stock for stock in self.hold_list if stock not in target_list and stock not in self.yesterday_HL_list]
            stocks_to_buy = [stock for stock in target_list if stock not in self.hold_list]
            self.stocks_to_buy = stocks_to_buy
            adjustment_count = len(stocks_to_sell) + len(stocks_to_buy)
            if adjustment_count > 3:
                # 显示具体股票代码而非仅数量
                alert_msg = f"大规模调仓警告：需调整{adjustment_count}只股票（卖出{len(stocks_to_sell)}只: {', '.join(stocks_to_sell)}, 买入{len(stocks_to_buy)}只: {', '.join(stocks_to_buy)}）"
                messager.sendLog(alert_msg)

    def weekly_adjustment_sell(self, context: Any):
        """
        每周调仓策略 - 卖出阶段：
        卖出当前持仓中不在目标列表且昨日未涨停的股票。
        """
        print('调仓卖出阶段...是否在禁止交易窗口：', self.no_trading_today_signal)
        if not self.no_trading_today_signal:
            for stock in self.hold_list:
                if stock not in self.target_list and stock not in self.yesterday_HL_list:
                    print(f"卖出股票 {stock}")
                    self.close_position(context, stock)
                else:
                    print(f"持有股票 {stock}")

    def weekly_adjustment_buy(self, context: Any):
        """
        每周调仓策略 - 买入阶段：
        对目标股票执行买入操作。
        """
        print('调仓买入阶段...是否在禁止交易窗口：', self.no_trading_today_signal)
        if not self.no_trading_today_signal:
            self.new_buy_target(context)

    def check_limit_up(self, context: Any):
        """
        检查昨日处于涨停状态的股票在当前是否破板。
        如破板（当前价格低于涨停价），则立即卖出该股票，并记录卖出原因为 "limitup"。

        """
        if self.yesterday_HL_list:
            ticksOfDay = context.get_market_data_ex(
                ['close'],                
                self.yesterday_HL_list,
                period="1d",
                start_time = context.tm.get_past_date(1),
                end_time = context.tm.date_str,
                count=2,
                dividend_type = "follow",
                fill_data = False,
                subscribe = True
            )
            print(ticksOfDay, '**持仓涨停票信息-day')
            for stock in self.yesterday_HL_list:
                try:
                    # 最新价 (使用今日日线数据作为当前价)
                    price = ticksOfDay[stock]["close"].iloc[-1]
                    # 昨日收盘价
                    lastClose = ticksOfDay[stock]["close"].iloc[0]
                    high_limit = self.get_limit_of_stock(stock, lastClose)[0]

                    if round(price, 2) < high_limit:
                        messager.sendLog(f"股票 {stock} 涨停破板，触发卖出操作。")
                        self.close_position(context, stock)
                        self.reason_to_sell = 'limitup'
                    else:
                        messager.sendLog(f"股票 {stock} 仍维持涨停状态。")
                except Exception as e:
                    print(f"股票{stock}涨停检查异常: {e}, 数据详情：{ticksOfDay.get(stock, '无数据')}")

    

    def check_remain_amount(self, context: Any):
        """
        检查账户资金与持仓数量：
        如果因涨停破板卖出导致持仓不足，则从目标股票中筛选未买入股票，进行补仓操作。

        """
        if self.reason_to_sell == 'limitup':
            if len(self.hold_list) < self.stock_num:
                target_list = self.filter_not_buy_again(self.target_list)
                target_list = target_list[:min(self.stock_num, len(target_list))]
                print(f"检测到补仓需求，候选补仓股票: {target_list}")
                self.buy_security(context, target_list)
            self.reason_to_sell = ''
        else:
            print("未检测到涨停破板卖出事件，不进行补仓买入。")

    def trade_afternoon(self, context: Any):
        """
        下午交易任务：
        1. 检查是否有因为涨停破板触发的卖出信号；
        2. 检查账户中是否需要补仓。
        """
        if not self.no_trading_today_signal:
            self.check_limit_up(context)
            self.check_remain_amount(context)

    # 获取板块的涨跌幅情况
    def get_whole_market_data(self, context):
        code = '399101.SZ'
        data = context.get_market_data_ex(
            [],                
            [code],
            period="1d",
            start_time = context.tm.date_str,
            end_time = context.tm.date_str,
            count=2,
            dividend_type = "follow",
            fill_data = False,
            subscribe = True
        )[code]
        lastPrice = data['close'][-1]
        lastClose = data['open'][-1]
        percent = round(100 * (lastPrice - lastClose) / lastClose, 2)
        return percent
        
    def sell_stocks(self, context: Any):
        """
        止盈与止损操作：
        根据策略（1: 个股止损；2: 大盘止损；3: 联合策略）判断是否执行卖出操作。
        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        if self.positions:
            print('有持仓，检查是否需要止损，当前止损策略:', self.run_stoploss, self.stoploss_strategy)
            if self.run_stoploss:
                if self.stoploss_strategy == 1:
                    # 个股止盈或止损判断
                    for stock in self.get_stock_list_of_positions(context):
                        pos = self.find_stock_of_positions(stock)
                        if pos.m_dSettlementPrice >= pos.m_dOpenPrice * 2:
                            self.close_position(context, stock)
                            print(f"股票 {stock} 实现100%盈利，执行止盈卖出。")
                        elif pos.m_dSettlementPrice < pos.m_dOpenPrice * self.stoploss_limit:
                            self.close_position(context, stock)
                            print(f"股票 {stock} 触及止损阈值，执行卖出。")
                            self.reason_to_sell = 'stoploss'
                elif self.stoploss_strategy == 2:
                    # 大盘止损判断，若整体市场跌幅过大则平仓所有股票
                    down_ratio = self.get_whole_market_data(context)
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        print(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in self.get_stock_list_of_positions(context):
                            self.close_position(context, stock)
                elif self.stoploss_strategy == 3:
                    # 联合止损策略：结合大盘和个股判断
                    down_ratio = self.get_whole_market_data(context)
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        print(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in self.get_stock_list_of_positions(context):
                            self.close_position(context, stock)
                    else:
                        for stock in self.get_stock_list_of_positions(context):
                            pos = self.find_stock_of_positions(stock)
                            if pos.m_dSettlementPrice < pos.m_dOpenPrice * self.stoploss_limit:
                                self.close_position(context, stock)
                                print(f"股票 {stock} 触及止损，执行卖出。")
                                self.reason_to_sell = 'stoploss'

    # 判断某只股票是否到达涨停
    def check_is_high_limit(self, context, stock):
        data = context.get_market_data_ex(
            ['lastPrice', 'lastClose'],                
            [stock],
            period="1m",
            start_time = context.tm.date_str,
            end_time = context.tm.date_str,
            count=1,
            dividend_type = "follow",
            fill_data = False,
            subscribe = True
        )[stock]
        price = data["lastPrice"]
        lastClose = data["lastClose"]
        high_limit = self.get_limit_of_stock(stock, lastClose)[0]
        return price >= high_limit

    # 过滤器函数（均采用列表推导式实现，确保在遍历时不会修改列表）

    def filter_paused_stock(self, context, stock_list: List[str]):
        """
        过滤停牌的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未停牌的股票代码列表
        """
        return [stock for stock in stock_list if not context.is_suspended_stock(stock)]

    def filter_st_stock(self, context, stock_list: List[str]):
        """
        过滤带有 ST 或其他风险标识的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            无 ST 或风险标识的股票代码列表
        """
        def not_st_stock(stock):
            name = context.get_stock_name(stock)
            stock_data = context.get_instrumentdetail(stock)
            return ('ST' not in name) and ('*' not in name) and ('退' not in name) and (stock_data['ExpireDate'] != 0 or stock_data['ExpireDate'] != 99999999)
        return [stock for stock in stock_list if not_st_stock(stock)]

    def filter_kcbj_stock(self, stock_list: List[str]):
        """
        过滤科创、北交股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表（排除以 '4'、'8' 开头以及以 '68' 起始的股票）
        """
        return [stock for stock in stock_list if stock[0] not in ('4', '8') and not stock.startswith('68')]

    def filter_limitup_stock(self, context: Any, stock_list: List[str]):
        """
        过滤当天已经涨停的股票（若未持仓则过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        data = self.find_limit_list(context, stock_list)
        return [stock for stock in stock_list if stock not in data['high_list']]

    def filter_limitdown_stock(self, context: Any, stock_list: List[str]):
        """
        过滤当天已经跌停的股票（若未持仓则过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        data = self.find_limit_list(context, stock_list)
        print('跌停列表', data['low_list'])
        return [stock for stock in stock_list if stock not in data['low_list']]

    def filter_new_stock(self, context: Any, stock_list: List[str]):
        """
        过滤次新股：排除上市时间不足375天的股票

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        today = datetime.fromtimestamp(context.currentTime / 1000)
        yesterday = today - timedelta(days=1)
        def is_new_stock(stock):
            try:
                opendate = datetime.strptime(str(context.get_open_date(stock)), "%Y%m%d")
                return yesterday - opendate < timedelta(days=375)
            except Exception:
                # 取不到数据的股票也有问题，可能是已退市，也当成新股过滤掉
                # print(context.get_open_date(stock), '计算新股出错啦', stock)
                return True
        return [stock for stock in stock_list if not is_new_stock(stock)]

    def filter_highprice_stock(self, context: Any, stock_list: List[str]):
        """
        过滤股价高于设定上限（up_price）的股票（非持仓股票参与过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        return [stock for stock in stock_list if context.get_instrumentdetail(stock)['PreClose'] <= self.up_price]

    def filter_not_buy_again(self, stock_list: List[str]):
        """
        过滤掉当日已买入的股票，避免重复下单

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未买入的股票代码列表
        """
        return [stock for stock in stock_list if stock not in self.not_buy_again]

    # 回测和实盘不一样，回测用目标比例，实盘用可用资金比例。注意这个value传参
    def open_position_in_test(self, context: Any, security: str, value: float):
        print("买入股票(回测):", security, context.get_stock_name(security), str(int(value * 100)) + '%')
        order_target_percent(security, round(value, 2), 'COMPETE', context, context.account)
    
    
    # 实盘的买入非常复杂，需要考虑部分成交的情况，以及长时间委托不成交的情况，这里单开一个函数进行，且进行定时循环调用
    # 这里有问题，不能和open_position在同一作用域。QMT貌似不支持多线程工作，因此需要整体循环买入后，整体定时检测再撤单。
    def open_position(self, context, security: str, value: float = 0):
        """
        开仓操作：尝试买入指定股票，支持指定股票数量或者金额

        参数:
            security: 股票代码
            value: 分配给该股票的资金
        """
        print("买入股票(实盘):", security, context.get_stock_name(security), value )
        
        # 走到这里则为首次下单，直接以目标金额数买入
        # 1102 表示总资金量下单
        lastOrderId = str(uuid.uuid4())
        
        passorder(23, 1102, context.account, security, 5, -1, value, lastOrderId, 1, lastOrderId, context)

    def close_position(self, context, stock: Any):
        """
        平仓操作：尽可能将指定股票仓位全部卖出

        参数:
            position: 持仓对象

        返回:
            若下单后订单全部成交返回 True，否则返回 False
        """
        if stock:
            if context.do_back_test:
                order_target_value(stock, 0, context, context.account)
            else:
                # 1123 表示可用股票数量下单，这里表示全卖
                # 这里实盘已经验证传参正确，因为1123模式下表示可用比例，所以传1表示全卖
                passorder(24, 1123, context.account, stock, 6, 1, 1, "卖出策略", 1, "", context)
            return True

    # 获取当前账户可用金额
    def get_account_money(self, context):        
        accounts = get_trade_detail_data(context.account, 'stock', 'account')
        money = 0
        for dt in accounts:
            money = dt.m_dAvailable
        return money
        

    def buy_security(self, context: Any, target_list: List[str]):
        """
        买入操作：对目标股票执行买入，下单资金均摊分配

        参数:
            context: 聚宽平台传入的交易上下文对象
            target_list: 要买的股票代码列表
        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions if position.m_dMarketValue > 10000]

        position_count = len(self.hold_list)
        target_num = len(target_list)
        print("下单逻辑: 持仓数: ", position_count, "目标数",  target_num)
        if target_num > position_count:
            try:
                # 回测是目标比例
                if context.do_back_test:
                    value = round(1 / target_num, 2) - 0.001
                    buy_num = 0
                    for stock in [i for i in target_list if i not in self.hold_list]:
                        self.open_position_in_test(context, stock, value)
                        buy_num += 1
                        if buy_num == target_num - position_count:
                            break
                else:
                    # 实盘是可用比例
                    value = round(1 /( target_num - position_count), 2) - 0.001                    
                    buy_num = 0
                    money = self.get_account_money(context)
                    # 单支股票需要的买入金额
                    single_mount = round(money * value, 2)
                    
                    for stock in [i for i in target_list if i not in self.hold_list]:
                        self.open_position(context, stock, single_mount)
                        buy_num += 1
                        if buy_num == target_num - position_count:
                            break
            except ZeroDivisionError as e:
                print(f"资金分摊时除零错误: {e}")
                return
        print("买入委托完毕.")
        
    
    def new_buy_target(self, context: Any):
        """
        新的买入目标：根据当前持仓和目标股票列表，计算新的买入目标股票列表

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        target_num = len(self.stocks_to_buy)
        if target_num == 0:
            return
        value = round(1 /target_num, 2)
        money = self.get_account_money(context)
        print("新的买入目标：", self.stocks_to_buy, "单支买入：", value)
        # # 单支股票需要的买入金额
        single_mount = round(money * value, 2) - 200 # 留资金buffer 防止资金不足下单失败
        for stock in self.stocks_to_buy:
            if context.do_back_test:
                order_target_value(stock, single_mount, context, context.account)
            else:
                self.open_position(context, stock, single_mount)

    def today_is_between(self, context: Any):
        """
        判断当前日期是否为资金再平衡（空仓）日，通常在04月或01月期间执行空仓操作

        参数:
            context: 聚宽平台传入的交易上下文对象

        返回:
            若为空仓日返回 True，否则返回 False
        """
        today_str = datetime.fromtimestamp(context.currentTime / 1000).strftime('%m-%d')
        print(today_str)
        if self.pass_april:
            return ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-30')
        else:
            return False        
    
    def check_escape_top(self, context):
        # 1. 直接获取连续主力合约代码 (规避换月数据断层)
        # 备注：IML0 是中金所 IM 连续主力
        main_continuous = "IML8.CFE" 
        main_stock = '000852.SH'  # 中证1000指数
        
        # 2. 获取数据 (增加 count 以确保对齐后仍有足够窗口)
        price_data = context.get_market_data_ex(
            fields=['close'],
            stock_list=[main_stock, main_continuous],
            period='1d',
            start_time = context.tm.date_str,
            end_time = context.tm.date_str,
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
        # 如果wma_basis < 2，开始逃顶。当wma_basis > 2时，恢复交易
        
        # 逃顶
        if wma_basis < 2:
            if not context.storage.getStorage('stop_trade'):
                context.storage.setStorage('stop_trade', True)
                messager.send_message(f"主力连续: {main_continuous} | 实时基差: {curr_basis:.2f}% | 7日加权: {wma_basis:.2f}%")
                messager.send_message("📢📢📢📢📢 重大风险清仓 !!! 📢📢📢📢📢")
                self.close_account(context)
            
        # 恢复交易
        else:
            if context.storage.getStorage('stop_trade'):
                context.storage.setStorage('stop_trade', False)
                self.weekly_adjustment_select(context)
                self.weekly_adjustment_buy(context)

    # 早盘检查是否处于逃顶状态，是否有遗留仓位待清空
    def check_escape_top_position(self, context):
        if context.storage.getStorage('stop_trade'):
            for stock in self.hold_list:
                self.close_position(context, stock)
    
    def find_stock_of_positions(self, stock):
        result = [position for position in self.positions if position.m_strInstrumentID == stock]
        if result:
            return result[0]

    def get_stock_list_of_positions(self, context):
        return [position.m_strInstrumentID for position in self.positions]

    def close_account(self, context: Any):
        """
        清仓操作：若当天为空仓日，则平仓所有持仓股票

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # 检查是否需要逃顶
        self.check_escape_top(context)
        if self.no_trading_today_signal:
            if self.hold_list:
                for stock in self.hold_list:
                    self.close_position(context, stock)
                    print(f"空仓日平仓，卖出股票 {stock}。")

    def print_position_info(self, context: Any):
        """
        打印当前持仓详细信息，包括股票代码、成本价、现价、涨跌幅、持仓股数和市值

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # TODO 这里其实可以使用持仓统计对象，有更多统计相关数据
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions]

        if self.positions:
            print(f"********** 持仓信息打印开始 {context.account}**********")
            messager.send_positions(context)
            messager.send_account_info(context)
            total = 0
            for position in self.positions:
                cost: float = position.m_dOpenPrice
                price: float = position.m_dLastPrice
                ret: float = 100 * (price / cost - 1) if cost != 0 else 0.0  # 避免除以零错误
                value: float = position.m_dMarketValue
                amount: int = position.m_nVolume
                code = self.codeOfPosition(position)
                print(f"股票: {self.codeOfPosition(position)}")
                print(f"股票名: {context.get_stock_name(code)}")
                print(f"成本价: {cost:.2f}")
                print(f"现价: {price:.2f}")
                print(f"涨跌幅: {ret:.2f}%")
                print(f"持仓: {amount}")
                print(f"市值: {value:.2f}")
                print("--------------------------------------")
                total += value
            print(f"总市值：{total:.2f}")
            print("********** 持仓信息打印结束 **********")
        else:
            print("**********没有持仓信息**********")
            messager.sendMsg("持仓状态：空仓")
            messager.send_account_info(context)

# 创建全局策略实例，策略入口处使用该实例
strategy = TradingStrategy()


# 全局包装函数，必须为顶层函数，保证调度任务可序列化，不使用 lambda

def prepare_stock_list_func(context: Any):
    """
    包装调用策略实例的 prepare_stock_list 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('准备当日股票...')
    strategy.prepare_stock_list(context)
    strategy.find_target_stock_list(context)



def check_holdings_yesterday_func(context: Any):
    """
    包装调用策略实例的 check_holdings_yesterday 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.check_holdings_yesterday(context)
    print('--------------------------------', '新的一天开始了', context.today, '--------------------------------')


def weekly_adjustment_func(context: Any):
    """
    包装调用策略实例的 weekly_adjustment_select 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('================== 每周调仓 - 选股时间 ==================')
    strategy.weekly_adjustment_select(context)
    print('================== 每周调仓 - 卖出时间 ==================')
    strategy.weekly_adjustment_sell(context)

def weekly_adjustment_buy_func(context: Any):
    """
    包装调用策略实例的 weekly_adjustment_buy 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('================== 每周调仓 - 买入时间 ==================')
    strategy.weekly_adjustment_buy(context)

def check_escape_top_position_func(context: Any):
    """
    包装调用策略实例的 check_escape_top_position 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.check_escape_top_position(context)

def sell_stocks_func(context: Any):
    """
    包装调用策略实例的 sell_stocks 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('早上交易阶段...')
    strategy.sell_stocks(context)


def trade_afternoon_func(context: Any):
    """
    包装调用策略实例的 trade_afternoon 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('下午交易阶段...')
    strategy.trade_afternoon(context)

def close_account_func(context: Any):
    """
    包装调用策略实例的 close_account 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('收盘前检查是否需要清仓...')
    strategy.close_account(context)


def print_position_info_func(context: Any):
    """
    包装调用策略实例的 print_position_info 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.print_position_info(context)
    
def log_target_list_info(context: Any):
    """
    打印目标股票池信息

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.internal_get_target_list(context)

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

class MinuteTask(ScheduledTask):
    """分钟级别任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = current_dt - timedelta(minutes=1) >= self.last_executed        
        should = should1 and should2
        # 当前时间已过执行时间 且 超过1分钟
        return should

class DailyTask(ScheduledTask):
    """每日任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = self.last_executed != current_dt.date()
        should = should1 and should2
        # 当前时间已过执行时间 且 当日未执行
        return should

class WeeklyTask(ScheduledTask):
    """每周任务"""
    def __init__(self, weekday, execution_time):
        super().__init__(execution_time)
        self.weekday = weekday  # 0-6 (周一至周日)
    
    def should_trigger(self, current_dt):
        should1 = int(current_dt.weekday()) == self.weekday
        should2 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        week_num = current_dt.isocalendar()[1]        
        should3 = self.last_executed != f"{current_dt.year}-{week_num}"
        should = should1 and should2 and should3
        # if should:
        #     print('每周调仓时间到', current_dt)
        # 周几匹配 且 时间已过 且 当周未执行
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
        self.daily_tasks.append( (DailyTask(time_str), task_func) )
    
    def run_weekly(self, weekday, time_str, task_func):
        """注册每周任务
        Args:
            weekday: 0-6 代表周一到周日
            time_str: 触发时间 "HH:MM"
            task_func: 任务函数
        """
        self.weekly_tasks.append( (WeeklyTask(weekday, time_str), task_func) )
    
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


# TODO 实盘调试代码，开盘时尝试运行进行调试。下面内容都是实时调用，会产生买入和卖出动作。
def testRunBuy(context):
    check_holdings_yesterday_func(context)
    prepare_stock_list_func(context)
    weekly_adjustment_func(context)
    print('一段时间以后，假设之前的卖出已成交...')
    print("执行买入逻辑")
    weekly_adjustment_buy_func(context)


def checkTask(context):
    context.runner.check_tasks(context.tm.now)

# 在handlebar函数中调用（假设当前K线时间戳为dt）
def handlebar(context):
    try:
        # 更新时间管理器状态
        context.tm.update()
        
        # 保持兼容性，同步旧的时间变量
        context.currentTime = context.tm.timestamp
        context.today = context.tm.now
        
        # 回测模式下需要手动触发任务检查
        if context.do_back_test:
            # 检查并执行任务
            context.runner.check_tasks(context.tm.now)
            
            if not strategy.pool_initialized:
                strategy.get_stock_pool_when_test(context)
                
    except Exception as e:
        print('handlebar异常', e)
        import traceback
        traceback.print_exc()
        

def deal_callback(context, dealInfo):
    stock = dealInfo.m_strInstrumentName
    value = dealInfo.m_dTradeAmount
    print(f"已{dealInfo.m_nDirection}股票 {stock}，成交额 {value:.2f}")
    strategy.not_buy_again.append(stock)
    
    code = strategy.codeOfPosition(dealInfo)
    if code in strategy.notified_codes:
        stock_name = context.get_stock_name(code)
        messager.sendLog(f"{stock}：{stock_name} 已成交")
        strategy.notified_codes.remove(code)

def order_callback(context, orderInfo):
    code = strategy.codeOfPosition(orderInfo)
    if code not in strategy.notified_codes:
        print("委托信息变更回调", context.get_stock_name(code))
        messager.sendLog("已委托： " + context.get_stock_name(code))
        strategy.notified_codes.add(code)

def orderError_callback(context, orderArgs, errMsg):
    messager.sendLog(f"下单异常回调，订单信息{orderArgs}，异常信息{errMsg}")
        

# ==============================================================
# 【工具类】
# ==============================================================

def is_trading():
    current_time = datetime.now().time()
    return time(9,0) <= current_time <= time(16,0)

class Messager:
    def __init__(self):
        # 消息通知
        self.webhook1 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e861e0b4-b8e2-42ed-a21a-1edf90c41618'
    def set_is_test(self, is_test):
        self.is_test = is_test
    def send_message(self, webhook, message):
        if self.is_test:
            return
        # 设置企业微信机器人的Webhook地址
        headers = {'Content-Type': 'application/json; charset=UTF-8'}
        data = {
            'msgtype': 'markdown', 
            'markdown': {
                'content': message
            }
        }
        response = requests.post(webhook, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            print('消息发送成功')
        else:
            print('消息发送失败')
    # 发送消息（支持控制只在开盘期间推送）
    def sendLog(self, message):
        if is_trading():
            self.send_message(self.webhook1, message)
        print(message)

    def sendMsg(self, message):
        self.send_message(self.webhook1, message)
  
    def send_deal(self, dealInfo):
        stock = dealInfo.m_strProductName
        price = dealInfo.m_dPrice
        amount = dealInfo.m_dTradeAmount
        markdown = f"""
        新增买入股票: <font color='warning'>{stock}</font>
        > 成交价: <font color='warning'>{price}/font>
        > 成交额: <font color='warning'>{amount}</font>
        """
        self.send_message(self.webhook1, markdown)
    
    def send_account_info(self, context):
        accounts = get_trade_detail_data(context.account, 'stock', 'account')
        for dt in accounts:
            self.sendMsg(f'总资产: {dt.m_dBalance:.2f},\n总市值: {dt.m_dInstrumentValue:.2f},\n' + f'可用金额: {dt.m_dAvailable:.2f},\n持仓总盈亏: {dt.m_dPositionProfit:.2f}')
        
    def send_positions(self, context):
        if context.do_back_test:
            return
        positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        df_result = pd.DataFrame(columns=['stock', 'price', 'open_price', 'amount', 'ratio', 'profit'])
        for position in positions:
            df_result = df_result.append({
            'stock': position.m_strInstrumentName,
            'price': position.m_dLastPrice,
            'open_price': position.m_dOpenPrice,
            'amount': position.m_dMarketValue,
            'ratio': position.m_dProfitRate,
            'profit': position.m_dFloatProfit,
            }, ignore_index=True)

        markdown = """
        ## 股票持仓报告
        """
        num = len(df_result)
        total_profit = df_result['profit'].sum()
        if total_profit > 0:
            total_profit = f"<font color='info'>{total_profit:.2f}</font>"
        else:
            total_profit = f"<font color='warning'>{total_profit:.2f}</font>"

        for index, row in df_result.iterrows():
            row_str = self.get_position_markdown(row)
            markdown += row_str
        markdown += f"""
        ---
        **持仓统计**
        总持仓数：{num} 只
        总盈亏额：{total_profit}
        """
        self.send_message(self.webhook1, markdown)

    def get_position_markdown(self, position):
        stock = position['stock']
        price = position['price']
        open_price = position['open_price']
        amount = position['amount']
        ratio = position['ratio']
        ratio_str = ratio * 100
        if ratio_str > 0:
            ratio_str = f"<font color='info'>{ratio_str:.2f}%</font>"
        else:
            ratio_str = f"<font color='warning'>{ratio_str:.2f}%</font>"
        profit = position['profit']
        if profit > 0:
            profit = f"<font color='info'>{profit:.2f}</font>"
        else:
            profit = f"<font color='warning'>{profit:.2f}</font>"
        return f"""
    **{stock}**
    ├─ 当前价：{price:.2f}
    ├─ 成本价：{open_price:.2f}
    ├─ 持仓额：{amount:.2f}
    ├─ 盈亏率：{ratio_str}
    └─ 当日盈亏：{profit}
        """
messager = Messager()
class Log:
    def debug(*args):
        print(*args)
    def error(*args):
        print('[log error]', *args)
log = Log()

class Storage:
    def __init__(self, context):
        self.context = context
        self.cache_file = g.cache_file
        if self.context.do_back_test:
            self._data = {}
        else:
            self._data = self._load_from_file()

    def _load_from_file(self):
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_to_file(self):
        if self.context.do_back_test:
            return
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self._data, f)
        except Exception as e:
            print(f"写入缓存文件 {self.cache_file} 失败: {e}")

    def getStorage(self, key):
        return self._data.get(key)

    def setStorage(self, key, value):
        self._data[key] = value
        self._save_to_file()

class TimeManager:
    """
    统一时间管理类
    解决实盘和回测中时间对象(datetime)与时间戳(timestamp)管理混乱的问题
    """
    def __init__(self, context):
        self.context = context
        self._timestamp = 0
        self._dt = datetime.now()
        # 初始化时间
        self.update(init=True)

    def update(self, init=False):
        """更新当前时间状态"""
        if init and not self.context.do_back_test:
            # 实盘初始化时使用系统时间
            self._timestamp = nativeTime.time() * 1000 + 8 * 3600 * 1000
            self._dt = pd.to_datetime(self._timestamp, unit='ms')
            print('TimeManager初始化时间:', self._timestamp)
            return

        # 获取当前K线时间
        index = self.context.barpos
        # get_bar_timetag返回的是毫秒时间戳，通常需要加8小时转北京时间
        current_k_time = self.context.get_bar_timetag(index) + 8 * 3600 * 1000
        
        if not self.context.do_back_test:
            # 实盘模式：只在时间推进时更新（过滤掉旧的K线数据）
            if self._timestamp < current_k_time:
                self._timestamp = current_k_time
                self._dt = pd.to_datetime(self._timestamp, unit='ms')
        else:
            # 回测模式：直接更新
            self._timestamp = current_k_time
            self._dt = pd.to_datetime(self._timestamp, unit='ms')

    @property
    def now(self) -> datetime:
        """获取当前datetime对象"""
        return self._dt

    @property
    def timestamp(self) -> float:
        """获取当前时间戳(毫秒)"""
        return self._timestamp

    @property
    def date_str(self) -> str:
        """获取YYYYMMDD格式日期字符串"""
        return self._dt.strftime('%Y%m%d')
    
    @property
    def time_str(self) -> str:
        """获取HH:MM:SS格式时间字符串"""
        return self._dt.strftime('%H:%M:%S')

    @property
    def year(self) -> int:
        return self._dt.year

    @property
    def month(self) -> int:
        return self._dt.month
        
    @property
    def day(self) -> int:
        return self._dt.day
        
    @property
    def weekday(self) -> int:
        """返回星期几 (0=周一, 6=周日)"""
        return self._dt.weekday()

    def get_past_date(self, days: int) -> str:
        """获取过去N天的日期字符串(YYYYMMDD)"""
        return (self._dt - timedelta(days=days)).strftime('%Y%m%d')
