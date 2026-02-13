import datetime
from datetime import timedelta
from typing import Any, List

import numpy as np
import requests

# 导入聚宽数据接口
from jqdata import *
from jqfactor import *


# ====================================================================
# 【消息推送类】
# ====================================================================
class Messager:
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = True # 如果是实盘或需要推送，请改为 False

    def send_message(self, text_content):
        if self.is_test:
            log.info(f"【消息推送(测试)】{text_content}")
            return
        try:
            current_time = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            payload = {"msgtype": "text", "text": {"content": current_time + text_content}}
            requests.post(self.hook_url, json=payload, timeout=5)
        except Exception as e:
            log.error(f"推送失败: {e}")

# ====================================================================
# 【数据辅助类】
# ====================================================================
class DataHelper:
    @staticmethod
    def get_price_safe(security, end_date, frequency, fields, count, panel=False, skip_paused=True, fq=None, fill_paused=False):
        try:
            df = get_price(security, end_date=end_date, frequency=frequency, fields=fields, count=count, panel=panel, skip_paused=skip_paused, fq=fq, fill_paused=fill_paused)
            return df
        except Exception as e:
            log.error(f"获取 {security} 价格出错: {e}")
            return None

    @staticmethod
    def get_history_safe(security, unit, field, count):
        try:
            data = history(count, unit=unit, field=field, security_list=security)
            return data
        except Exception as e:
            log.error(f"获取 {security} 历史出错: {e}")
            return None

# ====================================================================
# 【核心交易策略类】
# ====================================================================
class TradingStrategy:
    def __init__(self) -> None:
        # --- 原有小市值策略参数 ---
        self.no_trading_today_signal: bool = False
        self.pass_april: bool = True
        self.run_stoploss: bool = True
        self.hold_list: List[str] = []
        self.yesterday_HL_list: List[str] = []
        self.target_list: List[str] = []
        self.not_buy_again: List[str] = []
        self.stock_num: int = 7
        self.up_price: float = 100.0
        self.reason_to_sell: str = ''
        self.stoploss_strategy: int = 3
        self.stoploss_limit: float = 0.88
        self.stoploss_market: float = 0.94
        self.HV_control: bool = False
        self.HV_duration: int = 120
        self.HV_ratio: float = 0.9

        # --- 新增逃顶策略参数 ---
        self.HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx' # 请替换
        self.messager = Messager(self.HOOK)
        self.is_risk_warning = False      # 风险状态位
        self.warning_start_date = None
        self.basis_list = []
        self.wma_window = 7
        self.basis_trigger = -2.0         # 基差触发阈值
        self.breadth_trigger = 0.3        # 广度触发阈值
        self.basis_recovery = -1.2        # 基差恢复阈值
        self.breadth_recovery = 0.5       # 广度恢复阈值

    def initialize(self, context: Any) -> None:
        set_option('avoid_future_data', True)
        set_benchmark('000001.XSHG')
        set_option('use_real_price', True)
        set_slippage(FixedSlippage(3 / 10000))
        set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=2.5/10000, close_commission=2.5/10000, min_commission=5), type='stock')
        log.set_level('order', 'error')
        
        # 初始推送
        self.messager.send_message(">>> [策略启动] 小市值 + 14:50实时逃顶风控版已就绪。")
    
    # ================= 新增：早盘补单执行逻辑 =================
    def morning_risk_sell(self, context):
        """
        每天 09:31 执行，仅作为执行层，不计算新信号。
        目的是处理昨日未清干净的仓位，并确保风险状态下不留残余仓位。
        """
        if self.is_risk_warning:
            log.warn("⚠️ [早盘风控拦截] 当前处于避险状态，检查并清理剩余仓位。")
            self.sell_all_stocks_now(context)
        else:
            log.info("早盘风控检查：当前处于安全状态。")
            
    # ================= 逃顶风控核心逻辑 =================
    def market_risk_monitor(self, context):
        """14:50 执行的逃顶监测逻辑"""
        today = context.current_dt.date()
        
        # 1. 品种适配
        if today >= datetime.date(2022, 7, 22):
            target_future, target_spot = 'IM', '000852.XSHG'
        elif today >= datetime.date(2015, 4, 16):
            target_future, target_spot = 'IC', '000905.XSHG'
        else: return

        try:
            # 获取上个交易日用于选股计算广度
            trade_days = get_trade_days(end_date=today, count=2)
            prev_date = trade_days[0]
            current_data = get_current_data()

            # --- 基差计算 ---
            spot_p = current_data[target_spot].last_price
            main_contract = get_dominant_future(target_future, date=today)
            future_p = current_data[main_contract].last_price
            
            if np.isnan(spot_p) or np.isnan(future_p) or spot_p == 0: return
            
            curr_basis_rate = (future_p / spot_p - 1) * 100
            self.basis_list.append(curr_basis_rate)
            if len(self.basis_list) > self.wma_window: self.basis_list.pop(0)
            
            weights = np.arange(1, len(self.basis_list) + 1)
            wma_basis = np.sum(np.array(self.basis_list) * weights) / weights.sum()

            # --- 广度计算 ---
            q = query(valuation.code).filter(valuation.market_cap > 0).order_by(valuation.market_cap.asc()).limit(400)
            micro_stocks = get_fundamentals(q, date=prev_date)['code'].tolist()
            
            pre_close_data = get_price(micro_stocks, end_date=prev_date, count=1, fields=['close'], panel=False)
            pre_close_dict = dict(zip(pre_close_data['code'], pre_close_data['close']))
            
            rise_count, valid_count = 0, 0
            for stock in micro_stocks:
                if stock in pre_close_dict:
                    curr_p = current_data[stock].last_price
                    if not np.isnan(curr_p) and not current_data[stock].paused:
                        valid_count += 1
                        if curr_p > pre_close_dict[stock]: rise_count += 1
            
            micro_breadth = float(rise_count) / valid_count if valid_count > 0 else 0.5

            # 可视化
            record(WMA_Basis = wma_basis)
            record(Micro_Breadth = micro_breadth * 10)
            record(Risk_Status = 10 if self.is_risk_warning else 0)

            # --- 状态切换逻辑 ---
            risk_trigger = (wma_basis < self.basis_trigger and micro_breadth < self.breadth_trigger)
            risk_recovery = (wma_basis > self.basis_recovery or micro_breadth > self.breadth_recovery)

            # 触发逃顶
            if not self.is_risk_warning and risk_trigger:
                self.is_risk_warning = True
                self.warning_start_date = today
                msg = "🔴 [风控逃顶] 基差:%.2f 广度:%.1f%% -> 立即执行全仓平仓！" % (wma_basis, micro_breadth * 100)
                log.warn(msg); self.messager.send_message(msg)
                self.sell_all_stocks_now(context)

            # 触发恢复
            elif self.is_risk_warning and risk_recovery:
                duration = (today - self.warning_start_date).days
                self.is_risk_warning = False
                msg = "🟢 [风控解除] 信号好转 (持续%d天) -> 立即恢复买回逻辑！" % duration
                log.info(msg); self.messager.send_message(msg)
                self.warning_start_date = None
                # 【关键增加】：风险解除后立即调用原本的周调仓买入逻辑
                self.weekly_adjustment(context)

        except Exception as e:
            log.error("风控实时计算出错: %s" % e)

    def sell_all_stocks_now(self, context):
        """强制清仓函数"""
        for stock in list(context.portfolio.positions.keys()):
            order_target(stock, 0)
        log.info("【操作】已执行全仓清仓避险。")

    def check_holdings_yesterday(self, context: Any) -> None:
        """
        检查并输出每只持仓股票昨日的交易数据（开盘价、收盘价、涨跌幅）。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        positions = context.portfolio.positions
        if not positions:
            log.info("昨日没有持仓数据。")
            return

        log.info("检查每只持仓股票昨日交易数据：")
        for stock, position in positions.items():
            try:
                # 获取股票昨日的开盘价和收盘价
                df = DataHelper.get_price_safe(
                    stock,
                    end_date=context.previous_date,
                    frequency="daily",
                    fields=['open', 'close'],
                    count=1,
                    panel=False
                )
                if df is None or df.empty:
                    log.info(f"无法获取股票 {stock} 的昨日数据。")
                    continue
                open_price: float = df.iloc[0]['open']
                close_price: float = df.iloc[0]['close']
                change_pct: float = (close_price / open_price - 1) * 100
                log.info(f"股票 {stock}：持仓 {position.total_amount} 股，开盘价 {open_price:.2f}，收盘价 {close_price:.2f}，涨跌幅 {change_pct:.2f}%")
            except Exception as e:
                log.error(f"处理股票 {stock} 数据时出错: {e}")
    # ================= 原有策略逻辑适配 =================
    
    def weekly_adjustment(self, context: Any) -> None:
        # 【拦截】：如果处于风控期，禁止调仓买入
        if self.is_risk_warning:
            log.info("目前处于风控逃顶状态，取消本次买入/调仓。")
            return

        if not self.no_trading_today_signal:
            self.not_buy_again = []
            self.target_list = self.get_stock_list(context)
            target_list: List[str] = self.target_list[:self.stock_num]
            log.info(f"调仓目标股票: {target_list}")

            for stock in self.hold_list:
                if stock not in target_list and stock not in self.yesterday_HL_list:
                    position = context.portfolio.positions[stock]
                    self.close_position(position)
            
            self.buy_security(context, target_list)
            for position in list(context.portfolio.positions.values()):
                if position.security not in self.not_buy_again:
                    self.not_buy_again.append(position.security)

    # ... (此处保留你原有的 filter_xxx, get_stock_list, buy_security 等所有代码)
    # ... 为了篇幅，以下仅列出结构，实际运行请确保包含你提供的全部类方法 ...

    def prepare_stock_list(self, context: Any) -> None:
        self.hold_list = [position.security for position in list(context.portfolio.positions.values())]
        if self.hold_list:
            df = DataHelper.get_price_safe(self.hold_list, end_date=context.previous_date, frequency='daily', fields=['close', 'high_limit'], count=1)
            if df is not None and not df.empty:
                self.yesterday_HL_list = list(df[df['close'] == df['high_limit']]['code'])
            else: self.yesterday_HL_list = []
        else: self.yesterday_HL_list = []
        self.no_trading_today_signal = self.today_is_between(context)

    
    def get_stock_list(self, context: Any) -> List[str]:
        """
        选股模块：
        1. 从指定股票池（如 399101.XSHE 指数成分股）中获取初步股票列表；
        2. 应用多个过滤器筛选股票（次新股、科创股、ST、停牌、涨跌停等）；
        3. 基于基本面数据（EPS、市值）排序后返回候选股票列表。

        参数:
            context: 聚宽平台传入的交易上下文对象

        返回:
            筛选后的候选股票代码列表
        """
        # 从指定指数中获取初步股票列表
        MKT_index: str = '399101.XSHE'
        initial_list: List[str] = get_index_stocks(MKT_index)

        # 依次应用过滤器，筛去不符合条件的股票
        initial_list = self.filter_new_stock(context, initial_list)   # 过滤次新股
        initial_list = self.filter_kcbj_stock(initial_list)             # 过滤科创/北交股票
        initial_list = self.filter_st_stock(initial_list)               # 过滤ST或风险股票
        initial_list = self.filter_paused_stock(initial_list)           # 过滤停牌股票
        initial_list = self.filter_limitup_stock(context, initial_list)   # 过滤当日涨停（未持仓时）的股票
        initial_list = self.filter_limitdown_stock(context, initial_list) # 过滤当日跌停（未持仓时）的股票

        # 利用基本面查询获取股票代码和EPS数据，并按照市值升序排序
        q = query(valuation.code, indicator.eps) \
            .filter(valuation.code.in_(initial_list)) \
            .order_by(valuation.market_cap.asc())
        df = get_fundamentals(q)
        stock_list: List[str] = list(df.code)
        stock_list = stock_list[:100]  # 限制数据规模，防止一次处理数据过大
        # 取前2倍目标持仓股票数作为候选池
        final_list: List[str] = stock_list[:2 * self.stock_num]
        log.info(f"初选候选股票: {final_list}")

        # 查询并输出候选股票的财务信息（如财报日期、营业收入、EPS）
        if final_list:
            info_query = query(
                valuation.code,
                income.pubDate,
                income.statDate,
                income.operating_revenue,
                indicator.eps
            ).filter(valuation.code.in_(final_list))
            df_info = get_fundamentals(info_query)
            for _, row in df_info.iterrows():
                log.info(f"股票 {row['code']}：报告日期 {row.get('pubDate', 'N/A')}，统计日期 {row.get('statDate', 'N/A')}，营业收入 {row.get('operating_revenue', 'N/A')}，EPS {row.get('eps', 'N/A')}")
        return final_list


    def filter_paused_stock(self, stock_list: List[str]) -> List[str]:
        """
        过滤停牌的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未停牌的股票代码列表
        """
        current_data = get_current_data()
        return [stock for stock in stock_list if not current_data[stock].paused]

    def filter_st_stock(self, stock_list: List[str]) -> List[str]:
        """
        过滤带有 ST 或其他风险标识的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            无 ST 或风险标识的股票代码列表
        """
        current_data = get_current_data()
        return [stock for stock in stock_list if (not current_data[stock].is_st) and
                ('ST' not in current_data[stock].name) and
                ('*' not in current_data[stock].name) and
                ('退' not in current_data[stock].name)]

    def filter_kcbj_stock(self, stock_list: List[str]) -> List[str]:
        """
        过滤科创、北交股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表（排除以 '4'、'8' 开头以及以 '68' 起始的股票）
        """
        return [stock for stock in stock_list if stock[0] not in ('4', '8') and not stock.startswith('68')]

    def filter_limitup_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤当天已经涨停的股票（若未持仓则过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        history_data = DataHelper.get_history_safe(stock_list, unit='1m', field='close', count=1)
        current_data = get_current_data()
        if history_data is None:
            return stock_list
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys() or
                (history_data.get(stock, [0])[-1] < current_data[stock].high_limit)]

    def filter_limitdown_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤当天已经跌停的股票（若未持仓则过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        history_data = DataHelper.get_history_safe(stock_list, unit='1m', field='close', count=1)
        current_data = get_current_data()
        if history_data is None:
            return stock_list
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys() or
                (history_data.get(stock, [float('inf')])[-1] > current_data[stock].low_limit)]

    def filter_new_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤次新股：排除上市时间不足375天的股票

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        yesterday = context.previous_date
        return [stock for stock in stock_list if not (yesterday - get_security_info(stock).start_date < timedelta(days=375))]

    def filter_highprice_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤股价高于设定上限（up_price）的股票（非持仓股票参与过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        history_data = DataHelper.get_history_safe(stock_list, unit='1m', field='close', count=1)
        if history_data is None:
            return stock_list
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys() or 
                history_data.get(stock, [self.up_price + 1])[-1] <= self.up_price]

    def filter_not_buy_again(self, stock_list: List[str]) -> List[str]:
        """
        过滤掉当日已买入的股票，避免重复下单

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未买入的股票代码列表
        """
        return [stock for stock in stock_list if stock not in self.not_buy_again]

    def buy_security(self, context, target_list):
        if not target_list: return
        # 排除风控状态
        if self.is_risk_warning: return
        
        position_count = len(context.portfolio.positions)
        target_num = len(target_list)
        if target_num > position_count:
            cash_per_stock = context.portfolio.cash / (target_num - position_count)
            for stock in target_list:
                if context.portfolio.positions[stock].total_amount == 0:
                    if order_target_value(stock, cash_per_stock):
                        self.not_buy_again.append(stock)

    def close_position(self, position):
        return order_target_value(position.security, 0)

    def today_is_between(self, context):
        today_str = context.current_dt.strftime('%m-%d')
        if self.pass_april:
            return ('04-01' <= today_str <= '04-30')
        # or ('01-01' <= today_str <= '01-30')
        return False

    def close_account(self, context):
        if self.no_trading_today_signal:
            for stock in list(context.portfolio.positions.keys()):
                order_target_value(stock, 0)

    # 以下是原有其它辅助方法 (省略，请保留原样)
    
    def check_limit_up(self, context: Any) -> None:
        """
        检查昨日处于涨停状态的股票在当前是否破板。
        如破板（当前价格低于涨停价），则立即卖出该股票，并记录卖出原因为 "limitup"。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        now_time = context.current_dt  # 获取当前时间
        if self.yesterday_HL_list:
            for stock in self.yesterday_HL_list:
                # 使用1分钟周期数据判断当前价格和涨停价是否符合条件
                current_data = DataHelper.get_price_safe(
                    stock,
                    end_date=now_time,
                    frequency='1m',
                    fields=['close', 'high_limit'],
                    count=1,
                    panel=False,
                    fill_paused=True
                )
                if current_data is not None and not current_data.empty:
                    if current_data.iloc[0]['close'] < current_data.iloc[0]['high_limit']:
                        log.info(f"股票 {stock} 涨停破板，触发卖出操作。")
                        position = context.portfolio.positions[stock]
                        self.close_position(position)
                        self.reason_to_sell = 'limitup'
                    else:
                        log.info(f"股票 {stock} 仍维持涨停状态。")
    
    def check_remain_amount(self, context: Any) -> None:
        """
        检查账户资金与持仓数量：
        如果因涨停破板卖出导致持仓不足，则从目标股票中筛选未买入股票，进行补仓操作。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if self.reason_to_sell == 'limitup':
            self.hold_list = [position.security for position in list(context.portfolio.positions.values())]
            if len(self.hold_list) < self.stock_num:
                target_list = self.filter_not_buy_again(self.target_list)
                target_list = target_list[:min(self.stock_num, len(target_list))]
                log.info(f"检测到补仓需求，可用资金 {round(context.portfolio.cash, 2)}，候选补仓股票: {target_list}")
                self.buy_security(context, target_list)
            self.reason_to_sell = ''
        else:
            log.info("未检测到涨停破板卖出事件，不进行补仓买入。")

    def trade_afternoon(self, context: Any) -> None:
        """
        下午交易任务：
        1. 检查是否有因为涨停破板触发的卖出信号；
        2. 如启用了成交量监控，则检测是否有异常成交量；
        3. 检查账户中是否需要补仓。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if not self.no_trading_today_signal:
            self.check_limit_up(context)
            if self.HV_control:
                self.check_high_volume(context)
            self.check_remain_amount(context)

    
    def sell_stocks(self, context: Any) -> None:
        """
        止盈与止损操作：
        根据策略（1: 个股止损；2: 大盘止损；3: 联合策略）判断是否执行卖出操作。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if self.run_stoploss:
            if self.stoploss_strategy == 1:
                # 个股止盈或止损判断
                for stock in list(context.portfolio.positions.keys()):
                    pos = context.portfolio.positions[stock]
                    if pos.price >= pos.avg_cost * 2:
                        order_target_value(stock, 0)
                        log.debug(f"股票 {stock} 实现100%盈利，执行止盈卖出。")
                    elif pos.price < pos.avg_cost * self.stoploss_limit:
                        order_target_value(stock, 0)
                        log.debug(f"股票 {stock} 触及止损阈值，执行卖出。")
                        self.reason_to_sell = 'stoploss'
            elif self.stoploss_strategy == 2:
                # 大盘止损判断，若整体市场跌幅过大则平仓所有股票
                stock_list = get_index_stocks('399101.XSHE')
                df = DataHelper.get_price_safe(
                    stock_list,
                    end_date=context.previous_date,
                    frequency='daily',
                    fields=['close', 'open'],
                    count=1,
                    panel=False
                )
                if df is not None and not df.empty:
                    down_ratio = (df['close'] / df['open']).mean()
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        log.debug(f"市场检测到跌幅（平均跌幅 {down_ratio:.2%}），卖出所有持仓。")
                        for stock in list(context.portfolio.positions.keys()):
                            order_target_value(stock, 0)
            elif self.stoploss_strategy == 3:
                # 联合止损策略：结合大盘和个股判断
                stock_list = get_index_stocks('399101.XSHE')
                df = DataHelper.get_price_safe(
                    stock_list,
                    end_date=context.previous_date,
                    frequency='daily',
                    fields=['close', 'open'],
                    count=1,
                    panel=False
                )
                if df is not None and not df.empty:
                    down_ratio = (df['close'] / df['open']).mean()
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        log.debug(f"市场检测到跌幅（平均跌幅 {down_ratio:.2%}），卖出所有持仓。")
                        for stock in list(context.portfolio.positions.keys()):
                            order_target_value(stock, 0)
                    else:
                        for stock in list(context.portfolio.positions.keys()):
                            pos = context.portfolio.positions[stock]
                            if pos.price < pos.avg_cost * self.stoploss_limit:
                                order_target_value(stock, 0)
                                log.debug(f"股票 {stock} 触及止损，执行卖出。")
                                self.reason_to_sell = 'stoploss'


# ====================================================================
# 【全局调度入口】
# ====================================================================

strategy = TradingStrategy()

def initialize(context):
    strategy.initialize(context)
    
    run_daily(morning_risk_sell_func, time='09:31')
    
    # 1. 逃顶风控监控：每天14:50执行
    run_daily(market_risk_monitor_func, time='14:50')
    
    # 2. 原有小市值任务调度
    run_daily(prepare_stock_list_func, time='9:05')
    run_daily(check_holdings_yesterday_func, time='9:00')
    run_weekly(weekly_adjustment_func, 2, time='10:30') # 周二调仓
    run_daily(sell_stocks_func, time='10:00')
    run_daily(trade_afternoon_func, time='14:30')
    run_daily(close_account_func, time='14:50') # 注意此处与风控同频，风控会覆盖买卖

# 包装函数
def market_risk_monitor_func(context): strategy.market_risk_monitor(context)
def prepare_stock_list_func(context): strategy.prepare_stock_list(context)
def weekly_adjustment_func(context): strategy.weekly_adjustment(context)
def sell_stocks_func(context): strategy.sell_stocks(context)
def trade_afternoon_func(context): strategy.trade_afternoon(context)
def close_account_func(context): strategy.close_account(context)
def check_holdings_yesterday_func(context): strategy.check_holdings_yesterday(context)
def morning_risk_sell_func(context):
    strategy.morning_risk_sell(context)