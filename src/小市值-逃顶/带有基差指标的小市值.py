# 克隆自聚宽文章：https://www.joinquant.com/post/53225
# 标题：老代码新写之稳健小市值择时
# 作者：Ceng-Lucifffff

"""
优化后的交易策略代码

该策略主要功能包括：
1. 选股：从指定指数中获取股票池，并通过多个过滤器（如次新股、科创股、ST、停牌、涨跌停等）进行筛选。
2. 调仓：在每周期调仓时，卖出部分不符合条件的股票，并买入目标股票。
3. 买卖：对目标股票执行买入操作（资金均摊分配），并对持仓股票管理止损和止盈。
4. 风控：包括检查成交量异常、涨停破板以及大盘止损等风控逻辑。
5. 数据封装：使用 DataHelper 类封装 get_price 和 history 数据接口，并进行异常处理，使代码更简洁。
6. 调度任务：所有调度任务均通过全局包装函数调用策略方法，避免 lambda 序列化问题。

运行平台：聚宽
"""

from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入聚宽数据接口和基本面因子库
from jqdata import *
from jqfactor import *


class DataHelper:
    """
    数据操作辅助类

    封装了数据接口的调用，包括 get_price 与 history 函数，
    并在内部捕获异常、输出中文错误日志，避免重复编写 try/except 代码。
    """

    @staticmethod
    def get_price_safe(
        security: Any,
        end_date: Any,
        frequency: str,
        fields: List[str],
        count: int,
        panel: bool = False,
        skip_paused: bool = True,
        fq: Optional[str] = None,
        fill_paused: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        安全调用 get_price 数据接口

        参数:
            security: 单只股票代码或股票代码列表
            end_date: 数据截止日期
            frequency: 数据频率，如 "daily" 或 "1m"
            fields: 需要获取的数据字段列表（例如 ['open', 'close']）
            count: 请求数据的记录数
            panel: 是否返回面板数据（默认为 False）
            skip_paused: 是否跳过停牌股票（默认为 True）
            fq: 复权方式（例如“pre”或“post”，默认 None）
            fill_paused: 是否填充停牌数据（默认为 False）

        返回:
            返回包含数据的 DataFrame，如果出错则返回 None
        """
        try:
            # 调用聚宽提供的 get_price 获取数据
            df = get_price(
                security,
                end_date=end_date,
                frequency=frequency,
                fields=fields,
                count=count,
                panel=panel,
                skip_paused=skip_paused,
                fq=fq,
                fill_paused=fill_paused
            )
            return df
        except Exception as e:
            # 输出中文错误日志，并返回 None
            log.error(f"获取 {security} 的价格数据时出错: {e}")
            return None

    @staticmethod
    def get_history_safe(
        security: Any,
        unit: str,
        field: str,
        count: int
    ) -> Optional[Dict[str, List[float]]]:
        """
        安全调用 history 数据接口，批量获取历史数据

        参数:
            security: 单只或多只股票代码
            unit: 数据单位，例如 "1m" 表示1分钟数据
            field: 请求数据字段名称，如 "close"（收盘价）
            count: 请求历史数据记录数

        返回:
            返回一个字典，映射股票代码到对应的数据列表；出错则返回 None
        """
        try:
            # 调用聚宽的 history 函数获取数据
            data = history(count, unit=unit, field=field, security_list=security)
            return data
        except Exception as e:
            log.error(f"获取 {security} 的历史数据时出错: {e}")
            return None


class TradingStrategy:
    """
    交易策略类

    封装了选股、调仓、买卖、止损与风控管理的核心逻辑。
    通过类属性管理持仓、候选股票等状态，并使用状态机字典记录交易信号，
    便于后续调试、扩展和维护。
    """
    def __init__(self) -> None:
        # 策略基础配置和状态变量
        self.no_trading_today_signal: bool = False  # 当天是否执行空仓（资金再平衡）操作
        self.pass_april: bool = True                # 是否在04月或01月期间执行空仓策略
        self.run_stoploss: bool = False              # 是否启用止损策略

        # 持仓和调仓记录
        self.hold_list: List[str] = []             # 当前持仓股票代码列表
        self.yesterday_HL_list: List[str] = []       # 昨日涨停的股票列表（收盘价等于涨停价）
        self.target_list: List[str] = []             # 本次调仓候选股票列表
        self.not_buy_again: List[str] = []           # 当天已买入的股票列表，避免重复下单

        # 策略交易及风控的参数
        self.stock_num: int = 20                    # 每次调仓目标持仓股票数量
        self.up_price: float = 100.0               # 股票价格上限过滤条件（排除股价超过此值的股票）
        self.reason_to_sell: str = ''              # 记录卖出原因（例如：'limitup' 涨停破板 或 'stoploss' 止损）
        self.stoploss_strategy: int = 1            # 止损策略：1-个股止损；2-大盘止损；3-联合止损策略
        self.stoploss_limit: float = 0.88          # 个股止损阀值（成本价 × 0.88）
        self.stoploss_market: float = 0.94         # 大盘止损参数（若整体跌幅过大则触发卖出）

        self.HV_control: bool = False              # 是否启用成交量异常检测
        self.HV_duration: int = 120                # 检查成交量时参考的历史天数
        self.HV_ratio: float = 0.9                 # 当天成交量超过历史最高成交量的比例（如0.9即90%）

        # 状态机字典，记录交易信号和当前风险水平
        self.state: Dict[str, Any] = {
            'buy_signal': False,
            'sell_signal': False,
            'risk_level': 'normal'
        }

    def initialize(self, context: Any) -> None:
        """
        策略初始化函数

        配置交易环境参数，包括防未来数据、基准、滑点、订单成本以及日志输出等级。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # 启用防未来数据以确保历史回测的严谨性
        set_option('avoid_future_data', True)
        # 设置策略基准为上证指数
        set_benchmark('000001.XSHG')
        # 使用真实市场价格，模拟实际交易
        set_option('use_real_price', True)
        # 设置固定滑点，确保价格偏差不大
        set_slippage(FixedSlippage(3 / 10000))
        # 设置订单成本，包括印花税和佣金
        set_order_cost(OrderCost(
            open_tax=0,
            close_tax=0.001,  # 卖出时0.1%的印花税
            open_commission=2.5 / 10000,
            close_commission=2.5 / 10000,
            close_today_commission=0,
            min_commission=5  # 最低佣金为5元
        ), type='stock')
        # 设置日志输出等级（中文日志输出）
        log.set_level('order', 'error')
        log.set_level('system', 'error')
        log.set_level('strategy', 'debug')
        # 注意：调度任务由全局包装函数统一注册，避免 lambda 导致序列化问题
        
        # 设定监测对象
        g.spot_index = '000852.XSHG'  # 中证1000现货指数
        g.future_symbol = 'IM'        # 中证1000股指期货代码
        

    def record_im_basis(self, context):
        today = context.current_dt.strftime('%Y-%m-%d')
        
        # 窗口期变量，默认值为5
        window_size = 5
        
        # 获取包括今天在内的过去 window_size 个交易日
        trade_days = get_trade_days(end_date=today, count=window_size)
        
        basis_rates = []
        
        # 遍历日期计算基差率
        for date in trade_days:
            date_str = date.strftime('%Y-%m-%d')
            
            # 如果是今天，使用实时数据
            if date_str == today:
                main_contract = get_dominant_future(g.future_symbol, date=today)
                if main_contract:
                    current_data = get_current_data()
                    spot_price = current_data[g.spot_index].last_price
                    future_price = current_data[main_contract].last_price
                    if spot_price > 0:
                        rate = (future_price / spot_price - 1) * 100
                        basis_rates.append(rate)
            else:
                # 历史数据：获取当时的主力合约和收盘价
                dom_future = get_dominant_future(g.future_symbol, date=date_str)
                if dom_future:
                    spot_df = get_price(g.spot_index, end_date=date, count=1, frequency='daily', fields=['close'])
                    future_df = get_price(dom_future, end_date=date, count=1, frequency='daily', fields=['close'])
                    if not spot_df.empty and not future_df.empty:
                        s_close = spot_df['close'].iloc[0]
                        f_close = future_df['close'].iloc[0]
                        if s_close > 0:
                            rate = (f_close / s_close - 1) * 100
                            basis_rates.append(rate)
        
        if not basis_rates:
            return False
            
        # 计算加权平均
        # 动态生成权重数组，例如 basis_rates 长度为 5，则权重为 [1, 2, 3, 4, 5]
        n = len(basis_rates)
        weights = list(range(1, n + 1))
        
        # 计算加权平均值
        weighted_sum = sum(rate * weight for rate, weight in zip(basis_rates, weights))
        total_weight = sum(weights)
        avg_rate = weighted_sum / total_weight

        # 4. 绘图与记录
        record(IM_Basis_Rate = avg_rate)  # 绘制基差率曲线
        record(Zero_Line = 0)               # 0轴参考线
        record(Panic_Line = -1.5)           # 恐慌参考线（经验值：贴水超1.5%通常意味着异动）

        # 5. 辅助对冲压力计算：计算基差的偏离度
        # 获取过去 20 天的基差数据，判断当前是否属于“异常贴水”
        # 此处逻辑可根据需要开启，用于日志报警
        # if basis_rate < -2.0:
        #     log.warn(">>> ⚠️ IM基差异常：当前贴水率 %.2f%%，主力合约: %s，对冲压力巨大！" % (basis_rate, main_contract))
        return avg_rate < -1.5

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

    def prepare_stock_list(self, context: Any) -> None:
        """
        更新持仓股票列表和昨日涨停股票列表，同时判断是否为空仓日（资金再平衡日）。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # 从当前持仓中提取股票代码，更新持仓列表
        self.hold_list = [position.security for position in list(context.portfolio.positions.values())]
        if self.hold_list:
            # 获取持仓股票昨日数据（包括收盘价、涨停价、跌停价）
            df = DataHelper.get_price_safe(
                self.hold_list,
                end_date=context.previous_date,
                frequency='daily',
                fields=['close', 'high_limit', 'low_limit'],
                count=1,
                panel=False,
                fill_paused=False
            )
            if df is not None and not df.empty:
                # 过滤出收盘价等于涨停价的股票，作为昨日涨停股票
                self.yesterday_HL_list = list(df[df['close'] == df['high_limit']]['code'])
            else:
                self.yesterday_HL_list = []
        else:
            self.yesterday_HL_list = []

        # 根据当前日期判断是否为空仓日（例如04月或01月时资金再平衡）
        self.no_trading_today_signal = self.today_is_between(context)
    
    def check_im_basis(self, context: Any) -> None:
        """
        检查 IM 基差是否异常

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        self.no_trading_today_signal = self.record_im_basis(context)
        if self.no_trading_today_signal:
          log.error("IM 基差异常，空仓操作")


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

        current_data = get_current_data()
        # 查询并输出候选股票的财务信息（如财报日期、营业收入、EPS）
        if final_list:
            info_query = query(
                valuation.code,
                income.pubDate,
                income.statDate,
                income.operating_revenue,
                indicator.eps,
                valuation.market_cap
            ).filter(valuation.code.in_(final_list))
            df_info = get_fundamentals(info_query)
            for _, row in df_info.iterrows():
                name = current_data[row['code']].name
                log.info(f"股票 {row['code']} {name}：报告日期 {row.get('pubDate', 'N/A')}，统计日期 {row.get('statDate', 'N/A')}，营业收入 {row.get('operating_revenue', 'N/A')}，EPS {row.get('eps', 'N/A')}，总市值：{row.get('market_cap', 'N/A')}")
        return final_list

    def weekly_adjustment(self, context: Any) -> None:
        """
        每周调仓策略：
        如果非空仓日，先选股得到目标股票列表，再卖出当前持仓中不在目标列表且昨日未涨停的股票，
        最后买入目标股票，同时记录当天买入情况避免重复下单。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if not self.no_trading_today_signal:
            self.not_buy_again = []  # 重置当天已买入记录
            self.target_list = self.get_stock_list(context)
            # 取目标持仓数以内的股票作为调仓目标
            target_list: List[str] = self.target_list[:self.stock_num]
            log.info(f"每周调仓目标股票: {target_list}")

            # 遍历当前持仓，若股票不在目标列表且非昨日涨停，则执行卖出操作
            for stock in self.hold_list:
                if stock not in target_list and stock not in self.yesterday_HL_list:
                    log.info(f"卖出股票 {stock}")
                    position = context.portfolio.positions[stock]
                    self.close_position(position)
                else:
                    log.info(f"持有股票 {stock}")

            # 对目标股票执行买入操作
            self.buy_security(context, target_list)
            # 更新当天已买入记录，防止重复买入
            for position in list(context.portfolio.positions.values()):
                if position.security not in self.not_buy_again:
                    self.not_buy_again.append(position.security)

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
        self.check_im_basis(context)
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

    def check_high_volume(self, context: Any) -> None:
        """
        检查持仓股票当日成交量是否异常放量：
        如果当日成交量大于过去 HV_duration 天内最大成交量的 HV_ratio 倍，则视为异常，执行卖出操作。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        current_data = get_current_data()
        for stock in list(context.portfolio.positions.keys()):
            if current_data[stock].paused:
                continue
            if current_data[stock].last_price == current_data[stock].high_limit:
                continue
            if context.portfolio.positions[stock].closeable_amount == 0:
                continue
            df_volume = get_bars(
                stock,
                count=self.HV_duration,
                unit='1d',
                fields=['volume'],
                include_now=True,
                df=True
            )
            if df_volume is not None and not df_volume.empty:
                if df_volume['volume'].iloc[-1] > self.HV_ratio * df_volume['volume'].max():
                    log.info(f"检测到股票 {stock} 出现异常放量，执行卖出操作。")
                    position = context.portfolio.positions[stock]
                    self.close_position(position)

    # 过滤器函数（均采用列表推导式实现，确保在遍历时不会修改列表）

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

    # 以下为下单及仓位管理函数

    def order_target_value_(self, security: str, value: float) -> Any:
        """
        封装 order_target_value 函数进行下单，同时记录中文日志和异常信息

        参数:
            security: 股票代码
            value: 下单目标金额

        返回:
            下单后生成的订单对象；若失败返回 None
        """
        if value != 0:
            log.debug(f"正在为 {security} 下单，目标金额 {value}")
        try:
            order = order_target_value(security, value)
            return order
        except Exception as e:
            log.error(f"股票 {security} 下单时出错，目标金额 {value}，错误信息: {e}")
            return None

    def open_position(self, security: str, value: float) -> bool:
        """
        开仓操作：尝试买入指定股票

        参数:
            security: 股票代码
            value: 分配给该股票的资金

        返回:
            若下单成功（部分或全部成交）返回 True，否则返回 False
        """
        order = self.order_target_value_(security, value)
        if order is not None and order.filled > 0:
            return True
        return False

    def close_position(self, position: Any) -> bool:
        """
        平仓操作：尽可能将指定股票仓位全部卖出

        参数:
            position: 持仓对象

        返回:
            若下单后订单全部成交返回 True，否则返回 False
        """
        security = position.security
        order = self.order_target_value_(security, 0)
        if order is not None and order.status == OrderStatus.held and order.filled == order.amount:
            return True
        return False

    def buy_security(self, context: Any, target_list: List[str]) -> None:
        """
        买入操作：对目标股票执行买入，下单资金均摊分配

        参数:
            context: 聚宽平台传入的交易上下文对象
            target_list: 目标股票代码列表
        """
        position_count = len(context.portfolio.positions)
        target_num = len(target_list)
        if target_num > position_count:
            try:
                value = context.portfolio.cash / (target_num - position_count)
            except ZeroDivisionError as e:
                log.error(f"资金分摊时除零错误: {e}")
                return
            for stock in target_list:
                if context.portfolio.positions[stock].total_amount == 0:
                    if self.open_position(stock, value):
                        log.info(f"已买入股票 {stock}，分配资金 {value:.2f}")
                        self.not_buy_again.append(stock)
                        if len(context.portfolio.positions) == target_num:
                            break

    def today_is_between(self, context: Any) -> bool:
        """
        判断当前日期是否为资金再平衡（空仓）日，通常在04月或01月期间执行空仓操作

        参数:
            context: 聚宽平台传入的交易上下文对象

        返回:
            若为空仓日返回 True，否则返回 False
        """
        today_str = context.current_dt.strftime('%m-%d')
        # if self.pass_april:
        #     if ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-30'):
        #         return True
        #     else:
        #         return False
        # else:
        #     return False
        return False

    def close_account(self, context: Any) -> None:
        """
        清仓操作：若当天为空仓日，则平仓所有持仓股票

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if self.no_trading_today_signal:
            if self.hold_list:
                for stock in self.hold_list:
                    position = context.portfolio.positions[stock]
                    self.close_position(position)
                    log.info(f"空仓日平仓，卖出股票 {stock}。")

    def print_position_info(self, context: Any) -> None:
        """
        打印当前持仓详细信息，包括股票代码、成本价、现价、涨跌幅、持仓股数和市值

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        for position in list(context.portfolio.positions.values()):
            securities: str = position.security
            cost: float = position.avg_cost
            price: float = position.price
            ret: float = 100 * (price / cost - 1)
            value: float = position.value
            amount: int = position.total_amount
            print(f"股票: {securities}")
            print(f"成本价: {cost:.2f}")
            print(f"现价: {price:.2f}")
            print(f"涨跌幅: {ret:.2f}%")
            print(f"持仓: {amount}")
            print(f"市值: {value:.2f}")
            print("--------------------------------------")
        print("********** 持仓信息打印结束 **********")


# 创建全局策略实例，策略入口处使用该实例
strategy = TradingStrategy()


# 全局包装函数，必须为顶层函数，保证调度任务可序列化，不使用 lambda

def prepare_stock_list_func(context: Any) -> None:
    """
    包装调用策略实例的 prepare_stock_list 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.prepare_stock_list(context)


def check_holdings_yesterday_func(context: Any) -> None:
    """
    包装调用策略实例的 check_holdings_yesterday 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.check_holdings_yesterday(context)


def weekly_adjustment_func(context: Any) -> None:
    """
    包装调用策略实例的 weekly_adjustment 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.weekly_adjustment(context)


def sell_stocks_func(context: Any) -> None:
    """
    包装调用策略实例的 sell_stocks 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.sell_stocks(context)


def trade_afternoon_func(context: Any) -> None:
    """
    包装调用策略实例的 trade_afternoon 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.trade_afternoon(context)


def close_account_func(context: Any) -> None:
    """
    包装调用策略实例的 close_account 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.close_account(context)


def print_position_info_func(context: Any) -> None:
    """
    包装调用策略实例的 print_position_info 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.print_position_info(context)


def initialize(context: Any) -> None:
    """
    聚宽平台的全局初始化函数

    该函数用于：
      1. 调用策略实例的 initialize 方法配置交易环境；
      2. 通过全局包装函数注册各项调度任务（每天或每周定时运行）。

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    # 初始化策略环境及参数
    strategy.initialize(context)
    
    # 注册调度任务，所有任务均使用顶层包装函数（不使用 lambda 以确保可序列化）
    run_daily(prepare_stock_list_func, time='9:05')
    run_daily(check_holdings_yesterday_func, time='9:00')
    # run_weekly 的第二个参数为星期几（例如 2 表示星期二），以位置参数传入
    run_weekly(weekly_adjustment_func, 2, time='10:30')
    run_daily(sell_stocks_func, time='10:00')
    run_daily(trade_afternoon_func, time='14:30')
    run_daily(close_account_func, time='14:50')
    # run_weekly 的星期参数，此处传入 5 表示星期五
    run_weekly(print_position_info_func, 5, time='15:10')