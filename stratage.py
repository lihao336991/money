
from datetime import datetime,timedelta

class TradingStrategy:
    """
    交易策略类

    封装了选股、调仓、买卖、止损与风控管理的核心逻辑。
    通过类属性管理持仓、候选股票等状态，并使用状态机字典记录交易信号，
    便于后续调试、扩展和维护。
    """
    def __init__(self) -> None:
        # 策略基础配置和状态变量
        self.no_trading_today_signal: bool = False  # 【慎用！！！快捷平仓选项】当天是否执行空仓（资金再平衡）操作
        self.pass_april: bool = True                # 是否在04月或01月期间执行空仓策略
        self.run_stoploss: bool = True              # 是否启用止损策略

        # 持仓和调仓记录
        self.hold_list: List[str] = []             # 当前持仓股票代码列表
        self.yesterday_HL_list: List[str] = []       # 昨日涨停的股票列表（收盘价等于涨停价）
        self.target_list: List[str] = []             # 本次调仓候选股票列表
        self.not_buy_again: List[str] = []           # 当天已买入的股票列表，避免重复下单

        # 策略交易及风控的参数
        self.stock_num: int = 7                    # 每次调仓目标持仓股票数量
        self.up_price: float = 100.0               # 股票价格上限过滤条件（排除股价超过此值的股票）
        self.reason_to_sell: str = ''              # 记录卖出原因（例如：'limitup' 涨停破板 或 'stoploss' 止损）
        self.stoploss_strategy: int = 3            # 止损策略：1-个股止损；2-大盘止损；3-联合止损策略
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
        # TODO 以下设置需要在iquant上回测时设置
        # 启用防未来数据以确保历史回测的严谨性
        # set_option('avoid_future_data', True)
        # 设置策略基准为上证指数
        # set_benchmark('000001.XSHG')
        # 使用真实市场价格，模拟实际交易
        # set_option('use_real_price', True)
        # 设置固定滑点，确保价格偏差不大
        # set_slippage(FixedSlippage(3 / 10000))
        # 设置订单成本，包括印花税和佣金
        # set_order_cost(OrderCost(
        #     open_tax=0,
        #     close_tax=0.001,  # 卖出时0.1%的印花税
        #     open_commission=2.5 / 10000,
        #     close_commission=2.5 / 10000,
        #     close_today_commission=0,
        #     min_commission=5  # 最低佣金为5元
        # ), type='stock')
        # 设置日志输出等级（中文日志输出）
        # log.set_level('order', 'error')
        # log.set_level('system', 'error')
        # log.set_level('strategy', 'debug')
        # 注意：调度任务由全局包装函数统一注册，避免 lambda 导致序列化问题
        context.account = "620000204906"
        context.accountType = ""

    # Position的完整品种代码
    def codeOfPosition(position):
        return position.m_strInstrumentID + '.' + position.m_strExchangeID
    def get_limit_of_stock(stock_code, last_close):
        if str(stock_code).startswith(tuple(['3', '688'])):
            return [round(last_close * 1.2, 2), round(last_close * 0.8), 2]
        return [round(last_close * 1.1, 2), round(last_close * 0.9), 2]
    
    def check_holdings_yesterday(self, context: Any) -> None:
        """
        检查并输出每只持仓股票昨日的交易数据（开盘价、收盘价、涨跌幅）。

        此方法只做了日志打印，因此初始版本不要也罢，后续再完善。
        """
        log.info("检查每只持仓股票昨日交易数据——此功能待实现")
        # 这里给context挂一个positions持仓对象，仅盘前可以复用，盘中要实时取数据不能使用这个
        context.positions = context.get_trade_detail_data(context.account, context.accountType, 'position')

        # if not positions:
        #     log.info("昨日没有持仓数据。")
        #     return

        # log.info("检查每只持仓股票昨日交易数据：")
        # for position in positions.items():
        #     stock = self.codeOfPosition(position)
        #     try:
        #         # 获取股票昨日的开盘价和收盘价
        #         df = DataHelper.get_price_safe(
        #             stock,
        #             end_date=context.previous_date,
        #             frequency="daily",
        #             fields=['open', 'close'],
        #             count=1,
        #             panel=False
        #         )
        #         if df is None or df.empty:
        #             log.info(f"无法获取股票 {stock} 的昨日数据。")
        #             continue
        #         open_price: float = df.iloc[0]['open']
        #         close_price: float = df.iloc[0]['close']
        #         change_pct: float = (close_price / open_price - 1) * 100
        #         log.info(f"股票 {stock}：持仓 {position.total_amount} 股，开盘价 {open_price:.2f}，收盘价 {close_price:.2f}，涨跌幅 {change_pct:.2f}%")
        #     except Exception as e:
        #         log.error(f"处理股票 {stock} 数据时出错: {e}")

    # 通用方法，返回给定list里昨日涨跌停的股票
    def find_limit_list(self, context, stock_list):
        high_list = []
        low_list = []
        if stock_list:
            data = context.get_market_data_ex(
                ['open', 'close'],                
                stock_list,
                period="1d",
                start_time = "",
                end_time = "",
                count=1,
                dividend_type = "follow",
                fill_data = True,
                subscribe = True
            )
            for stock in data:
                df = data[stock]
                df['pre'] = df['close'].shift(1)
                df['high_limit'] = self.get_limit_of_stock(stock, df['pre'])[0]
                df['low_limit'] = self.get_limit_of_stock(stock, df['pre'])[1]
                df['is_up_to_hight_limit'] = df['close'] == df['high_limit']
                df['is_down_to_low_limit'] = df['close'] == df['low_limit']
                # 是否涨停
                if df.at[1, "is_up_to_hight_limit"]:
                    high_list.append(stock)
                # 是否跌停
                if df.at[1, "is_down_to_low_limit"]:
                    low_list.append(stock)
        return { high_list, low_list }

    def prepare_stock_list(self, context: Any) -> None:
        """
        更新持仓股票列表和昨日涨停股票列表，同时判断是否为空仓日（资金再平衡日）。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        # 从当前持仓中提取股票代码，更新持仓列表
        self.hold_list = [self.codeOfPosition(position) for position in list(context.positions.values())]
        # 取出涨停列表
        self.yesterday_HL_list = self.find_limit_list(self.hold_list).high_list
        # 根据当前日期判断是否为空仓日（例如04月或01月时资金再平衡）
        self.no_trading_today_signal = self.today_is_between(context)
        # 新增属性，快捷获取当前日期
        index = context.barpos
        currentTime = context.get_bar_timetag(index)
        context.today = currentTime.strftime('%y-%m-%d')
        
    def get_stock_pool(self, context: Any) -> List[str]:
        return context.get_sector('399101.XSHE')

    # Position的完整品种代码
    def codeOfPosition(position):
        return position.m_strInstrumentID + '.' + position.m_strExchangeID

    def get_stock_list(self, context: Any) -> List[str]:
        """
        选股模块：
        1. 从指定股票池（如 399101.XSHE 指数成分股）中获取初步股票列表；
        2. 应用多个过滤器筛选股票（次新股、科创股、ST、停牌、涨跌停等）；
        3. 基于基本面数据（EPS、市值）排序后返回候选股票列表。

        返回:
            筛选后的候选股票代码列表
        """
        # 从指定指数中获取初步股票列表
        initial_list: self.get_stock_pool(context)

        # 依次应用过滤器，筛去不符合条件的股票
        initial_list = self.filter_new_stock(context, initial_list)   # 过滤次新股
        # TODO 假如不过滤科创北交呢？
        initial_list = self.filter_kcbj_stock(initial_list)             # 过滤科创/北交股票
        initial_list = self.filter_st_stock(context, initial_list)               # 过滤ST或风险股票
        initial_list = self.filter_paused_stock(context, initial_list)           # 过滤停牌股票
        # TODO 这两个方法对板块内每个股票重复执行性能可能不好，重新实现，一次性获取整体数据，后续防止重复调用
        initial_list = self.filter_limitup_stock(context, initial_list)   # 过滤当日涨停（未持仓时）的股票
        initial_list = self.filter_limitdown_stock(context, initial_list) # 过滤当日跌停（未持仓时）的股票


        # TODO 核心  基本面选股因子，这里聚宽的API和IQuant的API差别巨大，很可能影响最终回测结果！！！
        # 聚宽版本-利用基本面查询获取股票代码和EPS数据，并按照市值升序排序
        # q = query(valuation.code, indicator.eps) \
        #     .filter(valuation.code.in_(initial_list)) \
        #     .order_by(valuation.market_cap.asc())
        # df = get_fundamentals(q)

        # 创建DataFrame容器
        df_result = pd.DataFrame(columns=['code', 'eps', 'market_cap'])
        for code in initial_list:
            eps = context.get_financial_data(['ASHAREINCOME'], initial_list, )

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
                    self.close_position(context, position)
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
                        self.close_position(context, position)
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
                    self.close_position(context, position)

    # 过滤器函数（均采用列表推导式实现，确保在遍历时不会修改列表）

    def filter_paused_stock(self, context, stock_list: List[str]) -> List[str]:
        """
        过滤停牌的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未停牌的股票代码列表
        """
        return [stock for stock in stock_list if not context.is_suspended_stock(stock)]

    def filter_st_stock(self, context, stock_list: List[str]) -> List[str]:
        """
        过滤带有 ST 或其他风险标识的股票

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            无 ST 或风险标识的股票代码列表
        """
        def not_st_stock(stock_data):
            return ('ST' not in stock_data.InstrumentName) and ('*' not in stock_data.InstrumentName) and ('退' not in stock_data.InstrumentName) and (stock_data.ExpireDate == 0 or stock_data.ExpireDate == 99999999)
        return [stock for stock in stock_list if not_st_stock(context.get_instrument_detail(stock))]

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
        data = self.filter_limitup_stock(context, stock_list)
        return [stock for stock in stock_list if stock not in data.high_list]

    def filter_limitdown_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤当天已经跌停的股票（若未持仓则过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        data = self.filter_limitup_stock(context, stock_list)
        return [stock for stock in stock_list if stock not in data.low_list]

    def filter_new_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤次新股：排除上市时间不足375天的股票

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        today = context.today
        yesterday = today - timedelta(days=1)
        return [stock for stock in stock_list if not (yesterday - context.get_open_date(stock) < timedelta(days=375) and context.get_instrument_detail(stock).OpenDate == 19700101)]

    def filter_highprice_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤股价高于设定上限（up_price）的股票（非持仓股票参与过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        return [stock for stock in stock_list if context.get_instrument_detail(stock).PreClose <= self.up_price]

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
    def order_target_value_(self, context, security: str, value: float) -> Any:
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
            order_target_value(security, value, context, context.account)
            return True
        except Exception as e:
            log.error(f"股票 {security} 下单时出错，目标金额 {value}，错误信息: {e}")
            return None

    def open_position(self, context, security: str, value: float) -> bool:
        """
        开仓操作：尝试买入指定股票

        参数:
            security: 股票代码
            value: 分配给该股票的资金

        返回:
            若下单成功（部分或全部成交）返回 True，否则返回 False
        """
        order = self.order_target_value_(context, security, value)
        if order is not None and order.filled > 0:
            return True
        return False

    def close_position(self, context, position: Any) -> bool:
        """
        平仓操作：尽可能将指定股票仓位全部卖出

        参数:
            position: 持仓对象

        返回:
            若下单后订单全部成交返回 True，否则返回 False
        """
        security = self.codeOfPosition(position)
        order = self.order_target_value_(context, security, 0)
        if order is not None:
            return True
        return False

    def buy_security(self, context: Any, target_list: List[str]) -> None:
        """
        买入操作：对目标股票执行买入，下单资金均摊分配

        参数:
            context: 聚宽平台传入的交易上下文对象
            target_list: 目标股票代码列表
        """
        position_count = len(context.positions)
        target_num = len(target_list)
        if target_num > position_count:
            try:
                value = context.accountInfo.m_dAvailable / (target_num - position_count)
            except ZeroDivisionError as e:
                log.error(f"资金分摊时除零错误: {e}")
                return
            for stock in target_list:
                if context.positions[stock].m_nVolume == 0:
                    if self.open_position(stock, value):
                        # TODO 放在成交主推回调中实现
                        log.info(f"已买入股票 {stock}，分配资金 {value:.2f}")
                        self.not_buy_again.append(stock)
                        if len(context.positions) == target_num:
                            break

    def today_is_between(self, context: Any) -> bool:
        """
        判断当前日期是否为资金再平衡（空仓）日，通常在04月或01月期间执行空仓操作

        参数:
            context: 聚宽平台传入的交易上下文对象

        返回:
            若为空仓日返回 True，否则返回 False
        """
        index = context.barpos
        currentTime = context.get_bar_timetag(index)
        today_str = currentTime.strftime('%m-%d')
        if self.pass_april:
            if ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-30'):
                return True
            else:
                return False
        else:
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
                    position = context.positions[stock]
                    self.close_position(context, position)
                    log.info(f"空仓日平仓，卖出股票 {stock}。")

    def print_position_info(self, context: Any) -> None:
        """
        打印当前持仓详细信息，包括股票代码、成本价、现价、涨跌幅、持仓股数和市值

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        for position in list(context.positions.values()):
            cost: float = position.m_dOpenPrice
            price: float = position.m_dLastPrice
            ret: float = 100 * (price / cost - 1)
            value: float = position.m_dMarketValue
            amount: int = position.m_nVolume
            print(f"股票: {self.codeOfPosition(position)}")
            print(f"成本价: {cost:.2f}")
            print(f"现价: {price:.2f}")
            print(f"涨跌幅: {ret:.2f}%")
            print(f"持仓: {amount}")
            print(f"市值: {value:.2f}")
            print("--------------------------------------")
        print("********** 持仓信息打印结束 **********")
    
    def account_callback(self, context, accountInfo):
        print(accountInfo)
        context.accountInfo = accountInfo
        return accountInfo

# 创建全局策略实例，策略入口处使用该实例
strategy = TradingStrategy()
