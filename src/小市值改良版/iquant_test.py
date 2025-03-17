
#coding:gbk

# 回测用的版本


from typing import Any, List, Dict, Optional
from datetime import datetime, timedelta, time
import numpy as np
import pandas as pd
import requests
import json

class Messager:
    def __init__(self):
        # 消息通知
        self.webhook1 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6c1bd45a-74a7-4bd0-93ce-00b2e7157adc'
        # 日志记录
        self.webhook2 = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=a1f9970c-4914-49de-b69a-e447a5d97c64'
    def send_message(self, webhook, message):
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
    # 发送消息
    def sendLog(self, message):
        # 开关控制，默认关闭
        # self.send_message(self.webhook2, message)
        print(message)
  
    def send_deal(self, dealInfo):
        stock = dealInfo['m_strProductName']
        price = dealInfo['m_dPrice']
        amount = dealInfo['m_dTradeAmount']
        markdown = f"""
        新增买入股票: <font color='warning'>{stock}</font>
        > 成交价: <font color='warning'>{price}/font>
        > 成交额: <font color='warning'>{amount}</font>
        """
        self.send_message(self.webhook1, markdown)

    def send_positions(self, positions):
        # stock = position['m_strProductName']
        df_result = pd.DataFrame(columns=['stock', 'price', 'open_price', 'amount', 'ratio', 'profit'])
        for position in positions:
            df_result = df_result.append({
            'stock': position['m_strInstrumentName'],
            'price': position['m_dLastPrice'],
            'open_price': position['m_dOpenPrice'],
            'amount': position['m_dMarketValue'],
            'ratio': position['m_dProfitRate'],
            'profit': position['m_dFloatProfit'],
            }, ignore_index=True)

        markdown = """
        ## 📈 股票持仓报告
        """
        num = len(df_result)
        total_profit = df_result['profit'].sum()
        if total_profit > 0:
            total_profit = f"<font color='info'>{total_profit}%</font>"
        else:
            total_profit = f"<font color='warning'>-{total_profit}%</font>"

        for index, row in df_result.iterrows():
            row_str = self.get_position_markdown(row)
            markdown += row_str
        markdown += f"""
        ---
        **持仓统计**
        ▶ 总持仓数：`{num} 只`
        ▶ 总盈亏额：{total_profit}
        > 数据更新频率：每小时自动刷新
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
            ratio_str = f"<font color='info'>{ratio_str}%</font>"
        else:
            ratio_str = f"<font color='warning'>-{ratio_str}%</font>"
        profit = position['profit']
        if profit > 0:
            profit = f"<font color='info'>{profit}%</font>"
        else:
            profit = f"<font color='warning'>-{profit}%</font>"
        return f"""
        ▪️ **{stock}**
        　├─ 当前价：`{price}`
        　├─ 成本价：`{open_price}`
        　├─ 持仓额：`¥{amount}`
        　├─ 盈亏率：`{ratio_str}`
        　└─ 盈亏额：`¥{profit}`
        """
messager = Messager()
class Log:
    def debug(*args):
        print(*args)
    def error(*args):
        print('[log error]', *args)
log = Log()

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
        self.stoploss_market: float = -0.94         # 大盘止损参数（若整体跌幅过大则触发卖出）

        self.HV_control: bool = False              # 是否启用成交量异常检测
        self.HV_duration: int = 120                # 检查成交量时参考的历史天数
        self.HV_ratio: float = 0.9                 # 当天成交量超过历史最高成交量的比例（如0.9即90%）

        # 状态机字典，记录交易信号和当前风险水平
        self.state: Dict[str, Any] = {
            'buy_signal': False,
            'sell_signal': False,
            'risk_level': 'normal'
        }

        self.pool_initialized = False

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
        context.set_account(context.account)
        context.accountType = ""

    # 根据股票代码和收盘价，计算次日涨跌停价格
    def get_limit_of_stock(self, stock_code, last_close):
        if str(stock_code).startswith(tuple(['3', '688'])):
            return [round(last_close * 1.2, 2), round(last_close * 0.8), 2]
        return [round(last_close * 1.1, 2), round(last_close * 0.9), 2]
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

    # 根据当前日期，返回对应的最新财报时间段
    def get_latest_report_date(self, context):
        index = context.barpos
        currentTime = context.get_bar_timetag(index) + 8 * 3600 * 1000
        year = int(datetime.fromtimestamp(currentTime).strftime('%Y'))
        month = int(datetime.fromtimestamp(currentTime).strftime('%m'))
        # 判断当前季度并设置报告截止日期
        if month <= 3:
            return datetime.date(year-1, 12, 31)  # 上一年年报
        elif month <= 6:
            return datetime.date(year, 3, 31)     # 一季度
        elif month <= 9:
            return datetime.date(year, 6, 30)     # 半年报
        else:
            return datetime.date(year, 9, 30)     # 三季报

    def check_holdings_yesterday(self, context: Any) -> None:
        """
        检查并输出每只持仓股票昨日的交易数据（开盘价、收盘价、涨跌幅）。

        此方法只做了日志打印，因此初始版本不要也罢，后续再完善。
        """
        # 这里给context挂一个positions持仓对象，仅盘前可以复用，盘中要实时取数据不能使用这个
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')

        # if not positions:
        #     print("昨日没有持仓数据。")
        #     return

        # print("检查每只持仓股票昨日交易数据：")
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
        #             print(f"无法获取股票 {stock} 的昨日数据。")
        #             continue
        #         open_price: float = df.iloc[0]['open']
        #         close_price: float = df.iloc[0]['close']
        #         change_pct: float = (close_price / open_price - 1) * 100
        #         print(f"股票 {stock}：持仓 {position.total_amount} 股，开盘价 {open_price:.2f}，收盘价 {close_price:.2f}，涨跌幅 {change_pct:.2f}%")
        #     except Exception as e:
        #         print(f"处理股票 {stock} 数据时出错: {e}")

    # 通用方法，返回给定list里昨日涨跌停的股票
    def find_limit_list(self, context, stock_list):
        high_list = []
        low_list = []
        if stock_list:
            data = context.get_market_data_ex(
                ['open', 'close'],                
                stock_list,
                period="1d",
                start_time = (context.today - timedelta(days=1)).strftime('%Y%m%d'),
                end_time = context.today.strftime('%Y%m%d'),
                count=2,
                dividend_type = "follow",
                fill_data = True,
                subscribe = True
            )
            for stock in data:
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
        dic = {}
        dic['high_list'] = high_list
        dic['low_list'] = low_list
        return dic

    def prepare_stock_list(self, context: Any) -> None:
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

    # 【回测时使用】回测初始状态跑一遍当时的市值前200名股票，之后都在这200只里选择，为了优化性能（取市值时只能跑全量最新价格，非常费性能）
    def get_stock_pool_when_test(self, context: Any) -> List[str]:
        whole_list = context.get_stock_list_in_sector('中小综指')
        list = self.sort_by_market_cup(context, whole_list)
        self.pool = list[:100]
        self.pool_initialized = True
        return self.pool

    # 正常来说，是每次都从中小板取所有股票来筛选，但是回测性能太差，只用于实盘    
    def get_stock_pool(self, context: Any) -> List[str]:
        return context.get_stock_list_in_sector('中小综指')

    # Position的完整品种代码
    def codeOfPosition(self, position):
        return position.m_strInstrumentID + '.' + position.m_strExchangeID
    
    def sort_by_market_cup(self, context, origin_list) -> List[str]:
        ticks = context.get_market_data_ex(
            ['close'],                
            origin_list,
            period="1d",
            start_time = (context.today - timedelta(days=1)).strftime('%Y%m%d'),
            end_time = context.today.strftime('%Y%m%d'),
            count=1,
            dividend_type = "follow",
            fill_data = True,
            subscribe = False
        )
        df_result = pd.DataFrame(columns=['code','name', 'lastPrice', 'market_cap', 'stock_num'])
        seconds_per_year = 365 * 24 * 60 * 60  # 未考虑闰秒
        lastYearCurrentTime = context.currentTime / 1000 - seconds_per_year
        end_date = datetime.fromtimestamp(context.currentTime / 1000).strftime('%Y%m%d')
        start_date = datetime.fromtimestamp(lastYearCurrentTime).strftime('%Y%m%d')
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
            except Exception as e:
                # continue
                print(code, ticks[code])
        df_result = df_result.sort_values(by='market_cap', ascending=True)
        return list(df_result['code'])


    # 基本面选股：根据国九条，过滤净利润为负且营业收入小于1亿的股票
    def filter_stock_by_gjt(self, context):
        print('开始每周选股环节（基本面初筛） =====================>')
        # 不每次取全量数据，这里首次
        if self.pool:
            initial_list = self.pool
        else:
            initial_list = self.get_stock_pool(context)
        
        seconds_per_year = 365 * 24 * 60 * 60  # 未考虑闰秒
        lastYearCurrentTime = context.currentTime / 1000 - seconds_per_year
        end_date = datetime.fromtimestamp(context.currentTime / 1000).strftime('%Y%m%d')
        start_date = datetime.fromtimestamp(lastYearCurrentTime).strftime('%Y%m%d')
        eps = context.get_raw_financial_data(['利润表.净利润', '利润表.营业收入', '股本表.总股本'], initial_list, start_date, end_date)
        
        df_result = pd.DataFrame(columns=['code', 'name', 'market_cap', 'lastPrice', 'stock_num'])
        finance = 0
        income = 0
        ticks = context.get_market_data_ex(
            ['close'],                
            initial_list,
            period="1d",
            start_time = context.today.strftime('%Y%m%d'),
            end_time = context.today.strftime('%Y%m%d'),
            count=1,
            dividend_type = "follow",
            fill_data = True,
            subscribe = False
        )
        # print(ticks, '看看tocks')
        for code in initial_list:
            # TODO 基本面筛选，去年净利润大于1e，营业收入大于1e
            finance_list = list(eps[code]['利润表.净利润'].values())
            income_list = list(eps[code]['利润表.营业收入'].values())
            stock_num_list = list(eps[code]['股本表.总股本'].values())
            if finance_list and income_list and stock_num_list:
                finance = finance_list[-1]
                income = income_list[-1]
                stock_num = stock_num_list[-1]
            # money = eps[code].loc[end_date, '资产负债表.固定资产']
            # 筛选出净利润大于0，营业收入大于1e的股票，期末净资产为正的 
            if eps is not None and eps[code] is not None and finance > 0 and income > 100000000:
                market_cap = ticks[code].iloc[0, 0] * stock_num
                df_result = df_result.append({
                    'code': code,
                    'name': context.get_stock_name(code),
                    'market_cap': market_cap,
                    'lastPrice': ticks[code].iloc[0, 0],
                    'stock_num': stock_num
                    }, ignore_index=True)
        df_result = df_result.sort_values(by='market_cap', ascending=True)
        stock_list: List[str] = list(df_result.code)
        # print("看看前20的股票", df_result[:20])
        return stock_list

    def get_stock_list(self, context: Any) -> List[str]:
        """
        选股模块：
        1. 从指定股票池（如 399101.XSHE 指数成分股）中获取初步股票列表；
        2. 应用多个过滤器筛选股票（次新股、科创股、ST、停牌、涨跌停等）；
        3. 基于基本面数据（EPS、市值）排序后返回候选股票列表。

        返回:
            筛选后的候选股票代码列表
        """
        print('开始每周选股环节 =====================>')
        # 从指定指数中获取初步股票列表
        initial_list = self.filter_stock_by_gjt(context)

        initial_list = self.filter_kcbj_stock(initial_list)             # 过滤科创/北交股票
        
        # 依次应用过滤器，筛去不符合条件的股票
        initial_list = self.filter_new_stock(context, initial_list)   # 过滤次新股
        initial_list = self.filter_st_stock(context, initial_list)    # 过滤ST或风险股票
        initial_list = self.filter_paused_stock(context, initial_list)           # 过滤停牌股票
        
        
        initial_list = initial_list[:100]  # 限制数据规模，防止一次处理数据过大
        # 性能不好，回测不开
        initial_list = self.filter_limitup_stock(context, initial_list)   # 过滤当日涨停（未持仓时）的股票
        initial_list = self.filter_limitdown_stock(context, initial_list) # 过滤当日跌停（未持仓时）的股票
        
        # 取前2倍目标持仓股票数作为候选池
        final_list: List[str] = initial_list[:2 * self.stock_num]


        # TODO 增加更多选股因子：30日均成交量（流动性），涨停基因（1年内有过>5次涨停记录）

        print(f"候选股票{len(final_list)}只: {final_list}")

        # 下面注释部分不参与实际功能，只是日志打印，暂时忽略
        # 查询并输出候选股票的财务信息（如财报日期、营业收入、EPS）
        # if final_list:
        #     info_query = query(
        #         valuation.code,
        #         income.pubDate,
        #         income.statDate,
        #         income.operating_revenue,
        #         indicator.eps
        #     ).filter(valuation.code.in_(final_list))
        #     df_info = get_fundamentals(info_query)
        #     for _, row in df_info.iterrows():
        #         print(f"股票 {row['code']}：报告日期 {row.get('pubDate', 'N/A')}，统计日期 {row.get('statDate', 'N/A')}，营业收入 {row.get('operating_revenue', 'N/A')}，EPS {row.get('eps', 'N/A')}")
        return final_list

    def weekly_adjustment(self, context: Any) -> None:
        """
        每周调仓策略：
        如果非空仓日，先选股得到目标股票列表，再卖出当前持仓中不在目标列表且昨日未涨停的股票，
        最后买入目标股票，同时记录当天买入情况避免重复下单。

        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions]
        print(self.no_trading_today_signal, '禁止交易信号')
        if not self.no_trading_today_signal:
            self.not_buy_again = []  # 重置当天已买入记录
            self.target_list = self.get_stock_list(context)
            # 取目标持仓数以内的股票作为调仓目标
            target_list: List[str] = self.target_list[:self.stock_num]
            print(f"每周调仓目标股票: {target_list}")
            print(f"当前持有股票: {self.hold_list}")
            for stock in self.hold_list:
                if stock not in target_list and stock not in self.yesterday_HL_list:
                    print(f"卖出股票 {stock}")
                    self.close_position(context, stock)
                else:
                    print(f"持有股票 {stock}")


    def weekly_adjustment_buy(self, context: Any) -> None:
        if not self.no_trading_today_signal:
            # 遍历当前持仓，若股票不在目标列表且非昨日涨停，则执行卖出操作
            target_list: List[str] = self.target_list[:self.stock_num]

            # 对目标股票执行买入操作
            self.buy_security(context, target_list)
            if self.positions:
                # 更新当天已买入记录，防止重复买入
                for position in self.positions:
                    if self.codeOfPosition(position) not in self.not_buy_again:
                        self.not_buy_again.append(self.codeOfPosition(position))

    def check_limit_up(self, context: Any) -> None:
        """
        检查昨日处于涨停状态的股票在当前是否破板。
        如破板（当前价格低于涨停价），则立即卖出该股票，并记录卖出原因为 "limitup"。

        """
        if self.yesterday_HL_list:
            # ticks = context.get_full_tick(self.yesterday_HL_list)
            ticksOfDay = context.get_market_data_ex(
                ['close'],                
                self.yesterday_HL_list,
                period="1d",
                start_time = (context.today - timedelta(days=1)).strftime('%Y%m%d'),
                end_time = context.today.strftime('%Y%m%d'),
                count=2,
                dividend_type = "follow",
                fill_data = True,
                subscribe = False
            )
            print(ticksOfDay, '**持仓票信息-day')
            for stock in self.yesterday_HL_list:
                price = ticksOfDay[stock]["close"].iloc[-1]
                lastClose = ticksOfDay[stock]["close"].iloc[0]
                high_limit = self.get_limit_of_stock(stock, lastClose)[0]

                if price < high_limit:
                    print(f"股票 {stock} 涨停破板，触发卖出操作。")
                    self.close_position(context, stock)
                    self.reason_to_sell = 'limitup'
                else:
                    print(f"股票 {stock} 仍维持涨停状态。")
    
    def check_remain_amount(self, context: Any) -> None:
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

    def trade_afternoon(self, context: Any) -> None:
        """
        下午交易任务：
        1. 检查是否有因为涨停破板触发的卖出信号；
        2. 如启用了成交量监控，则检测是否有异常成交量；
        3. 检查账户中是否需要补仓。
        """
        if not self.no_trading_today_signal:
            self.check_limit_up(context)
            if self.HV_control:
                self.check_high_volume(context)
            self.check_remain_amount(context)

    # 获取板块的涨跌幅情况
    def get_whole_market_data(self, context):
        code = '399101.SZ'
        data = context.get_market_data_ex(
            [],                
            [code],
            period="1d",
            start_time = context.today.strftime('%Y%m%d'),
            end_time = context.today.strftime('%Y%m%d'),
            count=2,
            dividend_type = "follow",
            fill_data = True,
            subscribe = True
        )[code]
        lastPrice = data['close'][-1]
        lastClose = data['open'][-1]
        percent = round(100 * (lastPrice - lastClose) / lastClose, 2)
        return percent
        
    def sell_stocks(self, context: Any) -> None:
        """
        止盈与止损操作：
        根据策略（1: 个股止损；2: 大盘止损；3: 联合策略）判断是否执行卖出操作。
        """
        if self.positions:
            # print(self.positions, '——————————sell_stocks')
            if self.run_stoploss:
                if self.stoploss_strategy == 1:
                    # 个股止盈或止损判断
                    for stock in self.get_stock_list_of_positions(context):
                        pos = self.find_stock_of_positions(stock)
                        if pos.m_dSettlementPrice >= pos.m_dOpenPrice * 2:
                            self.close_position(context, stock)
                            log.debug(f"股票 {stock} 实现100%盈利，执行止盈卖出。")
                        elif pos.m_dSettlementPrice < pos.m_dOpenPrice * self.stoploss_limit:
                            self.close_position(context, stock)
                            log.debug(f"股票 {stock} 触及止损阈值，执行卖出。")
                            self.reason_to_sell = 'stoploss'
                elif self.stoploss_strategy == 2:
                    # 大盘止损判断，若整体市场跌幅过大则平仓所有股票
                    down_ratio = self.get_whole_market_data(context)
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        log.debug(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in self.get_stock_list_of_positions(context):
                            self.close_position(context, stock)
                elif self.stoploss_strategy == 3:
                    # 联合止损策略：结合大盘和个股判断
                    down_ratio = self.get_whole_market_data(context)
                    if down_ratio <= self.stoploss_market:
                        self.reason_to_sell = 'stoploss'
                        log.debug(f"市场检测到跌幅（平均跌幅 {down_ratio}），卖出所有持仓。")
                        for stock in self.get_stock_list_of_positions(context):
                            self.close_position(context, stock)
                    else:
                        for stock in self.get_stock_list_of_positions(context):
                            pos = self.find_stock_of_positions(stock)
                            if pos.m_dSettlementPrice < pos.m_dOpenPrice * self.stoploss_limit:
                                self.close_position(context, stock)
                                log.debug(f"股票 {stock} 触及止损，执行卖出。")
                                self.reason_to_sell = 'stoploss'

    # 判断某只股票是否到达涨停
    def check_is_high_limit(self, context, stock):
        # data = context.get_full_tick([stock])[stock]
        data = context.get_market_data_ex(
            ['lastPrice', 'lastClose'],                
            [stock],
            period="1m",
            start_time = context.today.strftime('%Y%m%d'),
            end_time = context.today.strftime('%Y%m%d'),
            count=1,
            dividend_type = "follow",
            fill_data = True,
            subscribe = True
        )[stock]
        price = data["lastPrice"]
        lastClose = data["lastClose"]
        high_limit = self.get_limit_of_stock(stock, lastClose)[0]
        return price >= high_limit
    
    # 是否是过去n天内最大成交量
    def get_max_volume_last_period(self, context, stock):
        ticks = context.get_market_data_ex(
            ['volume'], 
            [stock],
            period="1d",
            start_time = context.today.strftime('%Y%m%d'),
            end_time = context.today.strftime('%Y%m%d'),
            count=self.HV_duration,
            dividend_type = "follow",
            fill_data = True,
            subscribe = True
        )
        df = ticks[stock]
        max_volume = df["volume"].max()
        cur_volume = df.at[1, "volume"]
        return {
            max_volume,
            cur_volume
        }

    def check_high_volume(self, context: Any) -> None:
        """
        检查持仓股票当日成交量是否异常放量：
        如果当日成交量大于过去 HV_duration 天内最大成交量的 HV_ratio 倍，则视为异常，执行卖出操作。

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        for stock in self.get_stock_list_of_positions(context):
            if self.check_is_high_limit(stock):
                continue
            if self.find_stock_of_positions(stock).m_nVolume == 0:
                continue
            max_volume = self.get_max_volume_last_period(context, stock)['max_volume']
            cur_volume = self.get_max_volume_last_period(context, stock)['cur_volume']
            if cur_volume >  self.HV_ratio * max_volume:
                print(f"检测到股票 {stock} 出现异常放量，执行卖出操作。")
                self.close_position(context, stock)

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
        def not_st_stock(stock):
            name = context.get_stock_name(stock)
            stock_data = context.get_instrumentdetail(stock)
            return ('ST' not in name) and ('*' not in name) and ('退' not in name) and (stock_data['ExpireDate'] != 0 or stock_data['ExpireDate'] != 99999999)
        return [stock for stock in stock_list if not_st_stock(stock)]

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
        data = self.find_limit_list(context, stock_list)
        return [stock for stock in stock_list if stock not in data['high_list']]

    def filter_limitdown_stock(self, context: Any, stock_list: List[str]) -> List[str]:
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

    def filter_new_stock(self, context: Any, stock_list: List[str]) -> List[str]:
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
            except Exception as e:
                # 取不到数据的股票也是有问题的，可能是已退市，也当成新股过滤掉
                # print(context.get_open_date(stock), '计算新股出错啦', stock)
                return True
        return [stock for stock in stock_list if not is_new_stock(stock)]

    def filter_highprice_stock(self, context: Any, stock_list: List[str]) -> List[str]:
        """
        过滤股价高于设定上限（up_price）的股票（非持仓股票参与过滤）

        参数:
            context: 交易上下文对象
            stock_list: 待过滤的股票代码列表

        返回:
            过滤后的股票代码列表
        """
        return [stock for stock in stock_list if context.get_instrumentdetail(stock)['PreClose'] <= self.up_price]

    def filter_not_buy_again(self, stock_list: List[str]) -> List[str]:
        """
        过滤掉当日已买入的股票，避免重复下单

        参数:
            stock_list: 待过滤的股票代码列表

        返回:
            未买入的股票代码列表
        """
        return [stock for stock in stock_list if stock not in self.not_buy_again]


    def open_position(self, context, security: str, value: float) -> bool:
        """
        开仓操作：尝试买入指定股票

        参数:
            security: 股票代码
            value: 分配给该股票的资金

        返回:
            若下单成功（部分或全部成交）返回 True，否则返回 False
        """
        print("买入股票:", security, context.get_stock_name(security), int(value * 100))
        # 该函数回测不生效，暂时注释
        if context.do_back_test:
            order_target_percent(security, round(value, 2), 'COMPETE', context, context.account)
        else:
            # 1113 表示总资金百分比下单
            passorder(23, 1113, context.account, security, 5, -1, int(value * 100), "买入策略", 2, "", context)

    def close_position(self, context, stock: Any) -> bool:
        """
        平仓操作：尽可能将指定股票仓位全部卖出

        参数:
            position: 持仓对象

        返回:
            若下单后订单全部成交返回 True，否则返回 False
        """
        if stock:
            if context.do_back_test:
                order_target_value(stock, value, context, context.account)
            else:
                # 1123 表示可用股票数量下单，这里表示全卖
                passorder(24, 1123, context.account, stock, 5, 1, 100, "卖出策略", 2, "", context)
            return True

    def buy_security(self, context: Any, target_list: List[str]) -> None:
        """
        买入操作：对目标股票执行买入，下单资金均摊分配

        参数:
            context: 聚宽平台传入的交易上下文对象
            target_list: 目标股票代码列表
        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions]

        position_count = len(self.positions)
        target_num = len(target_list)
        print("下单逻辑: 持仓数: ", position_count, "目标数",  target_num)
        if target_num > position_count:
            try:
                # avalable = TACCOUNT(2, context.account)
                # value = avalable / (target_num - position_count)
                value = round(1 / target_num, 2) - 0.01
            except ZeroDivisionError as e:
                print(f"资金分摊时除零错误: {e}")
                return
            buy_num = 0
            for stock in [i for i in target_list if i not in self.hold_list]:
                self.open_position(context, stock, value)
                # if stock in self.positionsDic.keys() and self.find_stock_of_positions(stock)['m_nVolume'] == 0:
                buy_num += 1
                if buy_num == target_num - position_count:
                    break
        print("买入完毕.")
    def today_is_between(self, context: Any) -> bool:
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
            if ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-30'):
                return True
            else:
                return False
        else:
            return False

    def find_stock_of_positions(self, stock):
        result = [position for position in self.positions if position.m_strInstrumentID == stock]
        if result:
            return result[0]

    def get_stock_list_of_positions(self, context):
        return [position.m_strInstrumentID for position in self.positions]

    def close_account(self, context: Any) -> None:
        """
        清仓操作：若当天为空仓日，则平仓所有持仓股票

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        if self.no_trading_today_signal:
            if self.hold_list:
                for stock in self.hold_list:
                    self.close_position(context, stock)
                    print(f"空仓日平仓，卖出股票 {stock}。")

    def print_position_info(self, context: Any) -> None:
        """
        打印当前持仓详细信息，包括股票代码、成本价、现价、涨跌幅、持仓股数和市值

        参数:
            context: 聚宽平台传入的交易上下文对象
        """
        self.positions = get_trade_detail_data(context.account, 'STOCK', 'POSITION')
        self.hold_list = [self.codeOfPosition(position) for position in self.positions]

        if self.positions:
            print(f"********** 持仓信息打印开始 {context.account}**********")
            total = 0
            for position in self.positions:
                cost: float = position.m_dOpenPrice
                price: float = position.m_dLastPrice
                ret: float = 100 * (price / cost - 1)
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
    
    def account_callback(self, context, accountInfo):
        print(accountInfo)
        context.accountInfo = accountInfo
        return accountInfo

# 创建全局策略实例，策略入口处使用该实例
strategy = TradingStrategy()

# 全局包装函数，必须为顶层函数，保证调度任务可序列化，不使用 lambda

def prepare_stock_list_func(context: Any) -> None:
    """
    包装调用策略实例的 prepare_stock_list 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('准备当日股票...')
    strategy.prepare_stock_list(context)



def check_holdings_yesterday_func(context: Any) -> None:
    """
    包装调用策略实例的 check_holdings_yesterday 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.check_holdings_yesterday(context)
    print('--------------------------------', '新的一天开始了', context.today, '--------------------------------')


def weekly_adjustment_func(context: Any) -> None:
    """
    包装调用策略实例的 weekly_adjustment 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('================== 每周调仓时间 ==================')
    strategy.weekly_adjustment(context)

def weekly_adjustment_buy_func(context: Any) -> None:
    """
    包装调用策略实例的 weekly_adjustment 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.weekly_adjustment_buy(context)


def sell_stocks_func(context: Any) -> None:
    """
    包装调用策略实例的 sell_stocks 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('早上交易阶段...')
    strategy.sell_stocks(context)


def trade_afternoon_func(context: Any) -> None:
    """
    包装调用策略实例的 trade_afternoon 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('下午交易阶段...')
    strategy.trade_afternoon(context)


def close_account_func(context: Any) -> None:
    """
    包装调用策略实例的 close_account 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    print('收盘前检查是否需要清仓...')
    strategy.close_account(context)


def print_position_info_func(context: Any) -> None:
    """
    包装调用策略实例的 print_position_info 方法

    参数:
        context: 聚宽平台传入的交易上下文对象
    """
    strategy.print_position_info(context)

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


def init(context: Any) -> None:
    # 初始化策略环境及参数
    strategy.initialize(context)
    context.runner = TaskRunner(context)

    # 注册调度任务，所有任务均使用顶层包装函数（不使用 lambda 以确保可序列化）
    
    # 实盘和回测不一样的地方在于，可以使用run_time函数，不需要等到盘中才执行定时逻辑，因此部分逻辑执行时间可以前置
    if context.do_back_test:
        # -------------------每日执行任务 --------------------------------
        # 9am 检查昨日持仓
        context.runner.run_daily("9:35", check_holdings_yesterday_func)
        # 9:05am 准备股票列表
        context.runner.run_daily("9:40", prepare_stock_list_func)
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
        # -------------------每日执行任务 --------------------------------
        # 9am 检查昨日持仓
        context.run_time("check_holdings_yesterday_func","1nDay","2025-03-0109:00:00","SH")
        # 9:05am 准备股票列表
        context.run_time("prepare_stock_list_func","1nDay","2025-03-0109:05:00","SH")
        # 9:30 am 止盈止损检测
        context.run_time("sell_stocks_func","1nDay","2025-03-0109:30:00","SH")
        # 14:30 pm 检查需要卖出的持仓
        context.run_time("trade_afternoon_func","1nDay","2025-03-0114:30:00","SH")
        # 14:50 pm 检查当日是否需要一键清仓
        context.run_time("close_account_func","1nDay","2025-03-0114:50:00","SH")
        # 15:05 pm 每日收盘后打印一次持仓
        context.run_time("print_position_info_func","1nDay","2025-03-0115:05:00","SH")
        # -------------------每周执行任务 --------------------------------
        # 09:40 am 每周做一次调仓动作，尽量早，流动性充足
        context.run_time("weekly_adjustment_func","7nDay","2025-03-0409:40:00","SH")
        # 09:50 am 每周调仓后买入股票
        context.run_time("weekly_adjustment_buy_func","7nDay","2025-03-0409:50:00","SH")


# 在handlebar函数中调用（假设当前K线时间戳为dt）
def handlebar(context):
    # 新增属性，快捷获取当前日期
    index = context.barpos
    currentTime = context.get_bar_timetag(index) + 8 * 3600 * 1000
    context.currentTime = currentTime
    context.today = pd.to_datetime(currentTime, unit='ms')

    # 检查并执行任务
    context.runner.check_tasks(context.today)

    if not strategy.pool_initialized:
        strategy.get_stock_pool_when_test(context)

def deal_callback(context, dealInfo):
    stock = dealInfo['m_strInstrumentName']
    value = dealInfo['m_dTradeAmount']
    print(f"已买入股票 {stock}，成交额 {value:.2f}")
    strategy.not_buy_again.append(stock)
    messager.sendLog(f"已买入股票 {stock}，成交额 {value:.2f}")    
    # 回测模式不发
    messager.send_deal(dealInfo)
    

def position_callback(context, positionInfo):
    messager.sendLog("持仓信息变更回调")
    messager.send_positions(positionInfo)
    
def orderError_callback(context, orderArgs, errMsg):
    messager.sendLog(f"下单异常回调，订单信息{orderArgs}，异常信息{errMsg}")
    
def order_callback(context, orderInfo):
    messager.sendLog(f"委托状态变化回调")
    