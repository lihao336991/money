# 导入必要的库
import datetime as dt  # 日期时间处理

from jqdata import *  # 聚宽数据接口
from jqfactor import *  # 聚宽因子分析
from jqlib.technical_analysis import *  # 聚宽技术分析


def initialize(context):
    """
    初始化函数，设置策略参数和定时任务
    :param context: 策略上下文对象
    """
    # 设置使用真实价格（非复权）
    set_option('use_real_price', True)
    # 设置日志级别为只显示错误
    log.set_level('system', 'error')
    # 避免未来数据
    set_option('avoid_future_data', True)
    # 设置滑点为固定万分之三
    set_slippage(FixedSlippage(0.01))
    
    # 设置每日定时任务
    run_daily(get_stock_list, '00:05')  # 00:05获取股票列表
    run_daily(buy, '09:25:41')           # 9:25执行买入
    # 设置两个卖出时间点
    run_daily(sell, time='11:25', reference_security='000300.XSHG')  # 上午卖出点
    run_daily(sell, time='14:50', reference_security='000300.XSHG')  # 下午卖出点
    

def get_stock_list(context): 
    """
    选股逻辑：筛选"一进二"模式的股票
    :param context: 策略上下文对象
    """
    # 获取前3个交易日日期
    date = context.previous_date
    date_2, date_1, date = get_trade_days(end_date=date, count=3)

    # 准备初始股票池
    initial_list = prepare_stock_list(date)
    print(f"【对比日志】日期: {date}, 初始池: {len(initial_list)}")
    # 获取昨日涨停股票列表
    hl0_list = get_hl_stock(initial_list, date)

    # 获取前两日曾涨停的股票列表
    hl1_list = get_ever_hl_stock(initial_list, date_1)
    hl2_list = get_ever_hl_stock(initial_list, date_2)
    
    # 合并前两日涨停股票为集合，用于快速查找
    elements_to_remove = set(hl1_list + hl2_list)
    
    print(f"【对比日志】T涨停: {len(hl0_list)}, T-1曾涨停: {len(hl1_list)}, T-2曾涨停: {len(hl2_list)}")
    
    # TODO 前天、大前天曾涨停的，需要被排除吗？只需要排除连板即可。只排除昨天涨停的
    # 筛选出昨日涨停但前两日未涨停的股票（"一进二"模式）
    hl_list = [stock for stock in hl0_list if stock not in hl1_list]  
    print(f"【对比日志】一进二初选: {len(hl_list)}, 列表: {hl_list}")

    # 计算昨日涨停情绪因子
    yesterday_high_limit_factor = get_high_limit_factor(initial_list, date)
    # 计算前日涨停情绪因子
    last_high_limit_factor = get_high_limit_factor(initial_list, date_1)
    
    print(f"【对比日志】情绪因子: 昨日={yesterday_high_limit_factor:.4f}, 前日={last_high_limit_factor:.4f}")
    
    # 情绪退潮判断（昨日因子下降超过10%）
    # if last_high_limit_factor == 0 or yesterday_high_limit_factor / last_high_limit_factor < 0.9:
    #     g.gap_up = []
    #     message = "涨停情绪退潮，今日空仓"
    #     log.info(message)
    #     messager.send_message(message)
    #     return
    # else:
    #     # 输出作为优先买入股票
    #     g.gap_up = get_priority_list(context, hl_list,date)
    #     return

    g.gap_up = get_priority_list(context, hl_list,date)
    return

# 输出作为优先买入股票
def get_priority_list(context, hl_list,date):
    stocks_list = []  # 符合条件的股票列表
    qualified_stocks = []  # 符合条件的股票列表

    # 遍历"一进二"股票池
    for s in hl_list:
        # 获取前一日数据
        prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
        if prev_day_data.empty:
            continue

        # 计算均价涨幅
        avg_price_increase_value = prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0] * 1.1 - 1
        # TODO 平均涨幅 7 -> 5.5
        # 筛选涨幅在7%以上且成交额在2.5亿到20亿之间的股票
        # or prev_day_data['money'][0] < 2.5e8 or prev_day_data['money'][0] > 20e8
        if avg_price_increase_value < 0.07 :
            print(f"【对比日志】过滤 {s}: 涨幅/成交额不符 (涨幅={avg_price_increase_value:.2%}, 成交额={prev_day_data['money'][0]/1e8:.2f}亿)")
            continue

        # 获取换手率和市值数据
        turnover_ratio_data = get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, 
                                          fields=['turnover_ratio', 'market_cap','circulating_market_cap'])
        # TODO 过滤股价低于5的股票
        if prev_day_data['close'][0] < 5:
            print(f"【对比日志】过滤 {s}: 股价低于5元")
            continue
        
        # TODO 市值调整为 20 - 300亿
        # 筛选市值在20亿以上且流通市值不超过520亿的股票
        if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 20 or turnover_ratio_data['circulating_market_cap'][0] > 300:
            print(f"【对比日志】过滤 {s}: 市值不符 (总市值={turnover_ratio_data['market_cap'][0] if not turnover_ratio_data.empty else 'N/A'}, 流通={turnover_ratio_data['circulating_market_cap'][0] if not turnover_ratio_data.empty else 'N/A'})")
            continue
        
        # TODO 成交量的限制改成换手率的限制，排除换手率过低和过高的
        turnover_ratio = turnover_ratio_data['turnover_ratio'][0] / 100
        if turnover_ratio < 0.025 or turnover_ratio > 0.5:
            print(f"【对比日志】过滤 {s}: 换手率不符 (换手率={turnover_ratio:.2%})")
            continue
        
        # 检查是否有左压且成交量不足的情况
        if rise_low_volume(s, context):
            print(f"【对比日志】过滤 {s}: 左压缩量")
            continue
        
        # 筛选短期均线在长期均线上方的多头排列股票（更宽松的条件）
        # 获取足够的历史收盘价数据（至少360天）
        hist = attribute_history(s, 360, '1d', fields=['close'], skip_paused=True)
        
        if not hist.empty and len(hist) >= 60:
            # 计算主要周期的移动平均线
            ma20 = hist['close'].rolling(window=20).mean()
            ma60 = hist['close'].rolling(window=60).mean()
            ma360 = hist['close'].rolling(window=360).mean()
            
            # 检查宽松的多头排列：主要检查20日均线在60日均线上方
            # 这是一个更宽松的条件，只要中期均线在长期均线上方即可
            if ma20.iloc[-1] <= ma60.iloc[-1] or ma60.iloc[-1] <= ma360.iloc[-1]:
                print(f"【对比日志】过滤 {s}: 非多头排列")
                continue

        # TODO 新增：计算过去一年股价高点，回调幅度不超过20%。相较于股价低点，上涨幅度不超过300%
        hist_year = attribute_history(s, 250, '1d', fields=['high','low'], skip_paused=True)
        if not hist_year.empty:
            year_high = hist_year['high'].max()
            year_low = hist_year['low'].min()
            current_close = prev_day_data['close'][0]
            drawdown = (year_high - current_close) / year_high
            if drawdown > 0.2:
                print(f"【对比日志】250天过滤 {s}: 回调幅度过大 (高点={year_high:.2f}, 当前={current_close:.2f}, 回调={drawdown:.2%})")
                continue
            elif (current_close - year_low) / year_low > 3:
                print(f"【对比日志】250天过滤 {s}: 上涨幅度过大 (低点={year_low:.2f}, 当前={current_close:.2f}, 上涨={(current_close - year_low) / year_low:.2%})")
                continue
        
        # TODO 新增 分级过滤，对于过去60天，回调幅度和上涨幅度更严格
        hist_year = attribute_history(s, 60, '1d', fields=['high','low'], skip_paused=True)
        if not hist_year.empty:
            year_high = hist_year['high'].max()
            year_low = hist_year['low'].min()
            current_close = prev_day_data['close'][0]
            drawdown = (year_high - current_close) / year_high
            if drawdown > 0.05:
                print(f"【对比日志】60天过滤 {s}: 回调幅度过大 (高点={year_high:.2f}, 当前={current_close:.2f}, 回调={drawdown:.2%})")
                continue
            elif (current_close - year_low) / year_low > 1.5:
                print(f"【对比日志】60天过滤 {s}: 上涨幅度过大 (低点={year_low:.2f}, 当前={current_close:.2f}, 上涨={(current_close - year_low) / year_low:.2%})")
                continue
        
        # 添加到候选列表
        qualified_stocks.append(s)

    print(f"【对比日志】最终入选: {len(qualified_stocks)}, 列表: {qualified_stocks}")
    print('可能买入股票：%s '% str([get_security_info(stock).display_name for stock in qualified_stocks]))
    log.info('可能买入target股票: %s' % str([get_security_info(stock).display_name for stock in qualified_stocks]))
        
    return qualified_stocks

def buy(context):
    """
    买入逻辑：执行符合条件的"一进二"股票买入
    :param context: 策略上下文对象
    """
    qualified_stocks = []  # 符合条件的股票列表
    gk_stocks = []        # 高开股票列表

    current_data = get_current_data()
    date_now = context.current_dt.strftime("%Y-%m-%d")
    # 设置集合竞价时间段
    mid_time1 = ' 09:15:00'
    end_times1 = ' 09:25:00'
    start = date_now + mid_time1
    end = date_now + end_times1

    # 遍历"一进二"股票池
    for s in g.gap_up:
        # 获取前一日数据
        prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
        # 获取集合竞价数据
        auction_data = get_call_auction(s, start_date=date_now, end_date=date_now, fields=['time','volume', 'current'])
        
        # TODO 降低成交量要求，4% -> 3%
        # 筛选集合竞价成交量大于前一日成交量3%的股票
        if auction_data.empty or auction_data['volume'][0] / prev_day_data['volume'][-1] < 0.03:
            continue
        
        # 竞竞比（今日竞价量比昨日竞价量比值），过滤竞价异常放量50%以上的
        # 获取昨日日期
        yesterday = context.previous_date.strftime("%Y-%m-%d")
        # 获取昨日集合竞价数据
        yesterday_auction_data = get_call_auction(s, start_date=yesterday, end_date=yesterday, fields=['volume'])
        
        # 计算竞竞比
        if not yesterday_auction_data.empty:
            yesterday_volume = yesterday_auction_data['volume'].sum()
            today_volume = auction_data['volume'].sum()
            
            if yesterday_volume > 0:
                auction_ratio = today_volume / yesterday_volume
                # 过滤竞竞比不合格的股票
                if auction_ratio < 1 or auction_ratio > 10:
                    print(f"【对比日志】过滤 {s}: 竞价异常（竞竞比={auction_ratio:.2f}）")
                    continue

        # 计算当前价格相对于涨停板价格的比率
        current_ratio = auction_data['current'][0] / (current_data[s].high_limit / 1.1)
        # 筛选开盘涨幅在0-6%之间的股票
        if current_ratio <= 1 or current_ratio >= 1.06:
            continue
        
        # 添加到候选列表
        qualified_stocks.append(s)

    # 执行买入操作
    if len(qualified_stocks) != 0 and context.portfolio.available_cash / context.portfolio.total_value > 0.3:
        # 分仓控制：最多持有4只股票，每只最多1/4仓位
        current_positions = list(context.portfolio.positions)
        max_stocks = 4
        max_position_per_stock = 0.25  # 每只股票最多1/4仓位
        
        # 计算可买入的股票数量
        available_slots = max_stocks - len(current_positions)
        if available_slots <= 0:
            print("【对比日志】已达最大持仓数量限制，今日不买入")
            return
        
        # 限制买入股票数量
        stocks_to_buy = qualified_stocks[:available_slots]
        
        # 计算每只股票分配的资金（不超过总资金的1/4）
        total_cash = context.portfolio.available_cash
        total_value = context.portfolio.total_value
        max_cash_per_stock = total_value * max_position_per_stock
        
        buy_stocks = []
        for s in stocks_to_buy:
            # 计算该股票可买入的最大金额
            position_value = 0
            if s in context.portfolio.positions:
                position = context.portfolio.positions[s]
                position_value = position.value
            available_cash_for_stock = max_cash_per_stock - position_value
            if available_cash_for_stock <= 0:
                print(f"【对比日志】{s} 已达仓位上限，跳过")
                continue
            
            # 确保有足够资金买入至少100股
            if total_cash / current_data[s].last_price > 100:
                # 实际买入金额取可用资金和单只股票上限的最小值
                buy_value = min(total_cash, available_cash_for_stock)
                order_value(s, buy_value, MarketOrderStyle(current_data[s].day_open))
                buy_stocks.append(s + '，名称：' + get_security_info(s).display_name)
                total_cash -= buy_value
                
                # print('买入' + s)
                # print('买入' + s + '，名称：' + get_security_info(s).display_name)
                # print('———————————————————————————————————')
        print(f"【对比日志】最终买入: {len(buy_stocks)}, 列表: {buy_stocks}")
        
def transform_date(date, date_type):
    """
    日期格式转换函数
    :param date: 输入日期
    :param date_type: 需要的输出类型('str','dt','d')
    :return: 转换后的日期
    """
    if type(date) == str:
        str_date = date
        dt_date = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date = dt_date.date()
    elif type(date) == dt.datetime:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = dt_date.date()
    elif type(date) == dt.date:
        str_date = date.strftime('%Y-%m-%d')
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    return {'str': str_date, 'dt': dt_date, 'd': d_date}[date_type]

def filter_st_paused_stock(initial_list):
    """
    过滤ST、停牌、退市股票
    :param initial_list: 初始股票列表
    :return: 过滤后的股票列表
    """
    current_data = get_current_data()
    return [stock for stock in initial_list if not any([
        current_data[stock].is_st,
        current_data[stock].paused,
        '退' in current_data[stock].name
    ]) ]

def filter_kcbj_stock(initial_list):
    """
    过滤科创板股票，只保留60、00、30开头的股票
    :param initial_list: 初始股票列表
    :return: 过滤后的股票列表
    """
    return [stock for stock in initial_list if stock[:2] in ('60', '00')]

def filter_new_stock(initial_list, date, days=50):
    """
    过滤上市不足days天的新股
    :param initial_list: 初始股票列表
    :param date: 当前日期
    :param days: 上市天数阈值，默认为50天
    :return: 过滤后的股票列表
    """
    d_date = transform_date(date, 'd')
    return [stock for stock in initial_list if d_date - get_security_info(stock).start_date > dt.timedelta(days=days)]

def prepare_stock_list(date): 
    """
    准备初始股票池
    :param date: 当前日期
    :return: 过滤后的股票列表
    """
    # 获取所有股票
    initial_list = get_all_securities('stock', date).index.tolist()
    initial_list = filter_kcbj_stock(initial_list)  # 过滤科创板
    initial_list = filter_new_stock(initial_list, date)  # 过滤新股
    initial_list = filter_st_paused_stock(initial_list)  # 过滤ST/停牌/退市股
    return initial_list

def get_hl_stock(initial_list, date):
    """
    获取指定日期涨停的股票
    :param initial_list: 初始股票列表
    :param date: 目标日期
    :return: 涨停股票列表
    """
    # 获取收盘价和涨停价数据
    df = get_price(initial_list, end_date=date, frequency='daily', 
                  fields=['close','high_limit'], count=1, panel=False, 
                  fill_paused=False, skip_paused=False)
    df = df.dropna()
    df = df[df['close'] == df['high_limit']]  # 收盘价等于涨停价
    return list(df.code)

def get_ever_hl_stock(initial_list, date):
    """
    获取指定日期曾触及涨停的股票
    :param initial_list: 初始股票列表
    :param date: 目标日期
    :return: 曾涨停股票列表
    """
    # 获取最高价和涨停价数据
    df = get_price(initial_list, end_date=date, frequency='daily', 
                  fields=['close', 'high','high_limit'], count=1, panel=False, 
                  fill_paused=False, skip_paused=False)
    df = df.dropna()
    # TODO 这里触及涨停改成收盘涨停
    df = df[df['close'] == df['high_limit']]  # 最高价等于涨停价
    return list(df.code)

def rise_low_volume(s, context):   
    """
    判断股票上涨时是否未放量（左压情况）
    :param s: 股票代码
    :param context: 策略上下文对象
    :return: 布尔值，True表示有左压且未放量
    """
    # 获取106天的历史数据
    hist = attribute_history(s, 106, '1d', fields=['high','volume'], skip_paused=True,df=False)
    high_prices = hist['high'][:102]
    prev_high = high_prices[-1]
    
    # 计算左压天数
    zyts_0 = next((i-1 for i, high in enumerate(high_prices[-3::-1], 2) if high >= prev_high), 100)
    zyts = zyts_0 + 5
    
    # TODO 左压的条件放宽一些，0.7
    # # 根据左压天数设置不同的成交量阈值
    # if zyts_0 < 20:  # 左压很近
    #     threshold = 0.9  # 更严格的成交量判断
    # elif zyts_0 < 50:
    #     threshold = 0.88
    # else:
    #     threshold = 0.85  # 宽松一点
    threshold = 0.6  # 宽松一点
    # 判断当前成交量是否低于历史最大成交量的阈值
    if hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * threshold:
        print(f"【对比日志】{s} 左压缩量, threshold: {threshold:.2f}, 缩量比例: {hist['volume'][-1]/max(hist['volume'][-zyts:-1]):.2%}")
        return True    
    return False

def get_shifted_date(date, days, days_type='T'):
    """
    获取偏移days个交易日/自然日的日期
    :param date: 基准日期
    :param days: 偏移天数
    :param days_type: 偏移类型('T'交易日/'N'自然日)
    :return: 偏移后的日期字符串
    """
    d_date = transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    
    # 自然日偏移
    if days_type == 'N':
        shifted_date = yesterday + dt.timedelta(days+1)
    
    # 交易日偏移
    if days_type == 'T':
        all_trade_days = [i.strftime('%Y-%m-%d') for i in list(get_all_trade_days())]
        # 如果昨天是交易日
        if str(yesterday) in all_trade_days:
            shifted_date = all_trade_days[all_trade_days.index(str(yesterday)) + days + 1]
        # 如果不是交易日，向前寻找最近的交易日
        else:
            for i in range(100):
                last_trade_date = yesterday - dt.timedelta(i)
                if str(last_trade_date) in all_trade_days:
                    shifted_date = all_trade_days[all_trade_days.index(str(last_trade_date)) + days + 1]
                    break
    return str(shifted_date)

def sell(context):
    """
    卖出股票逻辑
    :param context: 策略上下文对象
    """
    date = transform_date(context.previous_date, 'str')
    current_data = get_current_data()
    for s in list(context.portfolio.positions):
        # 条件：有可卖数量、未涨停
        if ((context.portfolio.positions[s].closeable_amount != 0) and 
            (current_data[s].last_price < current_data[s].high_limit)):
            order_target_value(s, 0)  # 清仓
            print('止损止盈卖出', [s, get_security_info(s, date).display_name])
            print('———————————————————————————————————')


# 涨停情绪因子计算（递归函数）
def get_high_limit_factor(stocks_list, date, count=0, high_limit_factor=0):
    temp_num = len(stocks_list)
    # 获取当日涨停股票
    df = get_price(stocks_list, end_date=date, frequency='daily', 
                  fields=['close', 'high_limit', 'paused'], count=1, panel=False)
    stocks_list = df.query('close == high_limit and paused == 0')['code'].tolist()
    # 终止条件：无涨停或递归超过3次
    if not stocks_list or count > 2:
        return math.log(high_limit_factor)  # 取对数平滑
    
    # 递归计算前日数据
    last_day = get_shifted_date(date, -1, 'T')
    # 加权计算情绪因子（指数衰减加权）
    high_limit_factor += (2 ** count) * (len(stocks_list) ** 2) / temp_num
    return get_high_limit_factor(stocks_list, last_day, count + 1, high_limit_factor)