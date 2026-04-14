# 克隆自聚宽文章：https://www.joinquant.com/post/66882
# 标题：一进二v4.0--实盘策略
# 作者：财富369888

# 克隆自聚宽文章：https://www.joinquant.com/post/66781
# 标题：一进二v3.0-近两年35倍
# 作者：鹏远

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
    
    # 筛选出昨日涨停但前两日未涨停的股票（"一进二"模式）
    hl_list = [stock for stock in hl0_list if stock not in elements_to_remove]  
    print(f"【对比日志】一进二初选: {len(hl_list)}, 列表: {hl_list}")

    # 计算昨日涨停情绪因子
    yesterday_high_limit_factor = get_high_limit_factor(initial_list, date)
    # 计算前日涨停情绪因子
    last_high_limit_factor = get_high_limit_factor(initial_list, date_1)
    
    print(f"【对比日志】情绪因子: 昨日={yesterday_high_limit_factor:.4f}, 前日={last_high_limit_factor:.4f}")
    
    # 情绪退潮判断（昨日因子下降超过10%）
    if last_high_limit_factor == 0 or yesterday_high_limit_factor / last_high_limit_factor < 0.9:
        g.gap_up = []
        message = "涨停情绪退潮，今日空仓"
        log.info(message)
        send_message(message)
        return
    else:
        # 输出作为优先买入股票
        g.gap_up = get_priority_list(context, hl_list,date)
        return

    # g.gap_up = get_priority_list(context, hl_list,date)
    # return

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
        # 筛选涨幅在7%以上且成交额在5.5亿到20亿之间的股票
        if avg_price_increase_value < 0.07 or prev_day_data['money'][0] < 5.5e8 or prev_day_data['money'][0] > 20e8:
            print(f"【对比日志】过滤 {s}: 涨幅/成交额不符 (涨幅={avg_price_increase_value:.2%}, 成交额={prev_day_data['money'][0]/1e8:.2f}亿)")
            continue

        # 获取换手率和市值数据
        turnover_ratio_data = get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, 
                                          fields=['turnover_ratio', 'market_cap','circulating_market_cap'])
        # 筛选市值在20亿以上且流通市值不超过520亿的股票
        if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 20 or turnover_ratio_data['circulating_market_cap'][0] > 520:
            print(f"【对比日志】过滤 {s}: 市值不符 (总市值={turnover_ratio_data['market_cap'][0] if not turnover_ratio_data.empty else 'N/A'}, 流通={turnover_ratio_data['circulating_market_cap'][0] if not turnover_ratio_data.empty else 'N/A'})")
            continue
        
        # 检查是否有左压且成交量不足的情况
        if rise_low_volume(s, context):
            print(f"【对比日志】过滤 {s}: 左压缩量")
            continue

        # 添加到候选列表
        qualified_stocks.append(s)

    print(f"【对比日志】最终入选: {len(qualified_stocks)}, 列表: {qualified_stocks}")
    send_message('可能买入股票：%s '% str([get_security_info(stock).display_name for stock in qualified_stocks]))
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
        # 筛选集合竞价成交量大于前一日成交量4%的股票
        if auction_data.empty or auction_data['volume'][0] / prev_day_data['volume'][-1] < 0.04:
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
        # 计算每只股票分配的资金
        value = context.portfolio.available_cash / len(qualified_stocks)
        for s in qualified_stocks:
            # 确保有足够资金买入至少100股
            if context.portfolio.available_cash / current_data[s].last_price > 100:
                order_value(s, value, MarketOrderStyle(current_data[s].day_open))
                print('买入' + s)
                print('———————————————————————————————————')

def get_turnover_ratio_change(s, context, period=5):
    """
    计算最近一日换手率与过去period日平均换手率的比值
    :param s: 股票代码
    :param context: 策略上下文对象
    :param period: 计算周期，默认为5天
    :return: 换手率变化比值
    """
    # 获取前period个交易日
    start_date = get_shifted_date(context.previous_date, -period, 'T')  
    end_date = context.previous_date
    
    # 获取换手率数据
    turnover_data = get_valuation(s, 
                               start_date=start_date, 
                               end_date=end_date, 
                               fields=['turnover_ratio'])
    
    # 数据有效性检查
    if turnover_data.empty or len(turnover_data) < period:
        return None

    recent_turnover = turnover_data['turnover_ratio'].values[-1]  # 昨日换手率
    avg_turnover = turnover_data['turnover_ratio'].mean()        # 过去N天平均换手率

    if avg_turnover == 0:
        return None

    # 计算换手率变化比值
    ratio_change = recent_turnover / avg_turnover  
    return ratio_change

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
    return [stock for stock in initial_list if stock[:2] in ('60', '00', '30')]

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
                  fields=['high','high_limit'], count=1, panel=False, 
                  fill_paused=False, skip_paused=False)
    df = df.dropna()
    df = df[df['high'] == df['high_limit']]  # 最高价等于涨停价
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
    
    # 根据左压天数设置不同的成交量阈值
    if zyts_0 < 20:  # 左压很近
        threshold = 0.9  # 更严格的成交量判断
    elif zyts_0 < 50:
        threshold = 0.88
    else:
        threshold = 0.85  # 宽松一点
    
    # 判断当前成交量是否低于历史最大成交量的阈值
    if hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * threshold:
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