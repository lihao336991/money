# 克隆自聚宽文章：https://www.joinquant.com/post/53831
# 标题：惊艳瞬间 年化7763.90%， 一进二打板买下全市场
# 作者：子匀

# 克隆自聚宽文章：https://www.joinquant.com/post/48680
# 标题：追首板涨停 过去两年年化304%
# 作者：子匀

# 克隆自聚宽文章：https://www.joinquant.com/post/44901
# 标题：首板低开策略
# 作者：wywy1995

# 克隆自聚宽文章：https://www.joinquant.com/post/48523
# 标题：一进二集合竞价策略
# 作者：十足的小市值迷

# 克隆自聚宽文章：https://www.joinquant.com/post/49364
# 标题：一种弱转强的选股策略，年化100%以上
# 作者：紫露薇霜

# 2024/08/01  止损卖出修改为跌破5日均线

import datetime as dt

import pandas as pd
from jqdata import *
from jqfactor import *
from jqlib.technical_analysis import *


def initialize(context):
    set_option('use_real_price', True)
    log.set_level('system', 'error')
    set_option('avoid_future_data', True)
    set_slippage(FixedSlippage(0.01))
    
    run_daily(get_stock_list, '9:01')
    run_daily(buy, '09:26')
    run_daily(sell, time='11:25', reference_security='000300.XSHG')
    run_daily(sell, time='14:50', reference_security='000300.XSHG')
    run_daily(print_portfolio, '15:00')


# 选股
def get_stock_list(context): 
    # 文本日期
    date = context.previous_date

    date_2,date_1,date = get_trade_days( end_date=date, count=3)
    print(f"【选股准备】开始执行选股...")
    print(f"【选股】基准日期: {date.strftime('%Y%m%d')}")
    print(f"【选股】日期范围: date={date.strftime('%Y%m%d')}, date_1={date_1.strftime('%Y%m%d')}, date_2={date_2.strftime('%Y%m%d')}")
    # 初始列表
    initial_list = prepare_stock_list(date)
    print(f"【选股】初步筛选后共 {len(initial_list)} 只股票")
    # 昨日涨停
    hl0_list = get_hl_stock(initial_list, date)
    print(f"【选股】get_hl_stock({date.strftime('%Y%m%d')}): 封板股票 {len(hl0_list)}只， 分别是 {hl0_list}")
    # 前日曾涨停
    hl1_list = get_ever_hl_stock(initial_list, date_1)
    print(f"【选股】get_ever_hl_stock({date_1.strftime('%Y%m%d')}): 曾涨停股票 {len(hl1_list)}只， 分别是 {hl1_list}")
    # 前前日曾涨停
    hl2_list = get_ever_hl_stock(initial_list, date_2)
    print(f"【选股】get_ever_hl_stock({date_2.strftime('%Y%m%d')}): 曾涨停股票 {len(hl2_list)}只， 分别是 {hl2_list}")
    # 合并 hl1_list 和 hl2_list 为一个集合，用于快速查找需要剔除的元素  
    elements_to_remove = set(hl1_list + hl2_list)  
    # 使用列表推导式来剔除 hl_list 中存在于 elements_to_remove 集合中的元素  

    # TODO
    g.gap_up = [stock for stock in hl0_list if stock not in elements_to_remove] 
    # g.gap_up = []

    print(f"【选股】一进二高开: {len(g.gap_up)}只")


    # TODO 
    g.gap_down = []
    # 昨日涨停，但前天没有涨停的
    # g.gap_down = [s for s in hl0_list if s not in hl1_list]


    print(f"【选股】首板低开: {len(g.gap_down)}只")
     # 昨日曾涨停
    h1_list = get_ever_hl_stock2(initial_list, date)
    print(f"【选股】get_ever_hl_stock2({date.strftime('%Y%m%d')}): 曾涨停未封板 {len(h1_list)}只， 分别是 {h1_list}")
    # 上上个交易日涨停过滤
    elements_to_remove = get_hl_stock(initial_list, date_1)
    print(f"【选股】上上个交易日涨停: {len(elements_to_remove)}只， 分别是 {elements_to_remove}")
    
    # 过滤上上个交易日涨停、曾涨停
    # TODO 
    g.reversal = [stock for stock in h1_list if stock not in elements_to_remove]
    # g.reversal = []

    print(f"【选股】弱转强: {len(g.reversal)}只， 分别是 {g.reversal}")
    print(f"************")
    print(f"************")
    print(f"【选股准备】选股完成 - 一进二: {len(g.gap_up)}只, 首板低开: {len(g.gap_down)}只, 弱转强: {len(g.reversal)}只")
    print(f"************")
    print(f"************")



# 交易
def buy(context):
    qualified_stocks = [] 
    gk_stocks=[]
    dk_stocks=[]
    rzq_stocks=[]
    current_data = get_current_data()
    date_now = context.current_dt.strftime("%Y-%m-%d")
    mid_time1 = ' 09:15:00'
    end_times1 =  ' 09:26:00'
    start = date_now + mid_time1
    end = date_now + end_times1
    print(f"【买入执行】开始筛选买入标的...")
    # 高开
    print(f"【买入执行】正在筛选一进二高开标的，共{len(g.gap_up)}只候选")
    gk_filtered = 0
    for s in g.gap_up:
        try:
            # 条件一：均价，金额，市值，换手率
            prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
            if len(prev_day_data) == 0:
                gk_filtered += 1
                print(f"过滤: {s} 无昨收数据")
                continue
            avg_price_increase_value = prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0] * 1.1 - 1
            if avg_price_increase_value < 0.07 or prev_day_data['money'][0] < 5.5e8 or prev_day_data['money'][0] > 20e8 :
                gk_filtered += 1
                print(f"过滤: {s} 均价涨幅={avg_price_increase_value:.2%}, 昨收金额={prev_day_data['money'][0]:.2f}，昨收量={prev_day_data['volume'][0]:.2e}，昨收收盘价={prev_day_data['close'][0]:.2f}")
                continue
            # market_cap 总市值(亿元) > 70亿 流通市值(亿元) < 520亿
            turnover_ratio_data=get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, fields=['turnover_ratio', 'market_cap','circulating_market_cap'])
            if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 70  or turnover_ratio_data['circulating_market_cap'][0] > 520 :
                gk_filtered += 1
                print(f"过滤: {s} 总市值={turnover_ratio_data['market_cap'][0]:.2f}亿, 流通市值={turnover_ratio_data['circulating_market_cap'][0]:.2f}亿")
                continue

            
            # 条件二：左压
            if rise_low_volume(s, context):
                gk_filtered += 1
                print(f"过滤: {s} 左压")
                continue
            # 条件三：高开,开比
            auction_data = get_call_auction(s, start_date=date_now, end_date=date_now, fields=['time','volume', 'current'])
            if auction_data.empty:
                gk_filtered += 1
                print(f"过滤: {s} 无集合竞价数据")
                continue
            volume_ratio = auction_data['volume'][0] / prev_day_data['volume'][-1]

            # 量比：集合竞价量/昨收量
            if volume_ratio < 0.03:
                gk_filtered += 1
                print(f"过滤: {s} 量比={volume_ratio:.2%}, 昨收量={prev_day_data['volume'][-1]:.2e}, 集合竞价量={auction_data['volume'][0]:.2e}, 昨收收盘价={prev_day_data['close'][-1]:.2f}")
                continue
            current_ratio = auction_data['current'][0] / (current_data[s].high_limit/1.1)
            if current_ratio<=1 or current_ratio>=1.06:
                gk_filtered += 1
                print(f"过滤: {s} 高开={current_ratio:.2%}")
                continue

            # 如果股票满足所有条件，则添加到列表中  
            gk_stocks.append(s)
            qualified_stocks.append(s)
        except Exception:
            gk_filtered += 1
            pass
    print(f"【买入执行】一进二高开筛选完成: 通过{len(gk_stocks)}只, 过滤{gk_filtered}只")

    
    # 低开    
    # 基础信息
    date = transform_date(context.previous_date, 'str')
    current_data = get_current_data()
    print(f"【买入执行】正在筛选首板低开标的，共{len(g.gap_down)}只候选")
    dk_filtered = 0

    if g.gap_down:
        stock_list = g.gap_down
        # 计算相对位置
        rpd = get_relative_position_df(stock_list, date, 60)
        if not rpd.empty:
            rpd = rpd[rpd['rp'] <= 0.5]
            stock_list = list(rpd.index)
            print(f"【买入执行】首板低开: 相对位置过滤后剩余{len(stock_list)}只")
        
        # 低开
        df =  get_price(stock_list, end_date=date, frequency='daily', fields=['close'], count=1, panel=False, fill_paused=False, skip_paused=True).set_index('code') if len(stock_list) != 0 else pd.DataFrame()
        if not df.empty:
            df['open_pct'] = [current_data[s].day_open/df.loc[s, 'close'] for s in stock_list]
            df = df[(0.955 <= df['open_pct']) & (df['open_pct'] <= 0.97)] #低开越多风险越大，选择3个多点即可
            stock_list = list(df.index)

            for s in stock_list:
                prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
                if prev_day_data['money'][0] >= 1e8  :
                    dk_stocks.append(s)
                    qualified_stocks.append(s)
                else:
                    dk_filtered += 1
        else:
            dk_filtered += len(stock_list)
    print(f"【买入执行】首板低开筛选完成: 通过{len(dk_stocks)}只, 过滤{dk_filtered}只")
    
    # 弱转强
    print(f"【买入执行】正在筛选弱转强标的，共{len(g.reversal)}只候选")
    rzq_filtered = 0
    for s in g.reversal:
        try:
            # 过滤前面三天涨幅超过28%的票
            price_data = attribute_history(s, 4, '1d', fields=['close'], skip_paused=True)
            if len(price_data) < 4:
                rzq_filtered += 1
                continue
            increase_ratio = (price_data['close'][-1] - price_data['close'][0]) / price_data['close'][0]
            if increase_ratio > 0.28:
                rzq_filtered += 1
                continue
            
            # 过滤前一日收盘价小于开盘价5%以上的票
            prev_day_data = attribute_history(s, 1, '1d', fields=['open', 'close'], skip_paused=True)
            if len(prev_day_data) < 1:
                rzq_filtered += 1
                continue
            open_close_ratio = (prev_day_data['close'][0] - prev_day_data['open'][0]) / prev_day_data['open'][0]
            if open_close_ratio < -0.05:
                rzq_filtered += 1
                continue
            
            prev_day_data = attribute_history(s, 1, '1d', fields=['close', 'volume','money'], skip_paused=True)
            avg_price_increase_value = prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0]  - 1
            if avg_price_increase_value < -0.04 or prev_day_data['money'][0] < 3e8 or prev_day_data['money'][0] > 19e8:
                rzq_filtered += 1
                continue
            turnover_ratio_data = get_valuation(s, start_date=context.previous_date, end_date=context.previous_date, fields=['turnover_ratio','market_cap','circulating_market_cap'])
            if turnover_ratio_data.empty or turnover_ratio_data['market_cap'][0] < 70  or turnover_ratio_data['circulating_market_cap'][0] > 520 :
                rzq_filtered += 1
                continue

            if rise_low_volume(s, context):
                rzq_filtered += 1
                continue
            auction_data = get_call_auction(s, start_date=date_now, end_date=date_now, fields=['time','volume', 'current'])
            
            if auction_data.empty:
                rzq_filtered += 1
                continue
            volume_ratio = auction_data['volume'][0] / prev_day_data['volume'][-1]
            if volume_ratio < 0.03:
                rzq_filtered += 1
                continue
            current_ratio = auction_data['current'][0] / (current_data[s].high_limit/1.1)
            if current_ratio <= 0.98 or current_ratio >= 1.09:
                rzq_filtered += 1
                continue
            rzq_stocks.append(s)
            qualified_stocks.append(s)
        except Exception:
            rzq_filtered += 1
            pass
    print(f"【买入执行】弱转强筛选完成: 通过{len(rzq_stocks)}只, 过滤{rzq_filtered}只")
    
    print(f"【买入执行】筛选完成 - 一进二: {len(gk_stocks)}只, 首板低开: {len(dk_stocks)}只, 弱转强: {len(rzq_stocks)}只")
    if len(qualified_stocks)>0:
        print('———————————————————————————————————')
        send_message('今日选股：'+','.join(qualified_stocks))
        print('一进二：'+','.join(gk_stocks))
        print('首板低开：'+','.join(dk_stocks))
        print('弱转强：'+','.join(rzq_stocks))
        print('今日选股：'+','.join(qualified_stocks))
        print('———————————————————————————————————')
    else:
        send_message('今日无目标个股')
        print('今日无目标个股')  
    
        
    if len(qualified_stocks)!=0  and context.portfolio.available_cash/context.portfolio.total_value>0.3:
        value = context.portfolio.available_cash / len(qualified_stocks)
        for s in qualified_stocks:
            # 下单
            #由于关闭了错误日志，不加这一句，不足一手买入失败也会打印买入，造成日志不准确
            if context.portfolio.available_cash/current_data[s].last_price>100: 
                order_value(s, value, MarketOrderStyle(current_data[s].day_open))
                print('买入' + s)
                print('———————————————————————————————————')

# 处理日期相关函数
def transform_date(date, date_type):
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
    dct = {'str':str_date, 'dt':dt_date, 'd':d_date}
    return dct[date_type]

def get_shifted_date(date, days, days_type='T'):
    #获取上一个自然日
    d_date = transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    #移动days个自然日
    if days_type == 'N':
        shifted_date = yesterday + dt.timedelta(days+1)
    #移动days个交易日
    if days_type == 'T':
        all_trade_days = [i.strftime('%Y-%m-%d') for i in list(get_all_trade_days())]
        #如果上一个自然日是交易日，根据其在交易日列表中的index计算平移后的交易日        
        if str(yesterday) in all_trade_days:
            shifted_date = all_trade_days[all_trade_days.index(str(yesterday)) + days + 1]
        #否则，从上一个自然日向前数，先找到最近一个交易日，再开始平移
        else:
            for i in range(100):
                last_trade_date = yesterday - dt.timedelta(i)
                if str(last_trade_date) in all_trade_days:
                    shifted_date = all_trade_days[all_trade_days.index(str(last_trade_date)) + days + 1]
                    break
    return str(shifted_date)



# 过滤函数
def filter_new_stock(initial_list, date, days=50):
    d_date = transform_date(date, 'd')
    return [stock for stock in initial_list if d_date - get_security_info(stock).start_date > dt.timedelta(days=days)]

def filter_st_paused_stock(initial_list):
    current_data = get_current_data()
    # 使用列表推导式结合any()函数，筛选出符合条件的股票
    return [stock for stock in initial_list 
            if not any([
                current_data[stock].is_st,          # 排除ST股
                current_data[stock].paused,         # 排除停牌股
                '退' in current_data[stock].name    # 排除名称中含'退'字的股票，避免退市股
            ])]

def filter_kcbj_stock(initial_list):
    return [stock for stock in initial_list if stock[:2] in (('60','00','30'))]


def filter_st_stock(initial_list, date):
    str_date = transform_date(date, 'str')
    if get_shifted_date(str_date, 0, 'N') != get_shifted_date(str_date, 0, 'T'):
        str_date = get_shifted_date(str_date, -1, 'T')
    df = get_extras('is_st', initial_list, start_date=str_date, end_date=str_date, df=True)
    df = df.T
    df.columns = ['is_st']
    df = df[df['is_st'] == False]
    filter_list = list(df.index)
    return filter_list



def filter_paused_stock(initial_list, date):
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['paused'], count=1, panel=False, fill_paused=True)
    df = df[df['paused'] == 0]
    paused_list = list(df.code)
    return paused_list

# 一字
def filter_extreme_limit_stock(context, stock_list, date):
    tmp = []
    for stock in stock_list:
        df = get_price(stock, end_date=date, frequency='daily', fields=['low','high_limit'], count=1, panel=False)
        if df.iloc[0,0] < df.iloc[0,1]:
            tmp.append(stock)
    return tmp



# 每日初始股票池
def prepare_stock_list(date): 
    initial_list = get_all_securities('stock', date).index.tolist()
    print(f"【选股】初始股票池共 {len(initial_list)} 只")
    initial_list = filter_kcbj_stock(initial_list)
    print(f"【选股】过滤科创板、创业板、北交所后共 {len(initial_list)} 只")
    initial_list = filter_new_stock(initial_list, date)
    print(f"【选股】过滤新股票后共 {len(initial_list)} 只")
    initial_list = filter_st_paused_stock(initial_list)
    print(f"【选股】过滤停牌股后共 {len(initial_list)} 只")
    return initial_list



def rise_low_volume(s, context):   # 上涨时，未放量 rising on low volume
    hist = attribute_history(s, 106, '1d', fields=['high','volume'], skip_paused=True,df=False)
    high_prices = hist['high'][:102]
    prev_high = high_prices[-1]
    zyts_0 = next((i-1 for i, high in enumerate(high_prices[-3::-1], 2) if high >= prev_high), 100)
    zyts = zyts_0 + 5
    if  hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * 0.9:
        return True
    return False

# 筛选出某一日涨停的股票
def get_hl_stock(initial_list, date):
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna() #去除停牌
    df = df[df['close'] == df['high_limit']]
    hl_list = list(df.code)
    return hl_list
    
# 筛选曾涨停
def get_ever_hl_stock(initial_list, date):
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['high','high_limit'], count=1, panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna() #去除停牌
    df = df[df['high'] == df['high_limit']]
    hl_list = list(df.code)
    return hl_list

# 筛选曾涨停
def get_ever_hl_stock2(initial_list, date):
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['close','high','high_limit'], count=1, panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna() #去除停牌
    cd1 = df['high'] == df['high_limit'] 
    cd2 = df['close']!= df['high_limit']
    df = df[cd1 & cd2]
    hl_list = list(df.code)
    return hl_list

# 计算涨停数
def get_hl_count_df(hl_list, date, watch_days):
    # 获取watch_days的数据
    df = get_price(hl_list, end_date=date, frequency='daily', fields=['close','high_limit','low'], count=watch_days, panel=False, fill_paused=False, skip_paused=False)
    df.index = df.code
    #计算涨停与一字涨停数，一字涨停定义为最低价等于涨停价
    hl_count_list = []
    extreme_hl_count_list = []
    for stock in hl_list:
        df_sub = df.loc[stock]
        hl_days = df_sub[df_sub.close==df_sub.high_limit].high_limit.count()
        extreme_hl_days = df_sub[df_sub.low==df_sub.high_limit].high_limit.count()
        hl_count_list.append(hl_days)
        extreme_hl_count_list.append(extreme_hl_days)
    #创建df记录
    df = pd.DataFrame(index=hl_list, data={'count':hl_count_list, 'extreme_count':extreme_hl_count_list})
    return df

# 计算连板数
def get_continue_count_df(hl_list, date, watch_days):
    df = pd.DataFrame()
    for d in range(2, watch_days+1):
        HLC = get_hl_count_df(hl_list, date, d)
        CHLC = HLC[HLC['count'] == d]
        df = df.append(CHLC)
    stock_list = list(set(df.index))
    ccd = pd.DataFrame()
    for s in stock_list:
        tmp = df.loc[[s]]
        if len(tmp) > 1:
            M = tmp['count'].max()
            tmp = tmp[tmp['count'] == M]
        ccd = ccd.append(tmp)
    if len(ccd) != 0:
        ccd = ccd.sort_values(by='count', ascending=False)    
    return ccd

# 计算昨涨幅
def get_index_increase_ratio(index_code, context):
    # 获取指数昨天和前天的收盘价
    close_prices = attribute_history(index_code, 2, '1d', fields=['close'], skip_paused=True)
    if len(close_prices) < 2:
        return 0  # 如果数据不足，返回0
    day_before_yesterday_close = close_prices['close'][0]
    yesterday_close = close_prices['close'][1]
    
    # 计算涨幅
    increase_ratio = (yesterday_close - day_before_yesterday_close) / day_before_yesterday_close
    return increase_ratio

#上午有利润就跑
def sell(context):
    # 基础信息
    date = transform_date(context.previous_date, 'str')
    current_data = get_current_data()
    
    
    # 根据时间执行不同的卖出策略
    if str(context.current_dt)[-8:] == '11:25:00' :
        for s in list(context.portfolio.positions):
            if ((context.portfolio.positions[s].closeable_amount != 0) and (current_data[s].last_price < current_data[s].high_limit) and (current_data[s].last_price > 1*context.portfolio.positions[s].avg_cost)):#avg_cost当前持仓成本
                order_target_value(s, 0)
                print( '止盈卖出', [s,get_security_info(s, date).display_name])
                print('———————————————————————————————————')
    
    if str(context.current_dt)[-8:] == '14:50:00':
        for s in list(context.portfolio.positions):

            close_data2 = attribute_history(s, 4, '1d', ['close'])
            M4=close_data2['close'].mean()
            MA5=(M4*4+current_data[s].last_price)/5
            if ((context.portfolio.positions[s].closeable_amount != 0) and (current_data[s].last_price < current_data[s].high_limit) and (current_data[s].last_price > 1*context.portfolio.positions[s].avg_cost)):#avg_cost当前持仓成本
                order_target_value(s, 0)
                print( '止盈卖出', [s,get_security_info(s, date).display_name])
                print('———————————————————————————————————')
            elif ((context.portfolio.positions[s].closeable_amount != 0) and (current_data[s].last_price < MA5)):
                #closeable_amount可卖出的仓位
                order_target_value(s, 0)
                print( '止损卖出', [s,get_security_info(s, date).display_name])
                print('———————————————————————————————————')  


def print_portfolio(context):
    current_data = get_current_data()
    positions = context.portfolio.positions
    
    print('【收盘持仓】========================================')
    print(f'总资产: {context.portfolio.total_value:.2f} 元')
    print(f'可用资金: {context.portfolio.available_cash:.2f} 元')
    print(f'持仓市值: {context.portfolio.positions_value:.2f} 元')
    print(f'持仓数量: {len(positions)} 只')
    print('———————————————————————————————————')
    
    if positions:
        for code, position in positions.items():
            security_info = get_security_info(code)
            name = security_info.display_name if security_info else '未知'
            last_price = current_data[code].last_price
            avg_cost = position.avg_cost
            pnl = (last_price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0
            value = position.value
            
            print(f'{code} {name}')
            print(f'  数量: {position.closeable_amount} 股')
            print(f'  现价: {last_price:.2f}  成本: {avg_cost:.2f}  盈亏: {pnl:+.2f}%')
            print(f'  市值: {value:.2f} 元')
            print('———————————————————————————————————')
    else:
        print('当前无持仓')
    
    print('【收盘持仓】========================================')


# 首版低开策略代码                
def filter_new_stock2(initial_list, date, days=250):
    d_date = transform_date(date, 'd')
    return [stock for stock in initial_list if d_date - get_security_info(stock).start_date > dt.timedelta(days=days)]
    
    
# 计算股票处于一段时间内相对位置
def get_relative_position_df(stock_list, date, watch_days):
    if len(stock_list) != 0:
        df = get_price(stock_list, end_date=date, fields=['high', 'low', 'close'], count=watch_days, fill_paused=False, skip_paused=False, panel=False).dropna()
        close = df.groupby('code').apply(lambda df: df.iloc[-1,-1])
        high = df.groupby('code').apply(lambda df: df['high'].max())
        low = df.groupby('code').apply(lambda df: df['low'].min())
        result = pd.DataFrame()
        result['rp'] = (close-low) / (high-low)
        return result
    else:
        return pd.DataFrame(columns=['rp'])    
    




