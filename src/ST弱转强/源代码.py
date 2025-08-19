import pandas as pd
import numpy as np
import math
import talib as tl
import datetime as dt
from datetime import datetime
from datetime import timedelta
from jqlib.technical_analysis import *
from jqdata import *

def initialize(context):
    set_option('use_real_price', True)
    # 开启防未来函数
    #set_option('avoid_future_data', True)
        # 将滑点设置为0
    set_slippage(FixedSlippage(0.01))
    # 设置交易成本万分之三，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, open_commission=0.0001, close_commission=0.0001,
                             close_today_commission=0, min_commission=5), type='stock')
    log.set_level('system', 'error')  # 过滤日志                         
    #----------------Settings--------------------------------------------------/
    g.stock_num=4 #最大持仓

    g.today_list=[]#当日观测股票
    g.today_HL_list=[]#当日涨停股票
    g.today_HL_remove_list=[]#当日涨停移除股票

    #----------------Settings--------------------------------------------------/
    run_daily(perpare,time="9:15")
    run_daily(buy,time="9:30")
    run_daily(sell,time='13:00')
    run_daily(sell,time='14:00')
    run_daily(sell,time='14:55')

def perpare(context):#筛选
    date = context.previous_date #昨日
    date = transform_date(date, 'str')
    current_data = get_current_data()
    g.yesterday_high_dict = {}
    g.today_list=[]
    stk_list=get_st(context)
    #1 4 12月 国九

    singal=today_is_between(context)
    if singal==True:
        #stk_list=get_stdf(stk_list,context)
        stk_list=GJT_filter_stocks(stk_list)
        log.info(f'筛选后：{len(stk_list)}')

    stk_list=filter_stocks(context,stk_list)
    if len(stk_list)==0:
        return
    stk_list=rzq_list(context,stk_list)
    if len(stk_list)==0:
        return
    df=get_valuation(stk_list, end_date=date,  
                                    count=1,
                                    fields=['turnover_ratio']
                                    )
    df = df.sort_values(by='turnover_ratio', ascending=False)

    if len(list(df.code))==0:
        return
    g.today_list=list(df.code)  
    log.info (f"股池数{len(g.today_list)}")
    log.info (f"股池{g.today_list}")

def sell(context):
    hold_list = [stock for stock in list(context.portfolio.positions.keys()) if stock not in g.today_list]
    if hold_list:
        current_data = get_current_data()
        yesterday=context.previous_date
        date = transform_date(yesterday, 'str')
        date_1=get_shifted_date(date, -1, 'T')#前日
        # 批量获取昨日涨停数据（加速核心）
        df_history = get_price(hold_list,end_date=yesterday,frequency='daily',fields=['money','close','high','high_limit','low_limit'],count=1,panel=False)
        df_history['avg_cost']=[context.portfolio.positions[s].avg_cost for s in hold_list]
        df_history['price']= [context.portfolio.positions[s].price for s in hold_list]
        df_history['hl']= [current_data[s].high_limit for s in hold_list]
        df_history['ll']= [current_data[s].low_limit for s in hold_list]
        df_history['last_price']= [current_data[s].last_price for s in hold_list]
        # 条件1：未涨停（向量化计算）
        cond1 = (df_history['last_price'] != df_history['hl'])
        # 条件2.1：亏损超过3%（矩阵运算）
        ret_matrix = (df_history['price'] / df_history['avg_cost'] - 1) * 100
        #cond2_0 = ret_matrix >= 10
        cond2_1 = ret_matrix < -3
        # 条件2.2：盈利超过0%（复用矩阵）
        cond2_2 = ret_matrix >= 0
        # 条件2.4：昨日涨停（批量计算）
        cond2_4 = (df_history['close'] == df_history['high_limit'])
        #正常止盈止损
        sell_condition = cond1 &(cond2_1 | cond2_2 | cond2_4)

        sell_list = df_history[
            sell_condition & 
            (df_history['last_price'] > df_history['low_limit'])
            ].code.tolist()
            
        if sell_list:
            # 批量下单（使用order_target_value加速）
            for s in sell_list:
                if context.portfolio.positions[s].closeable_amount>0:
                    order_target_value(s, 0)
                    log.info(f'卖出 {s} ')
                    log.info('-'*50)
                
def buy(context):
    date = context.previous_date #昨日
    date = transform_date(date, 'str')
    date_1=get_shifted_date(date, -1, 'T')#前日
    current_data = get_current_data()
    target=g.today_list
    if len(target)==0:
        return
    # 低开
    df =  get_price(target, end_date=context.previous_date, frequency='daily', fields=['close'], count=1, panel=False, fill_paused=False, skip_paused=True).set_index('code') 
    df['open_now'] = [current_data[s].last_price for s in target]
    log.info(df)
    df = df[(df['open_now']/df['close'])< 1.015] 
    df = df[(df['open_now']/df['close'])>0.951]
    target = list(df.index)
    if len(target)==0:
        return

    hold_list = list(context.portfolio.positions)
    num=g.stock_num-len(hold_list)
    if num==0:
        return
    target=[x for x in target  if x not in  hold_list][:num]#[:min(num,g.max_buy_num)]
    if len(target) > 0:
        # 分配资金（等权重买入）
        value=context.portfolio.available_cash
        cash_per_stock = value / num
        current_data = get_current_data()  # 实时数据对象
        for stock in target:
            # 排除停牌和涨跌停无法交易的股票
            if current_data[stock].paused or \
            current_data[stock].last_price==current_data[stock].low_limit or \
            current_data[stock].last_price==current_data[stock].high_limit:
                continue
            order_value(stock, cash_per_stock)  # 按金额买入[6](@ref)
            log.info (f"买入 {stock}")

           
# 定义分类规则
def classify_reason(text):
    if text is None or pd.isna(text):
        return "未知风险类别"  
    if "净利润" in text or "净资产" in text or "营业收入" in text:
        return "财务指标异常类"
    elif "审计报告" in text or "内部控制" in text:
        return "审计意见问题类"
    elif "生产经营" in text or "破产" in text:
        return "经营异常类"

    else:
        return "其他风险情形类"
        
##获取所有ST股的ST时间、原因##     
def get_stdf(stocks,context):
    df=finance.run_query(query(finance.STK_STATUS_CHANGE).filter(finance.STK_STATUS_CHANGE.code.in_(stocks),
                                                            finance.STK_STATUS_CHANGE.change_date<=context.previous_date))
    # 1. 筛选被 ST 的记录
    st_df = df
    # 2. 转换日期格式
    st_df['change_date'] = pd.to_datetime(st_df['change_date'])
    # 3. 按日期排序（升序，这样每个组的最后一条就是最大日期）
    st_df = st_df.sort_values(by='change_date')
    # 4. 按code分组，取每个组的最后一条记录
    latest_st_df = st_df.groupby('code').last().reset_index()
    # 5. 提取需要的列并重命名
    latest_st_df = latest_st_df[['code', 'change_date', 'change_reason']]
    latest_st_df = latest_st_df.rename(columns={'change_date': 'latest_st_date'})
    # 应用分类
    latest_st_df["风险类别"] = latest_st_df["change_reason"].apply(classify_reason)
    
    latest_st_df=latest_st_df[latest_st_df["风险类别"]!="其他风险情形类"]
    finla_list=list(latest_st_df.code)
    return finla_list
    
#----------------函数群--------------------------------------------------/    
def today_is_between(context):
        today = context.current_dt.strftime('%m-%d')
        if ('01-15' <= today) and (today <= '01-31'):
            return True
        elif ('04-15' <= today) and (today <= '04-30'):
            return True     
        elif ('12-15' <= today) and (today <= '12-31'):
            return True 
        else:
            return False
            
##获取所有ST股##               
def get_st(context):
    yesterday=context.previous_date
    stockList=get_all_securities(types='stock',date=yesterday).index
    st_data=get_extras('is_st',stockList, count = 1,end_date=yesterday)
    st_data = st_data.T
    st_data.columns = ['is_st']
    st_data=st_data[st_data['is_st']==True]
    df = st_data.index.tolist()
    return df    
    
##多头排列判断###    
def dtpl(context,stocks,date):
    current_data = get_current_data()
    # 获取过去20日收盘价（用于计算均线）
    filtered_stocks=[]
    for s in stocks:
        df_close = get_price(s, end_date=date, frequency='1d', fields=['close'], count=20, panel=False)
        latest_price = current_data[s].last_price
        updated_series = df_close.close.iloc[1:].tolist() + [latest_price]
        # 计算各周期均线
        ma_periods = [5, 10, 20]
        for period in ma_periods:
            df_close[f'MA{period}'] = pd.Series(updated_series).rolling(period).mean().iloc[-1]
        # 判断最新交易日的均线排列（假设数据已按日期排序）
        df_latest=df_close
        df_latest['is_multi_ma'] = (df_latest['MA5'] > df_latest['MA10']) & (df_latest['MA10'] > df_latest['MA20'])
        df_latest = df_latest.iloc[-1]
        # 剔除满足条件的股票
        if df_latest['is_multi_ma']==True:
            filtered_stocks.append(s)
            log.info(f'{s}多头排列，不卖')
    return filtered_stocks

    
##处理日期相关函数##
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

##处理日期相关函数##
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
##筛选不涨停##   
def get_ever_hl_stock(initial_list, date):#
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['close','high','high_limit'], count=1, panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna() #去除停牌
    cd2 = df['close'] != df['high_limit']
    df = df[cd2]
    hl_list = list(df.code)
    return hl_list        
##筛选出涨停的股票##
def get_hl_stock(initial_list, date):
    df = get_price(initial_list, end_date=date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False, skip_paused=False)
    df = df.dropna() #去除停牌
    df = df[df['close'] == df['high_limit']]
    hl_list = list(df.code)
    return hl_list

##筛选昨日不涨停的股票##
def rzq_list(context,initial_list): 
    # 文本日期
    date = context.previous_date #昨日
    date = transform_date(date, 'str')
    date_1=get_shifted_date(date, -1, 'T')#前日
    date_2=get_shifted_date(date, -2, 'T')#大前日
    # 昨日不涨停
    h1_list = get_ever_hl_stock(initial_list, date)
    # 前日涨停过滤
    elements_to_remove = get_hl_stock(initial_list, date_1)
 
    zb_list = [stock for stock in h1_list if stock  in elements_to_remove]
    return zb_list
    
# 处于近n天的低位
def get_relative_position_df(stock_list, date, watch_days, ratio):
    if stock_list:
        df = get_price(stock_list, end_date=date, fields=['high', 'low', 'close'], count=watch_days, fill_paused=False, skip_paused=False, panel=False).dropna()
        close = df.groupby('code')['close'].last()
        high = df.groupby('code')['high'].max()
        low = df.groupby('code')['low'].min()
        result = pd.DataFrame({
            'rp': (close - low) / (high - low)
        }, index=close.index)
    else:
        result = pd.DataFrame(columns=['rp'])
    result = result[result['rp'] >= ratio]
    low_n_pos_list = list(result.index)
    return low_n_pos_list
    
##技术指标筛选##
def filter_stocks(context, stocks):
    yesterday = context.previous_date
    stocks=get_relative_position_df(stocks, yesterday, 20, 0.6)
    valid_stocks=[]
    if stocks:
        df = get_price(
            stocks,
            count=11,
            frequency='1d',
            fields=['close', 'low', 'volume','money','low_limit'],
            end_date=yesterday,
            panel=False
            ).reset_index()
        # 按股票分组处理
        grouped = df.groupby('code')
        # 计算技术指标
        ma10 = grouped['close'].transform(lambda x: x.rolling(10).mean())  # 10日均线
        prev_low = grouped['low'].shift(1)  # 前一日最低价
        prev_volume = grouped['volume'].shift(1)  # 前一日成交量
        pct_series = grouped['low'].pct_change()*100   #
        prev_close = pct_series.iloc[-1]   
        # 构建筛选条件
        conditions = (
            (df['close'] > prev_low) &                # 多头排列
            (df['close'] > df['low_limit']) &         # 未曾跌停
            (df['close'] > ma10) &                    # 10日线上方
            #(df['volume'] > 0.95 * prev_volume) &            # 放量
            (df['volume'] < 10 * prev_volume) &       # 成交量未暴增
            (df['close'] > 1)                        # 股价>1
            
        )
        # 获取最新交易日数据
        latest_data = df[df['time'] == pd.Timestamp(yesterday)]
        valid_stocks = latest_data[conditions]['code'].unique().tolist()    
    return valid_stocks

##国九条筛选##
def GJT_filter_stocks(stocks):
    # 国九更新：过滤近一年净利润为负且营业收入小于1亿的
    # 国九更新：过滤近一年期末净资产为负的 (经查询没有为负数的，所以直接pass这条)
    q = query(
        valuation.code,
        valuation.market_cap,  # 总市值 circulating_market_cap/market_cap
        income.np_parent_company_owners,  # 归属于母公司所有者的净利润
        income.net_profit,  # 净利润
        income.operating_revenue,  # 营业收入
        #security_indicator.net_assets
    ).filter(
        valuation.code.in_(stocks),
        income.np_parent_company_owners > 0,
        income.net_profit > 0,
        income.operating_revenue > 1e8,
        indicator.roe>0,
        indicator.roa>0,
    )
    df = get_fundamentals(q)

    final_list=list(df.code)
            
    return final_list


# 分钟级别检测破板卖出: 
# 持仓是否涨停，如果涨停，加入当天涨停列表
# 当天涨停列表在当下是否有变化，涨停打开卖出一半
def handle_data(context, data):
    hold_list = [stock for stock in list(context.portfolio.positions.keys()) if stock not in g.today_list]
    current_data = get_current_data() 
    for stock in hold_list:
        # print(stock, '持仓数据:', "最新价", current_data[stock].last_price, "涨停价", current_data[stock].high_limit)
        # 涨停
        if current_data[stock].last_price == current_data[stock].high_limit:
            if stock not in g.today_HL_list:
                g.today_HL_list.append(stock)
                print('出现当天涨停股票', stock)
        else:
            if stock in g.today_HL_list and stock not in g.today_HL_remove_list:
                g.today_HL_remove_list.append(stock)
                log.info(f'{stock}已涨停破板，加入破板列表')
                # 卖出一半
                order_target_value(stock, 0)
                
                