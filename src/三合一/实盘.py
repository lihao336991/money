#coding:gbk
"""
三合一策略 - 迅投平台版
包含三个子策略：
1. 一进二高开 (gap_up)
2. 首板低开 (gap_down)
3. 弱转强 (reversal)

交易时间安排：
- 09:01: 选股准备
- 09:26: 买入执行
- 11:25: 上午止盈
- 14:50: 下午止盈/止损
"""
import datetime
import time
import uuid

import numpy as np
import pandas as pd
import requests


class G():
    """全局变量存储类"""
    pass

g = G()

# 股票池缓存
g.cached_stock_list = None
g.cached_stock_list_date = None

# 账户配置
MY_ACCOUNT = "190200026196"

# 企业微信Webhook配置
HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2a336b4c-c38e-4ae3-9ff6-f14f175b4f73'


def get_shifted_date(C, date, days, days_type='T'):
    """
    日期偏移函数
    
    Args:
        C: 迅投上下文对象
        date: 基准日期 (datetime或str格式YYYYMMDD)
        days: 偏移天数
        days_type: 偏移类型 'N'=自然日, 'T'=交易日
    
    Returns:
        偏移后的日期字符串 YYYYMMDD
    """
    try:
        if isinstance(date, str):
            base_date = datetime.datetime.strptime(date, "%Y%m%d")
        else:
            base_date = date
        
        if days_type == 'N':
            shifted_date = base_date + datetime.timedelta(days=days)
            return shifted_date.strftime("%Y%m%d")
        
        elif days_type == 'T':
            start_date = (base_date - datetime.timedelta(days=365)).strftime("%Y%m%d")
            end_date = (base_date + datetime.timedelta(abs(days) + 365)).strftime("%Y%m%d")
            
            trade_days = C.get_trading_dates(
                stockcode='SH',
                start_date=start_date,
                end_date=end_date,
                count=1000,
                period='1d'
            )
            
            if not trade_days:
                shifted_date = base_date + datetime.timedelta(days=days)
                return shifted_date.strftime("%Y%m%d")
            
            date_str = base_date.strftime("%Y%m%d")
            if date_str in trade_days:
                index = trade_days.index(date_str)
            else:
                found = False
                for i in range(1, 30):
                    prev_date = base_date - datetime.timedelta(days=i)
                    prev_str = prev_date.strftime("%Y%m%d")
                    if prev_str in trade_days:
                        index = trade_days.index(prev_str)
                        found = True
                        break
                if not found:
                    shifted_date = base_date + datetime.timedelta(days=days)
                    return shifted_date.strftime("%Y%m%d")
            
            new_index = index + days
            if new_index < 0:
                new_index = 0
            elif new_index >= len(trade_days):
                new_index = len(trade_days) - 1
            
            return trade_days[new_index]
        
        else:
            return date
            
    except Exception as e:
        print(f"【日期偏移】错误: {str(e)}")
        return date

def get_previous_trading_day(C, current_date):
    """
    获取前一个交易日
    
    Args:
        C: 迅投上下文对象
        current_date: 当前日期
    
    Returns:
        前一个交易日日期
    """
    try:
        current_str = current_date.strftime("%Y%m%d")
        prev_str = get_shifted_date(C, current_str, -1, 'T')
        return datetime.datetime.strptime(prev_str, "%Y%m%d").date()
    except Exception as e:
        print(f"【get_previous_trading_day】错误: {str(e)}")
        delta = 1
        while True:
            prev_date = current_date - datetime.timedelta(days=delta)
            if prev_date.weekday() < 5:
                return prev_date
            delta += 1
            if delta > 30:
                return current_date - datetime.timedelta(days=30)

def codeOfPosition(position):
    """
    从持仓对象获取股票代码
    
    Args:
        position: 持仓对象
    
    Returns:
        股票代码格式 "代码.交易所"
    """
    return position.m_strInstrumentID + '.' + position.m_strExchangeID

def get_limit_of_stock(stock_code, last_close):
    """
    获取股票涨跌停价格
    
    Args:
        stock_code: 股票代码
        last_close: 昨日收盘价
    
    Returns:
        [涨停价, 跌停价]
    """
    if str(stock_code).startswith(tuple(['3', '688'])):
        return [round(last_close * 1.2, 2), round(last_close * 0.8, 2)]
    return [round(last_close * 1.1, 2), round(last_close * 0.9, 2)]

def get_account_money(C):        
    """
    获取账户可用资金
    
    Args:
        C: 迅投上下文对象
    
    Returns:
        可用资金金额
    """
    accounts = get_trade_detail_data(C.account, 'stock', 'account')
    money = 0
    for dt in accounts:
        money = dt.m_dAvailable
    return money

def open_position_in_test(C, security, value):
    """
    回测模式下开仓
    
    Args:
        C: 迅投上下文对象
        security: 股票代码
        value: 仓位比例
    """
    print("买入股票(回测):", security, C.get_stock_name(security), str(int(value * 100)) + '%')
    order_target_percent(security, round(value, 2), 'COMPETE', C, C.account)

def open_position(C, security, value=0):
    """
    实盘模式下开仓
    
    Args:
        C: 迅投上下文对象
        security: 股票代码
        value: 买入金额
    """
    print("买入股票(实盘):", security, C.get_stock_name(security), value)
    lastOrderId = str(uuid.uuid4())
    passorder(23, 1102, C.account, security, 5, -1, value, lastOrderId, 1, lastOrderId, C)

def close_position(C, stock):
    """
    平仓（清仓）
    
    Args:
        C: 迅投上下文对象
        stock: 股票代码
    
    Returns:
        True表示成功执行
    """
    if stock:
        if C.do_back_test:
            order_target_value(stock, 0, C, C.account)
        else:
            passorder(24, 1123, C.account, stock, 6, 1, 1, "卖出策略", 1, "", C)
        return True

def transform_date(date, date_type):
    """
    日期格式转换函数
    
    Args:
        date: 日期 (str/datetime/datetime.date)
        date_type: 返回类型 'str'/'dt'/'d'
    
    Returns:
        指定格式的日期
    """
    if isinstance(date, str):
        try:
            dt_date = datetime.datetime.strptime(date, '%Y-%m-%d')
            str_date = date
        except ValueError:
            dt_date = datetime.datetime.strptime(date, '%Y%m%d')
            str_date = dt_date.strftime('%Y-%m-%d')
        d_date = dt_date.date()
    elif isinstance(date, datetime.datetime):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = dt_date.date()
    elif isinstance(date, datetime.date):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = datetime.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    dct = {'str':str_date, 'dt':dt_date, 'd':d_date}
    return dct[date_type]

def filter_new_stock(C, initial_list, date, days=50):
    """
    过滤新股
    
    Args:
        C: 迅投上下文对象
        initial_list: 股票列表
        date: 基准日期
        days: 新股天数阈值
    
    Returns:
        过滤后的股票列表
    """
    d_date = transform_date(date, 'd')
    result = []
    error_count = 0
    new_stock_count = 0
    start_time = time.time()
    
    for stock in initial_list:
        try:
            open_date_raw = C.get_open_date(stock)
            open_date_str = str(open_date_raw)
            opendate = datetime.datetime.strptime(open_date_str, "%Y%m%d")
            if d_date - opendate.date() > datetime.timedelta(days=days):
                result.append(stock)
            else:
                new_stock_count += 1
        except Exception as e:
            error_count += 1
            pass
    
    print(f"【选股】filter_new_stock: 基准日期={d_date}, 阈值={days}天, 错误={error_count}只, 新股={new_stock_count}只, 通过={len(result)}只, 耗时 {time.time()-start_time:.1f}秒")
    return result

def filter_st_paused_stock(C, initial_list):
    """
    过滤ST股和停牌股
    
    Args:
        C: 迅投上下文对象
        initial_list: 股票列表
    
    Returns:
        过滤后的股票列表
    """
    result = []
    start_time = time.time()
    
    for stock in initial_list:
        try:
            name = C.get_stock_name(stock)
            is_st = 'ST' in name or '*' in name or '退' in name
            is_paused = C.is_suspended_stock(stock)
            if not is_st and not is_paused:
                result.append(stock)
        except Exception as e:
            print(f"【选股】filter_st_paused_stock 股票 {stock} 处理异常: {e}")
            pass
    
    print(f"【选股】filter_st_paused_stock: 通过 {len(result)}只, 耗时 {time.time()-start_time:.1f}秒")
    return result

def filter_kcbj_stock(stock_list):
    """
    过滤科创板和北交所股票
    
    Args:
        stock_list: 股票列表
    
    Returns:
        只保留主板和创业板的股票列表
    """
    return [stock for stock in stock_list if stock[:2] in (('60','00','30'))]

def prepare_stock_list_func(C):
    """选股准备函数 - 定时任务入口"""
    print("【选股准备】开始执行选股...")
    get_stock_list_func(C)
    print(f"【选股准备】选股完成 - 一进二: {len(g.gap_up)}只, 首板低开: {len(g.gap_down)}只, 弱转强: {len(g.reversal)}只")

def get_market_data_batch(C, initial_list, dates):
    """
    批量获取多个日期的行情数据
    
    Args:
        C: 迅投上下文对象
        initial_list: 股票列表
        dates: 日期列表
    
    Returns:
        字典，key为日期，value为对应的行情数据
    """
    result = {}
    for date in dates:
        data = C.get_market_data_ex(
            ['close', 'high'],
            initial_list,
            period="1d",
            start_time='',
            end_time=date,
            count=2,
            dividend_type="follow",
            fill_data=False,
            subscribe=False
        )
        result[date] = data
    return result

def get_hl_stock_from_data(data, date):
    """
    从已获取的数据中获取指定日期封板股票（收盘价涨停）
    
    Args:
        data: 行情数据
        date: 日期（仅用于日志）
    
    Returns:
        封板股票列表
    """
    result = []
    for stock in data:
        try:
            df = data[stock]
            if len(df) < 2:
                continue
            last_close = df['close'].iloc[0]
            close = df['close'].iloc[1]
            high_limit = get_limit_of_stock(stock, last_close)[0]
            
            if round(close, 2) >= round(high_limit, 2):
                result.append(stock)
        except Exception as e:
            print(f"【调试-涨停】{date} {stock}: 错误 {e}")
            pass
    return result

def get_ever_hl_stock_from_data(data, date):
    """
    从已获取的数据中获取指定日期曾涨停股票（最高价涨停）
    
    Args:
        data: 行情数据
        date: 日期（仅用于日志）
    
    Returns:
        曾涨停股票列表
    """
    result = []
    for stock in data:
        try:
            df = data[stock]
            if len(df) < 2:
                continue
            last_close = df['close'].iloc[0]
            high = df['high'].iloc[1]
            high_limit = get_limit_of_stock(stock, last_close)[0]
            
            if round(high, 2) >= round(high_limit, 2):
                result.append(stock)
        except Exception as e:
            print(f"【选股】get_ever_hl_stock_from_data 股票 {stock} 处理异常: {e}")
            pass
    return result

def get_ever_hl_stock2_from_data(data, date):
    """
    从已获取的数据中获取指定日期曾涨停但未封板股票
    
    Args:
        data: 行情数据
        date: 日期（仅用于日志）
    
    Returns:
        曾涨停但未封板股票列表
    """
    result = []
    for stock in data:
        try:
            df = data[stock]
            if len(df) < 2:
                continue
            last_close = df['close'].iloc[0]
            close = df['close'].iloc[1]
            high = df['high'].iloc[1]
            high_limit = get_limit_of_stock(stock, last_close)[0]
            cd1 = round(high, 2) >= round(high_limit, 2)
            cd2 = round(close, 2) < round(high_limit, 2)
            
            if cd1 and cd2:
                result.append(stock)
        except Exception as e:
            print(f"【选股】get_ever_hl_stock2_from_data 股票 {stock} 处理异常: {e}")
            pass
    return result

def get_stock_list_func(C):
    """
    获取三个子策略的股票池
    
    Args:
        C: 迅投上下文对象
    
    结果存储到全局变量 g 中:
        g.gap_up: 一进二高开股票池
        g.gap_down: 首板低开股票池
        g.reversal: 弱转强股票池
    """
    date = C.yesterday
    print(f"【选股】基准日期: {date}")
    date_2 = get_shifted_date(C, date, -2, 'T')
    date_1 = get_shifted_date(C, date, -1, 'T')
    print(f"【选股】日期范围: date={date}, date_1={date_1}, date_2={date_2}")
    
    initial_list = prepare_stock_list(C, date)
    print(f"【选股】初步筛选后共 {len(initial_list)} 只股票")
    
    start_time = time.time()
    print(f"【选股】开始批量获取行情数据...")
    market_data = get_market_data_batch(C, initial_list, [date, date_1, date_2])
    elapsed = time.time() - start_time
    print(f"【选股】批量获取行情数据完成，耗时 {elapsed:.1f}秒")

    hl0_list = get_hl_stock_from_data(market_data[date], date)
    # print(f"【选股】get_hl_stock({date}): 封板股票 {len(hl0_list)}只")
    
    hl1_list = get_ever_hl_stock_from_data(market_data[date_1], date_1)
    # print(f"【选股】get_ever_hl_stock({date_1}): 曾涨停股票 {len(hl1_list)}只")
    
    hl2_list = get_ever_hl_stock_from_data(market_data[date_2], date_2)
    # print(f"【选股】get_ever_hl_stock({date_2}): 曾涨停股票 {len(hl2_list)}只")
    
    elements_to_remove = set(hl1_list + hl2_list)
    g.gap_up = [stock for stock in hl0_list if stock not in elements_to_remove]
    g.gap_down = [s for s in hl0_list if s not in hl1_list]
    # print(f"【选股】一进二高开: {len(g.gap_up)}只, 首板低开: {len(g.gap_down)}只")
    
    h1_list = get_ever_hl_stock2_from_data(market_data[date], date)
    # print(f"【选股】get_ever_hl_stock2({date}): 曾涨停未封板 {len(h1_list)}只")
    
    elements_to_remove = get_hl_stock_from_data(market_data[date_1], date_1)
    # print(f"【选股】get_hl_stock({date_1}): 封板股票 {len(elements_to_remove)}只")
    
    g.reversal = [stock for stock in h1_list if stock not in elements_to_remove]
    # print(f"【选股】弱转强: {len(g.reversal)}只")

def filter_market_cap(C, initial_list, date, max_cap=500):
    """
    过滤市值大于指定值的股票
    
    Args:
        C: 迅投上下文对象
        initial_list: 股票列表
        date: 日期
        max_cap: 最大市值（亿），默认500亿
    
    Returns:
        过滤后的股票列表
    """
    result = []
    if not initial_list:
        return result
    
    start_time = time.time()
    print(f"【选股】开始市值筛选，目标市值<{max_cap}亿...")
    
    try:
        # 获取收盘价数据
        ticks = C.get_market_data_ex(
            ['close'],
            initial_list,
            period="1d",
            start_time='',
            end_time=date,
            count=1,
            dividend_type="follow",
            fill_data=False,
            subscribe=False
        )
        
        # 获取财务数据（总股本）
        end_date = date
        start_date = get_shifted_date(C, date, -365, 'T')
        eps = C.get_raw_financial_data(['股本表.总股本'], initial_list, start_date, end_date)
        
        for stock in initial_list:
            try:
                # 获取总股本
                stock_num_list = list(eps[stock]['股本表.总股本'].values())
                if not stock_num_list:
                    continue
                stock_num = stock_num_list[-1]
                
                # 获取收盘价
                if stock not in ticks:
                    continue
                df = ticks[stock]
                if df.empty:
                    continue
                close = df['close'].iloc[-1]
                
                # 计算市值
                market_cap = close * stock_num / 100000000
                
                if market_cap <= max_cap:
                    result.append(stock)
            except Exception as e:
                print(f"【选股】filter_market_cap 股票 {stock} 处理异常: {e}")
                pass
    except Exception as e:
        print(f"【选股】市值筛选异常: {str(e)}")
    
    elapsed = time.time() - start_time
    print(f"【选股】市值筛选完成，通过 {len(result)}只，耗时 {elapsed:.1f}秒")
    return result

def prepare_stock_list(C, date):
    """
    准备初始股票池（基础过滤）
    
    Args:
        C: 迅投上下文对象
        date: 日期
    
    Returns:
        过滤后的股票列表
    """
    if g.cached_stock_list is not None:
        print(f"【选股】使用缓存的股票池")
        return g.cached_stock_list
    
    initial_list = C.get_stock_list_in_sector('沪深A股')
    print(f"【选股】沪深A股总数: {len(initial_list)}只")
    
    initial_list = filter_kcbj_stock(initial_list)
    print(f"【选股】过滤科创板北交所后: {len(initial_list)}只")
    
    initial_list = filter_new_stock(C, initial_list, date, 50)
    print(f"【选股】过滤新股后: {len(initial_list)}只")
    
    initial_list = filter_st_paused_stock(C, initial_list)
    print(f"【选股】过滤ST停牌后: {len(initial_list)}只")
    
    initial_list = filter_market_cap(C, initial_list, date, 300)
    print(f"【选股】过滤市值后: {len(initial_list)}只")
    
    g.cached_stock_list = initial_list
    g.cached_stock_list_date = date
    
    return initial_list

def rise_low_volume(C, s):
    """
    量价配合检查：判断是否为缩量上涨
    
    Args:
        C: 迅投上下文对象
        s: 股票代码
    
    Returns:
        True表示缩量（需要过滤），False表示放量
    """
    try:
        data = C.get_market_data_ex(
            ['high', 'volume'],
            [s],
            period="1d",
            start_time='',
            end_time=C.yesterday,
            count=106,
            dividend_type="follow",
            fill_data=False,
            subscribe=False
        )
        
        if s not in data or data[s].empty:
            return False
        
        df = data[s]
        if len(df) < 106:
            return False
        
        high_prices = df['high'].values[:102]
        prev_high = high_prices[-1]
        
        zyts_0 = 100
        for i, high in enumerate(reversed(high_prices[:-3]), 2):
            if high >= prev_high:
                zyts_0 = i - 1
                break
        
        zyts = zyts_0 + 5
        volume_slice = df['volume'].iloc[-zyts:-1].values
        if len(volume_slice) == 0:
            return False
        if df['volume'].iloc[-1] <= max(volume_slice) * 0.9:
            return True
        return False
    except Exception as e:
        print(f"【买入】rise_low_volume 股票 {s} 处理异常: {e}")
        return False

def get_relative_position_df(C, stock_list, date, watch_days):
    """
    计算股票在指定周期内的相对位置
    
    Args:
        C: 迅投上下文对象
        stock_list: 股票列表
        date: 日期
        watch_days: 观察天数
    
    Returns:
        包含相对位置(rp)的DataFrame
    """
    result = pd.DataFrame(columns=['rp'])
    if not stock_list:
        return result
    
    try:
        data = C.get_market_data_ex(
            ['high', 'low', 'close'],
            stock_list,
            period="1d",
            start_time='',
            end_time=date,
            count=watch_days,
            dividend_type="follow",
            fill_data=False,
            subscribe=False
        )
        
        rp_list = []
        for code in data:
            try:
                df = data[code]
                if df.empty:
                    continue
                close = df['close'].iloc[-1]
                high = df['high'].max()
                low = df['low'].min()
                if high - low < 1e-6:
                    rp = 0.0
                else:
                    rp = (close - low) / (high - low)
                rp_list.append({'code': code, 'rp': rp})
            except Exception as e:
                print(f"【买入】get_relative_position_df 股票 {code} 处理异常: {e}")
                continue
        
        if rp_list:
            result = pd.DataFrame(rp_list).set_index('code')
        return result
    except Exception as e:
        print(f"【买入】get_relative_position_df 整体异常: {e}")
        return result

def buy_func(C):
    """
    买入执行函数 - 09:26执行
    
    筛选三个子策略的合格股票并执行买入:
    1. 一进二高开策略
    2. 首板低开策略
    3. 弱转强策略
    
    Args:
        C: 迅投上下文对象
    """
    print("【买入执行】开始筛选买入标的...")
    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []
    
    date_now = C.today.strftime("%Y-%m-%d")
    
    # ========== 一进二高开策略筛选 ==========
    print(f"【买入执行】正在筛选一进二高开标的，共{len(g.gap_up)}只候选")
    gk_filtered = 0
    for s in g.gap_up:
        try:
            prev_day_data = C.get_market_data_ex(
                ['close', 'volume', 'amount'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in prev_day_data or prev_day_data[s].empty:
                print(f"【一进二过滤】{s} {C.get_stock_name(s)} 无昨日数据")
                gk_filtered += 1
                continue
            
            df = prev_day_data[s]
            avg_price_increase_value = df['amount'].iloc[0] / df['volume'].iloc[0] / df['close'].iloc[0] * 1.1 - 1
            if avg_price_increase_value < 0.07 or df['amount'].iloc[0] < 5.5e8 or df['amount'].iloc[0] > 20e8:
                reason = []
                if avg_price_increase_value < 0.07:
                    reason.append(f"平均涨幅{avg_price_increase_value:.2%}<7%")
                if df['amount'].iloc[0] < 5.5e8:
                    reason.append(f"成交额{df['amount'].iloc[0]:.2e}<5.5亿")
                if df['amount'].iloc[0] > 20e8:
                    reason.append(f"成交额{df['amount'].iloc[0]:.2e}>20亿")
                # print(f"【一进二过滤】{s} {C.get_stock_name(s)} {', '.join(reason)}")
                gk_filtered += 1
                continue
            
            try:
                eps = C.get_raw_financial_data(['股本表.总股本'], [s], C.tm.get_past_date(365), C.today.strftime('%Y%m%d'))
                stock_num = 0
                if s in eps and eps[s]:
                    stock_num_list = list(eps[s]['股本表.总股本'].values())
                    if stock_num_list:
                        stock_num = stock_num_list[-1]
                
                if stock_num <= 0:
                    # print(f"【一进二过滤】{s} {C.get_stock_name(s)} 股本数量{stock_num}<=0")
                    gk_filtered += 1
                    continue
                
                market_cap = df['close'].iloc[0] * stock_num
                circulating_market_cap = market_cap
                
                if market_cap < 70e8 or circulating_market_cap > 520e8:
                    reason = []
                    if market_cap < 70e8:
                        reason.append(f"市值{market_cap:.2e}<70亿")
                    if circulating_market_cap > 520e8:
                        reason.append(f"流通市值{circulating_market_cap:.2e}>520亿")
                    # print(f"【一进二过滤】{s} {C.get_stock_name(s)} {', '.join(reason)}")
                    gk_filtered += 1
                    continue
            except Exception as e:
                print(f"【一进二过滤】{s} {C.get_stock_name(s)} 市值计算异常: {e}")
                gk_filtered += 1
                continue
            
            if rise_low_volume(C, s):
                # print(f"【一进二过滤】{s} {C.get_stock_name(s)} 缩量上涨")
                gk_filtered += 1
                continue
            
            auction_data = C.get_market_data_ex(
                ['open', 'volume'],
                [s],
                period="1m",
                start_time=(C.today - datetime.timedelta(days=1)).strftime('%Y%m%d%H%M%S'),
                end_time=C.today.strftime('%Y%m%d%H%M%S'),
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            # print(auction_data[s], '竞价数据')
            # print(df, '昨日数据')
            
            if s not in auction_data or auction_data[s].empty:
                # print(f"【一进二过滤】{s} {C.get_stock_name(s)} 无集合竞价数据")
                gk_filtered += 1
                continue
            
            if auction_data[s]['volume'].iloc[0] / df['volume'].iloc[0] < 0.03:
                ratio = auction_data[s]['volume'].iloc[0] / df['volume'].iloc[0]
                # print(f"【一进二过滤】{s} {C.get_stock_name(s)} 集合竞价量比{ratio:.2%}<3%")
                gk_filtered += 1
                continue
            
            current_data = auction_data[s]
            current_ratio = current_data['open'].iloc[0] / df['close'].iloc[0]
            if current_ratio <= 1 or current_ratio >= 1.06:
                # print(f"【一进二过滤】{s} {C.get_stock_name(s)} 高开比例{current_ratio:.2%}不在(100%, 106%)之间")
                gk_filtered += 1
                continue
            
            gk_stocks.append(s)
            qualified_stocks.append(s)
        except Exception as e:
            print(f"【一进二过滤】{s} {C.get_stock_name(s)} 整体异常: {e}")
            gk_filtered += 1
            pass
    print(f"【买入执行】一进二高开筛选完成: 通过{len(gk_stocks)}只, 过滤{gk_filtered}只")
    
    # ========== 首板低开策略筛选 ==========
    print(f"【买入执行】正在筛选首板低开标的，共{len(g.gap_down)}只候选")
    dk_filtered = 0
    
    if g.gap_down:
        stock_list = g.gap_down
        # 相对位置过滤，只选低位股票
        rpd = get_relative_position_df(C, stock_list, C.yesterday, 60)
        if not rpd.empty:
            rpd = rpd[rpd['rp'] <= 0.5]
            stock_list = list(rpd.index)
            print(f"【买入执行】首板低开: 相对位置过滤后剩余{len(stock_list)}只")
        else:
            print(f"【买入执行】首板低开: 相对位置数据为空")
        
        if stock_list:
            df_price = C.get_market_data_ex(
                ['close'],
                stock_list,
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            df_open = C.get_market_data_ex(
                ['open'],
                stock_list,
                period="1d",
                start_time=C.today.strftime('%Y%m%d'),
                end_time=C.today.strftime('%Y%m%d'),
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            for s in stock_list:
                try:
                    if s not in df_price or s not in df_open:
                        dk_filtered += 1
                        continue
                    close = df_price[s]['close'].iloc[0]
                    open_p = df_open[s]['open'].iloc[0]
                    open_pct = open_p / close
                    if 0.955 <= open_pct <= 0.97:
                        prev_day_data = C.get_market_data_ex(
                            ['amount'],
                            [s],
                            period="1d",
                            start_time='',
                            end_time=C.yesterday,
                            count=1,
                            dividend_type="follow",
                            fill_data=False,
                            subscribe=False
                        )
                        if s in prev_day_data and prev_day_data[s]['amount'].iloc[0] >= 1e8:
                            dk_stocks.append(s)
                            qualified_stocks.append(s)
                        else:
                            dk_filtered += 1
                except Exception as e:
                    print(f"【买入】首板低开 股票 {s} 处理异常: {e}")
                    dk_filtered += 1
                    pass
    print(f"【买入执行】首板低开筛选完成: 通过{len(dk_stocks)}只, 过滤{dk_filtered}只")
    
    # ========== 弱转强策略筛选 ==========
    print(f"【买入执行】正在筛选弱转强标的，共{len(g.reversal)}只候选")
    rzq_filtered = 0
    for s in g.reversal:
        try:
            price_data = C.get_market_data_ex(
                ['close'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=4,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in price_data or len(price_data[s]) < 4:
                rzq_filtered += 1
                continue
            
            df = price_data[s]
            increase_ratio = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
            if increase_ratio > 0.28:
                rzq_filtered += 1
                continue
            
            prev_day_data = C.get_market_data_ex(
                ['open', 'close'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in prev_day_data:
                rzq_filtered += 1
                continue
            
            df_prev = prev_day_data[s]
            open_close_ratio = (df_prev['close'].iloc[0] - df_prev['open'].iloc[0]) / df_prev['open'].iloc[0]
            if open_close_ratio < -0.05:
                rzq_filtered += 1
                continue
            
            prev_day_data2 = C.get_market_data_ex(
                ['close', 'volume', 'amount'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in prev_day_data2:
                rzq_filtered += 1
                continue
            
            df2 = prev_day_data2[s]
            avg_price_increase_value = df2['amount'].iloc[0] / df2['volume'].iloc[0] / df2['close'].iloc[0] - 1
            if avg_price_increase_value < -0.04 or df2['amount'].iloc[0] < 3e8 or df2['amount'].iloc[0] > 19e8:
                rzq_filtered += 1
                continue
            
            try:
                eps = C.get_raw_financial_data(['股本表.总股本'], [s], C.tm.get_past_date(365), C.today.strftime('%Y%m%d'))
                stock_num = 0
                if s in eps and eps[s]:
                    stock_num_list = list(eps[s]['股本表.总股本'].values())
                    if stock_num_list:
                        stock_num = stock_num_list[-1]
                
                if stock_num <= 0:
                    rzq_filtered += 1
                    continue
                
                market_cap = df2['close'].iloc[0] * stock_num
                circulating_market_cap = market_cap
                
                if market_cap < 70e8 or circulating_market_cap > 520e8:
                    rzq_filtered += 1
                    continue
            except Exception as e:
                print(f"【买入】弱转强 股票 {s} 市值计算异常: {e}")
                rzq_filtered += 1
                continue
            
            if rise_low_volume(C, s):
                rzq_filtered += 1
                continue
            
            auction_data = C.get_market_data_ex(
                ['open', 'volume'],
                [s],
                period="1d",
                start_time=C.today.strftime('%Y%m%d'),
                end_time=C.today.strftime('%Y%m%d'),
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in auction_data or auction_data[s].empty:
                rzq_filtered += 1
                continue
            
            if auction_data[s]['volume'].iloc[0] / df2['volume'].iloc[0] < 0.03:
                rzq_filtered += 1
                continue
            
            current_ratio = auction_data[s]['open'].iloc[0] / df2['close'].iloc[0]
            if current_ratio <= 0.98 or current_ratio >= 1.09:
                rzq_filtered += 1
                continue
            
            rzq_stocks.append(s)
            qualified_stocks.append(s)
        except Exception as e:
            print(f"【买入】弱转强 股票 {s} 整体异常: {e}")
            rzq_filtered += 1
            pass
    print(f"【买入执行】弱转强筛选完成: 通过{len(rzq_stocks)}只, 过滤{rzq_filtered}只")
    
    # ========== 输出选股结果 ==========
    print(f"【买入执行】筛选完成 - 一进二: {len(gk_stocks)}只, 首板低开: {len(dk_stocks)}只, 弱转强: {len(rzq_stocks)}只")
    
    if len(qualified_stocks) > 0:
        print('———————————————————————————————————')
        msg = '今日选股：' + ','.join(qualified_stocks)
        print(msg)
        messager.send_message(msg)
        print('一进二：' + ','.join(gk_stocks))
        print('首板低开：' + ','.join(dk_stocks))
        print('弱转强：' + ','.join(rzq_stocks))
        print('今日选股：' + ','.join(qualified_stocks))
        print('———————————————————————————————————')
    else:
        messager.send_message('今日无目标个股')
        print('今日无目标个股')
    
    # ========== 执行买入 ==========
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    hold_list = [codeOfPosition(position) for position in positions]
    
    if len(qualified_stocks) != 0:
        total_asset = get_account_money(C)
        print(f"【买入执行】账户可用资金: {total_asset:.2f}元")
        # 仓位控制：如果持仓市值超过70%则不再买入
        if total_asset / (total_asset + sum([p.m_dMarketValue for p in positions])) > 0.3:
            value = total_asset / len(qualified_stocks)
            print(f"【买入执行】每只股票分配资金: {value:.2f}元")
            for s in qualified_stocks:
                if s in hold_list:
                    continue
                try:
                    tick_data = C.get_market_data_ex(
                        ['close'],
                        [s],
                        period="1m",
                        start_time=C.today.strftime('%Y%m%d%H%M%S'),
                        end_time=C.today.strftime('%Y%m%d%H%M%S'),
                        count=1,
                        dividend_type="follow",
                        fill_data=False,
                        subscribe=False
                    )
                    
                    if s in tick_data and total_asset / tick_data[s]['close'].iloc[0] > 100:
                        if C.do_back_test:
                            order_target_value(s, value, C, C.account)
                        else:
                            open_position(C, s, value)
                        print('买入' + s)
                        messager.send_message('买入' + s + ' ' + C.get_stock_name(s))
                        print('———————————————————————————————————')
                except Exception as e:
                    print(f"【买入】执行买入 股票 {s} 异常: {e}")
                    pass

def print_holdings_func(C):
    """
    收盘后打印持仓信息函数
    
    Args:
        C: 迅投上下文对象
    """
    print("【收盘持仓】开始打印持仓信息...")
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    
    if not positions:
        print("【收盘持仓】当前无持仓")
        messager.send_message("今日收盘无持仓")
        return
    
    total_market_value = 0
    holdings_info = []
    
    print("="*80)
    print(f"【收盘持仓】{C.today.strftime('%Y-%m-%d')} 持仓汇总")
    print("="*80)
    print(f"{'股票代码':<15}{'股票名称':<15}{'持仓数量':<12}{'可用数量':<12}{'成本价':<12}{'最新价':<12}{'市值':<15}{'盈亏':<12}")
    print("-"*80)
    
    for position in positions:
        s = codeOfPosition(position)
        stock_name = C.get_stock_name(s)
        hold_volume = position.m_nVolume
        can_use_volume = position.m_nCanUseVolume
        avg_cost = position.m_dOpenPrice
        market_value = position.m_dMarketValue
        total_market_value += market_value
        
        try:
            tick_data = C.get_market_data_ex(
                ['close'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            last_price = tick_data[s]['close'].iloc[0] if s in tick_data else avg_cost
            profit = (last_price - avg_cost) * hold_volume if hold_volume > 0 else 0
        except Exception as e:
            print(f"【收盘持仓】股票 {s} 获取最新价异常: {e}")
            last_price = avg_cost
            profit = 0
        
        holdings_info.append(f"{s} {stock_name}")
        print(f"{s:<15}{stock_name:<15}{hold_volume:<12}{can_use_volume:<12}{avg_cost:<12.2f}{last_price:<12.2f}{market_value:<15.2f}{profit:<12.2f}")
    
    print("-"*80)
    print(f"【收盘持仓】持仓总数: {len(positions)}只, 总市值: {total_market_value:.2f}元")
    print("="*80)
    
    msg = f"今日收盘持仓({len(positions)}只): " + ", ".join(holdings_info)
    messager.send_message(msg)

def sell_func(C):
    """
    卖出执行函数
    
    两个执行时间点：
    1. 11:25: 上午止盈（有利润就卖出）
    2. 14:50: 下午止盈/止损（有利润卖出，无利润则看均线止损）
    
    Args:
        C: 迅投上下文对象
    """
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    if not positions:
        return
    
    current_time_str = C.today.strftime("%H:%M:%S")
    print(f"【卖出执行】当前时间: {current_time_str}, 持仓数: {len(positions)}")
    
    for position in positions:
        s = codeOfPosition(position)
        if position.m_nCanUseVolume <= 0:
            continue
        
        try:
            tick_data = C.get_market_data_ex(
                ['close'],
                [s],
                period="1m",
                start_time=C.today.strftime('%Y%m%d%H%M%S'),
                end_time=C.today.strftime('%Y%m%d%H%M%S'),
                count=1,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            if s not in tick_data:
                continue
            
            last_price = tick_data[s]['close'].iloc[0]
            avg_cost = position.m_dOpenPrice
            
            day_data = C.get_market_data_ex(
                ['close', 'lastClose'],
                [s],
                period="1d",
                start_time='',
                end_time=C.yesterday,
                count=4,
                dividend_type="follow",
                fill_data=False,
                subscribe=False
            )
            
            high_limit = 0
            if s in day_data and len(day_data[s]) >= 2:
                try:
                    last_close = day_data[s]['lastClose'].iloc[-2]
                    high_limit = get_limit_of_stock(s, last_close)[0]
                except (IndexError, KeyError):
                    pass
            
            # ========== 11:25 上午止盈 ==========
            if current_time_str <= '11:30:00':
                if last_price < high_limit and last_price > avg_cost:
                    close_position(C, s)
                    print('止盈卖出', [s, C.get_stock_name(s)])
                    messager.send_message('止盈卖出 ' + C.get_stock_name(s))
                    print('———————————————————————————————————')
            
            # ========== 14:50 下午止盈/止损 ==========
            if current_time_str >= '14:50:00':
                if last_price < high_limit and last_price > avg_cost:
                    close_position(C, s)
                    print('止盈卖出', [s, C.get_stock_name(s)])
                    messager.send_message('止盈卖出 ' + C.get_stock_name(s))
                    print('———————————————————————————————————')
                else:
                    # 跌破5日均线止损
                    if s in day_data and len(day_data[s]) >= 4:
                        closes = day_data[s]['close'].values[-4:]
                        M4 = np.mean(closes)
                        MA5 = (M4 * 4 + last_price) / 5
                        if last_price < MA5:
                            close_position(C, s)
                            print('止损卖出', [s, C.get_stock_name(s)])
                            messager.send_message('止损卖出 ' + C.get_stock_name(s))
                            print('———————————————————————————————————')
        except Exception as e:
            print(f"【卖出】股票 {s} 处理异常: {e}")
            pass

def init(C):
    """
    初始化函数
    
    Args:
        C: 迅投上下文对象
    """
    print("【初始化】策略开始初始化...")
    C.account = MY_ACCOUNT
    C.set_account(C.account)
    
    C.runner = TaskRunner(C)
    messager.set_is_test(C.do_back_test)
    
    C.currentTime = 0
    
    # 实盘模式初始化时间相关变量
    if not C.do_back_test:
        currentTime = time.time() * 1000 + 8 * 3600 * 1000
        C.currentTime = currentTime
        C.today = pd.to_datetime(currentTime, unit='ms')
        C.tm = type('TimeManager', (), {})()
        C.tm.date_str = C.today.strftime('%Y%m%d')
        C.tm.get_past_date = lambda days: (C.today - datetime.timedelta(days=days)).strftime('%Y%m%d')
        
        current_dt = datetime.datetime.fromtimestamp(currentTime / 1000)
        yesterday_dt = get_previous_trading_day(C, current_dt.date())
        C.yesterday = yesterday_dt.strftime("%Y%m%d")
        C.yesterday_dt = yesterday_dt
    
    # 周末检查
    current_weekday = datetime.datetime.now().weekday()
    if current_weekday >= 5 and not C.do_back_test:
        print('当前日期为周末，不执行任务')
        return
    
    # 初始化股票池
    g.gap_up = []
    g.gap_down = []
    g.reversal = []
    
    # 注册定时任务
    if C.do_back_test:
        print('【初始化】回测模式，注册定时任务')
        C.runner.run_daily("9:16", prepare_stock_list_func)
        C.runner.run_daily("09:26", buy_func)
        C.runner.run_daily("11:25", sell_func)
        C.runner.run_daily("14:50", sell_func)
        C.runner.run_daily("15:00", print_holdings_func)
    else:
        print('【初始化】实盘模式，注册定时任务')
        C.run_time("prepare_stock_list_func","1nDay","2025-03-01 09:16:00","SH")
        C.run_time("buy_func","1nDay","2025-03-01 09:26:00","SH")
        C.run_time("sell_func","1nDay","2025-03-01 11:25:00","SH")
        C.run_time("sell_func","1nDay","2025-03-01 14:50:00","SH")
        C.run_time("print_holdings_func","1nDay","2025-03-01 15:00:00","SH")
    
    print("【初始化】策略初始化完成")

def handlebar(C):
    """
    K线处理函数（回测模式使用）
    
    Args:
        C: 迅投上下文对象
    """
    index = C.barpos
    currentTime = C.get_bar_timetag(index) + 8 * 3600 * 1000
    try:
        if C.currentTime < currentTime:
            C.currentTime = currentTime
            C.today = pd.to_datetime(currentTime, unit='ms')
            C.tm = type('TimeManager', (), {})()
            C.tm.date_str = C.today.strftime('%Y%m%d')
            C.tm.get_past_date = lambda days: (C.today - datetime.timedelta(days=days)).strftime('%Y%m%d')
    except Exception as e:
        print('handlebar异常', currentTime, e)

    if (datetime.datetime.now() - datetime.timedelta(days=1) > C.today) and not C.do_back_test:
        return
    else:
        if C.do_back_test:
            index = C.barpos
            currentTime = C.get_bar_timetag(index) + 8 * 3600 * 1000
            C.currentTime = currentTime
            C.today = pd.to_datetime(currentTime, unit='ms')
            C.tm = type('TimeManager', (), {})()
            C.tm.date_str = C.today.strftime('%Y%m%d')
            C.tm.get_past_date = lambda days: (C.today - datetime.timedelta(days=days)).strftime('%Y%m%d')
            
            current_dt = datetime.datetime.fromtimestamp(currentTime / 1000)
            yesterday_dt = get_previous_trading_day(C, current_dt.date())
            C.yesterday = yesterday_dt.strftime("%Y%m%d")
            C.yesterday_dt = yesterday_dt
            C.current_dt = current_dt
            
            C.runner.check_tasks(C.today)



class Messager:
    """
    企业微信消息推送类
    """
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = False

    def set_is_test(self, is_test):
        """
        设置是否为测试模式
        
        Args:
            is_test: True表示测试模式（仅打印不推送）
        """
        self.is_test = is_test

    def send_message(self, text_content):
        """
        发送文本消息
        
        Args:
            text_content: 消息内容
        """
        if self.is_test:
            print(f"【消息推送(测试)】{text_content}")
            return

        try:
            current_time = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            content = current_time + text_content
            
            payload = {
                "msgtype": "text",
                "text": {
                    "content": content
                }
            }
            response = requests.post(self.hook_url, json=payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"【消息推送失败】错误: {e}")

    def sendLog(self, text_content):
        """发送日志消息（send_message的别名）"""
        self.send_message(text_content)

messager = Messager(HOOK)

class ScheduledTask:
    """定时任务基类"""
    def __init__(self, execution_time):
        self.last_executed = None
        self.execution_time = self._parse_time(execution_time)
    
    def _parse_time(self, time_str):
        try:
            return datetime.datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError("Invalid time format, use HH:MM")

class DailyTask(ScheduledTask):
    """每日定时任务"""
    def should_trigger(self, current_dt):
        """
        判断是否应该触发任务
        
        Args:
            current_dt: 当前时间
        
        Returns:
            True表示应该触发
        """
        should1 = current_dt.time() >= datetime.datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = self.last_executed != current_dt.date()
        return should1 and should2

class TaskRunner:
    """任务调度器（回测模式使用）"""
    def __init__(self, context):
        self.daily_tasks = []
        self.weekly_tasks = []
        self.context = context

    def run_daily(self, time_str, task_func):
        """
        注册每日任务
        
        Args:
            time_str: 执行时间 HH:MM
            task_func: 任务函数
        """
        self.daily_tasks.append( (DailyTask(time_str), task_func) )
    
    def check_tasks(self, bar_time):
        """
        检查并执行到期任务
        
        Args:
            bar_time: 当前K线时间
        """
        for task, func in self.daily_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                task.last_executed = bar_time.date()
