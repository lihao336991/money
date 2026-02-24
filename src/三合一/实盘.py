#coding:gbk
"""
单纯一进二策略 - 迅投平台版
克隆自聚宽文章：https://www.joinquant.com/post/66882
标题：一进二v4.0--实盘策略
作者：财富369888

交易时间安排：
- 09:05: 选股准备 (get_stock_list)
- 09:25:41: 买入执行 (buy)
- 11:25: 上午卖出 (sell)
- 14:50: 下午卖出 (sell)
"""
import datetime
import time
import uuid
import math
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
g.gap_up = [] # 选出的股票池

# 账户配置
# MY_ACCOUNT = "19164901653"
MY_ACCOUNT = "170100005993"

# 企业微信Webhook配置
HOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2a336b4c-c38e-4ae3-9ff6-f14f175b4f73'

# -------------------------------------------------------------------------------------------
# 工具类和基础函数
# -------------------------------------------------------------------------------------------

class Messager:
    """企业微信消息推送类"""
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = False

    def set_is_test(self, is_test):
        self.is_test = is_test

    def send_message(self, text_content):
        if self.is_test:
            print(f"【消息推送(测试)】{text_content}")
            return
        try:
            current_time = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            content = current_time + text_content
            payload = {"msgtype": "text", "text": {"content": content}}
            requests.post(self.hook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"【消息推送失败】错误: {e}")

messager = Messager(HOOK)

def get_shifted_date(C, date, days, days_type='T'):
    """日期偏移函数"""
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
            trade_days = C.get_trading_dates(stockcode='SH', start_date=start_date, end_date=end_date, count=1000, period='1d')
            
            if not trade_days:
                return (base_date + datetime.timedelta(days=days)).strftime("%Y%m%d")
            
            date_str = base_date.strftime("%Y%m%d")
            if date_str in trade_days:
                index = trade_days.index(date_str)
            else:
                # 如果当前日期不是交易日，找前一个交易日
                found = False
                for i in range(1, 30):
                    prev_date = base_date - datetime.timedelta(days=i)
                    prev_str = prev_date.strftime("%Y%m%d")
                    if prev_str in trade_days:
                        index = trade_days.index(prev_str)
                        found = True
                        break
                if not found:
                    return (base_date + datetime.timedelta(days=days)).strftime("%Y%m%d")
            
            new_index = index + days
            new_index = max(0, min(new_index, len(trade_days) - 1))
            return trade_days[new_index]
        return date
    except Exception as e:
        print(f"【日期偏移】错误: {str(e)}")
        return date

def get_previous_trading_day(C, current_date):
    """获取前一个交易日"""
    current_str = current_date.strftime("%Y%m%d")
    prev_str = get_shifted_date(C, current_str, -1, 'T')
    return datetime.datetime.strptime(prev_str, "%Y%m%d").date()

def codeOfPosition(position):
    """从持仓对象获取股票代码"""
    return position.m_strInstrumentID + '.' + position.m_strExchangeID

def get_limit_of_stock(stock_code, last_close):
    """获取股票涨跌停价格"""
    if str(stock_code).startswith(tuple(['3', '688'])):
        return [round(last_close * 1.2, 2), round(last_close * 0.8, 2)]
    return [round(last_close * 1.1, 2), round(last_close * 0.9, 2)]

def get_account_money(C):        
    """获取账户可用资金"""
    accounts = get_trade_detail_data(C.account, 'stock', 'account')
    for dt in accounts:
        return dt.m_dAvailable
    return 0

def get_account_total_asset(C):
    """获取账户总资产"""
    available_cash = get_account_money(C)
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    total_market_value = sum(p.m_dMarketValue for p in positions)
    return available_cash + total_market_value

def open_position(C, security, value=0):
    """实盘模式下开仓"""
    print("买入股票(实盘):", security, C.get_stock_name(security), value)
    lastOrderId = str(uuid.uuid4())
    passorder(23, 1102, C.account, security, 5, -1, value, lastOrderId, 1, lastOrderId, C)

def close_position(C, stock):
    """平仓（清仓）"""
    if stock:
        if C.do_back_test:
            order_target_value(stock, 0, C, C.account)
        else:
            passorder(24, 1123, C.account, stock, 6, 1, 1, "卖出策略", 1, "", C)
        return True

def transform_date(date, date_type):
    """日期格式转换"""
    if isinstance(date, str):
        try:
            dt_date = datetime.datetime.strptime(date, '%Y-%m-%d')
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
    return {'str':str_date, 'dt':dt_date, 'd':d_date}[date_type]

def filter_new_stock(C, initial_list, date, days=50):
    """过滤新股"""
    d_date = transform_date(date, 'd')
    result = []
    for stock in initial_list:
        try:
            open_date_raw = C.get_open_date(stock)
            opendate = datetime.datetime.strptime(str(open_date_raw), "%Y%m%d")
            if d_date - opendate.date() > datetime.timedelta(days=days):
                result.append(stock)
        except:
            pass
    return result

def filter_st_paused_stock(C, initial_list):
    """过滤ST和停牌"""
    result = []
    for stock in initial_list:
        try:
            name = C.get_stock_name(stock)
            is_st = 'ST' in name or '*' in name or '退' in name
            is_paused = C.is_suspended_stock(stock)
            if not is_st and not is_paused:
                result.append(stock)
        except:
            pass
    return result

def filter_kcbj_stock(stock_list):
    """过滤科创板和北交所"""
    return [stock for stock in stock_list if stock[:2] in (('60','00','30'))]

def prepare_stock_list(C, date): 
    """准备初始股票池"""
    if g.cached_stock_list is not None and g.cached_stock_list_date == date:
        return g.cached_stock_list
    
    initial_list = C.get_stock_list_in_sector('沪深A股')
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_new_stock(C, initial_list, date)
    initial_list = filter_st_paused_stock(C, initial_list)
    
    g.cached_stock_list = initial_list
    g.cached_stock_list_date = date
    return initial_list

# -------------------------------------------------------------------------------------------
# 策略核心逻辑
# -------------------------------------------------------------------------------------------

def rise_low_volume(C, s):
    """
    判断股票上涨时是否未放量（左压情况）
    """
    try:
        data = C.get_market_data_ex(
            ['high', 'volume'], [s], period="1d", start_time='', end_time=C.yesterday,
            count=106, dividend_type="follow", fill_data=False, subscribe=False
        )
        if s not in data or len(data[s]) < 106:
            return False
        
        df = data[s]
        high_prices = df['high'].values[:102]
        prev_high = high_prices[-1]
        
        # 计算左压天数
        # 寻找最近一个大于等于prev_high的索引（倒序查找）
        # high_prices[-3::-1] 对应 聚宽 high_prices[-3::-1]
        # enumerate(..., 2)
        zyts_0 = 100
        # high_prices[-1] is index 101.
        # Check from 99 down to 0.
        # 聚宽逻辑: enumerate(high_prices[-3::-1], 2). high_prices[-3] is index 99.
        # If match at index i (0-based in slice), real index is 99-i.
        # Distance from end (101) is 101 - (99-i) = 2 + i. So enumerate starts at 2.
        
        # Python list slice logic:
        # high_prices = [0...101] (len 102)
        # prev_high = high_prices[101]
        # Check high_prices[99], [98], ...
        
        for i, high in enumerate(high_prices[-3::-1], 2):
            if high >= prev_high:
                zyts_0 = i - 1
                break
        
        zyts = zyts_0 + 5
        
        if zyts_0 < 20:
            threshold = 0.9
        elif zyts_0 < 50:
            threshold = 0.88
        else:
            threshold = 0.85
        
        # 判断当前成交量是否低于历史最大成交量的阈值
        # 聚宽: hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * threshold
        # QMT: df['volume'] includes today (index -1 is yesterday relative to strategy run time)
        # We need to check history window [-zyts : -1]
        
        volume_slice = df['volume'].iloc[-zyts:-1].values
        if len(volume_slice) == 0:
            return False
            
        if df['volume'].iloc[-1] <= np.max(volume_slice) * threshold:
            return True
        return False
    except Exception as e:
        print(f"【左压检查】{s} 异常: {e}")
        return False

def get_high_limit_factor(C, stocks_list, date, count=0, high_limit_factor=0):
    """
    涨停情绪因子计算（递归函数）
    """
    temp_num = len(stocks_list)
    if temp_num == 0:
        return math.log(high_limit_factor) if high_limit_factor > 0 else 0

    # 获取当日涨停股票
    # 批量获取数据
    # 需要获取 close 和 last_close (通过 count=2 获取 close of T and T-1)
    
    # 注意：date是字符串 YYYYMMDD
    try:
        data = C.get_market_data_ex(
            ['close'], stocks_list, period="1d", start_time='', end_time=date,
            count=2, dividend_type="follow", fill_data=False, subscribe=False
        )
        
        limit_up_stocks = []
        for s in data:
            df = data[s]
            if len(df) < 2:
                continue
            last_close = df['close'].iloc[0]
            close = df['close'].iloc[1]
            high_limit = get_limit_of_stock(s, last_close)[0]
            if round(close, 2) >= round(high_limit, 2):
                limit_up_stocks.append(s)
                
        # 终止条件：无涨停或递归超过3次
        if not limit_up_stocks or count > 2:
            return math.log(high_limit_factor) if high_limit_factor > 0 else 0
        
        # 递归计算前日数据
        last_day = get_shifted_date(C, date, -1, 'T')
        
        # 加权计算情绪因子（指数衰减加权）
        high_limit_factor += (2 ** count) * (len(limit_up_stocks) ** 2) / temp_num
        
        return get_high_limit_factor(C, limit_up_stocks, last_day, count + 1, high_limit_factor)
        
    except Exception as e:
        print(f"【情绪因子】计算异常: {e}")
        return 0

def get_priority_list(C, hl_list, date):
    """输出作为优先买入股票"""
    qualified_stocks = []
    
    print(f"【选股】开始筛选优先买入股票，候选池 {len(hl_list)} 只")
    
    # 批量获取前一日数据
    # 需要: close, volume, amount
    data = C.get_market_data_ex(
        ['close', 'volume', 'amount'], hl_list, period="1d", start_time='', end_time=date,
        count=1, dividend_type="follow", fill_data=False, subscribe=False
    )
    
    # 批量获取财务数据
    # turnover_ratio (换手率), market_cap (市值), circulating_market_cap (流通市值)
    # QMT get_valuation 对应 get_raw_financial_data? 
    # QMT没有直接的turnover_ratio历史数据接口，需要计算或查表
    # 聚宽: get_valuation fields=['turnover_ratio', 'market_cap','circulating_market_cap']
    # QMT: 股本表.总股本, 股本表.流通股本. 市值 = close * 总股本.
    
    try:
        financial_data = C.get_raw_financial_data(
            ['股本表.总股本', '股本表.流通股本'], hl_list, 
            get_shifted_date(C, date, -365, 'T'), date
        )
    except Exception as e:
        print(f"【对比日志】获取财务数据异常: {e}")
        financial_data = {}

    for s in hl_list:
        if s not in data or data[s].empty:
            print(f"【对比日志】过滤 {s}: 无前一日数据")
            continue
            
        df = data[s]
        close = df['close'].iloc[0]
        volume = df['volume'].iloc[0] * 100
        amount = df['amount'].iloc[0]
        
        # 计算均价涨幅
        # 聚宽: prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0] * 1.1 - 1
        if volume == 0 or close == 0:
            print(f"【对比日志】过滤 {s}: 成交量或收盘价为0")
            continue
            
        avg_price_increase_value = amount / volume / close * 1.1 - 1
        
        # 筛选涨幅在7%以上且成交额在5.5亿到20亿之间的股票
        if avg_price_increase_value < 0.07 or amount < 5.5e8 or amount > 20e8:
            print(f"【对比日志】过滤 {s}: 涨幅/成交额不符 (涨幅={avg_price_increase_value}, 成交额={amount/1e8:.2f}亿)")
            print(amount, volume, close)
            continue
            
        # 获取市值数据
        stock_num = 0
        circulating_stock_num = 0
        
        if s in financial_data:
            try:
                if '股本表.总股本' in financial_data[s]:
                    vals = list(financial_data[s]['股本表.总股本'].values())
                    if vals: stock_num = vals[-1]
                if '股本表.流通股本' in financial_data[s]:
                    vals = list(financial_data[s]['股本表.流通股本'].values())
                    if vals: circulating_stock_num = vals[-1]
            except:
                print(f"【对比日志】获取 {s} 财务数据异常")
                pass
        
        if stock_num == 0:
            print(f"【对比日志】过滤 {s}: 总股本为0")
            continue
            
        market_cap_val = close * stock_num / 1e8 # 亿
        circulating_market_cap_val = close * circulating_stock_num / 1e8 # 亿
        
        # 筛选市值在20亿以上且流通市值不超过520亿的股票
        # 注意：聚宽 market_cap 单位是亿元
        if market_cap_val < 20 or circulating_market_cap_val > 520:
            print(f"【对比日志】过滤 {s}: 市值不符 (总市值={market_cap_val:.2f}, 流通={circulating_market_cap_val:.2f})")
            continue
            
        # 检查是否有左压且成交量不足的情况
        if rise_low_volume(C, s):
            print(f"【对比日志】过滤 {s}: 左压缩量")
            continue
            
        qualified_stocks.append(s)
        
    log_msg = '可能买入target股票: ' + str([C.get_stock_name(stock) for stock in qualified_stocks])
    print(log_msg)
    print(f"【对比日志】最终入选: {len(qualified_stocks)}, 列表: {qualified_stocks}")
    messager.send_message(log_msg)
    
    return qualified_stocks

def get_stock_list_func(C): 
    """
    选股逻辑：筛选"一进二"模式的股票
    """
    # 获取前3个交易日日期
    date = C.yesterday # T
    date_1 = get_shifted_date(C, date, -1, 'T') # T-1
    date_2 = get_shifted_date(C, date, -2, 'T') # T-2
    
    print(f"【选股】日期: T={date}, T-1={date_1}, T-2={date_2}")

    # 准备初始股票池
    initial_list = prepare_stock_list(C, date)
    print(f"【对比日志】日期: {date}, 初始池: {len(initial_list)}")
    
    # 批量获取数据用于判断涨停
    # 我们需要 T, T-1, T-2 的涨停情况
    # T日涨停: Close(T) == Limit(T)
    # T-1日涨停: Close(T-1) == Limit(T-1)
    # T-2日涨停: Close(T-2) == Limit(T-2)
    
    # 批量获取最近3天数据 (T, T-1, T-2) + T-3 (for Limit calculation of T-2)
    # count=4: T, T-1, T-2, T-3
    data = C.get_market_data_ex(
        ['close'], initial_list, period="1d", start_time='', end_time=date,
        count=4, dividend_type="follow", fill_data=False, subscribe=False
    )
    
    hl0_list = [] # T日涨停
    hl1_list = [] # T-1曾涨停 (聚宽原代码 get_ever_hl_stock 是 "曾" 触及涨停)
    hl2_list = [] # T-2曾涨停
    
    # 聚宽 get_ever_hl_stock 用的是 high == high_limit
    # 所以我们需要 high 数据
    data_high = C.get_market_data_ex(
        ['high'], initial_list, period="1d", start_time='', end_time=date,
        count=4, dividend_type="follow", fill_data=False, subscribe=False
    )
    
    for s in initial_list:
        if s not in data or len(data[s]) < 4:
            continue
        
        closes = data[s]['close'].values
        highs = data_high[s]['high'].values
        
        # Data structure: [T-3, T-2, T-1, T]
        # Limit(T) needs Close(T-1)
        # Limit(T-1) needs Close(T-2)
        # Limit(T-2) needs Close(T-3)
        
        # T日涨停 (收盘价)
        limit_T = get_limit_of_stock(s, closes[-2])[0]
        if round(closes[-1], 2) == round(limit_T, 2):
            hl0_list.append(s)
            
        # T-1日曾涨停 (最高价)
        limit_T_1 = get_limit_of_stock(s, closes[-3])[0]
        if round(highs[-2], 2) == round(limit_T_1, 2):
            hl1_list.append(s)
            
        # T-2日曾涨停 (最高价)
        limit_T_2 = get_limit_of_stock(s, closes[-4])[0]
        if round(highs[-3], 2) == round(limit_T_2, 2):
            hl2_list.append(s)

        if '300490.SZ' in s:
            print(f"查看badcase的数据: {s}, {closes}, {highs}, {limit_T}, {limit_T_1}, {limit_T_2}")

    # 合并前两日涨停股票为集合，用于快速查找
    elements_to_remove = set(hl1_list + hl2_list)
    # 筛选出昨日涨停但前两日未涨停的股票（"一进二"模式）
    hl_list = [stock for stock in hl0_list if stock not in elements_to_remove]  

    print(f"【对比日志】T涨停: {len(hl0_list)}, T-1曾涨停: {len(hl1_list)}, T-2曾涨停: {len(hl2_list)}")
    print(f'hl0_list: {hl0_list}')
    print(f'hl1_list: {hl1_list}')
    print(f'hl2_list: {hl2_list}')
    
    print(f"【对比日志】一进二初选: {len(hl_list)}, 列表: {hl_list}")

    # 计算昨日涨停情绪因子
    yesterday_high_limit_factor = get_high_limit_factor(C, initial_list, date)
    # 计算前日涨停情绪因子
    last_high_limit_factor = get_high_limit_factor(C, initial_list, date_1)
    
    print(f"【对比日志】情绪因子: 昨日={yesterday_high_limit_factor:.4f}, 前日={last_high_limit_factor:.4f}")
    
    # 情绪退潮判断（昨日因子下降超过10%）
    # if last_high_limit_factor == 0 or yesterday_high_limit_factor / last_high_limit_factor < 0.9:
    #     g.gap_up = []
    #     message = "涨停情绪退潮，今日空仓"
    #     print(message)
    #     messager.send_message(message)
    #     return
    # else:
    #     # 输出作为优先买入股票
    #     g.gap_up = get_priority_list(C, hl_list, date)
    #     print(f"【选股】最终入选 {len(g.gap_up)} 只")
    #     return
    g.gap_up = get_priority_list(C, hl_list, date)
    messager.send_message(f"【选股】最终入选 {len(g.gap_up)} 只")
    
def buy_func(C):
    """
    买入逻辑：执行符合条件的"一进二"股票买入
    """
    messager.send_message("【买入执行】开始...")
    qualified_stocks = []  # 符合条件的股票列表

    date_now = C.today.strftime("%Y%m%d")
    
    # 遍历"一进二"股票池
    for s in g.gap_up:
        # 获取前一日数据
        prev_day_data = C.get_market_data_ex(
            ['close', 'volume'], [s], period="1d", start_time='', end_time=C.yesterday,
            count=1, dividend_type="follow", fill_data=False, subscribe=False
        )
        if s not in prev_day_data or prev_day_data[s].empty:
            continue
        prev_vol = prev_day_data[s]['volume'].iloc[0]
        prev_close = prev_day_data[s]['close'].iloc[0]
        
        # 获取集合竞价数据 (09:15-09:25)
        # QMT get_market_data_ex period='1m' start=092500 end=092500 doesn't give auction summary well.
        # But we can try getting the first bar of the day or tick.
        # 策略运行时间 09:25:41. 此时已有集合竞价结束的数据。
        # 取当日第1分钟数据，或者 tick.
        
        auction_data = C.get_market_data_ex(
            ['open', 'volume', 'amount'], [s], period="1d", start_time=date_now, end_time=date_now,
            count=1, dividend_type="follow", fill_data=False, subscribe=False
        )
        # 注意: 9:25:41时，1d数据的 open, volume 是集合竞价的.
        
        if s not in auction_data or auction_data[s].empty:
            continue
            
        auc_vol = auction_data[s]['volume'].iloc[0]
        auc_current = auction_data[s]['open'].iloc[0] # current price is open

        print(f"【debug】{s}, 前一日成交量={prev_vol}, 集合竞价成交量={auc_vol}, 集合竞价开盘价={auc_current}，竞价量比={auc_vol/prev_vol:.2%}")
        
        # 筛选集合竞价成交量大于前一日成交量4%的股票
        if prev_vol == 0 or auc_vol / prev_vol < 0.04:
            print(f"【买入过滤】{s} 竞价量比不足: {auc_vol/prev_vol:.2%}")
            continue

        # 计算当前价格相对于涨停板价格的比率
        # 原策略: current_ratio = auction_data['current'][0] / (current_data[s].high_limit / 1.1)
        # high_limit / 1.1 大概是 昨日收盘价.
        # 所以 ratio = current / last_close.
        
        current_ratio = auc_current / prev_close
        
        # 筛选开盘涨幅在0-6%之间的股票
        # 原策略: if current_ratio <= 1 or current_ratio >= 1.06: continue
        if current_ratio <= 1 or current_ratio >= 1.06:
            print(f"【买入过滤】{s} 开盘涨幅不符: {current_ratio:.2%}")
            continue
            
        qualified_stocks.append(s)

    # 执行买入操作
    total_asset = get_account_total_asset(C)
    available_cash = get_account_money(C) - 500 # 预留500元作为buffer
    
    if len(qualified_stocks) != 0 and available_cash / total_asset > 0.3:
        # 计算每只股票分配的资金
        value = available_cash / len(qualified_stocks)
        
        # 获取当前最新价用于计算是否够买100股
        # 使用 auction open price 近似
        
        for s in qualified_stocks:
            # 获取最新tick price
            tick = C.get_market_data_ex(['close'], [s], period="1m", count=1)
            if s in tick and not tick[s].empty:
                current_price = tick[s]['close'].iloc[0]
            else:
                continue
                
            # 确保有足够资金买入至少100股
            if available_cash / current_price > 100:
                if C.do_back_test:
                     order_target_value(s, value, C, C.account)
                else:
                     open_position(C, s, value)
                
                print('买入' + s)
                messager.send_message(f"买入 {s} {C.get_stock_name(s)}, 价格={current_price:.2f}, 金额={value:.2f}")
                print('———————————————————————————————————')

def sell_func(C):
    """
    卖出股票逻辑
    """
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    if not positions:
        return
        
    print("【卖出执行】检查持仓...")
    for position in positions:
        s = codeOfPosition(position)
        if position.m_nCanUseVolume <= 0:
            continue
            
        # 获取当前价格和涨停价
        tick = C.get_market_data_ex(['close'], [s], period="1m", count=1)
        if s not in tick or tick[s].empty:
            continue
        last_price = tick[s]['close'].iloc[0]
        
        # 获取昨日收盘价计算涨停价
        day_data = C.get_market_data_ex(['close'], [s], period="1d", count=2, end_time=C.yesterday)
        if s not in day_data or len(day_data[s]) < 1:
             continue
        last_close = day_data[s]['close'].iloc[-1]
        
        high_limit = get_limit_of_stock(s, last_close)[0]
        
        # 条件：有可卖数量、未涨停
        # 原策略: if last_price < high_limit: sell
        if round(last_price, 2) < round(high_limit, 2):
            close_position(C, s)
            print('止损止盈卖出', [s, C.get_stock_name(s)])
            messager.send_message(f"止损止盈卖出 {s} {C.get_stock_name(s)}")
            print('———————————————————————————————————')

def print_holdings_func(C):
    """收盘后打印持仓信息函数"""
    print("【收盘持仓】开始打印持仓信息...")
    positions = get_trade_detail_data(C.account, 'STOCK', 'POSITION')
    total_asset = get_account_total_asset(C)
    available_cash = get_account_money(C)
    
    msg = f"今日收盘总资产: {total_asset:.2f}元"
    if positions:
        holdings = [f"{codeOfPosition(p)} {C.get_stock_name(codeOfPosition(p))}" for p in positions]
        msg += f", 持仓: {', '.join(holdings)}"
    else:
        msg += ", 无持仓"
    
    print(msg)
    messager.send_message(msg)

# -------------------------------------------------------------------------------------------
# 调度与初始化
# -------------------------------------------------------------------------------------------

class ScheduledTask:
    """定时任务基类"""
    def __init__(self, execution_time):
        self.last_executed = None
        self.execution_time = self._parse_time(execution_time)
    def _parse_time(self, time_str):
        try:
            return datetime.datetime.strptime(time_str, "%H:%M:%S").time()
        except ValueError:
            try:
                return datetime.datetime.strptime(time_str, "%H:%M").time()
            except ValueError:
                 raise ValueError("Invalid time format")

class DailyTask(ScheduledTask):
    """每日定时任务"""
    def should_trigger(self, current_dt):
        should1 = current_dt.time() >= self.execution_time
        should2 = self.last_executed != current_dt.date()
        return should1 and should2

class TaskRunner:
    """任务调度器（回测模式使用）"""
    def __init__(self, context):
        self.daily_tasks = []
        self.context = context
    def run_daily(self, time_str, task_func):
        self.daily_tasks.append( (DailyTask(time_str), task_func) )
    def check_tasks(self, bar_time):
        for task, func in self.daily_tasks:
            if task.should_trigger(bar_time):
                func(self.context)
                task.last_executed = bar_time.date()

def init(C):
    """初始化函数"""
    print("【初始化】策略开始初始化...")
    C.account = MY_ACCOUNT
    C.set_account(C.account)
    C.runner = TaskRunner(C)
    messager.set_is_test(C.do_back_test)
    C.currentTime = 0
    
    if not C.do_back_test:
        currentTime = time.time() * 1000 + 8 * 3600 * 1000
        C.currentTime = currentTime
        C.today = pd.to_datetime(currentTime, unit='ms')
        C.yesterday = get_previous_trading_day(C, C.today.date()).strftime("%Y%m%d")

    # 周末检查
    if not C.do_back_test and datetime.datetime.now().weekday() >= 5:
        print('当前日期为周末，不执行任务')
        return
    
    if C.do_back_test:
        C.runner.run_daily("09:05", get_stock_list_func)
        C.runner.run_daily("09:26", buy_func) # 09:25:41 round to 09:26 for backtest bars
        C.runner.run_daily("11:25", sell_func)
        C.runner.run_daily("14:50", sell_func)
        C.runner.run_daily("15:00", print_holdings_func)
    else:
        # 实盘时间
        C.run_time("get_stock_list_func", "1nDay", "2025-03-01 09:05:00", "SH")
        C.run_time("buy_func", "1nDay", "2025-03-01 09:25:41", "SH")
        C.run_time("sell_func", "1nDay", "2025-03-01 11:25:00", "SH")
        C.run_time("sell_func", "1nDay", "2025-03-01 14:50:00", "SH")
        C.run_time("print_holdings_func", "1nDay", "2025-03-01 15:00:00", "SH")
        
    print("【初始化】策略初始化完成")

def handlebar(C):
    """K线处理函数（回测模式使用）"""
    index = C.barpos
    currentTime = C.get_bar_timetag(index) + 8 * 3600 * 1000
    try:
        if C.currentTime < currentTime:
            C.currentTime = currentTime
            C.today = pd.to_datetime(currentTime, unit='ms')
    except Exception as e:
        pass

    if (datetime.datetime.now() - datetime.timedelta(days=1) > C.today) and not C.do_back_test:
        return
    else:
        if C.do_back_test:
            C.yesterday = get_previous_trading_day(C, C.today.date()).strftime("%Y%m%d")
            C.runner.check_tasks(C.today)
