#coding:gbk
import pandas as pd
import numpy as np
import datetime
import time
import uuid

# 全局状态存储器
class G():pass
g = G()
g.stock_num = 4


def init(ContextInfo):
    # account = '620000369618'
    # 李浩 股票账户
    ContextInfo.account = '620000204906'
    ContextInfo.set_account(ContextInfo.account)
    ContextInfo.runner = TaskRunner(ContextInfo)
    
    # 定时任务设定
    if ContextInfo.do_back_test:
        print('doing test')
        ContextInfo.runner.run_daily("9:30", prepare)
        ContextInfo.runner.run_daily("9:31", buy)
        ContextInfo.runner.run_daily("13:00", sell)
        
    else:
        ContextInfo.run_time("prepare","1nDay","2025-08-01 09:20:00","SH")
        ContextInfo.run_time("buy","1nDay","2025-08-01 09:30:03","SH")
        ContextInfo.run_time("sell","1nDay","2025-08-01 13:00:00","SH")
        ContextInfo.run_time("sell","1nDay","2025-08-01 14:55:00","SH")
        ContextInfo.run_time("sell","1nDay","2025-08-01 14:30:00","SH")

def handlebar(ContextInfo):
    # 新增属性，快捷获取当前日期
    index = ContextInfo.barpos
    currentTime = ContextInfo.get_bar_timetag(index) + 8 * 3600 * 1000
    ContextInfo.currentTime = currentTime
    ContextInfo.today = pd.to_datetime(currentTime, unit='ms')

    if (datetime.datetime.now() - datetime.timedelta(days=1) > ContextInfo.today) and not ContextInfo.do_back_test:
        # print('非回测模式，历史不处理')
        return
    else:
        # 检查并执行任务
        ContextInfo.runner.check_tasks(ContextInfo.today)
        
def prepare(ContextInfo):
    print('prepare', ContextInfo.today)
    # 初始化-获取A股所有股票
    allStocks = ContextInfo.get_stock_list_in_sector('沪深A股')
    if not allStocks:
        print("未获取到沪深A股股票列表")
        return
        
    ContextInfo.set_universe(allStocks)
    
    # 筛选ST
    stock_info = {}
    stStockList = [] 
    for code in allStocks:
        try:
            name = ContextInfo.get_stock_name(code)
            stock_info[code] = name
            # 筛选名字中包含ST或st的股票（仅存代码）
            if 'ST' in name.upper() and not any(p in code for p in ['300', '688', '301']):
                stStockList.append(code) 
        except Exception as e:
            print(f"获取股票 {code} 名称失败：{e}")
    
    # 国九条筛选
    GJTFliterStockList = []
    for stock_code in stStockList:
        if today_is_between(ContextInfo):
            if GJT_filter_stocks(ContextInfo, stock_code):
                GJTFliterStockList.append(stock_code)
        else:
            GJTFliterStockList.append(stock_code)
    
    # 技术指标筛选-# 多头排列，未曾跌停，10日线上方，放量，成交量未暴增，股价>1
    fStockList = []
    if GJTFliterStockList:
        fStockList = filter_stocks(ContextInfo, GJTFliterStockList)
    else:
        print("\n国九条筛选后无股票，跳过技术筛选")
        
    # RZQ筛选- 昨日不涨停且前日涨停
    rzqStockList = []    
    if fStockList:

        rzqStockList = rzq_list_new(ContextInfo, fStockList)
        # rzqStockList = rzq_list(ContextInfo, fStockList)
        for code in rzqStockList:
            print(f"股票代码：{code}，通过RZQ验证")
    else:
        print("\nRZQ筛选前无股票，跳过RZQ筛选")
    
    # 换手率-倒序排一下，优先换手率高的
    resultStockList = []
    g.today_list = []
    if rzqStockList:
        # 获取当前日期作为换手率筛选的基准日
        index = ContextInfo.barpos if hasattr(ContextInfo, 'barpos') else 0
        bar_timetag = ContextInfo.get_bar_timetag(index)
        if bar_timetag:
            current_dt = datetime.datetime.fromtimestamp(bar_timetag / 1000)
            target_date = current_dt.strftime("%Y%m%d")
            
            # 调用换手率筛选函数
            resultStockList = get_turnover_stocks(ContextInfo, rzqStockList, target_date)
            g.today_list = resultStockList
            # 输出最终筛选结果
            print(f"【最终结果】经过所有筛选后剩余 {len(resultStockList)} 只股票")
            # 将股票代码转换为股票名称后显示
            stock_names = [ContextInfo.get_stock_name(stock) for stock in resultStockList]
            print(f"【最终结果】{stock_names}")
        else:
            print("【换手率筛选】无法获取当前日期，跳过筛选")
    else:
        print("\nRZQ筛选后无股票，跳过换手率筛选")
        

# 以下为原有函数（保持不变）
def GJT_filter_stocks(ContextInfo, stockCode):
    """国九条筛选函数，确保股票代码为字符串"""
    # 确保stockCode是字符串（防御性处理）
    if isinstance(stockCode, tuple):
        stock_str = str(stockCode[0]) if stockCode else ""
    else:
        stock_str = str(stockCode)
    if not stock_str:
        print("股票代码为空，跳过筛选")
        return False
    
    try:
        # 获取当前K线时间戳
        index = ContextInfo.barpos if hasattr(ContextInfo, 'barpos') else 0
        bar_timetag = ContextInfo.get_bar_timetag(index)
        if bar_timetag is None:
            print(f"股票 {stock_str} 无法获取K线时间戳，跳过")
            return False
        
        # 时间转换（毫秒→datetime→字符串）
        current_dt = datetime.datetime.fromtimestamp(bar_timetag / 1000)
        start_dt = current_dt - datetime.timedelta(days=365)
        startDate = start_dt.strftime("%Y%m%d")
        endDate = current_dt.strftime("%Y%m%d")
        
        # 财务字段列表（带表名前缀，匹配接口要求）
        fieldList = [
            'ASHAREINCOME.net_profit_excl_min_int_inc',  # 归母净利润
            'ASHAREINCOME.net_profit_incl_min_int_inc',  # 净利润
            'ASHAREINCOME.revenue',                      # 营业收入
            'PERSHAREINDEX.equity_roe',                  # ROE
            'PERSHAREINDEX.total_roe',                   # ROA
            'ASHAREBALANCESHEET.tot_shrhldr_eqy_excl_min_int',  # 股东权益
            'ASHAREINCOME.m_timetag'                     # 报告时间
        ]
        
        # 获取财务数据
        data = ContextInfo.get_financial_data(
            fieldList,
            [stock_str],  # 股票代码列表（字符串元素）
            startDate,
            endDate,
        )
        
        # 检查数据有效性
        if data is None or data.empty:
            print(f"【国九条筛选】{stock_str} 无近一年财务数据")
            return False
        
        # 取最新报告数据
        data = data.sort_values('m_timetag')
        latest_data = data.iloc[-1]
        
        # 国九条条件判断
        pass_all = (
            latest_data['net_profit_excl_min_int_inc'] > 0 and
            latest_data['net_profit_incl_min_int_inc'] > 0 and
            latest_data['revenue'] > 1e8 and
            latest_data['equity_roe'] > 0 and
            latest_data['total_roe'] > 0 and
            latest_data['tot_shrhldr_eqy_excl_min_int'] > 0
        )
        
        return pass_all
        
    except KeyError as e:
        print(f"【国九条筛选】{stock_str} 缺少字段：{e}")
        return False
    except Exception as e:
        return False

def get_relative_position_df(ContextInfo, stock_list, date, watch_days, ratio):
    # 保持原有逻辑不变
    stock_list = [str(code) for code in stock_list if code]
    if not stock_list:
        print("【相对位置筛选】股票列表为空，直接返回空结果")
        return []
    
    try:
        market_data = ContextInfo.get_market_data_ex(
            fields=['high', 'low', 'close', 'code', 'time'],
            stock_code=stock_list,
            period='1d',
            start_time='',
            end_time=date,
            count=watch_days,
            fill_data=False,
            subscribe=False
        )
        
        dfs = []
        for code, df in market_data.items():
            if df.empty:
                print(f"【相对对位置筛选筛选】股票 {code} 无近{watch_days}天数据，跳过")
                continue
            df = df.copy()
            
            if pd.api.types.is_integer_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
            else:
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
            
            if df['time'].isna().any():
                print(f"【相对位置筛选】{code} 存在无效时间，已过滤")
                df = df.dropna(subset=['time'])
            
            df.loc[:, 'code'] = code
            dfs.append(df)
        if not dfs:
            print("【相对位置筛选筛选】所有股票均无有效数据，返回空结果")
            return []
        df = pd.concat(dfs, ignore_index=True)
        
        grouped = df.groupby('code')
        result_list = []
        for code, group in grouped:
            group_sorted = group.sort_values('time')
            close = group_sorted['close'].iloc[-1]
            high = group_sorted['high'].max()
            low = group_sorted['low'].min()
            
            if high - low < 1e-6:
                rp = 0.0
            else:
                rp = (close - low) / (high - low)
            
            result_list.append({'code': code, 'rp': rp})
        
        result_df = pd.DataFrame(result_list).set_index('code')
        pass_stocks = result_df[result_df['rp'] >= ratio].index.tolist()
        return pass_stocks
        
    except Exception as e:
        print(f"【相对对位置筛选】错误: {str(e)}")
        return []


def filter_stocks(ContextInfo, stocks):
    stocks = [str(code) for code in stocks if code]
    if not stocks:
        print("【技术指标筛选】输入股票列表为空")
        return []
    
    try:
        index = ContextInfo.barpos if hasattr(ContextInfo, 'barpos') else 0
        bar_timetag = ContextInfo.get_bar_timetag(index)
        if bar_timetag is None:
            print("【技术指标筛选】无法获取K线时间戳")
            return []
        
        current_dt = datetime.datetime.fromtimestamp(bar_timetag / 1000)
        yesterday_dt = get_previous_trading_day(ContextInfo, current_dt.date())
        yesterday = yesterday_dt.strftime("%Y%m%d")
        
        stocks = get_relative_position_df(ContextInfo, stocks, yesterday, 20, 0.6)
        if not stocks:
            print("【技术指标筛选】相对位置筛选后无股票，终止筛选")
            return []
        
        market_data = ContextInfo.get_market_data_ex(
            fields=['close', 'low', 'volume', 'low_limit', 'code', 'time'],
            stock_code=stocks,
            period='1d',
            start_time='',
            end_time=yesterday,
            count=11,
            fill_data=False,
            subscribe=False
        )
        
        dfs = []
        for code, df in market_data.items():
            if df.empty:
                print(f"【技术指标筛选】{code} 无11天数据，排除")
                continue
            
            df = df.copy()
            
            try:
                if pd.api.types.is_integer_dtype(df['time']):
                    df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
                else:
                    df['time'] = pd.to_datetime(df['time'], errors='coerce')
                
                invalid_count = df['time'].isna().sum()
                if invalid_count > 0:
                    print(f"【技术指标筛选】{code} 存在 {invalid_count} 条无效时间，已过滤")
                    df = df.dropna(subset=['time'])
                
                df.loc[:, 'time'] = df['time'].dt.strftime("%Y%m%d")
                
                # 计算前一交易日收盘价，并生成跌停价（收盘价×0.95）
                df = df.sort_values('time')
                df['prev_close'] = df['close'].shift(1)  # 前一交易日收盘价
                df['calculated_low_limit'] = df['prev_close'] * 0.95  # 计算跌停价
                
            except Exception as e:
                print(f"【技术指标筛选】{code} 时间转换失败：{e}，跳过该股票")
                continue
            
            df.loc[:, 'code'] = code
            dfs.append(df)
        
        if not dfs:
            print("\n【技术指标筛选】所有股票均无有效数据，返回空结果")
            return []
        df = pd.concat(dfs, ignore_index=True).reset_index(drop=True)
        
        grouped = df.groupby('code')
        valid_stocks = []
        for code, group in grouped:
            if len(group) < 11:
                print(f"【技术指标筛选】{code} 数据不足11天，排除")
                continue
            
            group = group.sort_values('time').copy()
            sorted_times = group['time'].tolist()
            
            try:
                group.loc[:, 'close'] = pd.to_numeric(group['close'], errors='coerce')
                group.loc[:, 'ma10'] = group['close'].rolling(window=10, min_periods=10).mean()
                group.loc[:, 'prev_low'] = pd.to_numeric(group['low'], errors='coerce').shift(1)
                group.loc[:, 'prev_volume'] = pd.to_numeric(group['volume'], errors='coerce').shift(1)
                # 使用计算出的跌停价替代原始low_limit
                group.loc[:, 'low_limit'] = group['calculated_low_limit']
            except Exception as e:
                print(f"【技术指标筛选】{code} 指标计算错误：{e}，排除")
                continue
            
            mask = group['time'] == yesterday
            match_count = mask.sum()
            
            if match_count == 0:
                valid_times = [t for t in sorted_times if t < yesterday]
                if not valid_times:
                    print(f"【技术指标筛选】{code} 无有效历史数据，排除")
                    continue
                closest_time = max(valid_times)
                mask = group['time'] == closest_time
                latest = group[mask].iloc[0]
            else:
                latest = group[mask].iloc[0]
            
            # 检查关键缺失值
            critical_nan_fields = [col for col in ['close', 'prev_low', 'ma10', 'volume', 'prev_volume', 'low_limit'] 
                                 if pd.isna(latest[col])]
            
            if critical_nan_fields:
                print(f"【技术指标筛选】{code} 存在关键缺失值字段：{critical_nan_fields}，排除")
                continue
            
            # 条件判断
            cond1 = latest['close'] > latest['prev_low']
            cond2 = latest['close'] > latest['low_limit']  # 使用计算出的跌停价
            cond3 = latest['close'] > latest['ma10']
            cond4 = latest['volume'] < 10 * latest['prev_volume'] if latest['prev_volume'] > 0 else True
            cond5 = latest['close'] > 1
            
            if all([cond1, cond2, cond3, cond4, cond5]):
                valid_stocks.append(code)
        
        return valid_stocks
        
    except Exception as e:
        print(f"【技术指标筛选】错误：{str(e)}")
        return []


def get_shifted_date(ContextInfo, date, days, days_type='T'):
    """获取偏移后的日期（适配iQuant平台）"""
    try:
        # 转换输入日期为datetime对象
        base_date = datetime.datetime.strptime(str(date), "%Y%m%d")
        
        # 自然日偏移
        if days_type == 'N':
            shifted_date = base_date + datetime.timedelta(days=days)
            result = shifted_date.strftime("%Y%m%d")
            return result
        
        # 交易日偏移（适配iQuant的get_trading_dates接口）
        elif days_type == 'T':
            # 通过沪市代码获取市场交易日列表
            start_date = (base_date - datetime.timedelta(days=365)).strftime("%Y%m%d")
            end_date = (base_date + datetime.timedelta(days=abs(days) + 365)).strftime("%Y%m%d")
            
            # 调用iQuant接口获取交易日列表
            trade_days = ContextInfo.get_trading_dates(
                stockcode='SH',  # 使用沪市市场代码获取交易日历
                start_date=start_date,
                end_date=end_date,
                count=1000,
                period='1d'
            )
            # 处理接口调用失败的情况
            if not trade_days:
                print("【日期偏移】无法获取交易日列表，使用自然日偏移")
                shifted_date = base_date + datetime.timedelta(days=days)
                return shifted_date.strftime("%Y%m%d")
            
            # 查找基准日期在交易日列表中的位置
            date_str = base_date.strftime("%Y%m%d")
            if date_str in trade_days:
                index = trade_days.index(date_str)
            else:
                # 找到基准日期之前最近的交易日（最多向前找30天）
                found = False
                for i in range(1, 30):
                    prev_date = base_date - datetime.timedelta(days=i)
                    prev_str = prev_date.strftime("%Y%m%d")
                    if prev_str in trade_days:
                        index = trade_days.index(prev_str)
                        found = True
                        break
                if not found:
                    print(f"【日期偏移】未找到有效交易日，使用自然日偏移")
                    shifted_date = base_date + datetime.timedelta(days=days)
                    return shifted_date.strftime("%Y%m%d")
            
            # 计算偏移后的索引并处理边界情况
            new_index = index + days
            if new_index < 0:
                new_index = 0
            elif new_index >= len(trade_days):
                new_index = len(trade_days) - 1
            
            result = trade_days[new_index]
            return result
        
        else:
            print(f"【日期偏移】不支持的偏移类型: {days_type}")
            return date
            
    except Exception as e:
        print(f"【日期偏移】错误: {str(e)}")
        return date


def get_previous_trading_day(ContextInfo, current_date):
    """获取前一个交易日（基于iQuant接口实现）"""
    try:
        current_str = current_date.strftime("%Y%m%d")
        # 直接调用适配后的交易日偏移函数
        prev_str = get_shifted_date(ContextInfo, current_str, -1, 'T')
        return datetime.datetime.strptime(prev_str, "%Y%m%d").date()
    except Exception as e:
        print(f"【get_previous_trading_day】错误: {str(e)}")
        # 出错时的备用逻辑
        delta = 1
        while True:
            prev_date = current_date - datetime.timedelta(days=delta)
            if prev_date.weekday() < 5:  # 仅排除周末，不考虑节假日
                return prev_date
            delta += 1
            if delta > 30:
                print(f"【交易日判断】备用逻辑 - 超过30天未找到交易日")
                return current_date - datetime.timedelta(days=30)

def rzq_list_new(ContextInfo, initial_list):
    # 查询持仓昨日信息
    ticksOfDay = ContextInfo.get_market_data_ex(
        [],                
        initial_list,
        period="1d",
        start_time = (ContextInfo.today - datetime.timedelta(days=14)).strftime('%Y%m%d'),
        end_time = ContextInfo.today.strftime('%Y%m%d'),
        count=4, # 取日线4根k线，大前天，前天，昨天，今天。足以判断前两天的涨停情况
        dividend_type = "follow",
        fill_data = True,
        subscribe = False
    )
    target = []
    for stock in initial_list:
        print(stock, '单个股票的day data', ticksOfDay[stock])
        # 昨日收盘价
        lastClose = ticksOfDay[stock]["close"].iloc[-2]
        # 前日收盘价
        last2dClose = ticksOfDay[stock]["close"].iloc[-3]
        # 大前日收盘价
        last3dClose = ticksOfDay[stock]["close"].iloc[-4]
        # 前天涨停价
        last2dHighLimit = get_limit_of_stock(last3dClose)[0]
        # 昨天涨停价
        last1dHighLimit = get_limit_of_stock(last2dClose)[0]

        last1dIsHL = lastClose >= last1dHighLimit
        last2dIsHL = last2dClose >= last2dHighLimit
        if last2dIsHL and not last1dIsHL:
            target.append(stock)

        print('前日收盘价', last2dClose)
        print('昨日收盘价', lastClose)
        print('大前日收盘价', last3dClose)
        print('前天涨停价', last2dHighLimit)
        print('昨天涨停价', last1dHighLimit)
        print('昨日是否涨停', last1dIsHL)
        print('前日是否涨停', last2dIsHL)
    return target



def rzq_list(ContextInfo, initial_list):
    """筛选昨日不涨停但前日涨停的股票"""
    try:
        # 获取当前K线时间戳并转换为日期
        index = ContextInfo.barpos if hasattr(ContextInfo, 'barpos') else 0
        bar_timetag = ContextInfo.get_bar_timetag(index)
        if bar_timetag is None:
            print("【rzq_list】无法获取K线时间戳，返回空列表")
            return []
        
        current_dt = datetime.datetime.fromtimestamp(bar_timetag / 1000)
        yesterday_str = current_dt.strftime("%Y%m%d")
        
        # 计算前日和大前日（交易日）
        date_1 = get_shifted_date(ContextInfo, yesterday_str, -1, 'T')  # 前日
        date_2 = get_shifted_date(ContextInfo, yesterday_str, -2, 'T')  # 大前日
        
        # 筛选昨日不涨停的股票
        h1_list = get_ever_hl_stock(ContextInfo, initial_list, yesterday_str)
        
        # 筛选前日涨停的股票
        hl_stocks = get_hl_stock(ContextInfo, initial_list, date_1)
        
        # 取交集：昨日不涨停且前日涨停
        result_list = [stock for stock in h1_list if stock in hl_stocks]
        
        return result_list
        
    except Exception as e:
        print(f"【rzq_list】错误: {str(e)}")
        return []


def get_ever_hl_stock(ContextInfo, stock_list, date):
    """筛选在指定日期不涨停的股票"""
    try:
        if not stock_list:
            print("【get_ever_hl_stock】股票列表为空")
            return []
        
        # 获取指定日期及前一交易日的行情数据
        prev_trading_day = get_shifted_date(ContextInfo, date, -1, 'T')
        
        market_data = ContextInfo.get_market_data_ex(
            fields=['close', 'code', 'time'],
            stock_code=stock_list,
            period='1d',
            start_time=prev_trading_day,
            end_time=date,
            count=2,
            fill_data=False,
            subscribe=False
        )

        # 筛选不涨停的股票
        result = []
        for code, df in market_data.items():
            if df.empty or len(df) < 2:
                print(f"【get_ever_hl_stock】{code} 缺少足够数据，跳过")
                continue
                
            try:
                # 排序确保日期正确
                df = df.sort_values('time')
                
                # 前一交易日收盘价
                prev_close = df['close'].iloc[0]
                # 指定日期收盘价
                current_close = df['close'].iloc[1]
                
                # 计算涨停价（前一交易日收盘价 * 1.05）
                high_limit = prev_close * 1.05
                
                # 考虑浮点精度问题
                if not np.isclose(current_close, high_limit, atol=0.01):
                    result.append(code)
            except Exception as e:
                print(f"【get_ever_hl_stock】处理 {code} 时出错: {e}")
        
        return result
        
    except Exception as e:
        print(f"【get_ever_hl_stock】错误: {str(e)}")
        return []


def get_hl_stock(ContextInfo, stock_list, date):
    """筛选在指定日期涨停的股票"""
    try:
        if not stock_list:
            print("【get_hl_stock】股票列表为空")
            return []
        
        # 获取指定日期及前一交易日的行情数据
        prev_trading_day = get_shifted_date(ContextInfo, date, -1, 'T')
        
        market_data = ContextInfo.get_market_data_ex(
            fields=['close', 'code', 'time'],
            stock_code=stock_list,
            period='1d',
            start_time=prev_trading_day,
            end_time=date,
            count=2,
            fill_data=False,
            subscribe=False
        )

        # 筛选涨停的股票
        result = []
        for code, df in market_data.items():
            if df.empty or len(df) < 2:
                print(f"【get_hl_stock】{code} 缺少足够数据，跳过")
                continue
                
            try:
                # 排序确保日期正确
                df = df.sort_values('time')
                
                # 前一交易日收盘价
                prev_close = df['close'].iloc[0]
                # 指定日期收盘价
                current_close = df['close'].iloc[1]
                
                # 计算涨停价（前一交易日收盘价 * 1.05）
                high_limit = prev_close * 1.05
                
                # 考虑浮点精度问题
                if np.isclose(current_close, high_limit, atol=0.01):
                    result.append(code)
            except Exception as e:
                print(f"【get_hl_stock】处理 {code} 时出错: {e}")
        
        return result
        
    except Exception as e:
        print(f"【get_hl_stock】错误: {str(e)}")
        return []
    

def get_turnover_stocks(ContextInfo, stk_list, date):
    """获取指定日期的换手率数据并按换手率降序排列"""
    try:
        if not stk_list:
            print("【换手率筛选】股票列表为空，返回空结果")
            return []
        
        # 获取指定日期的换手率数据
        turnover_data = ContextInfo.get_market_data_ex(
            fields=['code', 'time', 'turnover_ratio'],  # 换手率字段
            stock_code=stk_list,
            period='1d',
            start_time=date,
            end_time=date,
            count=1,  # 只获取1天数据
            fill_data=False,
            subscribe=False
        )
        
        # 处理返回的数据
        dfs = []
        for code, df in turnover_data.items():
            if df.empty:
                print(f"【换手率筛选】{code} 无指定日期换手率数据，跳过")
                continue
            
            df = df.copy()
            # 转换时间格式
            if pd.api.types.is_integer_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
            else:
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
            
            df.loc[:, 'code'] = code
            dfs.append(df)
        
        if not dfs:
            print("【换手率筛选】所有股票均无有效换手率数据")
            return []
        
        # 合并数据并排序
        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values(by='turnover_ratio', ascending=False)
        
        # 提取股票代码列表
        result_list = list(df['code'].unique())
        
        print(f"【换手率筛选】股池数 {len(result_list)}")
        print(f"【换手率筛选】股池 {result_list}")
        
        return result_list
        
    except Exception as e:
        print(f"【换手率筛选】错误: {str(e)}")
        return []

def codeOfPosition(position):
    return position.m_strInstrumentID + '.' + position.m_strExchangeID

# 根据股票代码和收盘价，计算次日涨跌停价格
def get_limit_of_stock(last_close):
    return [round(last_close * 1.05, 2), round(last_close * 0.95), 2]

# 买入函数
def buy(ContextInfo):
    positions = get_trade_detail_data(ContextInfo.account, 'STOCK', 'POSITION')
    hold_list = [codeOfPosition(position) for position in positions if position.m_dMarketValue > 1000]
    today_list=g.today_list
    if len(today_list)==0:
        return
    ticksOfDay = ContextInfo.get_market_data_ex(
        [],                
        today_list,
        period="1d",
        start_time = (ContextInfo.today - datetime.timedelta(days=1)).strftime('%Y%m%d'),
        end_time = ContextInfo.today.strftime('%Y%m%d'),
        count=1,
        dividend_type = "follow",
        fill_data = True,
        subscribe = False
    )
    ticksData = ContextInfo.get_market_data_ex(
        [],
        today_list,
        period="1m",
        start_time = (ContextInfo.today - datetime.timedelta(days=1)).strftime('%Y%m%d%H%M%S'),
        end_time = ContextInfo.today.strftime('%Y%m%d%H%M%S'),
        count=1,
        dividend_type = "follow",
        fill_data = True,
        subscribe = True
    )
    
    target = []
    # 遍历股票代码进行双数据源校验
    for stock in today_list:
        try:
            # 获取tick数据和日线数据
            tick_price = ticksData[stock]["close"].iloc[0]
            day_close = ticksOfDay[stock]["close"].iloc[0]
            
            # 计算价格波动比例
            price_ratio = tick_price / day_close
            print('看看波动', stock, tick_price, day_close, price_ratio)

            # 执行筛选条件
            if 0.951 < price_ratio < 1.015:
                target.append(stock)
                
        except KeyError as e:
            print(f"股票{stock}数据异常: {str(e)}")
        except Exception as e:
            print(f"处理{stock}时发生错误: {str(e)}")

    # 数据校验（处理空数据情况）
    # 修正字典类型判空方式
    if not ticksData or not ticksOfDay:
        print("行情数据为空，终止处理")
        return
    
    # 获取交集索引
    # 字典结构处理交集
    common_stocks = list(set(ticksData.keys()) & set(ticksOfDay.keys()))
    if len(common_stocks) == 0:
        print("无共同标的股票")
        return

    print('当日开盘筛选后股票池:', target)
    if len(target)==0:
        return
    num = g.stock_num - len(hold_list)
    target=[x for x in target  if x not in  hold_list][:num]
    if len(target) > 0:
        # 剩余份数
        leftNum = g.stock_num - len(hold_list)
        buyPercent = round(1 / leftNum, 2) - 0.001
        money = get_account_money(ContextInfo)
        # 单支股票需要的买入金额
        single_mount = round(money * buyPercent, 2)
        print("买入目标：", target,  [ContextInfo.get_stock_name(stock) for stock in target], "单支买入剩余比例：", buyPercent, "金额：", single_mount)
        for stock in target:
            if ContextInfo.do_back_test:
                open_position_in_test(ContextInfo, stock, round(1 / g.stock_num, 2) - 0.001)
            else:
                open_position(ContextInfo, stock, single_mount)

# 卖出函数
def sell(ContextInfo):
    positions = get_trade_detail_data(ContextInfo.account, 'STOCK', 'POSITION')
    hold_list = [codeOfPosition(position) for position in positions if position.m_nCanUseVolume > 1000]
    if hold_list:
        # 查询持仓昨日信息
        ticksOfDay = ContextInfo.get_market_data_ex(
            [],                
            hold_list,
            period="1d",
            start_time = (ContextInfo.today - datetime.timedelta(days=14)).strftime('%Y%m%d'),
            end_time = ContextInfo.today.strftime('%Y%m%d'),
            count=3,
            dividend_type = "follow",
            fill_data = True,
            subscribe = False
        )
        ticksData = ContextInfo.get_market_data_ex(
            [],                
            hold_list,
            period="1m",
            start_time = (ContextInfo.today - datetime.timedelta(days=1)).strftime('%Y%m%d%H%M%S'),
            end_time = ContextInfo.today.strftime('%Y%m%d%H%M%S'),
            count=1,
            dividend_type = "follow",
            fill_data = True,
            subscribe = False
        )
        for stock in hold_list:
            print('单个股票的tick data', ticksData[stock])
            print('单个股票的day data', ticksOfDay[stock])
            # 最新价
            lastPrice = ticksData[stock]["close"].iloc[0]
            # 前日收盘价
            last2dClose = ticksOfDay[stock]["close"].iloc[-3]
            # 昨日收盘价
            lastClose = ticksOfDay[stock]["close"].iloc[-2]
            print('最新价', lastPrice)
            print('前日收盘价', last2dClose)
            print('昨日收盘价', lastClose)


            # 昨涨停价
            yeserday_high_limit = get_limit_of_stock(last2dClose)[0]
            # 今涨停价
            high_limit = get_limit_of_stock(lastClose)[0]
            # 盈亏比例
            position = find_stock_of_positions(positions, stock)
            profit = position.m_dProfitRate
            print('持仓信息', stock, profit)

            # 条件1：今天未涨停
            cond1 = lastPrice < high_limit
            # 条件2.1：亏损超过3%（矩阵运算）
            ret_matrix = profit * 100
            #cond2_0 = ret_matrix >= 10
            cond2_1 = ret_matrix < -3
            # 条件2.2：盈利超过0%（复用矩阵）
            cond2_2 = ret_matrix >= 0
            # 条件2.4：昨日涨停（批量计算）
            cond2_4 = lastClose == yeserday_high_limit
            #正常止盈止损
            sell_condition = cond1 &(cond2_1 | cond2_2 | cond2_4)
            
            # 需要卖出
            if sell_condition:
                print('需要卖出:', stock, ContextInfo.get_stock_name(stock))
                if ContextInfo.do_back_test:
                    order_target_value(stock, 0, ContextInfo, ContextInfo.account)
                else:
                    # 1123 表示可用股票数量下单，这里表示全卖
                    # 这里实盘已经验证传参正确，因为1123模式下表示可用比例，所以传1表示全卖
                    passorder(24, 1123, ContextInfo.account, stock, 6, 1, 1, "卖出策略", 1, "", ContextInfo)

class ScheduledTask:
    """定时任务基类"""
    def __init__(self, execution_time):
        self.last_executed = None
        self.execution_time = self._parse_time(execution_time)
    
    def _parse_time(self, time_str):
        """将HH:MM格式字符串转换为time对象"""
        try:
            return datetime.datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError("Invalid time format, use HH:MM")

class MinuteTask(ScheduledTask):
    """分钟级别任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = current_dt - datetime.timedelta(minutes=1) >= self.last_executed        
        should = should1 and should2
        # 当前时间已过执行时间 且 超过1分钟
        return should

# ===================== 以下为工具函数 ************************ 
class DailyTask(ScheduledTask):
    """每日任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.datetime.combine(current_dt.date(), self.execution_time).time()
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


# 获取当前账户可用金额
def get_account_money(ContextInfo):        
    accounts = get_trade_detail_data(ContextInfo.account, 'stock', 'account')
    money = 0
    for dt in accounts:
        money = dt.m_dAvailable
    return money


# 回测和实盘不一样，回测用目标比例，实盘用可用资金比例。注意这个value传参
def open_position_in_test(context, security: str, value: float):
    print("买入股票(回测):", security, context.get_stock_name(security), str(int(value * 100)) + '%')
    order_target_percent(security, round(value, 2), 'COMPETE', context, context.account)


# 实盘的买入非常复杂，需要考虑部分成交的情况，以及长时间委托不成交的情况，这里单开一个函数进行，且进行定时循环调用
# 这里有问题，不能和open_position在同一作用域。QMT貌似不支持多线程工作，因此需要整体循环买入后，整体定时检测再撤单。
def open_position(context, security: str, value: float = 0) -> bool:
    """
    开仓操作：尝试买入指定股票，支持指定股票数量或者金额

    参数:
        security: 股票代码
        value: 分配给该股票的资金
    """
    print("买入股票(实盘):", security, context.get_stock_name(security), value )
    
    # 走到这里则为首次下单，直接以目标金额数买入
    # 1102 表示总资金量下单
    lastOrderId = str(uuid.uuid4())
    g.orderIdMap[security] = lastOrderId
    passorder(23, 1102, context.account, security, 5, -1, value, lastOrderId, 1, lastOrderId, context)


def find_stock_of_positions(positions, stock):
    result = [position for position in positions if codeOfPosition(position) == stock]
    if result:
        print('有持仓', stock, result[0])

        return result[0]


def today_is_between(self, ContextInfo: Any) -> bool:
    """
    判断当前日期是否为资金再平衡（空仓）日，通常在04月或01月期间执行空仓操作

    参数:
        context: 聚宽平台传入的交易上下文对象

    返回:
        若为空仓日返回 True，否则返回 False
    """
    today_str = datetime.fromtimestamp(ContextInfo.currentTime / 1000).strftime('%m-%d')
    if self.pass_april:
        if ('04-01' <= today_str <= '04-30') or ('01-01' <= today_str <= '01-30'):
            return True
        else:
            return False
    else:
        return False