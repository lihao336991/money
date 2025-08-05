#coding:gbk
import pandas as pd
import numpy as np
import datetime
import time

def init(ContextInfo):
    account = '620000369618'
    ContextInfo.set_account(account)
    ContextInfo.runner = TaskRunner(ContextInfo)
    
    # 定时任务设定
    if ContextInfo.do_back_test:
        ContextInfo.runner.run_daily("9:20", prepare)

        ContextInfo.runner.run_daily("13:00", sell)
        
    else:
        ContextInfo.run_time("prepare","1nDay","2025-08-01 09:20:00","SH")
        ContextInfo.run_time("sell","1nDay","2025-08-01 13:00:00","SH")

def bar_time(ContextInfo):    
    index = ContextInfo.barpos
    currentTime = ContextInfo.get_bar_timetag(index) + 8 * 3600 * 1000
    ContextInfo.currentTime = currentTime
    ContextInfo.today = pd.to_datetime(currentTime, unit='ms')
    return pd.to_datetime(currentTime, unit='ms')

def handlebar(ContextInfo):
    print(bar_time(ContextInfo))

def prepare(ContextInfo):
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
            if 'ST' in name or 'st' in name:
                stStockList.append(code) 
        except Exception as e:
            print(f"获取股票 {code} 名称失败：{e}")
    
    # 国九条筛选
    GJTFliterStockList = []
    for stock_code in stStockList:
        if GJT_filter_stocks(ContextInfo, stock_code):
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
        rzqStockList = rzq_list(ContextInfo, fStockList)
        for code in rzqStockList:
            print(f"股票代码：{code}，通过RZQ验证")
    else:
        print("\nRZQ筛选前无股票，跳过RZQ筛选")
    
    # 换手率-倒序排一下，优先换手率高的
    resultStockList = []
    if rzqStockList:
        # 获取当前日期作为换手率筛选的基准日
        index = ContextInfo.barpos if hasattr(ContextInfo, 'barpos') else 0
        bar_timetag = ContextInfo.get_bar_timetag(index)
        if bar_timetag:
            current_dt = datetime.datetime.fromtimestamp(bar_timetag / 1000)
            target_date = current_dt.strftime("%Y%m%d")
            
            # 调用换手率筛选函数
            resultStockList = get_turnover_stocks(ContextInfo, rzqStockList, target_date)
            
            # 输出最终筛选结果
            print(f"【最终结果】经过所有筛选后剩余 {len(resultStockList)} 只股票")
            print(f"【最终结果】{resultStockList}")
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

def codeOfPosition(ContextInfo, position):
    return position.m_strInstrumentID + '.' + position.m_strExchangeID

# 根据股票代码和收盘价，计算次日涨跌停价格
def get_limit_of_stock(ContextInfo, last_close):
    return [round(last_close * 1.05, 2), round(last_close * 0.95), 2]

def sell(ContextInfo):
    positions = get_trade_detail_data(ContextInfo.account, 'STOCK', 'POSITION')
    hold_list = [codeOfPosition(position) for position in positions if position.m_dMarketValue > 1000]
    if hold_list:
        # 查询持仓昨日信息
        ticksOfDay = ContextInfo.get_market_data_ex(
            ['close'],                
            list,
            period="1d",
            start_time = (ContextInfo.today - timedelta(days=1)).strftime('%Y%m%d'),
            end_time = ContextInfo.today.strftime('%Y%m%d'),
            count=3,
            dividend_type = "follow",
            fill_data = True,
            subscribe = ~ContextInfo.do_back_test
        )
        ticksData = ContextInfo.get_market_data_ex(
            ['lastPrice'],                
            list,
            period="tick",
            count=1,
            dividend_type = "follow",
            fill_data = True,
            subscribe = ~ContextInfo.do_back_test
        )
        for stock in list:
            # 最新价
            lastPrice = ticksData[stock]["lastPrice"].iloc[0]
            # 前日收盘价
            last2dClose = ticksOfDay[stock]["close"].iloc[0]
            # 昨日收盘价
            lastClose = ticksOfDay[stock]["close"].iloc[1]
            # 昨涨停价
            yeserday_high_limit = get_limit_of_stock(last2dClose)[0]
            # 今涨停价
            high_limit = get_limit_of_stock(lastClose)[0]
            # 盈亏比例
            profit = positions[stock].m_dProfitRate

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
                print('需要卖出:', stock)
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
            return datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError("Invalid time format, use HH:MM")

class MinuteTask(ScheduledTask):
    """分钟级别任务"""
    def should_trigger(self, current_dt):
        # 生成当日理论执行时间
        should1 = current_dt.time() >= datetime.combine(current_dt.date(), self.execution_time).time()
        should2 = current_dt - timedelta(minutes=1) >= self.last_executed        
        should = should1 and should2
        # 当前时间已过执行时间 且 超过1分钟
        return should

# ===================== 以下为工具函数 ************************ 
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

