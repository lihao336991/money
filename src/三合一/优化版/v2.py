# 三合一策略优化重构版 v2
# 代码结构更好，但是参数数值没有改变
# 买入采用排他优先级：一进二 > 低开

import datetime as dt

import pandas as pd
from jqdata import *


STRATEGIES = ['一进二', '低开']


def initialize(context):
    set_option('use_real_price', True)
    log.set_level('system', 'error')
    set_option('avoid_future_data', True)
    set_slippage(FixedSlippage(0.01))

    reset_runtime_state()
    g.initial_total_value = context.portfolio.total_value

    run_daily(get_stock_list, '9:01')
    run_daily(buy, '09:26')
    run_daily(sell, time='11:25', reference_security='000300.XSHG')
    run_daily(sell, time='14:50', reference_security='000300.XSHG')
    run_daily(record_strategy_attribution, '15:00')


def reset_runtime_state():
    g.pre_gap_up = []
    g.pre_gap_down = []
    g.selection_trade_date = ''
    g.stock_strategy = {}
    g.strategy_realized_pnl = {strategy: 0.0 for strategy in STRATEGIES}
    g.initial_total_value = None


def get_stock_list(context):
    previous_trade_date = context.previous_date
    date_2, date_1, date = get_trade_days(end_date=previous_trade_date, count=3)
    initial_list = prepare_stock_list(date)

    hl0_list = get_hl_stock(initial_list, date)
    hl1_list = get_ever_hl_stock(initial_list, date_1)
    hl2_list = get_ever_hl_stock(initial_list, date_2)

    gap_up_base = [stock for stock in hl0_list if stock not in set(hl1_list + hl2_list)]
    gap_down_base = [stock for stock in hl0_list if stock not in hl1_list]

    g.pre_gap_up = filter_gap_up_candidates(context, gap_up_base)
    g.pre_gap_down = filter_gap_down_candidates(gap_down_base, transform_date(date, 'str'))
    g.selection_trade_date = context.current_dt.strftime('%Y-%m-%d')

    log_preselection_summary(
        context.current_dt,
        len(hl0_list),
        len(hl1_list),
        len(hl2_list),
        len(g.pre_gap_up),
        len(g.pre_gap_down),
    )


def filter_gap_up_candidates(context, stock_list):
    candidates = []
    for stock in stock_list:
        prev_day_data = attribute_history(
            stock, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True
        )
        if prev_day_data.empty:
            continue

        prev_close = prev_day_data['close'][0]
        prev_volume = prev_day_data['volume'][0]
        prev_money = prev_day_data['money'][0]
        if prev_close <= 0 or prev_volume <= 0:
            continue

        avg_price_increase_value = prev_money / prev_volume / prev_close * 1.1 - 1
        if avg_price_increase_value < 0.07 or prev_money < 5.5e8 or prev_money > 20e8:
            continue

        valuation_data = get_valuation(
            stock,
            start_date=context.previous_date,
            end_date=context.previous_date,
            fields=['turnover_ratio', 'market_cap', 'circulating_market_cap'],
        )
        if valuation_data.empty:
            continue
        if valuation_data['market_cap'][0] < 70 or valuation_data['circulating_market_cap'][0] > 520:
            continue

        if rise_low_volume(stock):
            continue

        candidates.append(stock)
    return candidates


def filter_gap_down_candidates(stock_list, date):
    if not stock_list:
        return []

    rpd = get_relative_position_df(stock_list, date, 60)
    if rpd.empty:
        return []

    return list(rpd[rpd['rp'] <= 0.5].index)


def buy(context):
    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []

    current_data = get_current_data()
    date_now = context.current_dt.strftime('%Y-%m-%d')
    previous_date = transform_date(context.previous_date, 'str')

    gk_stocks = filter_gap_up_by_auction(g.pre_gap_up, current_data, date_now)
    dk_stocks = filter_gap_down_by_open(g.pre_gap_down, current_data, previous_date)
    gk_stocks, dk_stocks = apply_exclusive_priority(gk_stocks, dk_stocks)
    strategy_by_stock = build_strategy_map(gk_stocks, dk_stocks)
    qualified_stocks.extend(gk_stocks)
    qualified_stocks.extend(dk_stocks)

    log_auction_summary(
        context.current_dt,
        gk_stocks,
        dk_stocks,
    )

    if qualified_stocks:
        send_message('今日选股：' + ','.join(qualified_stocks))
    else:
        send_message('今日无目标个股')

    if not qualified_stocks:
        print('今日无目标个股')
        return

    if context.portfolio.total_value <= 0:
        return
    if context.portfolio.available_cash / context.portfolio.total_value <= 0.3:
        return

    value = context.portfolio.available_cash / len(qualified_stocks)
    for stock in qualified_stocks:
        last_price = current_data[stock].last_price
        if last_price <= 0:
            continue
        if context.portfolio.available_cash / last_price > 100:
            order_value(stock, value, MarketOrderStyle(current_data[stock].day_open))
            tag_position_strategy(stock, strategy_by_stock)
            print('买入' + stock)
            print('———————————————————————————————————')


def apply_exclusive_priority(gap_up_selected, gap_down_selected):
    if gap_up_selected:
        return gap_up_selected, []
    return [], gap_down_selected


def build_strategy_map(gap_up_selected, gap_down_selected):
    strategy_by_stock = {}
    for strategy, stock_list in [
        ('一进二', gap_up_selected),
        ('低开', gap_down_selected),
    ]:
        for stock in stock_list:
            if stock not in strategy_by_stock:
                strategy_by_stock[stock] = strategy
    return strategy_by_stock


def tag_position_strategy(stock, strategy_by_stock):
    if stock not in g.stock_strategy:
        g.stock_strategy[stock] = strategy_by_stock.get(stock, '未归因')


def filter_gap_up_by_auction(stock_list, current_data, date_now):
    selected = []
    for stock in stock_list:
        prev_day_data = attribute_history(stock, 1, '1d', fields=['volume'], skip_paused=True)
        if prev_day_data.empty or prev_day_data['volume'][0] <= 0:
            continue

        auction_data = get_call_auction(
            stock, start_date=date_now, end_date=date_now, fields=['time', 'volume', 'current']
        )
        if auction_data.empty:
            continue
        if auction_data['volume'][0] / prev_day_data['volume'][0] < 0.03:
            continue

        high_limit = current_data[stock].high_limit
        if high_limit <= 0:
            continue
        current_ratio = auction_data['current'][0] / (high_limit / 1.1)
        if current_ratio <= 1 or current_ratio >= 1.06:
            continue

        selected.append(stock)
    return selected


def filter_gap_down_by_open(stock_list, current_data, previous_date):
    if not stock_list:
        return []

    df = get_price(
        stock_list,
        end_date=previous_date,
        frequency='daily',
        fields=['close'],
        count=1,
        panel=False,
        fill_paused=False,
        skip_paused=True,
    )
    if df.empty:
        return []

    df = df.set_index('code')
    open_filtered = []
    for stock in stock_list:
        if stock not in df.index:
            continue

        prev_close = df.loc[stock, 'close']
        day_open = current_data[stock].day_open
        if pd.isnull(prev_close) or prev_close <= 0 or day_open <= 0:
            continue

        open_pct = day_open / prev_close
        if 0.955 <= open_pct <= 0.97:
            open_filtered.append(stock)

    selected = []
    for stock in open_filtered:
        prev_day_data = attribute_history(stock, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
        if prev_day_data.empty:
            continue
        if prev_day_data['money'][0] >= 1e8:
            selected.append(stock)
    return selected


def log_preselection_summary(current_dt, hl0_count, hl1_count, hl2_count, gap_up_count, gap_down_count):
    lines = [
        '%s 选股开始, T涨停%s只，T-1曾涨停%s只，T-2曾涨停%s只。'
        % (format_log_date(current_dt), hl0_count, hl1_count, hl2_count),
        '',
        '初选：',
        '一进二 %s只' % gap_up_count,
        '低开 %s只' % gap_down_count,
        '',
    ]
    log_multiline(lines)


def log_auction_summary(current_dt, gap_up_selected, gap_down_selected):
    total_count = len(gap_up_selected + gap_down_selected)
    lines = [
        '%s 集合竞价共选中%s只，如下：' % (format_log_date(current_dt), total_count),
        '一进二：%s只，%s' % (len(gap_up_selected), gap_up_selected),
        '低开：%s只，%s' % (len(gap_down_selected), gap_down_selected),
        '',
    ]
    log_multiline(lines)


def log_multiline(lines):
    text = '\n'.join(lines)
    print(text)
    log.info(text)


def format_log_date(date_value):
    return transform_date(date_value, 'dt').strftime('%Y.%m%d')


def transform_date(date, date_type):
    if isinstance(date, str):
        str_date = date
        dt_date = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date = dt_date.date()
    elif isinstance(date, dt.datetime):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = dt_date.date()
    elif isinstance(date, dt.date):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    dct = {'str': str_date, 'dt': dt_date, 'd': d_date}
    return dct[date_type]


def get_shifted_date(date, days, days_type='T'):
    d_date = transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    if days_type == 'N':
        shifted_date = yesterday + dt.timedelta(days + 1)
    if days_type == 'T':
        all_trade_days = [i.strftime('%Y-%m-%d') for i in list(get_all_trade_days())]
        if str(yesterday) in all_trade_days:
            shifted_date = all_trade_days[all_trade_days.index(str(yesterday)) + days + 1]
        else:
            for i in range(100):
                last_trade_date = yesterday - dt.timedelta(i)
                if str(last_trade_date) in all_trade_days:
                    shifted_date = all_trade_days[all_trade_days.index(str(last_trade_date)) + days + 1]
                    break
    return str(shifted_date)


def filter_new_stock(initial_list, date, days=50):
    d_date = transform_date(date, 'd')
    return [stock for stock in initial_list if d_date - get_security_info(stock).start_date > dt.timedelta(days=days)]


def filter_st_paused_stock(initial_list):
    current_data = get_current_data()
    return [
        stock for stock in initial_list
        if not any([
            current_data[stock].is_st,
            current_data[stock].paused,
            '退' in current_data[stock].name,
        ])
    ]


def filter_kcbj_stock(initial_list):
    return [stock for stock in initial_list if stock[:2] in ('60', '00', '30')]


def prepare_stock_list(date):
    initial_list = get_all_securities('stock', date).index.tolist()
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_new_stock(initial_list, date)
    initial_list = filter_st_paused_stock(initial_list)
    return initial_list


def rise_low_volume(stock):
    hist = attribute_history(stock, 106, '1d', fields=['high', 'volume'], skip_paused=True, df=False)
    if len(hist['high']) < 102 or len(hist['volume']) < 2:
        return False

    high_prices = hist['high'][:102]
    prev_high = high_prices[-1]
    zyts_0 = next((i - 1 for i, high in enumerate(high_prices[-3::-1], 2) if high >= prev_high), 100)
    zyts = zyts_0 + 5
    if hist['volume'][-1] <= max(hist['volume'][-zyts:-1]) * 0.9:
        return True
    return False


def get_hl_stock(initial_list, date):
    df = get_price(
        initial_list,
        end_date=date,
        frequency='daily',
        fields=['close', 'high_limit'],
        count=1,
        panel=False,
        fill_paused=False,
        skip_paused=False,
    )
    df = df.dropna()
    df = df[df['close'] == df['high_limit']]
    return list(df.code)


def get_ever_hl_stock(initial_list, date):
    df = get_price(
        initial_list,
        end_date=date,
        frequency='daily',
        fields=['high', 'high_limit'],
        count=1,
        panel=False,
        fill_paused=False,
        skip_paused=False,
    )
    df = df.dropna()
    df = df[df['high'] == df['high_limit']]
    return list(df.code)


def get_ever_hl_stock2(initial_list, date):
    df = get_price(
        initial_list,
        end_date=date,
        frequency='daily',
        fields=['close', 'high', 'high_limit'],
        count=1,
        panel=False,
        fill_paused=False,
        skip_paused=False,
    )
    df = df.dropna()
    df = df[(df['high'] == df['high_limit']) & (df['close'] != df['high_limit'])]
    return list(df.code)


def get_relative_position_df(stock_list, date, watch_days):
    if not stock_list:
        return pd.DataFrame(columns=['rp'])

    df = get_price(
        stock_list,
        end_date=date,
        fields=['high', 'low', 'close'],
        count=watch_days,
        fill_paused=False,
        skip_paused=False,
        panel=False,
    ).dropna()
    if df.empty:
        return pd.DataFrame(columns=['rp'])

    close = df.groupby('code').apply(lambda df_sub: df_sub.iloc[-1, -1])
    high = df.groupby('code').apply(lambda df_sub: df_sub['high'].max())
    low = df.groupby('code').apply(lambda df_sub: df_sub['low'].min())
    result = pd.DataFrame()
    result['close'] = close
    result['high'] = high
    result['low'] = low
    result = result[result['high'] > result['low']]
    result['rp'] = (result['close'] - result['low']) / (result['high'] - result['low'])
    return result[['rp']]


def close_position_with_attribution(stock, position, price):
    strategy = g.stock_strategy.get(stock)
    if strategy in g.strategy_realized_pnl and position.closeable_amount > 0:
        g.strategy_realized_pnl[strategy] += (price - position.avg_cost) * position.closeable_amount
    order_target_value(stock, 0)


def calculate_strategy_pnl(context, current_data):
    strategy_pnl = {strategy: g.strategy_realized_pnl.get(strategy, 0.0) for strategy in STRATEGIES}
    for stock, position in context.portfolio.positions.items():
        strategy = g.stock_strategy.get(stock)
        if strategy not in strategy_pnl:
            continue

        last_price = current_data[stock].last_price
        if last_price <= 0 or position.avg_cost <= 0:
            continue

        amount = position.value / last_price
        strategy_pnl[strategy] += position.value - position.avg_cost * amount
    return strategy_pnl


def record_strategy_attribution(context):
    current_data = get_current_data()
    strategy_pnl = calculate_strategy_pnl(context, current_data)
    total_strategy_pnl = 0.0
    for pnl in strategy_pnl.values():
        total_strategy_pnl += pnl

    initial_value = g.initial_total_value
    if initial_value is None or initial_value <= 0:
        initial_value = context.portfolio.total_value
        g.initial_total_value = initial_value

    record_data = {}
    for strategy in STRATEGIES:
        pnl = strategy_pnl[strategy]
        record_data[strategy + '收益率'] = pnl / initial_value * 100
        record_data[strategy + '占比'] = pnl / total_strategy_pnl * 100 if total_strategy_pnl != 0 else 0

    record(**record_data)
    log_strategy_attribution(context.current_dt, strategy_pnl, total_strategy_pnl)

    current_positions = set(context.portfolio.positions.keys())
    g.stock_strategy = {
        stock: strategy for stock, strategy in g.stock_strategy.items()
        if stock in current_positions
    }


def log_strategy_attribution(current_dt, strategy_pnl, total_strategy_pnl):
    lines = [
        '%s 子策略收益归因：' % format_log_date(current_dt),
    ]
    for strategy in STRATEGIES:
        pnl = strategy_pnl[strategy]
        contribution = pnl / total_strategy_pnl * 100 if total_strategy_pnl != 0 else 0
        lines.append('%s：收益 %.2f，占比 %.2f%%' % (strategy, pnl, contribution))
    lines.append('')
    log_multiline(lines)


def sell(context):
    date = transform_date(context.previous_date, 'str')
    current_data = get_current_data()

    if str(context.current_dt)[-8:] == '11:25:00':
        for stock in list(context.portfolio.positions):
            position = context.portfolio.positions[stock]
            if (
                position.closeable_amount != 0
                and current_data[stock].last_price < current_data[stock].high_limit
                and current_data[stock].last_price > position.avg_cost
            ):
                close_position_with_attribution(stock, position, current_data[stock].last_price)
                print('止盈卖出', [stock, get_security_info(stock, date).display_name])
                print('———————————————————————————————————')

    if str(context.current_dt)[-8:] == '14:50:00':
        for stock in list(context.portfolio.positions):
            position = context.portfolio.positions[stock]
            close_data = attribute_history(stock, 4, '1d', ['close'])
            if close_data.empty:
                continue

            ma4 = close_data['close'].mean()
            ma5 = (ma4 * 4 + current_data[stock].last_price) / 5

            if (
                position.closeable_amount != 0
                and current_data[stock].last_price < current_data[stock].high_limit
                and current_data[stock].last_price > position.avg_cost
            ):
                close_position_with_attribution(stock, position, current_data[stock].last_price)
                print('止盈卖出', [stock, get_security_info(stock, date).display_name])
                print('———————————————————————————————————')
            elif position.closeable_amount != 0 and current_data[stock].last_price < ma5:
                close_position_with_attribution(stock, position, current_data[stock].last_price)
                print('止损卖出', [stock, get_security_info(stock, date).display_name])
                print('———————————————————————————————————')
