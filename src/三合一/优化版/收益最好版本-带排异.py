# 三合一策略优化重构版 v2
# 代码结构更好，但是参数数值没有改变
# 买入采用排他优先级：一进二 > 低开 > 弱转强

import datetime as dt

import pandas as pd
from jqdata import *


STRATEGIES = ['一进二', '低开', '弱转强']


def diag_code(stock):
    return stock.replace('.XSHG', '.SH').replace('.XSHE', '.SZ')


def diag_codes(stock_list):
    return '[' + ','.join(sorted(diag_code(stock) for stock in stock_list)) + ']'


def diag_log_list(date, label, stock_list):
    print('【排查】%s|%s|count=%s|codes=%s' % (date, label, len(stock_list), diag_codes(stock_list)))


def diag_num(value):
    if pd.isnull(value):
        return 'nan'
    return '%.6f' % float(value)


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
    g.pre_reversal = []
    g.selection_trade_date = ''
    g.stock_strategy = {}
    g.strategy_realized_pnl = {strategy: 0.0 for strategy in STRATEGIES}
    g.initial_total_value = None


def get_stock_list(context):
    # ============================== 开始新交易日 ==============================
    current_date_str = context.current_dt.strftime('%Y-%m-%d')
    separator = '=' * 80
    print(separator)
    print('【新交易日】%s' % current_date_str)
    print(separator)
    # =========================================================================

    previous_trade_date = context.previous_date
    date_2, date_1, date = get_trade_days(end_date=previous_trade_date, count=3)
    print('【排查】交易日|run_date=%s|T=%s|T_1=%s|T_2=%s' % (current_date_str, date, date_1, date_2))
    initial_list = prepare_stock_list(date)
    print('【排查】%s|股票池_最终|count=%s' % (date, len(initial_list)))

    hl0_list = get_hl_stock(initial_list, date)
    hl1_list = get_ever_hl_stock(initial_list, date_1)
    hl2_list = get_ever_hl_stock(initial_list, date_2)
    hl0_ever_list = get_ever_hl_stock2(initial_list, date)

    gap_up_base = [stock for stock in hl0_list if stock not in set(hl1_list + hl2_list)]
    gap_down_base = [stock for stock in hl0_list if stock not in hl1_list]

    previous_hl_list = get_hl_stock(initial_list, date_1)
    reversal_base = [stock for stock in hl0_ever_list if stock not in previous_hl_list]

    diag_log_list(date, 'T涨停_收盘', hl0_list)
    diag_log_list(date_1, 'T_1曾涨停_最高', hl1_list)
    diag_log_list(date_2, 'T_2曾涨停_最高', hl2_list)
    diag_log_list(date, 'T曾涨停未封板', hl0_ever_list)
    diag_log_list(date_1, 'T_1涨停_收盘', previous_hl_list)
    diag_log_list(date, '一进二_初选', gap_up_base)
    diag_log_list(date, '低开_初选', gap_down_base)
    diag_log_list(date, '弱转强_初选', reversal_base)

    g.pre_gap_up = filter_gap_up_candidates(context, gap_up_base)
    g.pre_gap_down = filter_gap_down_candidates(gap_down_base, transform_date(date, 'str'))
    g.pre_reversal = filter_reversal_candidates(context, reversal_base)
    diag_log_list(date, '一进二_预选', g.pre_gap_up)
    diag_log_list(date, '低开_预选', g.pre_gap_down)
    diag_log_list(date, '弱转强_预选', g.pre_reversal)
    g.selection_trade_date = context.current_dt.strftime('%Y-%m-%d')

    log_preselection_summary(
        context.current_dt,
        hl0_list,
        hl1_list,
        hl2_list,
        g.pre_gap_up,
        g.pre_gap_down,
        g.pre_reversal,
    )


def filter_gap_up_candidates(context, stock_list):
    candidates = []
    filter_stats = {'无数据': 0, '量价异常': 0, '涨幅成交额不符': 0, '估值无数据': 0, '市值不符': 0, '左压缩量': 0}
    for stock in stock_list:
        prev_day_data = attribute_history(
            stock, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True
        )
        if prev_day_data.empty:
            filter_stats['无数据'] += 1
            continue

        prev_close = prev_day_data['close'][0]
        prev_volume = prev_day_data['volume'][0]
        prev_money = prev_day_data['money'][0]
        if prev_close <= 0 or prev_volume <= 0:
            filter_stats['量价异常'] += 1
            print('【排查】%s|一进二_过滤|code=%s|reason=量价异常|close=%s|volume=%s|money=%s' % (
                context.previous_date, diag_code(stock), diag_num(prev_close), diag_num(prev_volume), diag_num(prev_money)
            ))
            continue

        avg_price_increase_value = prev_money / prev_volume / prev_close * 1.1 - 1
        if avg_price_increase_value < 0.07 or prev_money < 5.5e8 or prev_money > 20e8:
            filter_stats['涨幅成交额不符'] += 1
            print('【排查】%s|一进二_过滤|code=%s|reason=涨幅成交额不符|avg_inc=%s|money=%s|volume=%s|close=%s' % (
                context.previous_date,
                diag_code(stock),
                diag_num(avg_price_increase_value),
                diag_num(prev_money),
                diag_num(prev_volume),
                diag_num(prev_close),
            ))
            continue

        valuation_data = get_valuation(
            stock,
            start_date=context.previous_date,
            end_date=context.previous_date,
            fields=['turnover_ratio', 'market_cap', 'circulating_market_cap'],
        )
        if valuation_data.empty:
            filter_stats['估值无数据'] += 1
            print('【排查】%s|一进二_过滤|code=%s|reason=估值无数据' % (context.previous_date, diag_code(stock)))
            continue
        if valuation_data['market_cap'][0] < 70 or valuation_data['circulating_market_cap'][0] > 520:
            filter_stats['市值不符'] += 1
            print('【排查】%s|一进二_过滤|code=%s|reason=市值不符|market_cap=%s|circulating_market_cap=%s' % (
                context.previous_date,
                diag_code(stock),
                diag_num(valuation_data['market_cap'][0]),
                diag_num(valuation_data['circulating_market_cap'][0]),
            ))
            continue

        if rise_low_volume(stock):
            filter_stats['左压缩量'] += 1
            print('【排查】%s|一进二_过滤|code=%s|reason=左压缩量' % (context.previous_date, diag_code(stock)))
            continue

        candidates.append(stock)
        print('【排查】%s|一进二_通过|code=%s|avg_inc=%s|money=%s|volume=%s|close=%s|market_cap=%s|circulating_market_cap=%s' % (
            context.previous_date,
            diag_code(stock),
            diag_num(avg_price_increase_value),
            diag_num(prev_money),
            diag_num(prev_volume),
            diag_num(prev_close),
            diag_num(valuation_data['market_cap'][0]),
            diag_num(valuation_data['circulating_market_cap'][0]),
        ))

    # 打印过滤统计
    detail = '、'.join('%s%s只' % (k, v) for k, v in filter_stats.items() if v > 0)
    print('【一进二过滤】候选%s只，通过%s只，过滤明细：%s' % (len(stock_list), len(candidates), detail))
    return candidates


def filter_gap_down_candidates(stock_list, date):
    if not stock_list:
        return []

    rpd = get_relative_position_df(stock_list, date, 60)
    if rpd.empty:
        print('【低开过滤】候选%s只，全部无历史数据' % len(stock_list))
        return []

    before = len(rpd)
    result = list(rpd[rpd['rp'] <= 0.5].index)
    for stock in stock_list:
        if stock not in rpd.index:
            print('【排查】%s|低开_过滤|code=%s|reason=无相对位置数据' % (date, diag_code(stock)))
            continue
        rp = rpd.loc[stock, 'rp']
        if rp <= 0.5:
            print('【排查】%s|低开_通过|code=%s|rp=%s' % (date, diag_code(stock), diag_num(rp)))
        else:
            print('【排查】%s|低开_过滤|code=%s|reason=相对位置过高|rp=%s' % (date, diag_code(stock), diag_num(rp)))
    filtered = before - len(result)
    if filtered > 0:
        print('【低开过滤】候选%s只，通过%s只，过滤：相对位置过高%s只' % (len(stock_list), len(result), filtered))
    else:
        print('【低开过滤】候选%s只，全部通过' % len(stock_list))
    return result


def filter_reversal_candidates(context, stock_list):
    candidates = []
    filter_stats = {'K线不足': 0, '涨幅过高': 0, '开盘异常': 0, '收盘弱': 0, '量价无数据': 0, '涨幅成交额不符': 0, '估值无数据': 0, '市值不符': 0, '左压缩量': 0}
    for stock in stock_list:
        price_data = attribute_history(stock, 4, '1d', fields=['close'], skip_paused=True)
        if len(price_data) < 4 or price_data['close'][0] <= 0:
            filter_stats['K线不足'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=K线不足|rows=%s' % (context.previous_date, diag_code(stock), len(price_data)))
            continue

        increase_ratio = (price_data['close'][-1] - price_data['close'][0]) / price_data['close'][0]
        if increase_ratio > 0.28:
            filter_stats['涨幅过高'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=涨幅过高|increase_ratio=%s' % (
                context.previous_date, diag_code(stock), diag_num(increase_ratio)
            ))
            continue

        prev_oc_data = attribute_history(stock, 1, '1d', fields=['open', 'close'], skip_paused=True)
        if prev_oc_data.empty or prev_oc_data['open'][0] <= 0:
            filter_stats['开盘异常'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=开盘异常' % (context.previous_date, diag_code(stock)))
            continue

        open_close_ratio = (prev_oc_data['close'][0] - prev_oc_data['open'][0]) / prev_oc_data['open'][0]
        if open_close_ratio < -0.05:
            filter_stats['收盘弱'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=收盘弱|open_close_ratio=%s|open=%s|close=%s' % (
                context.previous_date,
                diag_code(stock),
                diag_num(open_close_ratio),
                diag_num(prev_oc_data['open'][0]),
                diag_num(prev_oc_data['close'][0]),
            ))
            continue

        prev_day_data = attribute_history(
            stock, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True
        )
        if prev_day_data.empty:
            filter_stats['量价无数据'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=量价无数据' % (context.previous_date, diag_code(stock)))
            continue

        prev_close = prev_day_data['close'][0]
        prev_volume = prev_day_data['volume'][0]
        prev_money = prev_day_data['money'][0]
        if prev_close <= 0 or prev_volume <= 0:
            filter_stats['量价无数据'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=量价异常|close=%s|volume=%s|money=%s' % (
                context.previous_date, diag_code(stock), diag_num(prev_close), diag_num(prev_volume), diag_num(prev_money)
            ))
            continue

        avg_price_increase_value = prev_money / prev_volume / prev_close - 1
        if avg_price_increase_value < -0.04 or prev_money < 3e8 or prev_money > 19e8:
            filter_stats['涨幅成交额不符'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=涨幅成交额不符|avg_inc=%s|money=%s|volume=%s|close=%s' % (
                context.previous_date,
                diag_code(stock),
                diag_num(avg_price_increase_value),
                diag_num(prev_money),
                diag_num(prev_volume),
                diag_num(prev_close),
            ))
            continue

        valuation_data = get_valuation(
            stock,
            start_date=context.previous_date,
            end_date=context.previous_date,
            fields=['turnover_ratio', 'market_cap', 'circulating_market_cap'],
        )
        if valuation_data.empty:
            filter_stats['估值无数据'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=估值无数据' % (context.previous_date, diag_code(stock)))
            continue
        if valuation_data['market_cap'][0] < 70 or valuation_data['circulating_market_cap'][0] > 520:
            filter_stats['市值不符'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=市值不符|market_cap=%s|circulating_market_cap=%s' % (
                context.previous_date,
                diag_code(stock),
                diag_num(valuation_data['market_cap'][0]),
                diag_num(valuation_data['circulating_market_cap'][0]),
            ))
            continue

        if rise_low_volume(stock):
            filter_stats['左压缩量'] += 1
            print('【排查】%s|弱转强_过滤|code=%s|reason=左压缩量' % (context.previous_date, diag_code(stock)))
            continue

        candidates.append(stock)
        print('【排查】%s|弱转强_通过|code=%s|increase_ratio=%s|open_close_ratio=%s|avg_inc=%s|money=%s|volume=%s|close=%s|market_cap=%s|circulating_market_cap=%s' % (
            context.previous_date,
            diag_code(stock),
            diag_num(increase_ratio),
            diag_num(open_close_ratio),
            diag_num(avg_price_increase_value),
            diag_num(prev_money),
            diag_num(prev_volume),
            diag_num(prev_close),
            diag_num(valuation_data['market_cap'][0]),
            diag_num(valuation_data['circulating_market_cap'][0]),
        ))

    # 打印过滤统计
    detail = '、'.join('%s%s只' % (k, v) for k, v in filter_stats.items() if v > 0)
    print('【弱转强过滤】候选%s只，通过%s只，过滤明细：%s' % (len(stock_list), len(candidates), detail))
    return candidates


def buy(context):
    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []

    current_data = get_current_data()
    date_now = context.current_dt.strftime('%Y-%m-%d')
    previous_date = transform_date(context.previous_date, 'str')

    gk_stocks = filter_gap_up_by_auction(g.pre_gap_up, current_data, date_now)
    dk_stocks = filter_gap_down_by_open(g.pre_gap_down, current_data, previous_date)
    rzq_stocks = filter_reversal_by_auction(g.pre_reversal, current_data, date_now)
    gk_stocks, dk_stocks, rzq_stocks = apply_exclusive_priority(gk_stocks, dk_stocks, rzq_stocks)
    strategy_by_stock = build_strategy_map(gk_stocks, dk_stocks, rzq_stocks)
    qualified_stocks.extend(gk_stocks)
    qualified_stocks.extend(dk_stocks)
    qualified_stocks.extend(rzq_stocks)

    log_auction_summary(
        context.current_dt,
        gk_stocks,
        dk_stocks,
        rzq_stocks,
    )

    # -------- 最终买入汇总日志 --------
    total_final = len(qualified_stocks)
    if total_final > 0:
        lines = [
            '%s 最终确定买入 %s 只' % (format_log_date(context.current_dt), total_final),
            '一进二 %s只：%s' % (len(gk_stocks), format_stock_list(gk_stocks)),
            '低开   %s只：%s' % (len(dk_stocks), format_stock_list(dk_stocks)),
            '弱转强 %s只：%s' % (len(rzq_stocks), format_stock_list(rzq_stocks)),
            '合计：%s' % format_stock_list(qualified_stocks),
            '',
        ]
        log_multiline(lines)
        send_message('今日选股：' + format_stock_list(qualified_stocks))
    else:
        print('%s 最终无目标个股' % format_log_date(context.current_dt))
        send_message('今日无目标个股')
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
            print('买入 %s  %s  金额=%.2f' % (stock, get_security_info(stock).display_name, value))
            print('———————————————————————————————————')


def apply_exclusive_priority(gap_up_selected, gap_down_selected, reversal_selected):
    if gap_up_selected:
        return gap_up_selected, [], []
    if gap_down_selected:
        return [], gap_down_selected, []
    return [], [], reversal_selected


def build_strategy_map(gap_up_selected, gap_down_selected, reversal_selected):
    strategy_by_stock = {}
    for strategy, stock_list in [
        ('一进二', gap_up_selected),
        ('低开', gap_down_selected),
        ('弱转强', reversal_selected),
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
            print('【排查】%s|竞价一进二_过滤|code=%s|reason=昨日成交量无效' % (date_now, diag_code(stock)))
            continue

        auction_data = get_call_auction(
            stock, start_date=date_now, end_date=date_now, fields=['time', 'volume', 'current']
        )
        if auction_data.empty:
            print('【排查】%s|竞价一进二_过滤|code=%s|reason=无集合竞价数据|prev_volume=%s' % (
                date_now, diag_code(stock), diag_num(prev_day_data['volume'][0])
            ))
            continue
        if auction_data['volume'][0] / prev_day_data['volume'][0] < 0.03:
            print('【排查】%s|竞价一进二_过滤|code=%s|reason=竞价量比不足|auction_current=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
                date_now,
                diag_code(stock),
                diag_num(auction_data['current'][0]),
                diag_num(auction_data['volume'][0]),
                diag_num(prev_day_data['volume'][0]),
                diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
            ))
            continue

        high_limit = current_data[stock].high_limit
        if high_limit <= 0:
            print('【排查】%s|竞价一进二_过滤|code=%s|reason=涨停价无效|high_limit=%s' % (
                date_now, diag_code(stock), diag_num(high_limit)
            ))
            continue
        current_ratio = auction_data['current'][0] / (high_limit / 1.1)
        if current_ratio <= 1 or current_ratio >= 1.06:
            print('【排查】%s|竞价一进二_过滤|code=%s|reason=竞价涨幅不符|auction_current=%s|high_limit=%s|base_close=%s|price_ratio=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
                date_now,
                diag_code(stock),
                diag_num(auction_data['current'][0]),
                diag_num(high_limit),
                diag_num(high_limit / 1.1),
                diag_num(current_ratio),
                diag_num(auction_data['volume'][0]),
                diag_num(prev_day_data['volume'][0]),
                diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
            ))
            continue

        selected.append(stock)
        print('【排查】%s|竞价一进二_通过|code=%s|auction_current=%s|high_limit=%s|base_close=%s|price_ratio=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
            date_now,
            diag_code(stock),
            diag_num(auction_data['current'][0]),
            diag_num(high_limit),
            diag_num(high_limit / 1.1),
            diag_num(current_ratio),
            diag_num(auction_data['volume'][0]),
            diag_num(prev_day_data['volume'][0]),
            diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
        ))
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
            print('【排查】%s|竞价低开_过滤|code=%s|reason=开盘或昨收无效|day_open=%s|prev_close=%s' % (
                previous_date, diag_code(stock), diag_num(day_open), diag_num(prev_close)
            ))
            continue

        open_pct = day_open / prev_close
        if 0.955 <= open_pct <= 0.97:
            open_filtered.append(stock)
            print('【排查】%s|竞价低开_开盘通过|code=%s|day_open=%s|prev_close=%s|open_pct=%s' % (
                previous_date, diag_code(stock), diag_num(day_open), diag_num(prev_close), diag_num(open_pct)
            ))
        else:
            print('【排查】%s|竞价低开_过滤|code=%s|reason=低开幅度不符|day_open=%s|prev_close=%s|open_pct=%s' % (
                previous_date, diag_code(stock), diag_num(day_open), diag_num(prev_close), diag_num(open_pct)
            ))

    selected = []
    for stock in open_filtered:
        prev_day_data = attribute_history(stock, 1, '1d', fields=['close', 'volume', 'money'], skip_paused=True)
        if prev_day_data.empty:
            print('【排查】%s|竞价低开_过滤|code=%s|reason=昨日量价无数据' % (previous_date, diag_code(stock)))
            continue
        if prev_day_data['money'][0] >= 1e8:
            selected.append(stock)
            print('【排查】%s|竞价低开_通过|code=%s|prev_money=%s' % (
                previous_date, diag_code(stock), diag_num(prev_day_data['money'][0])
            ))
        else:
            print('【排查】%s|竞价低开_过滤|code=%s|reason=昨日成交额不足|prev_money=%s' % (
                previous_date, diag_code(stock), diag_num(prev_day_data['money'][0])
            ))
    return selected


def filter_reversal_by_auction(stock_list, current_data, date_now):
    selected = []
    for stock in stock_list:
        prev_day_data = attribute_history(stock, 1, '1d', fields=['volume'], skip_paused=True)
        if prev_day_data.empty or prev_day_data['volume'][0] <= 0:
            print('【排查】%s|竞价弱转强_过滤|code=%s|reason=昨日成交量无效' % (date_now, diag_code(stock)))
            continue

        auction_data = get_call_auction(
            stock, start_date=date_now, end_date=date_now, fields=['time', 'volume', 'current']
        )
        if auction_data.empty:
            print('【排查】%s|竞价弱转强_过滤|code=%s|reason=无集合竞价数据|prev_volume=%s' % (
                date_now, diag_code(stock), diag_num(prev_day_data['volume'][0])
            ))
            continue
        if auction_data['volume'][0] / prev_day_data['volume'][0] < 0.03:
            print('【排查】%s|竞价弱转强_过滤|code=%s|reason=竞价量比不足|auction_current=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
                date_now,
                diag_code(stock),
                diag_num(auction_data['current'][0]),
                diag_num(auction_data['volume'][0]),
                diag_num(prev_day_data['volume'][0]),
                diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
            ))
            continue

        high_limit = current_data[stock].high_limit
        if high_limit <= 0:
            print('【排查】%s|竞价弱转强_过滤|code=%s|reason=涨停价无效|high_limit=%s' % (
                date_now, diag_code(stock), diag_num(high_limit)
            ))
            continue
        current_ratio = auction_data['current'][0] / (high_limit / 1.1)
        if current_ratio <= 0.98 or current_ratio >= 1.09:
            print('【排查】%s|竞价弱转强_过滤|code=%s|reason=竞价涨幅不符|auction_current=%s|high_limit=%s|base_close=%s|price_ratio=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
                date_now,
                diag_code(stock),
                diag_num(auction_data['current'][0]),
                diag_num(high_limit),
                diag_num(high_limit / 1.1),
                diag_num(current_ratio),
                diag_num(auction_data['volume'][0]),
                diag_num(prev_day_data['volume'][0]),
                diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
            ))
            continue

        selected.append(stock)
        print('【排查】%s|竞价弱转强_通过|code=%s|auction_current=%s|high_limit=%s|base_close=%s|price_ratio=%s|auction_volume=%s|prev_volume=%s|volume_ratio=%s' % (
            date_now,
            diag_code(stock),
            diag_num(auction_data['current'][0]),
            diag_num(high_limit),
            diag_num(high_limit / 1.1),
            diag_num(current_ratio),
            diag_num(auction_data['volume'][0]),
            diag_num(prev_day_data['volume'][0]),
            diag_num(auction_data['volume'][0] / prev_day_data['volume'][0]),
        ))
    return selected


def format_stock_list(stock_list):
    """格式化股票列表，显示股票名称。"""
    if not stock_list:
        return '无'
    result = []
    for stock in stock_list:
        try:
            name = get_security_info(stock).display_name
        except Exception:
            name = ''
        result.append('%s(%s)' % (stock, name) if name else stock)
    return '、'.join(result)


def log_preselection_summary(current_dt, hl0_list, hl1_list, hl2_list, pre_gap_up, pre_gap_down, pre_reversal):
    lines = [
        '%s 选股开始' % format_log_date(current_dt),
        'T涨停%s只，T-1曾涨停%s只，T-2曾涨停%s只'
        % (len(hl0_list), len(hl1_list), len(hl2_list)),
        '',
        '初筛结果：',
        '一进二 %s只：%s' % (len(pre_gap_up), format_stock_list(pre_gap_up)),
        '低开   %s只：%s' % (len(pre_gap_down), format_stock_list(pre_gap_down)),
        '弱转强 %s只：%s' % (len(pre_reversal), format_stock_list(pre_reversal)),
        '',
    ]
    log_multiline(lines)


def log_auction_summary(current_dt, gap_up_selected, gap_down_selected, reversal_selected):
    total_count = len(gap_up_selected + gap_down_selected + reversal_selected)
    lines = [
        '%s 集合竞价共选中 %s 只，如下：' % (format_log_date(current_dt), total_count),
        '一进二 %s只：%s' % (len(gap_up_selected), format_stock_list(gap_up_selected)),
        '低开   %s只：%s' % (len(gap_down_selected), format_stock_list(gap_down_selected)),
        '弱转强 %s只：%s' % (len(reversal_selected), format_stock_list(reversal_selected)),
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
    print('【排查】%s|股票池_全部|count=%s' % (date, len(initial_list)))
    initial_list = filter_kcbj_stock(initial_list)
    print('【排查】%s|股票池_过滤科创北交后|count=%s' % (date, len(initial_list)))
    initial_list = filter_new_stock(initial_list, date)
    print('【排查】%s|股票池_过滤新股后|count=%s' % (date, len(initial_list)))
    initial_list = filter_st_paused_stock(initial_list)
    print('【排查】%s|股票池_过滤ST停牌后|count=%s' % (date, len(initial_list)))
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
    result_df = df[df['close'] == df['high_limit']]
    for _, row in result_df.iterrows():
        print('【排查】%s|涨停识别_T收盘|code=%s|close=%s|high_limit=%s' % (
            date, diag_code(row['code']), diag_num(row['close']), diag_num(row['high_limit'])
        ))
    return list(result_df.code)


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
    result_df = df[df['high'] == df['high_limit']]
    for _, row in result_df.iterrows():
        print('【排查】%s|涨停识别_曾涨停|code=%s|high=%s|high_limit=%s' % (
            date, diag_code(row['code']), diag_num(row['high']), diag_num(row['high_limit'])
        ))
    return list(result_df.code)


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
    result_df = df[(df['high'] == df['high_limit']) & (df['close'] != df['high_limit'])]
    for _, row in result_df.iterrows():
        print('【排查】%s|涨停识别_T曾涨停未封板|code=%s|close=%s|high=%s|high_limit=%s' % (
            date, diag_code(row['code']), diag_num(row['close']), diag_num(row['high']), diag_num(row['high_limit'])
        ))
    return list(result_df.code)


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
