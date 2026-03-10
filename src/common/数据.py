#coding:gbk
# 数据补充策略（实盘专用）
# - 覆盖策略：三合一/实盘.py，小市值改良版/实盘.py，ETF轮动/实盘-Gemini.py
# - 盘前：检查交易日历、股票池规模、日线本地完整性（按策略所需窗口抽样校验；缺失则分片下载补齐）
# - 盘前：抽样检查股本数据（总股本/流通股本）是否可取（仅告警）
# - 盘后 16:00：download_history_data（优先）显式落库，补齐最近N个交易日日线（分片+进度）
import datetime
import time

import pandas as pd
import requests


class G:
    pass

# ================= 账号配置 =================
# ROLE 可选值: '小市值' | '三合一' | 'ETF轮动'
# ROLE = '小市值'
ROLE = '三合一'
# ROLE = 'ETF轮动'

CONFIGS = {
    '小市值': {
        'account': "190200051469",
        'webhook': "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e861e0b4-b8e2-42ed-a21a-1edf90c41618",
        'name': '小市值'
    },
    '三合一': {
        'account': "170100005993",
        'webhook': "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2a336b4c-c38e-4ae3-9ff6-f14f175b4f73",
        'name': '三合一'
    },
    'ETF轮动': {
        'account': "190200026196",
        'webhook': "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=599439e6-4132-48b6-a05a-c1fbb32e33d8",
        'name': 'ETF轮动'
    }
}

# 自动加载配置
# 如果 ROLE 不在配置中，直接报错提示，而不是静默加载默认值，防止误操作
if ROLE not in CONFIGS:
    raise ValueError(f"无效的 ROLE: {ROLE}，可选值: {list(CONFIGS.keys())}")

current_config = CONFIGS[ROLE]
MY_ACCOUNT = current_config['account']
MY_WEBHOOK = current_config['webhook']
NAME = current_config['name']

g = G()

# ================= 基础配置 =================
g.chunk_size = 400            # 分片下载大小，避免超时
g.sample_size = 200           # 抽样检查样本数
g.min_trade_days = 10         # 交易日历最小有效天数
g.max_missing_ratio = 0.2     # 数据缺失容忍度 (超过触发全量下载)
g.precheck_ran_date = None    # 记录前置检查运行日期（仅内存级，重启失效）
g.post_ran_date = None        # 记录盘后补全运行日期（仅内存级，重启失效）
g.post_trade_days = 5         # 盘后强制补全最近N天数据

# ================= 策略1: 三合一 (沪深A股) =================
g.prefetch_days_a_share = 30  # 预取天数 (满足MA20等指标)

# ================= 策略2: 小市值 (中小综指) =================
g.prefetch_days_smallcap = 5 # 预取天数
g.smallcap_sector = "中小综指"
g.smallcap_index_code = "399101.SZ"

# ================= 策略3: 逃顶风控 (基差+广度) =================
# 包含：中证1000指数 + IM股指期货主力连续
g.risk_control_pool = ["000852.SH", "IM.IF"]

# ================= 策略4: ETF轮动 =================
g.prefetch_days_etf = 40       # 预取天数
g.etf_m_days = 25              # 动量计算周期
g.etf_pool = [
    # --- 跨境ETF ---
    "513100.SH", # 纳指ETF
    "513520.SH", # 日经ETF
    "513030.SH", # 德国30ETF
    "518880.SH", # 黄金ETF
    "159980.SZ", # 有色ETF
    "159985.SZ", # 豆粕ETF
    "501018.SH", # 南方原油
    "513130.SH", # 恒生科技
    # --- 宽基/行业ETF ---
    "510180.SH", # 上证180
    "159915.SZ", # 创业板
    "588120.SH", # 科创50
    "512290.SH", # 生物医药
    "515070.SH", # AI人工智能
    "159851.SZ", # 金融科技
    "159637.SZ", # 中证2000
    "159550.SZ", # 半导体
    "512710.SH", # 军工
    "159692.SZ", # 港股通医药
]

HOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=76383113-1a5b-4932-9f19-3f990405ec96"


class Messager:
    def __init__(self, hook_url):
        self.hook_url = hook_url
        self.is_test = False

    def set_is_test(self, is_test):
        self.is_test = is_test

    def send_message(self, text_content, webhook_url=None):
        if not webhook_url:
            webhook_url = self.hook_url
        if not webhook_url:
            return
        if self.is_test:
            print(f"【消息推送(测试)】{text_content}")
            return
        try:
            current_time = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
            content = current_time + str(text_content)
            payload = {"msgtype": "text", "text": {"content": content}}
            requests.post(webhook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"【消息推送失败】错误: {e}")


messager = Messager(HOOK)


def _now_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_yyyymmdd(value):
    if value is None:
        return ""
    if isinstance(value, str):
        if len(value) >= 8 and value[:8].isdigit():
            return value[:8]
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").strftime("%Y%m%d")
        except Exception:
            return ""
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y%m%d")
    try:
        return pd.to_datetime(value).strftime("%Y%m%d")
    except Exception:
        return ""


def print_daily_profit(C):
    """
    打印账户持仓及当日收益报告
    逻辑迁移自：小市值改良版/实盘.py
    """
    # 1. 获取账号 (兼容多策略)
    # QMT中 C.get_trade_detail_data 需要 account_id
    # 这里尝试从 C 中获取，或遍历所有账号
    account_id = MY_ACCOUNT

    if not account_id:
        print("未找到有效账号，跳过收益播报")
        return

    positions = get_trade_detail_data(account_id, "stock", "position")
    if not positions:
        _notify("【收益播报】空仓", f"账号 {account_id} 当前无持仓")
        return

    # 2. 构建持仓数据 DataFrame
    data_list = []
    total_market_value = 0.0
    total_profit = 0.0
    
    print(f"********** 持仓信息打印开始 {account_id} **********")
    for pos in positions:
        stock_code = f"{pos.m_strInstrumentID}.{pos.m_strExchangeID}"
        stock_name = pos.m_strInstrumentName
        price = pos.m_dLastPrice
        cost = pos.m_dOpenPrice
        amount = pos.m_dMarketValue
        vol = pos.m_nVolume
        profit = pos.m_dFloatProfit
        ratio = pos.m_dProfitRate
        
        # 计算涨跌幅 (避免除零)
        ret_pct = (price / cost - 1) * 100 if cost != 0 else 0.0
        
        total_market_value += amount
        total_profit += profit
        
        data_list.append({
            'stock': stock_name,
            'code': stock_code,
            'price': price,
            'cost': cost,
            'amount': amount,
            'ratio': ratio,
            'profit': profit
        })
        
        print(f"股票: {stock_code}")
        print(f"股票名: {stock_name}")
        print(f"成本价: {cost:.2f}")
        print(f"现价: {price:.2f}")
        print(f"涨跌幅: {ret_pct:.2f}%")
        print(f"持仓: {vol}")
        print(f"市值: {amount:.2f}")
        print("--------------------------------------")
    
    print(f"总市值：{total_market_value:.2f}")
    print("********** 持仓信息打印结束 **********")

    # 3. 生成 Markdown 报告推送
    num = len(data_list)
    
    # 格式化总盈亏颜色
    profit_color = "info" if total_profit > 0 else "warning" # info=绿(涨)/warning=橙(跌) 在企业微信通常对应颜色
    # 在Markdown中简单处理
    profit_str = f"{total_profit:.2f}"
    if total_profit > 0:
        profit_str = f"+{total_profit:.2f}"
    else:
        profit_str = f"{total_profit:.2f}"

    markdown = f"""
股票持仓报告 ({_get_today_str(C)})
---
策略账号: {NAME}
"""
    for item in data_list:
        stock = item['stock']
        price = item['price']
        cost = item['cost']
        amount = item['amount']
        ratio = item['ratio']
        profit = item['profit']
        
        ratio_pct = ratio * 100
        ratio_str = f"{ratio_pct:.2f}%"
        if ratio > 0:
            ratio_str = f"+{ratio_str}"
        else:
            ratio_str = f"{ratio_str}"
            
        item_profit_str = f"{profit:.2f}"
        if profit > 0:
            item_profit_str = f"+{item_profit_str}"
        else:
            item_profit_str = f"{item_profit_str}"
            
        markdown += f"""
{stock}
├─ 当前价：{price:.2f}
├─ 成本价：{cost:.2f}
├─ 持仓额：{amount:.2f}
├─ 盈亏率：{ratio_str}
└─ 当日盈亏：{item_profit_str}
"""
    
    markdown += f"""
---
持仓统计
总持仓数：{num} 只
总盈亏额：{profit_str}
"""
    messager.send_message(markdown, webhook_url=MY_WEBHOOK)


def _get_today_str(C):
    if hasattr(C, "today"):
        v = _to_yyyymmdd(C.today)
        if v:
            return v
    return datetime.datetime.now().strftime("%Y%m%d")


def _get_yesterday_str(C):
    """获取上一个交易日字符串"""
    if hasattr(C, "yesterday") and C.yesterday:
        return _to_yyyymmdd(C.yesterday)
    
    # 统一使用 _get_previous_trading_day (基于交易日历)
    return _get_previous_trading_day(C, datetime.datetime.now())


def _chunk_list(items, chunk_size):
    if not items:
        return []
    chunk_size = max(1, int(chunk_size))
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _get_stock_pool(C):
    try:
        stocks = C.get_stock_list_in_sector("沪深A股")
    except Exception:
        stocks = []
    if stocks and len(stocks) >= 1000:
        return stocks
    try:
        sh = C.get_stock_list_in_sector("上证A股")
    except Exception:
        sh = []
    try:
        sz = C.get_stock_list_in_sector("深证A股")
    except Exception:
        sz = []
    return list(set((stocks or []) + (sh or []) + (sz or [])))

def _get_stock_pool_smallcap(C):
    try:
        stocks = C.get_stock_list_in_sector(g.smallcap_sector)
        return stocks or []
    except Exception:
        return []

def _get_stock_pool_etf(_C):
    return list(g.etf_pool)

def _check_basic_info(C, name, stock_list):
    sample = stock_list[: min(50, max(10, int(g.sample_size / 10)))]
    if not sample:
        return {"name": name, "status": "skip_empty", "desc": "股票池为空"}

    name_missing = 0
    open_date_err = 0
    instrument_err = 0
    for code in sample:
        try:
            if not C.get_stock_name(code):
                name_missing += 1
        except Exception:
            name_missing += 1
        try:
            _ = C.get_open_date(code)
        except Exception:
            open_date_err += 1
        try:
            _ = C.get_instrumentdetail(code)
        except Exception:
            instrument_err += 1
            
    is_bad = name_missing / len(sample) >= 0.8 or open_date_err / len(sample) >= 0.5 or instrument_err / len(sample) >= 0.5
    
    stat = {
        "name": name, 
        "sample": len(sample), 
        "name_missing": name_missing, 
        "open_date_err": open_date_err, 
        "instrument_err": instrument_err,
        "status": "fail" if is_bad else "ok",
        "desc": "缺失严重" if is_bad else "正常"
    }
    
    if is_bad:
        print(f"【前置检查】警告 {name} 基础库抽检异常: {stat}")
    else:
        print(f"【前置检查】{_now_str()} {name} 基础库抽检: {stat}")
    return stat


def _download_history_data_single(stock_list, period, start_time, end_time, chunk_size, incrementally=True):
    downloader = globals().get("download_history_data")
    if not callable(downloader):
        return {"mode": "missing", "ok": 0, "fail": len(stock_list)}

    ok = 0
    fail = 0
    chunks = _chunk_list(stock_list, chunk_size)
    total = len(stock_list)
    for idx, chunk in enumerate(chunks, 1):
        for code in chunk:
            try:
                try:
                    downloader(code, period, start_time, end_time, incrementally=incrementally)
                except TypeError:
                    downloader(code, period, start_time, end_time)
                ok += 1
            except Exception:
                fail += 1
        print(f"【数据下载】{_now_str()} download_history_data {idx}/{len(chunks)} 片 完成, 进度={ok+fail}/{total}, ok={ok}, fail={fail}")
    return {"mode": "download_history_data", "ok": ok, "fail": fail}


def _download_history_data_batch(stock_list, period, start_time, end_time, chunk_size, incrementally=True):
    downloader = globals().get("download_history_data2")
    if not callable(downloader):
        return {"mode": "missing", "ok": 0, "fail": len(stock_list)}

    ok = 0
    fail = 0
    chunks = _chunk_list(stock_list, chunk_size)
    total = len(stock_list)
    for idx, chunk in enumerate(chunks, 1):
        try:
            try:
                downloader(chunk, period, start_time, end_time, incrementally=incrementally)
            except TypeError:
                downloader(chunk, period, start_time, end_time)
            ok += len(chunk)
            print(f"【数据下载】{_now_str()} download_history_data2 {idx}/{len(chunks)} 片 完成, 进度={ok+fail}/{total}, ok={ok}, fail={fail}")
        except Exception as e:
            fail += len(chunk)
            print(f"【数据下载】{_now_str()} download_history_data2 {idx}/{len(chunks)} 片 失败: {e}")
    return {"mode": "download_history_data2", "ok": ok, "fail": fail}


def download_history_data_general(stock_list, period, start_time, end_time, chunk_size=None, prefer_single=False):
    chunk_size = chunk_size or g.chunk_size
    if not stock_list:
        return {"mode": "skip", "ok": 0, "fail": 0}

    if prefer_single:
        ret = _download_history_data_single(stock_list, period, start_time, end_time, chunk_size, incrementally=True)
        if ret["mode"] != "missing":
            return ret
        return _download_history_data_batch(stock_list, period, start_time, end_time, chunk_size, incrementally=True)

    ret = _download_history_data_batch(stock_list, period, start_time, end_time, chunk_size, incrementally=True)
    if ret["mode"] != "missing":
        return ret
    return _download_history_data_single(stock_list, period, start_time, end_time, chunk_size, incrementally=True)


def _check_trade_calendar(C):
    try:
        days = C.get_trading_dates(stockcode="SH", start_date="", end_date="", count=30, period="1d")
        if not days:
            return False, 0
        return len(days) >= g.min_trade_days, len(days)
    except Exception:
        return False, 0


def _get_shifted_date(C, date, days, days_type='T'):
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

def _get_previous_trading_day(C, current_date):
    """获取前一个交易日"""
    if isinstance(current_date, str):
        current_date = datetime.datetime.strptime(current_date, "%Y%m%d")
    current_str = current_date.strftime("%Y%m%d")
    prev_str = _get_shifted_date(C, current_str, -1, 'T')
    return prev_str

def _check_daily_bars_available(C, stock_list, end_date, count):
    sample = stock_list[: g.sample_size]
    if not sample:
        return False, {"sample": 0, "missing": 0}

    # 获取期望的交易日历序列
    expected_dates = []
    try:
        # 确保 end_date 是交易日，或者获取截止到 end_date 的最近 count 个交易日
        # 注意: get_trading_dates(end_date=end_date) 会包含 end_date (如果是交易日)
        # 我们需要 count 个交易日
        trade_days = C.get_trading_dates(stockcode="SH", start_date="", end_date=end_date, count=count, period="1d")
        expected_dates = trade_days if trade_days else []
    except Exception as e:
        print(f"【前置检查】获取交易日历异常: {e}")
        # 如果获取不到日历，回退到原来的简单检查
        expected_dates = []

    try:
        data = C.get_market_data_ex(
            ["open", "high", "low", "close", "volume", "amount"],
            sample,
            period="1d",
            start_time="",
            end_time=end_date,
            count=count,
            dividend_type="follow",
            fill_data=False,
            subscribe=False,
        )
    except Exception as e:
        print(f"【前置检查】{_now_str()} get_market_data_ex 异常: {e}")
        return False, {"sample": len(sample), "missing": len(sample)}

    missing = 0
    # 精确获取前一个交易日 (target_date_str)
    # 如果 expected_dates 存在，最后一个就是 target_date_str
    if expected_dates:
        target_date_str = expected_dates[-1]
    else:
        try:
            target_trade_day = _get_shifted_date(C, _to_yyyymmdd(end_date), 0, 'T')
            target_date_str = target_trade_day
        except Exception:
            target_date_str = _to_yyyymmdd(end_date)

    # 统计每个日期的缺失情况 (用于检测系统性缺失)
    # missing_counts: { date_str: missing_count }
    missing_counts = {d: 0 for d in expected_dates}

    for s in sample:
        if s not in data or data[s] is None:
            missing += 1
            # 该股票完全缺失，所有日期都计入
            for d in expected_dates:
                missing_counts[d] += 1
            continue
        try:
            df = data[s]
            if len(df) < count:
                missing += 1
                continue
            
            # 1. 检查最新日期
            last_dt = df.index[-1]
            last_date_str = _to_yyyymmdd(last_dt)
            
            if last_date_str < target_date_str:
                if missing < 3: 
                    print(f"【数据校验】{s} 数据滞后: 实际最新={last_date_str}, 预期={target_date_str}")
                missing += 1
                continue
            
            # 2. 检查中间日期的连续性 (针对 expected_dates)
            if expected_dates:
                # 获取该股票的日期列表 (转字符串)
                stock_dates = set([_to_yyyymmdd(d) for d in df.index])
                for d in expected_dates:
                    if d not in stock_dates:
                        missing_counts[d] += 1

        except Exception:
            missing += 1
            
    # 分析系统性缺失
    # 如果某个日期在超过 30% 的样本中都缺失，视为该日数据未下载
    systematic_missing = False
    for d, cnt in missing_counts.items():
        missing_ratio = cnt / len(sample)
        if missing_ratio > 0.3:
            print(f"【严重警告】检测到日期 {d} 存在系统性缺失! 样本缺失率: {missing_ratio:.1%}")
            systematic_missing = True
            
    if systematic_missing:
        return False, {"sample": len(sample), "missing": "systematic", "detail": missing_counts}

    ok = (missing / len(sample)) <= g.max_missing_ratio
    return ok, {"sample": len(sample), "missing": missing}


def _try_check_capital_structure(C, stock_list, end_date):
    sample = stock_list[: min(200, max(20, g.sample_size))]
    ok_cnt = 0
    try:
        fin = C.get_raw_financial_data(
            ["股本表.总股本", "股本表.流通股本"],
            sample,
            "20100101",
            end_date,
        )
        for s in sample:
            if s not in fin:
                continue
            total_vals = list(fin[s].get("股本表.总股本", {}).values())
            if total_vals and total_vals[-1]:
                ok_cnt += 1
    except Exception as e:
        print(f"【前置检查】{_now_str()} 股本抽检异常: {e}")
        return False, {"sample": len(sample), "ok": 0}
    return ok_cnt >= max(10, int(len(sample) * 0.2)), {"sample": len(sample), "ok": ok_cnt}


def _get_trade_days(C, end_date, count):
    try:
        days = C.get_trading_dates(stockcode="SH", start_date="", end_date=end_date, count=count, period="1d")
        return days or []
    except Exception:
        return []


def _notify(title, detail):
    msg = f"{title}\n{detail}"
    print(msg)
    messager.send_message(msg)

def _prefetch_universe_daily(C, name, stock_list, end_date, prefetch_days, required_count):
    if not stock_list:
        _notify("【前置检查】警告", f"{name} 股票池为空，跳过")
        return {"name": name, "status": "skip_empty", "desc": "股票池为空"}

    bars_ok, bars_stat = _check_daily_bars_available(C, stock_list, end_date=end_date, count=required_count)
    print(f"【前置检查】{_now_str()} {name} 日线可用性(样本): ok={bars_ok}, stat={bars_stat}, need={required_count}")

    if bars_ok:
        return {"name": name, "status": "ok", "bars": bars_stat, "need": required_count, "desc": "数据完整"}

    days = _get_trade_days(C, end_date=end_date, count=prefetch_days)
    start_time = days[0] if days else "20100101"
    
    _notify(f"【前置检查】{name} 触发补全", f"目标日期: {end_date}\n补全区间: {start_time} - {end_date}\n缺失样本: {bars_stat['missing']}/{bars_stat['sample']}")
    
    ret = download_history_data_general(
        stock_list=stock_list,
        period="1d",
        start_time=start_time,
        end_time=end_date,
        chunk_size=g.chunk_size,
        prefer_single=False,
    )

    bars_ok2, bars_stat2 = _check_daily_bars_available(C, stock_list, end_date=end_date, count=required_count)
    print(f"【前置检查】{_now_str()} {name} 日线复检(样本): ok={bars_ok2}, stat={bars_stat2}, need={required_count}")
    
    if not bars_ok2:
        _notify(f"【前置检查】{name} 补全失败", f"依然缺失: {bars_stat2['missing']}/{bars_stat2['sample']}\n下载结果: ok={ret['ok']}, fail={ret['fail']}")
        return {"name": name, "status": "fail", "bars": bars_stat2, "need": required_count, "download": ret, "desc": "补全失败"}

    return {"name": name, "status": "fixed", "bars": bars_stat2, "need": required_count, "download": ret, "desc": "补全成功"}


def precheck_and_fix(C):
    today_str = _get_today_str(C)
    if g.precheck_ran_date == today_str:
        return
    g.precheck_ran_date = today_str

    t0 = time.time()
    yesterday_str = _get_yesterday_str(C)
    # _notify("【前置检查】开始", f"today={today_str}, yesterday={yesterday_str}")

    cal_ok, cal_len = _check_trade_calendar(C)
    print(f"【前置检查】{_now_str()} 交易日历: ok={cal_ok}, len={cal_len}")

    a_share_pool = _get_stock_pool(C)
    smallcap_pool = _get_stock_pool_smallcap(C)
    etf_pool = _get_stock_pool_etf(C)

    print(f"【前置检查】{_now_str()} 股票池: 沪深A股={len(a_share_pool)}, 中小综指={len(smallcap_pool)}, ETF池={len(etf_pool)}")

    precheck_results = []
    precheck_results.append(_check_basic_info(C, "三合一基础", a_share_pool))
    if smallcap_pool:
        precheck_results.append(_check_basic_info(C, "小市值基础", smallcap_pool))
    precheck_results.append(_check_basic_info(C, "ETF轮动基础", etf_pool))

    precheck_results.append(
        _prefetch_universe_daily(
            C,
            name="三合一",
            stock_list=a_share_pool,
            end_date=yesterday_str,
            prefetch_days=g.prefetch_days_a_share,
            required_count=4,
        )
    )

    if smallcap_pool:
        precheck_results.append(
            _prefetch_universe_daily(
                C,
                name="小市值",
                stock_list=smallcap_pool,
                end_date=yesterday_str,
                prefetch_days=g.prefetch_days_smallcap,
                required_count=g.prefetch_days_smallcap,
            )
        )
        precheck_results.append(
            _prefetch_universe_daily(
                C,
                name="小市值指数",
                stock_list=[g.smallcap_index_code],
                end_date=yesterday_str,
                prefetch_days=10,
                required_count=2,
            )
        )
    precheck_results.append(
        _prefetch_universe_daily(
            C,
            name="ETF轮动",
            stock_list=etf_pool,
            end_date=yesterday_str,
            prefetch_days=g.prefetch_days_etf,
            required_count=max(g.prefetch_days_etf, g.etf_m_days + 10),
        )
    )

    # 逃顶风控数据补全
    precheck_results.append(
        _prefetch_universe_daily(
            C,
            name="逃顶风控",
            stock_list=g.risk_control_pool,
            end_date=yesterday_str,
            prefetch_days=20,
            required_count=15,
        )
    )

    if smallcap_pool:
        cap_ok, cap_stat = _try_check_capital_structure(C, smallcap_pool, end_date=yesterday_str)
    else:
        cap_ok, cap_stat = _try_check_capital_structure(C, a_share_pool, end_date=yesterday_str)
    print(f"【前置检查】{_now_str()} 股本可用性(样本): ok={cap_ok}, stat={cap_stat}")
    if not cap_ok:
        _notify("【前置检查】警告", f"股本数据抽检偏少: stat={cap_stat}（需要在客户端界面下载财务数据或提供下载函数）")

    elapsed = time.time() - t0
    
    # 生成 Markdown 报告
    markdown = f"""
数据前置检查报告 ({today_str})
---
策略账号: {NAME}
目标日期: {yesterday_str}
总耗时: {elapsed:.2f}s

| 策略 | 状态 | 说明 |
| :--- | :--- | :--- |
"""
    status_map = {
        "ok": "✅ 正常",
        "fixed": "🛠️ 已补全",
        "fail": "❌ 失败",
        "skip_empty": "⚠️ 跳过"
    }
    
    for item in precheck_results:
        name = item.get("name", "未知")
        status = item.get("status", "fail")
        desc = item.get("desc", "")
        status_icon = status_map.get(status, status)
        
        # 简化名字，去掉括号内容
        simple_name = name.split("(")[0]
        
        markdown += f"| {simple_name} | {status_icon} | {desc} |\n"

    # 如果有补全操作，增加详细统计
    fixed_items = [i for i in precheck_results if i.get("status") == "fixed"]
    if fixed_items:
        markdown += "\n**补全详情:**\n"
        for item in fixed_items:
            dl = item.get("download", {})
            markdown += f"- {item['name']}: 成功 {dl.get('ok',0)}, 失败 {dl.get('fail',0)}\n"
            
    _notify("【前置检查】完成", markdown)


def post_market_download(C):
    today_str = _get_today_str(C)
    if g.post_ran_date == today_str:
        return
    g.post_ran_date = today_str

    t0 = time.time()
    
    a_share_pool = _get_stock_pool(C)
    smallcap_pool = _get_stock_pool_smallcap(C)
    etf_pool = _get_stock_pool_etf(C)
    _notify("【盘后补全】开始", f"today={today_str}, 沪深A股={len(a_share_pool)}, 中小综指={len(smallcap_pool)}, ETF池={len(etf_pool)}")

    days = _get_trade_days(C, end_date=today_str, count=g.post_trade_days)
    start_time = days[0] if days else today_str

    results = []
    for name, pool in [
        ("三合一(沪深A股)", a_share_pool),
        ("小市值(中小综指)", smallcap_pool),
        ("小市值(指数399101)", [g.smallcap_index_code] if smallcap_pool else []),
        ("ETF轮动(ETF池)", etf_pool),
        ("逃顶风控(1000指数+IM)", g.risk_control_pool),
    ]:
        if not pool:
            continue
        ret = download_history_data_general(
            stock_list=pool,
            period="1d",
            start_time=start_time,
            end_time=today_str,
            chunk_size=g.chunk_size,
            prefer_single=True,
        )
        results.append({"name": name, "result": ret})
        _notify("【盘后补全】日线下载触发", f"{name} start={start_time}, end={today_str}, result={ret}")

    elapsed = time.time() - t0
    results_lines = []
    for item in results:
        results_lines.append(f"\n{item}")
    _notify("【盘后补全】完成", f"耗时={elapsed:.2f}s\n" + "\n".join(results_lines))


def print_daily_profit_func(C):    
    try:
        print_daily_profit(C)
    except Exception as e:
        _notify("【收益播报】异常", str(e))

def init(C):
    messager.set_is_test(getattr(C, "do_back_test", False))

    try:
        now_dt = datetime.datetime.now()
        C.today = pd.to_datetime(now_dt)
        # 统一使用 _get_previous_trading_day 计算昨日
        C.yesterday = _get_previous_trading_day(C, now_dt)
    except Exception:
        # 兜底：昨天
        C.yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")

    C.run_time("print_daily_profit_func", "1nDay", "2025-03-01 15:00:00", "SH")
    C.run_time("post_market_download", "1nDay", "2025-03-01 16:00:00", "SH")

    try:
        precheck_and_fix(C)
        print_daily_profit_func(C)
    except Exception as e:
        _notify("【前置检查】init 触发失败", str(e))


def handlebar(C):
    return
